package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	DefaultReconnectInterval = 5 * time.Second
	DefaultPingInterval      = 3 * time.Second
)

type subscribeRequest struct {
	Addresses []string `json:"addresses"`
	// Note: "subcribed_types" matches the Klever node wire format.
	Types []string `json:"subcribed_types"`
}

// Option configures a Subscriber.
type Option func(*Subscriber)

// WithScheme sets the WebSocket scheme ("ws" or "wss").
func WithScheme(scheme string) Option {
	return func(s *Subscriber) {
		if scheme != "ws" && scheme != "wss" {
			panic(fmt.Sprintf("subscriber: invalid scheme %q (must be ws or wss)", scheme))
		}
		s.scheme = scheme
	}
}

// WithAddresses sets the addresses to watch.
func WithAddresses(addresses []string) Option {
	return func(s *Subscriber) { s.addresses = addresses }
}

// WithReconnectInterval sets the delay between reconnection attempts.
func WithReconnectInterval(d time.Duration) Option {
	return func(s *Subscriber) { s.reconnectInterval = d }
}

// WithPingInterval sets the WebSocket ping frequency.
func WithPingInterval(d time.Duration) Option {
	return func(s *Subscriber) { s.pingInterval = d }
}

// WithOnConnect registers a callback invoked on successful connection.
func WithOnConnect(fn func()) Option {
	return func(s *Subscriber) { s.onConnect = fn }
}

// WithOnDisconnect registers a callback invoked when the connection drops.
func WithOnDisconnect(fn func()) Option {
	return func(s *Subscriber) { s.onDisconnect = fn }
}

// WithOnError registers a callback invoked on connection errors.
func WithOnError(fn func(error)) Option {
	return func(s *Subscriber) { s.onError = fn }
}

// Subscription represents an active event subscription. Call Close to
// unregister and release resources.
type Subscription struct {
	ch  chan Event
	sub *Subscriber
}

// C returns the channel that receives events.
func (s *Subscription) C() <-chan Event {
	return s.ch
}

// Close unregisters the subscription and closes its channel.
// It is safe to call Close multiple times.
func (s *Subscription) Close() {
	s.sub.removeSub(s.ch)
}

// Subscriber connects to a Klever node WebSocket and fans out events
// to multiple consumers.
type Subscriber struct {
	host              string
	scheme            string
	reconnectInterval time.Duration
	pingInterval      time.Duration
	onConnect         func()
	onDisconnect      func()
	onError           func(error)

	connected atomic.Bool

	mu          sync.RWMutex
	types       []EventType
	addresses   []string
	subscribers []chan Event
	closed      map[chan Event]struct{}
	connCancel  context.CancelFunc
	events      <-chan Event

	reconnectCh chan struct{}
}

// New creates a Subscriber with the given options.
func New(host string, types []EventType, opts ...Option) *Subscriber {
	s := &Subscriber{
		host:              host,
		types:             types,
		scheme:            "ws",
		reconnectInterval: DefaultReconnectInterval,
		pingInterval:      DefaultPingInterval,
		reconnectCh:       make(chan struct{}, 1),
		closed:            make(map[chan Event]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}

	// Pre-register the default events channel.
	sub := s.Subscribe()
	s.events = sub.C()

	return s
}

// Subscribe returns a Subscription that receives a copy of every event.
// Each subscription gets its own independent channel. Events are dropped
// (non-blocking send) if a subscriber's channel is full.
// Call Subscription.Close to unregister when done.
func (s *Subscriber) Subscribe() *Subscription {
	ch := make(chan Event, 256)
	s.mu.Lock()
	s.subscribers = append(s.subscribers, ch)
	s.mu.Unlock()
	return &Subscription{ch: ch, sub: s}
}

func (s *Subscriber) removeSub(ch chan Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sub := range s.subscribers {
		if sub == ch {
			s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
			if _, already := s.closed[ch]; !already {
				close(ch)
				s.closed[ch] = struct{}{}
			}
			return
		}
	}
}

// Events returns the pre-registered event channel.
func (s *Subscriber) Events() <-chan Event {
	return s.events
}

// Connected reports whether the subscriber is currently connected.
func (s *Subscriber) Connected() bool {
	return s.connected.Load()
}

// URL returns the full WebSocket URL.
func (s *Subscriber) URL() string {
	return fmt.Sprintf("%s://%s/subscribe", s.scheme, s.host)
}

// Types returns the current event types being subscribed to.
func (s *Subscriber) Types() []EventType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]EventType, len(s.types))
	copy(out, s.types)
	return out
}

// Addresses returns the current addresses being watched.
func (s *Subscriber) Addresses() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.addresses))
	copy(out, s.addresses)
	return out
}

// Reconfigure updates the subscription types and addresses and forces
// an immediate reconnect with the new parameters.
func (s *Subscriber) Reconfigure(types []EventType, addresses []string) {
	s.mu.Lock()
	s.types = types
	s.addresses = addresses
	cancel := s.connCancel
	s.mu.Unlock()

	// Force the current connection to close.
	if cancel != nil {
		cancel()
	}

	// Signal the Start loop to reconnect immediately.
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

// Start connects to the node and streams events until ctx is cancelled.
// It automatically reconnects on connection loss. If no event types are
// configured, it waits for a Reconfigure call before connecting.
func (s *Subscriber) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.closeSubscribers()
			return
		default:
		}

		// If no types configured, wait for Reconfigure.
		s.mu.RLock()
		hasTypes := len(s.types) > 0
		s.mu.RUnlock()

		if !hasTypes {
			select {
			case <-ctx.Done():
				s.closeSubscribers()
				return
			case <-s.reconnectCh:
				continue
			}
		}

		// Create a connection-scoped context that Reconfigure can cancel.
		connCtx, connCancel := context.WithCancel(ctx)
		s.mu.Lock()
		s.connCancel = connCancel
		s.mu.Unlock()

		err := s.connect(connCtx)
		connCancel()

		s.mu.Lock()
		s.connCancel = nil
		s.mu.Unlock()

		// Parent context done — shut down.
		if ctx.Err() != nil {
			s.closeSubscribers()
			return
		}

		if err == nil {
			s.closeSubscribers()
			return
		}

		// Don't report errors caused by Reconfigure.
		if s.onError != nil && connCtx.Err() == nil {
			s.onError(err)
		}

		select {
		case <-ctx.Done():
			s.closeSubscribers()
			return
		case <-s.reconnectCh:
			// Reconfigure requested, reconnect immediately.
		case <-time.After(s.reconnectInterval):
		}
	}
}

func (s *Subscriber) closeSubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.subscribers {
		if _, already := s.closed[ch]; !already {
			close(ch)
			s.closed[ch] = struct{}{}
		}
	}
	s.subscribers = nil
}

func (s *Subscriber) fanOut(evt Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- evt:
		default:
			// slow consumer, drop event
		}
	}
}

func (s *Subscriber) connect(ctx context.Context) error {
	// Snapshot config under lock.
	s.mu.RLock()
	types := make([]EventType, len(s.types))
	copy(types, s.types)
	addrs := make([]string, len(s.addresses))
	copy(addrs, s.addresses)
	s.mu.RUnlock()

	conn, _, err := websocket.DefaultDialer.Dial(s.URL(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	var writeMu sync.Mutex
	writeMsg := func(msgType int, data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteMessage(msgType, data)
	}

	conn.SetPongHandler(func(string) error { return nil })

	typeStrings := make([]string, len(types))
	for i, t := range types {
		typeStrings[i] = string(t)
	}
	req := subscribeRequest{
		Addresses: addrs,
		Types:     typeStrings,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal subscription: %w", err)
	}
	if err := writeMsg(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("send subscription: %w", err)
	}

	s.connected.Store(true)
	if s.onConnect != nil {
		s.onConnect()
	}

	done := make(chan struct{})

	// Ping loop to keep the connection alive through proxies.
	stopPing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(s.pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-ticker.C:
				if err := writeMsg(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	// Read loop.
	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
					if s.onError != nil {
						s.onError(fmt.Errorf("read: %w", err))
					}
				}
				return
			}

			evt, decErr := DecodeEvent(message)
			if decErr != nil {
				evt = Event{Raw: message, Data: string(message)}
			}
			s.fanOut(evt)
		}
	}()

	select {
	case <-done:
		close(stopPing)
		s.connected.Store(false)
		if s.onDisconnect != nil {
			s.onDisconnect()
		}
		return fmt.Errorf("connection closed by server")
	case <-ctx.Done():
		close(stopPing)
		_ = writeMsg(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		s.connected.Store(false)
		if s.onDisconnect != nil {
			s.onDisconnect()
		}
		return ctx.Err()
	}
}

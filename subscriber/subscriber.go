package subscriber

import (
	"context"
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
	Types     []string `json:"subscribed_types"`
}

type Option func(*Subscriber)

func WithScheme(scheme string) Option {
	return func(s *Subscriber) {
		if scheme != "ws" && scheme != "wss" {
			panic(fmt.Sprintf("subscriber: invalid scheme %q (must be ws or wss)", scheme))
		}
		s.scheme = scheme
	}
}

func WithAddresses(addresses []string) Option {
	return func(s *Subscriber) { s.addresses = addresses }
}

func WithReconnectInterval(d time.Duration) Option {
	return func(s *Subscriber) { s.reconnectInterval = d }
}

func WithPingInterval(d time.Duration) Option {
	return func(s *Subscriber) { s.pingInterval = d }
}

func WithOnConnect(fn func()) Option {
	return func(s *Subscriber) { s.onConnect = fn }
}

func WithOnDisconnect(fn func()) Option {
	return func(s *Subscriber) { s.onDisconnect = fn }
}

func WithOnError(fn func(error)) Option {
	return func(s *Subscriber) { s.onError = fn }
}

func WithQueryOnly() Option {
	return func(s *Subscriber) { s.queryOnly = true }
}

type Subscription struct {
	ch       chan Event
	sub      *Subscriber
	closeMu  sync.Once
}

func (s *Subscription) C() <-chan Event {
	return s.ch
}

func (s *Subscription) Close() {
	s.closeMu.Do(func() {
		s.sub.removeSub(s.ch)
	})
}

type Subscriber struct {
	host              string
	scheme            string
	reconnectInterval time.Duration
	pingInterval      time.Duration
	onConnect         func()
	onDisconnect      func()
	onError           func(error)
	queryOnly         bool

	connected atomic.Bool

	// reconfigureMu serializes Reconfigure / AddSubscriptions /
	// RemoveSubscriptions so their wire messages and local state
	// updates never interleave.
	reconfigureMu sync.Mutex

	mu          sync.RWMutex
	types       []EventType
	addresses   []string
	subscribers []chan Event
	connCancel  context.CancelFunc
	events      <-chan Event

	reconnectCh chan struct{}

	connWriteMu sync.Mutex
	activeConn  *websocket.Conn

	pendingMu sync.Mutex
	pending   map[string]chan *Response

	nextID atomic.Int64
}

func New(host string, types []EventType, opts ...Option) *Subscriber {
	s := &Subscriber{
		host:              host,
		types:             types,
		scheme:            "ws",
		reconnectInterval: DefaultReconnectInterval,
		pingInterval:      DefaultPingInterval,
		reconnectCh:       make(chan struct{}, 1),
		pending:           make(map[string]chan *Response),
	}
	for _, opt := range opts {
		opt(s)
	}

	sub := s.Subscribe()
	s.events = sub.C()

	return s
}

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
			close(ch)
			return
		}
	}
}

func (s *Subscriber) Events() <-chan Event {
	return s.events
}

func (s *Subscriber) Connected() bool {
	return s.connected.Load()
}

func (s *Subscriber) URL() string {
	return fmt.Sprintf("%s://%s/subscribe", s.scheme, s.host)
}

func (s *Subscriber) Types() []EventType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]EventType, len(s.types))
	copy(out, s.types)
	return out
}

func (s *Subscriber) Addresses() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.addresses))
	copy(out, s.addresses)
	return out
}

func (s *Subscriber) closeSubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.subscribers {
		close(ch)
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
		}
	}
}

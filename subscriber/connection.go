package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func (s *Subscriber) Start(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil && s.onError != nil {
			s.onError(fmt.Errorf("subscriber Start panic: %v", r))
		}
	}()
	for {
		select {
		case <-ctx.Done():
			s.closeSubscribers()
			return
		default:
		}

		s.mu.RLock()
		hasTypes := len(s.types) > 0 || s.queryOnly
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

		connCtx, connCancel := context.WithCancel(ctx)
		s.mu.Lock()
		s.connCancel = connCancel
		s.mu.Unlock()

		err := s.connect(connCtx)
		connCancel()

		s.mu.Lock()
		s.connCancel = nil
		s.mu.Unlock()

		if ctx.Err() != nil {
			s.closeSubscribers()
			return
		}

		if err == nil {
			s.closeSubscribers()
			return
		}

		if s.onError != nil && connCtx.Err() == nil {
			s.onError(err)
		}

		select {
		case <-ctx.Done():
			s.closeSubscribers()
			return
		case <-s.reconnectCh:
		case <-time.After(s.reconnectInterval):
		}
	}
}

func (s *Subscriber) connect(ctx context.Context) error {
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

	s.connWriteMu.Lock()
	s.activeConn = conn
	s.connWriteMu.Unlock()

	defer func() {
		s.connWriteMu.Lock()
		s.activeConn = nil
		s.connWriteMu.Unlock()
		conn.Close()
		s.failAllPending()
	}()

	// Treat the connection as dead if no frame (data or pong) arrives within
	// 2 ping intervals. Each pong from the peer extends the deadline so a
	// silent half-open TCP connection (NAT/LB idle timeout) is detected
	// instead of hanging the read loop forever.
	readTimeout := 2 * s.pingInterval
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	// The Klever node rejects an empty subscribe ("subscribed_types must
	// not be empty") and closes the socket, which would kill any
	// pending request. For query-only clients we send a placeholder
	// subscribe to an address-scoped type with no addresses so the node
	// accepts the connection without delivering any events.
	typeStrings := make([]string, 0, len(types))
	for _, t := range types {
		typeStrings = append(typeStrings, string(t))
	}
	if len(typeStrings) == 0 {
		typeStrings = []string{string(EventAccounts)}
	}
	req := subscribeRequest{
		Addresses: addrs,
		Types:     typeStrings,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal subscription: %w", err)
	}
	if err := s.writeMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("send subscription: %w", err)
	}

	s.connected.Store(true)
	if s.onConnect != nil {
		s.onConnect()
	}

	done := make(chan struct{})

	stopPing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(s.pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-ticker.C:
				if err := s.writeMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

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
			_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

			// gorilla/websocket reuses internal read buffers; copy before
			// fan-out so async consumers can't observe the next frame's bytes.
			frame := append([]byte(nil), message...)

			var probe struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(frame, &probe); err != nil {
				if s.onError != nil {
					s.onError(fmt.Errorf("decode frame: %w", err))
				}
				continue
			}

			if probe.ID != "" {
				var resp Response
				if err := json.Unmarshal(frame, &resp); err != nil {
					if s.onError != nil {
						s.onError(fmt.Errorf("decode response %s: %w", probe.ID, err))
					}
					continue
				}
				s.routeResponse(&resp)
				continue
			}

			if probe.Error != "" {
				if s.onError != nil {
					s.onError(fmt.Errorf("server: %s", probe.Error))
				}
				continue
			}

			if probe.Type != "" {
				evt, decErr := DecodeEvent(frame)
				if decErr != nil {
					evt = Event{Raw: frame, Data: string(frame)}
				}
				s.fanOut(evt)
				continue
			}

			evt := Event{Raw: frame, Data: string(frame)}
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
		_ = s.writeMessage(
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

func (s *Subscriber) writeMessage(msgType int, data []byte) error {
	s.connWriteMu.Lock()
	defer s.connWriteMu.Unlock()
	if s.activeConn == nil {
		return ErrNotConnected
	}
	return s.activeConn.WriteMessage(msgType, data)
}

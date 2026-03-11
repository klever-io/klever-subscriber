package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func (s *Subscriber) Start(ctx context.Context) {
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

			var probe struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				Error string `json:"error"`
			}
			json.Unmarshal(message, &probe)

			if probe.ID != "" {
				var resp Response
				json.Unmarshal(message, &resp)
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
				evt, decErr := DecodeEvent(message)
				if decErr != nil {
					evt = Event{Raw: message, Data: string(message)}
				}
				s.fanOut(evt)
				continue
			}

			evt := Event{Raw: message, Data: string(message)}
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

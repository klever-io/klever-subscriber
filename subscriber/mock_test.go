package subscriber

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type mockServer struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu          sync.Mutex
	onSubscribe func(req subscribeRequest)
	events      []rawEvent
	onRequest   func(conn *websocket.Conn, req Request)
}

func newMockServer(events []rawEvent) *mockServer {
	m := &mockServer{
		events:   events,
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *mockServer) handler(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return
	}

	var req subscribeRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		return
	}

	m.mu.Lock()
	if m.onSubscribe != nil {
		m.onSubscribe(req)
	}
	events := make([]rawEvent, len(m.events))
	copy(events, m.events)
	handler := m.onRequest
	m.mu.Unlock()

	for _, evt := range events {
		data, err := json.Marshal(evt)
		if err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return
		}
	}

	if handler == nil {
		conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		conn.SetReadDeadline(time.Now().Add(time.Second))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var r Request
		if err := json.Unmarshal(msg, &r); err != nil {
			continue
		}
		if r.ID != "" && r.Method != "" {
			handler(conn, r)
		}
	}
}

func (m *mockServer) host() string {
	return strings.TrimPrefix(m.srv.URL, "http://")
}

func (m *mockServer) close() {
	m.srv.Close()
}

func makeEvent(typ, hash, data string) rawEvent {
	return rawEvent{
		Type: typ,
		Hash: hash,
		Data: json.RawMessage(data),
	}
}

func collectN(ch <-chan Event, n int, cancel context.CancelFunc) []Event {
	var events []Event
	for evt := range ch {
		events = append(events, evt)
		if len(events) >= n {
			cancel()
		}
	}
	return events
}

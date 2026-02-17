package subscriber

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockServer is a test WebSocket server that emulates a Klever node.
type mockServer struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu          sync.Mutex
	onSubscribe func(req subscribeRequest)
	events      []rawEvent
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

	// Read the subscription request.
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
	m.mu.Unlock()

	// Send events.
	for _, evt := range events {
		data, err := json.Marshal(evt)
		if err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return
		}
	}

	// Close cleanly after all events sent.
	conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
	)

	// Wait for the client to acknowledge the close.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
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
		Data: base64.StdEncoding.EncodeToString([]byte(data)),
	}
}

// collectN reads exactly n events from the channel, then cancels the context.
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

func TestSubscriber_ReceivesEvents(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{"height":1}`),
		makeEvent("blocks", "h2", `{"height":2}`),
		makeEvent("transactions", "tx1", `{"amount":100}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks, EventTransactions},
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	received := collectN(sub.Events(), 3, cancel)

	if len(received) != 3 {
		t.Fatalf("expected 3 events, got %d", len(received))
	}
	if received[0].Type != EventBlocks {
		t.Errorf("event 0 type = %q, want blocks", received[0].Type)
	}
	if received[0].Hash != "h1" {
		t.Errorf("event 0 hash = %q, want h1", received[0].Hash)
	}
	if received[2].Type != EventTransactions {
		t.Errorf("event 2 type = %q, want transactions", received[2].Type)
	}
}

func TestSubscriber_FanOut(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{"height":1}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	sub1 := sub.Subscribe()
	sub2 := sub.Subscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	// All channels should receive the event.
	evt1 := <-sub1.C()
	evt2 := <-sub2.C()
	evtDefault := <-sub.Events()

	cancel()

	if evt1.Hash != "h1" || evt2.Hash != "h1" || evtDefault.Hash != "h1" {
		t.Errorf("fan-out failed: got hashes %q, %q, %q", evt1.Hash, evt2.Hash, evtDefault.Hash)
	}
}

func TestSubscription_Close(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{"a":1}`),
		makeEvent("blocks", "h2", `{"a":2}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	extra := sub.Subscribe()

	// Close the extra subscription before starting.
	extra.Close()

	// Verify the channel is closed.
	_, ok := <-extra.C()
	if ok {
		t.Error("expected closed channel after Close")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	received := collectN(sub.Events(), 2, cancel)
	if len(received) != 2 {
		t.Errorf("expected 2 events on main channel, got %d", len(received))
	}
}

func TestSubscriber_OnConnectCallback(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	var connected atomic.Bool

	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() { connected.Store(true) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	collectN(sub.Events(), 1, cancel)

	if !connected.Load() {
		t.Error("OnConnect callback was not called")
	}
}

func TestSubscriber_SubscriptionRequest(t *testing.T) {
	var gotReq subscribeRequest
	var reqReceived atomic.Bool

	events := []rawEvent{makeEvent("blocks", "h1", `{}`)}
	mock := newMockServer(events)
	mock.onSubscribe = func(req subscribeRequest) {
		gotReq = req
		reqReceived.Store(true)
	}
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks, EventUserTransaction},
		WithAddresses([]string{"klv1abc", "klv1def"}),
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	collectN(sub.Events(), 1, cancel)

	if !reqReceived.Load() {
		t.Fatal("subscription request was not received")
	}
	if len(gotReq.Types) != 2 || gotReq.Types[0] != "blocks" || gotReq.Types[1] != "user_transaction" {
		t.Errorf("types = %v, want [blocks user_transaction]", gotReq.Types)
	}
	if len(gotReq.Addresses) != 2 || gotReq.Addresses[0] != "klv1abc" {
		t.Errorf("addresses = %v, want [klv1abc klv1def]", gotReq.Addresses)
	}
}

func TestSubscriber_Connected(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	if sub.Connected() {
		t.Error("should not be connected before Start")
	}

	var wasConnected atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	for evt := range sub.Events() {
		if sub.Connected() {
			wasConnected.Store(true)
		}
		_ = evt
		cancel()
	}

	if !wasConnected.Load() {
		t.Error("Connected() should be true while events are flowing")
	}
}

func TestSubscriber_URL(t *testing.T) {
	sub := New("node.example.com:8080", []EventType{EventBlocks})
	if got := sub.URL(); got != "ws://node.example.com:8080/subscribe" {
		t.Errorf("URL() = %q, want ws://node.example.com:8080/subscribe", got)
	}

	sub2 := New("node.example.com:443", []EventType{EventBlocks}, WithScheme("wss"))
	if got := sub2.URL(); got != "wss://node.example.com:443/subscribe" {
		t.Errorf("URL() = %q, want wss://node.example.com:443/subscribe", got)
	}
}

func TestSubscription_DoubleClose(t *testing.T) {
	sub := New("unused:8080", []EventType{EventBlocks})
	s := sub.Subscribe()

	// First close should work.
	s.Close()

	// Second close should not panic.
	s.Close()
}

func TestSubscriber_Reconfigure(t *testing.T) {
	// First connection: sends 1 "blocks" event.
	// After Reconfigure: sends 1 "transactions" event.
	var connCount atomic.Int32

	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := mock.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Read subscription request.
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req subscribeRequest
		json.Unmarshal(msg, &req)

		n := connCount.Add(1)
		var evt rawEvent
		if n == 1 {
			evt = makeEvent("blocks", "h1", `{"n":1}`)
		} else {
			evt = makeEvent("transactions", "tx1", `{"n":2}`)
		}

		data, _ := json.Marshal(evt)
		conn.WriteMessage(websocket.TextMessage, data)

		// Keep connection alive until client closes.
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	sub := New(host, []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	// Receive the first event (blocks).
	evt1 := <-sub.Events()
	if evt1.Type != EventBlocks {
		t.Errorf("first event type = %q, want blocks", evt1.Type)
	}

	// Reconfigure to transactions.
	sub.Reconfigure([]EventType{EventTransactions}, nil)

	// Receive the second event (transactions).
	evt2 := <-sub.Events()
	if evt2.Type != EventTransactions {
		t.Errorf("second event type = %q, want transactions", evt2.Type)
	}

	cancel()
}

func TestSubscriber_StartWithNoTypes(t *testing.T) {
	// Start with no types — should wait for Reconfigure.
	events := []rawEvent{
		makeEvent("blocks", "h1", `{}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), nil,
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	// No events should be received yet (no types configured).
	select {
	case <-sub.Events():
		t.Fatal("should not receive events before Reconfigure")
	case <-time.After(200 * time.Millisecond):
		// Good, no events.
	}

	// Reconfigure with types.
	sub.Reconfigure([]EventType{EventBlocks}, nil)

	// Now we should receive events.
	evt := <-sub.Events()
	if evt.Type != EventBlocks {
		t.Errorf("type = %q, want blocks", evt.Type)
	}
	cancel()
}

func TestSubscriber_TypesAndAddresses(t *testing.T) {
	sub := New("unused:8080", []EventType{EventBlocks, EventAccounts},
		WithAddresses([]string{"klv1a", "klv1b"}),
	)

	types := sub.Types()
	if len(types) != 2 || types[0] != EventBlocks || types[1] != EventAccounts {
		t.Errorf("Types() = %v, want [blocks accounts]", types)
	}

	addrs := sub.Addresses()
	if len(addrs) != 2 || addrs[0] != "klv1a" {
		t.Errorf("Addresses() = %v, want [klv1a klv1b]", addrs)
	}

	// Reconfigure should update.
	sub.Reconfigure([]EventType{EventTransactions}, []string{"klv1c"})

	types = sub.Types()
	if len(types) != 1 || types[0] != EventTransactions {
		t.Errorf("Types() after Reconfigure = %v, want [transactions]", types)
	}
	addrs = sub.Addresses()
	if len(addrs) != 1 || addrs[0] != "klv1c" {
		t.Errorf("Addresses() after Reconfigure = %v, want [klv1c]", addrs)
	}
}

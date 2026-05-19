package subscriber

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSubscriber_AddSubscriptions(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		if req.Method != "subscribe" {
			t.Errorf("method = %q, want subscribe", req.Method)
		}
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`"subscribed"`),
		}
		data, _ := json.Marshal(resp)
		conn.WriteMessage(websocket.TextMessage, data)
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(mock.handler))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	connected := make(chan struct{})
	sub := New(host, []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() { close(connected) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	<-connected

	err := sub.AddSubscriptions(ctx, []EventType{EventTransactions}, []string{"klv1new"})
	if err != nil {
		t.Fatalf("AddSubscriptions error: %v", err)
	}

	types := sub.Types()
	found := false
	for _, ty := range types {
		if ty == EventTransactions {
			found = true
		}
	}
	if !found {
		t.Errorf("types after AddSubscriptions = %v, missing transactions", types)
	}

	addrs := sub.Addresses()
	foundAddr := false
	for _, a := range addrs {
		if a == "klv1new" {
			foundAddr = true
		}
	}
	if !foundAddr {
		t.Errorf("addresses after AddSubscriptions = %v, missing klv1new", addrs)
	}

	cancel()
}

func TestSubscriber_RemoveSubscriptions(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		if req.Method != "unsubscribe" {
			t.Errorf("method = %q, want unsubscribe", req.Method)
		}
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`"unsubscribed"`),
		}
		data, _ := json.Marshal(resp)
		conn.WriteMessage(websocket.TextMessage, data)
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(mock.handler))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	connected := make(chan struct{})
	sub := New(host, []EventType{EventBlocks, EventTransactions},
		WithAddresses([]string{"klv1a", "klv1b"}),
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() { close(connected) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	<-connected

	err := sub.RemoveSubscriptions(ctx, []EventType{EventBlocks}, []string{"klv1a"})
	if err != nil {
		t.Fatalf("RemoveSubscriptions error: %v", err)
	}

	types := sub.Types()
	if len(types) != 1 || types[0] != EventTransactions {
		t.Errorf("types after RemoveSubscriptions = %v, want [transactions]", types)
	}

	addrs := sub.Addresses()
	if len(addrs) != 1 || addrs[0] != "klv1b" {
		t.Errorf("addresses after RemoveSubscriptions = %v, want [klv1b]", addrs)
	}

	cancel()
}

func TestSubscriber_ReconfigureReplaceAddressOnly(t *testing.T) {
	type captured struct {
		method string
		params map[string]json.RawMessage
	}
	var calls []captured
	var callsMu sync.Mutex

	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		raw, _ := json.Marshal(req.Params)
		var params map[string]json.RawMessage
		_ = json.Unmarshal(raw, &params)
		callsMu.Lock()
		calls = append(calls, captured{method: req.Method, params: params})
		callsMu.Unlock()

		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`"ok"`),
		}
		data, _ := json.Marshal(resp)
		conn.WriteMessage(websocket.TextMessage, data)
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(mock.handler))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	connected := make(chan struct{})
	sub := New(host, []EventType{EventBlocks, EventAccounts},
		WithAddresses([]string{"klv1old"}),
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	<-connected

	// Replace All: same types, swap the only address.
	sub.Reconfigure([]EventType{EventBlocks, EventAccounts}, []string{"klv1new"})

	time.Sleep(250 * time.Millisecond)

	callsMu.Lock()
	got := append([]captured(nil), calls...)
	callsMu.Unlock()

	var unsub *captured
	gotMethods := make([]string, 0, len(got))
	for i := range got {
		gotMethods = append(gotMethods, got[i].method)
		if got[i].method == "unsubscribe" && unsub == nil {
			unsub = &got[i]
		}
	}
	if unsub == nil {
		t.Fatalf("expected an unsubscribe call, got methods=%v", gotMethods)
	}

	if _, ok := unsub.params["types"]; ok {
		t.Errorf("unsubscribe must not carry types field when removing addresses; params=%v", unsub.params)
	}
	addrsRaw, ok := unsub.params["addresses"]
	if !ok {
		t.Fatalf("unsubscribe missing addresses; params=%v", unsub.params)
	}
	var addrs []string
	_ = json.Unmarshal(addrsRaw, &addrs)
	if len(addrs) != 1 || addrs[0] != "klv1old" {
		t.Errorf("unsubscribe addresses = %v, want [klv1old]", addrs)
	}

	cancel()
}

func TestSubscriber_ReconfigureDynamic(t *testing.T) {
	var methods []string
	var methodsMu sync.Mutex

	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		methodsMu.Lock()
		methods = append(methods, req.Method)
		methodsMu.Unlock()
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`"ok"`),
		}
		data, _ := json.Marshal(resp)
		conn.WriteMessage(websocket.TextMessage, data)
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(mock.handler))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	connected := make(chan struct{})
	sub := New(host, []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	<-connected

	sub.Reconfigure([]EventType{EventBlocks, EventTransactions}, []string{"klv1a"})

	time.Sleep(200 * time.Millisecond)

	methodsMu.Lock()
	gotMethods := make([]string, len(methods))
	copy(gotMethods, methods)
	methodsMu.Unlock()

	if len(gotMethods) == 0 {
		t.Fatal("expected dynamic subscribe request, got none")
	}
	if gotMethods[0] != "subscribe" {
		t.Errorf("first method = %q, want subscribe", gotMethods[0])
	}

	types := sub.Types()
	foundTx := false
	for _, ty := range types {
		if ty == EventTransactions {
			foundTx = true
		}
	}
	if !foundTx {
		t.Errorf("types after dynamic Reconfigure = %v, want to include transactions", types)
	}

	cancel()
}

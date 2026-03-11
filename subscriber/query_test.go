package subscriber

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSubscriber_GetTransaction(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		if req.Method != "get_transaction" {
			t.Errorf("method = %q, want get_transaction", req.Method)
		}
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`{"hash":"abc","status":"onChain"}`),
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

	data, err := sub.GetTransaction(ctx, "abc", true)
	if err != nil {
		t.Fatalf("GetTransaction error: %v", err)
	}

	var result map[string]string
	json.Unmarshal(data, &result)
	if result["hash"] != "abc" {
		t.Errorf("hash = %q, want abc", result["hash"])
	}

	cancel()
}

func TestSubscriber_GetBlock(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		if req.Method != "get_block" {
			t.Errorf("method = %q, want get_block", req.Method)
		}
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`{"nonce":42,"hash":"blockhash"}`),
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

	nonce := uint64(42)
	data, err := sub.GetBlock(ctx, GetBlockParams{Nonce: &nonce, WithTxs: true})
	if err != nil {
		t.Fatalf("GetBlock error: %v", err)
	}

	var result map[string]any
	json.Unmarshal(data, &result)
	if result["nonce"] != float64(42) {
		t.Errorf("nonce = %v, want 42", result["nonce"])
	}

	cancel()
}

package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSubscriber_SendRequest(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`{"result":"ok"}`),
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

	resp, err := sub.sendRequest(ctx, "test_method", map[string]string{"key": "val"})
	if err != nil {
		t.Fatalf("sendRequest error: %v", err)
	}
	if string(resp.Data) != `{"result":"ok"}` {
		t.Errorf("response data = %s, want {\"result\":\"ok\"}", resp.Data)
	}

	cancel()
}

func TestSubscriber_ReadLoopRouting(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		events:   []rawEvent{makeEvent("blocks", "h1", `{"height":1}`)},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		resp := Response{
			ID:   req.ID,
			Data: json.RawMessage(`{"routed":true}`),
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

	evt := <-sub.Events()
	if evt.Type != EventBlocks || evt.Hash != "h1" {
		t.Errorf("event = %+v, want blocks/h1", evt)
	}

	resp, err := sub.sendRequest(ctx, "test", nil)
	if err != nil {
		t.Fatalf("sendRequest error: %v", err)
	}
	if string(resp.Data) != `{"routed":true}` {
		t.Errorf("response = %s, want {\"routed\":true}", resp.Data)
	}

	cancel()
}

func TestSubscriber_PendingCleanupOnDisconnect(t *testing.T) {
	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.onRequest = func(conn *websocket.Conn, req Request) {
		conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(mock.handler))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	connected := make(chan struct{})
	sub := New(host, []EventType{EventBlocks},
		WithReconnectInterval(5*time.Second),
		WithOnConnect(func() { close(connected) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	<-connected

	_, err := sub.sendRequest(ctx, "test", nil)
	if err == nil {
		t.Fatal("expected error from sendRequest after disconnect")
	}

	cancel()
}

func TestSubscriber_RequestNotConnected(t *testing.T) {
	sub := New("unused:8080", nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := sub.GetTransaction(ctx, "abc", false)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("error = %v, want ErrNotConnected", err)
	}
}

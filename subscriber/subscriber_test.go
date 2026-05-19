package subscriber

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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

	extra.Close()

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

	sub := New(mock.host(), []EventType{EventBlocks, EventUserTransactions},
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
	if len(gotReq.Types) != 2 || gotReq.Types[0] != "blocks" || gotReq.Types[1] != "user_transactions" {
		t.Errorf("types = %v, want [blocks user_transactions]", gotReq.Types)
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

	var wasConnected atomic.Bool
	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
		WithOnConnect(func() {
			wasConnected.Store(true)
		}),
	)

	if sub.Connected() {
		t.Error("should not be connected before Start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	for evt := range sub.Events() {
		_ = evt
		cancel()
	}

	if !wasConnected.Load() {
		t.Error("OnConnect should have fired during the run")
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

	s.Close()

	s.Close()
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

func TestSubscriber_WireFormatFieldName(t *testing.T) {
	var gotReq subscribeRequest
	var reqReceived atomic.Bool

	events := []rawEvent{makeEvent("blocks", "h1", `{}`)}
	mock := newMockServer(events)
	mock.onSubscribe = func(req subscribeRequest) {
		gotReq = req
		reqReceived.Store(true)
	}
	defer mock.close()

	sub := New(mock.host(), []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)
	collectN(sub.Events(), 1, cancel)

	if !reqReceived.Load() {
		t.Fatal("subscription request was not received")
	}

	payload, _ := json.Marshal(gotReq)
	if !strings.Contains(string(payload), `"subscribed_types"`) {
		t.Errorf("wire format uses wrong field name: %s", payload)
	}
	if strings.Contains(string(payload), `"subcribed_types"`) {
		t.Errorf("wire format still uses old typo field name: %s", payload)
	}
}

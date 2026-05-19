package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func TestSSEClientLimit(t *testing.T) {
	b := New(2, func() bool { return false }, "")
	b.sseCount.Store(2)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	b.HandleSSE(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStatsResponseJSON(t *testing.T) {
	stats := StatsSnapshot{
		Connected:    true,
		Total:        42,
		Counts:       map[subscriber.EventType]uint64{subscriber.EventBlocks: 30, subscriber.EventTransactions: 12},
		EventsPerSec: 2.5,
	}

	data, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}

	var decoded StatsSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}

	if decoded.Total != 42 {
		t.Errorf("Total = %d, want 42", decoded.Total)
	}
	if decoded.EventsPerSec != 2.5 {
		t.Errorf("EventsPerSec = %f, want 2.5", decoded.EventsPerSec)
	}
	if !decoded.Connected {
		t.Error("Connected should be true")
	}
	if decoded.Counts[subscriber.EventBlocks] != 30 {
		t.Errorf("Counts[blocks] = %d, want 30", decoded.Counts[subscriber.EventBlocks])
	}
}

func TestNoCORSHeader(t *testing.T) {
	b := New(1, func() bool { return false }, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so the SSE loop exits after headers are written

	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	b.HandleSSE(rec, req)

	if rec.Code == http.StatusServiceUnavailable {
		t.Fatal("handler rejected request at capacity check; test is not exercising the SSE path")
	}

	cors := rec.Header().Get("Access-Control-Allow-Origin")
	if cors != "" {
		t.Errorf("expected no CORS header, got %q", cors)
	}
}

func TestHandleSSESendsInitialStatsFrame(t *testing.T) {
	b := New(1, func() bool { return true }, "wss://example/subscribe")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	b.HandleSSE(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "event: stats\n") {
		t.Fatalf("expected initial stats frame, body=%q", body)
	}
	if !strings.Contains(body, `"connected":true`) {
		t.Errorf("stats payload missing connected:true, body=%q", body)
	}
	if !strings.Contains(body, `"url":"wss://example/subscribe"`) {
		t.Errorf("stats payload missing url, body=%q", body)
	}
}

func TestStartStatsLoopPushesOnStateChange(t *testing.T) {
	var connected atomic.Bool
	b := New(1, func() bool { return connected.Load() }, "")

	c := &client{frames: make(chan sseFrame, 4)}
	b.clientsMu.Lock()
	b.clients[c] = struct{}{}
	b.clientsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go b.StartStatsLoop(ctx, 50*time.Millisecond)

	// Heartbeat frame should arrive within a few ticks.
	first, err := waitFrame(c.frames, time.Second)
	if err != nil {
		t.Fatalf("no heartbeat frame: %v", err)
	}
	if first.event != "stats" {
		t.Errorf("event = %q, want %q", first.event, "stats")
	}
	if !strings.Contains(string(first.data), `"connected":false`) {
		t.Errorf("first heartbeat should report disconnected, got %s", first.data)
	}

	// Flip the state — loop should push immediately on transition.
	connected.Store(true)
	next, err := waitFrame(c.frames, time.Second)
	if err != nil {
		t.Fatalf("no transition frame: %v", err)
	}
	if !strings.Contains(string(next.data), `"connected":true`) {
		t.Errorf("transition frame should report connected, got %s", next.data)
	}
}

func waitFrame(ch <-chan sseFrame, d time.Duration) (sseFrame, error) {
	select {
	case f := <-ch:
		return f, nil
	case <-time.After(d):
		return sseFrame{}, context.DeadlineExceeded
	}
}

func TestBroadcastSubscription(t *testing.T) {
	b := New(1, func() bool { return true }, "")

	c := &client{frames: make(chan sseFrame, 4)}
	b.clientsMu.Lock()
	b.clients[c] = struct{}{}
	b.clientsMu.Unlock()

	b.BroadcastSubscription(
		[]subscriber.EventType{subscriber.EventBlocks, subscriber.EventAccounts},
		[]string{"klv1abc"},
	)

	f, err := waitFrame(c.frames, time.Second)
	if err != nil {
		t.Fatalf("no subscription frame: %v", err)
	}
	if f.event != "subscription" {
		t.Errorf("event = %q, want %q", f.event, "subscription")
	}

	var got struct {
		Types     []subscriber.EventType `json:"types"`
		Addresses []string               `json:"addresses"`
	}
	if err := json.Unmarshal(f.data, &got); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if len(got.Types) != 2 || got.Types[0] != subscriber.EventBlocks || got.Types[1] != subscriber.EventAccounts {
		t.Errorf("types = %v, want [blocks accounts]", got.Types)
	}
	if len(got.Addresses) != 1 || got.Addresses[0] != "klv1abc" {
		t.Errorf("addresses = %v, want [klv1abc]", got.Addresses)
	}
}

package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
	b := New(0, func() bool { return false }, "")
	b.sseCount.Store(0)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	b.HandleSSE(rec, req)

	cors := rec.Header().Get("Access-Control-Allow-Origin")
	if cors != "" {
		t.Errorf("expected no CORS header, got %q", cors)
	}
}

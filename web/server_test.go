package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func TestSecurityHeaders(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := securityHeaders(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	tests := []struct {
		header string
		want   string
	}{
		{"X-Content-Type-Options", "nosniff"},
		{"X-Frame-Options", "DENY"},
		{"Referrer-Policy", "no-referrer"},
		{"Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'"},
	}

	for _, tt := range tests {
		got := rec.Header().Get(tt.header)
		if got != tt.want {
			t.Errorf("%s = %q, want %q", tt.header, got, tt.want)
		}
	}
}

func TestSSEClientLimit(t *testing.T) {
	s := &Server{
		maxSSEClients: 2,
		clients:       make(map[*sseClient]struct{}),
	}

	// Simulate 2 connected clients.
	s.sseCount.Store(2)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	s.handleSSE(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStatsResponseJSON(t *testing.T) {
	stats := statsResponse{
		Connected:    true,
		Total:        42,
		Counts:       map[subscriber.EventType]uint64{subscriber.EventBlocks: 30, subscriber.EventTransactions: 12},
		EventsPerSec: 2.5,
	}

	data, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}

	var decoded statsResponse
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
	s := &Server{
		maxSSEClients: 0, // will reject immediately
		clients:       make(map[*sseClient]struct{}),
	}
	s.sseCount.Store(0) // but maxSSEClients=0 means limit reached

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	s.handleSSE(rec, req)

	cors := rec.Header().Get("Access-Control-Allow-Origin")
	if cors != "" {
		t.Errorf("expected no CORS header, got %q", cors)
	}
}

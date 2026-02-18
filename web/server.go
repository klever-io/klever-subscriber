package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

//go:embed static
var staticFiles embed.FS

const (
	DefaultMaxSSEClients = 100
)

type sseClient struct {
	events chan subscriber.Event
}

// Server serves the web dashboard and SSE event stream.
//
// Security: The subscription endpoint allows reconfiguring the subscriber.
// Bind to localhost (e.g. "127.0.0.1:3000") when running on shared networks.
type Server struct {
	addr          string
	sub           *subscriber.Subscriber
	maxSSEClients int

	mu       sync.RWMutex
	clients  map[*sseClient]struct{}
	total    uint64
	counts   map[subscriber.EventType]uint64
	recentTs []time.Time

	sseCount atomic.Int64
}

// NewServer creates a new web dashboard server.
func NewServer(addr string, sub *subscriber.Subscriber) *Server {
	return &Server{
		addr:          addr,
		sub:           sub,
		maxSSEClients: DefaultMaxSSEClients,
		clients:       make(map[*sseClient]struct{}),
		counts:        make(map[subscriber.EventType]uint64),
	}
}

// securityHeaders wraps an http.Handler and sets standard security headers.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'")
		next.ServeHTTP(w, r)
	})
}

// Start runs the web server until ctx is cancelled.
// It returns a non-nil error if the server fails to bind.
func (s *Server) Start(ctx context.Context) error {
	subscription := s.sub.Subscribe()
	go s.processEvents(ctx, subscription.C())

	staticFS, _ := fs.Sub(staticFiles, "static")

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("/events", s.handleSSE)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/subscription", s.handleSubscription)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		<-ctx.Done()
		subscription.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	err := srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("web server: %w", err)
	}
	return nil
}

func (s *Server) processEvents(ctx context.Context, events <-chan subscriber.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-events:
			if !ok {
				return
			}
			now := time.Now()
			s.mu.Lock()
			s.total++
			s.counts[evt.Type]++
			s.recentTs = append(s.recentTs, now)
			// Keep only last 10 seconds of timestamps for rate calc.
			cutoff := now.Add(-10 * time.Second)
			start := 0
			for start < len(s.recentTs) && s.recentTs[start].Before(cutoff) {
				start++
			}
			s.recentTs = s.recentTs[start:]
			// Compact backing array to prevent slow memory growth.
			if cap(s.recentTs) > 2*len(s.recentTs) && cap(s.recentTs) > 1024 {
				compacted := make([]time.Time, len(s.recentTs))
				copy(compacted, s.recentTs)
				s.recentTs = compacted
			}
			s.mu.Unlock()

			// Fan out to SSE clients.
			s.mu.RLock()
			for c := range s.clients {
				select {
				case c.events <- evt:
				default:
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	// Enforce SSE client limit.
	if int(s.sseCount.Load()) >= s.maxSSEClients {
		http.Error(w, "too many SSE clients", http.StatusServiceUnavailable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := &sseClient{events: make(chan subscriber.Event, 256)}
	s.mu.Lock()
	s.clients[client] = struct{}{}
	s.mu.Unlock()
	s.sseCount.Add(1)

	defer func() {
		s.sseCount.Add(-1)
		s.mu.Lock()
		delete(s.clients, client)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-client.events:
			if !ok {
				return
			}
			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

type statsResponse struct {
	Connected    bool                            `json:"connected"`
	Total        uint64                          `json:"total"`
	Counts       map[subscriber.EventType]uint64 `json:"counts"`
	EventsPerSec float64                         `json:"eventsPerSec"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	now := time.Now()
	cutoff := now.Add(-10 * time.Second)
	count := 0
	for _, ts := range s.recentTs {
		if ts.After(cutoff) {
			count++
		}
	}
	counts := make(map[subscriber.EventType]uint64, len(s.counts))
	for k, v := range s.counts {
		counts[k] = v
	}
	total := s.total
	s.mu.RUnlock()

	stats := statsResponse{
		Connected:    s.sub.Connected(),
		Total:        total,
		Counts:       counts,
		EventsPerSec: float64(count) / 10.0,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

type subscriptionPayload struct {
	Types     []subscriber.EventType `json:"types"`
	Addresses []string               `json:"addresses"`
}

func (s *Server) handleSubscription(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		resp := subscriptionPayload{
			Types:     s.sub.Types(),
			Addresses: s.sub.Addresses(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)

	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
		var req subscriptionPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Validate types.
		valid := subscriber.ValidEventTypes()
		for _, t := range req.Types {
			if !valid[t] {
				http.Error(w, fmt.Sprintf("unknown event type %q", t), http.StatusBadRequest)
				return
			}
		}

		// Validate addresses.
		for _, addr := range req.Addresses {
			if len(addr) == 0 || len(addr) > 128 {
				http.Error(w, fmt.Sprintf("invalid address length: %q", addr), http.StatusBadRequest)
				return
			}
		}

		s.sub.Reconfigure(req.Types, req.Addresses)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(req)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

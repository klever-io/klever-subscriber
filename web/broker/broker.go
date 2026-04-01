package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

const DefaultMaxClients = 100

type client struct {
	events chan subscriber.Event
}

type Broker struct {
	maxClients  int
	isConnected func() bool
	url         string

	mu       sync.RWMutex
	clients  map[*client]struct{}
	total    uint64
	counts   map[subscriber.EventType]uint64
	recentTs []time.Time

	sseCount atomic.Int64
}

func New(maxClients int, isConnected func() bool, url string) *Broker {
	return &Broker{
		maxClients:  maxClients,
		isConnected: isConnected,
		url:         url,
		clients:     make(map[*client]struct{}),
		counts:      make(map[subscriber.EventType]uint64),
	}
}

func (b *Broker) HandleSSE(w http.ResponseWriter, r *http.Request) {
	if int(b.sseCount.Load()) >= b.maxClients {
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

	c := &client{events: make(chan subscriber.Event, 256)}
	b.mu.Lock()
	b.clients[c] = struct{}{}
	b.mu.Unlock()
	b.sseCount.Add(1)

	defer func() {
		b.sseCount.Add(-1)
		b.mu.Lock()
		delete(b.clients, c)
		b.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-c.events:
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

func (b *Broker) ProcessEvents(ctx context.Context, events <-chan subscriber.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-events:
			if !ok {
				return
			}
			now := time.Now()
			b.mu.Lock()
			b.total++
			b.counts[evt.Type]++
			b.recentTs = append(b.recentTs, now)
			cutoff := now.Add(-10 * time.Second)
			start := 0
			for start < len(b.recentTs) && b.recentTs[start].Before(cutoff) {
				start++
			}
			b.recentTs = b.recentTs[start:]
			if cap(b.recentTs) > 2*len(b.recentTs) && cap(b.recentTs) > 1024 {
				compacted := make([]time.Time, len(b.recentTs))
				copy(compacted, b.recentTs)
				b.recentTs = compacted
			}
			b.mu.Unlock()

			b.mu.RLock()
			for c := range b.clients {
				select {
				case c.events <- evt:
				default:
				}
			}
			b.mu.RUnlock()
		}
	}
}

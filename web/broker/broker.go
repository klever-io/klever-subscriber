package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

const (
	DefaultMaxClients = 100
	rateWindowSeconds = 10
)

type sseFrame struct {
	event string
	data  []byte
}

type client struct {
	frames chan sseFrame
}

// Broker fans out subscriber events to SSE clients and tracks delivery
// counters. Hot counters are atomic so per-event work doesn't contend with
// the SSE clients map.
type Broker struct {
	maxClients  int
	isConnected func() bool
	url         string

	clientsMu sync.RWMutex
	clients   map[*client]struct{}

	total      atomic.Uint64
	blocks     atomic.Uint64
	txs        atomic.Uint64
	userTxs    atomic.Uint64
	accounts   atomic.Uint64
	otherCount atomic.Uint64

	rateMu      sync.Mutex
	rateBuckets [rateWindowSeconds]uint64
	rateAnchor  int64 // unix second that owns rateBuckets[len-1]

	sseCount atomic.Int64
}

func New(maxClients int, isConnected func() bool, url string) *Broker {
	return &Broker{
		maxClients:  maxClients,
		isConnected: isConnected,
		url:         url,
		clients:     make(map[*client]struct{}),
	}
}

func (b *Broker) HandleSSE(w http.ResponseWriter, r *http.Request) {
	if int(b.sseCount.Add(1)) > b.maxClients {
		b.sseCount.Add(-1)
		http.Error(w, "too many SSE clients", http.StatusServiceUnavailable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		b.sseCount.Add(-1)
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Hint the browser EventSource to retry quickly on disconnect.
	fmt.Fprint(w, "retry: 3000\n\n")
	flusher.Flush()

	c := &client{frames: make(chan sseFrame, 256)}
	b.clientsMu.Lock()
	b.clients[c] = struct{}{}
	b.clientsMu.Unlock()

	defer func() {
		b.sseCount.Add(-1)
		b.clientsMu.Lock()
		delete(b.clients, c)
		b.clientsMu.Unlock()
	}()

	if data, err := json.Marshal(b.snapshot()); err == nil {
		writeFrame(w, flusher, sseFrame{event: "stats", data: data})
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case f, ok := <-c.frames:
			if !ok {
				return
			}
			writeFrame(w, flusher, f)
		}
	}
}

func writeFrame(w io.Writer, flusher http.Flusher, f sseFrame) {
	if f.event != "" {
		fmt.Fprintf(w, "event: %s\n", f.event)
	}
	fmt.Fprintf(w, "data: %s\n\n", f.data)
	flusher.Flush()
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
			b.recordEvent(evt.Type)

			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			b.broadcast(sseFrame{data: data})
		}
	}
}

func (b *Broker) recordEvent(t subscriber.EventType) {
	b.total.Add(1)
	switch t {
	case subscriber.EventBlocks:
		b.blocks.Add(1)
	case subscriber.EventTransactions:
		b.txs.Add(1)
	case subscriber.EventUserTransactions:
		b.userTxs.Add(1)
	case subscriber.EventAccounts:
		b.accounts.Add(1)
	default:
		b.otherCount.Add(1)
	}
	b.bumpRate(time.Now().Unix())
}

// bumpRate advances the ring to `now` (zeroing buckets that aged out)
// and increments the current bucket. O(min(delta, window)).
func (b *Broker) bumpRate(now int64) {
	b.rateMu.Lock()
	defer b.rateMu.Unlock()
	if b.rateAnchor == 0 {
		b.rateAnchor = now
	}
	delta := now - b.rateAnchor
	if delta < 0 {
		delta = 0
	}
	if delta >= rateWindowSeconds {
		for i := range b.rateBuckets {
			b.rateBuckets[i] = 0
		}
	} else {
		for i := int64(0); i < delta; i++ {
			copy(b.rateBuckets[:], b.rateBuckets[1:])
			b.rateBuckets[len(b.rateBuckets)-1] = 0
		}
	}
	b.rateAnchor = now
	b.rateBuckets[len(b.rateBuckets)-1]++
}

// rateSnapshot returns events received in the trailing rateWindowSeconds.
func (b *Broker) rateSnapshot(now int64) uint64 {
	b.rateMu.Lock()
	defer b.rateMu.Unlock()
	if b.rateAnchor == 0 {
		return 0
	}
	delta := now - b.rateAnchor
	if delta >= rateWindowSeconds {
		for i := range b.rateBuckets {
			b.rateBuckets[i] = 0
		}
		b.rateAnchor = now
		return 0
	}
	for i := int64(0); i < delta; i++ {
		copy(b.rateBuckets[:], b.rateBuckets[1:])
		b.rateBuckets[len(b.rateBuckets)-1] = 0
	}
	b.rateAnchor = now
	var sum uint64
	for _, v := range b.rateBuckets {
		sum += v
	}
	return sum
}

func (b *Broker) broadcast(f sseFrame) {
	b.clientsMu.RLock()
	for c := range b.clients {
		select {
		case c.frames <- f:
		default:
		}
	}
	b.clientsMu.RUnlock()
}

package broker

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

type StatsSnapshot struct {
	Connected    bool                            `json:"connected"`
	Total        uint64                          `json:"total"`
	Counts       map[subscriber.EventType]uint64 `json:"counts"`
	EventsPerSec float64                         `json:"eventsPerSec"`
	URL          string                          `json:"url"`
}

func (b *Broker) HandleStats(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	now := time.Now()
	cutoff := now.Add(-10 * time.Second)
	count := 0
	for _, ts := range b.recentTs {
		if ts.After(cutoff) {
			count++
		}
	}
	counts := make(map[subscriber.EventType]uint64, len(b.counts))
	for k, v := range b.counts {
		counts[k] = v
	}
	total := b.total
	b.mu.RUnlock()

	stats := StatsSnapshot{
		Connected:    b.isConnected(),
		Total:        total,
		Counts:       counts,
		EventsPerSec: float64(count) / 10.0,
		URL:          b.url,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

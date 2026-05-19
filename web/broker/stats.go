package broker

import (
	"context"
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

func (b *Broker) snapshot() StatsSnapshot {
	counts := map[subscriber.EventType]uint64{
		subscriber.EventBlocks:           b.blocks.Load(),
		subscriber.EventTransactions:     b.txs.Load(),
		subscriber.EventUserTransactions: b.userTxs.Load(),
		subscriber.EventAccounts:         b.accounts.Load(),
	}
	return StatsSnapshot{
		Connected:    b.isConnected(),
		Total:        b.total.Load(),
		Counts:       counts,
		EventsPerSec: float64(b.rateSnapshot(time.Now().Unix())) / float64(rateWindowSeconds),
		URL:          b.url,
	}
}

func (b *Broker) HandleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(b.snapshot())
}

// StartStatsLoop broadcasts a stats SSE frame on every heartbeat tick and
// immediately whenever the upstream connection state flips, so dashboards
// reflect connect/disconnect without polling. It returns when ctx is canceled.
func (b *Broker) StartStatsLoop(ctx context.Context, heartbeat time.Duration) {
	if heartbeat <= 0 {
		heartbeat = 2 * time.Second
	}
	probe := heartbeat / 4
	if probe < 250*time.Millisecond {
		probe = 250 * time.Millisecond
	}

	ticker := time.NewTicker(probe)
	defer ticker.Stop()

	lastConnected := b.isConnected()
	lastPush := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			connected := b.isConnected()
			stateChanged := connected != lastConnected
			heartbeatDue := now.Sub(lastPush) >= heartbeat
			if !stateChanged && !heartbeatDue {
				continue
			}
			data, err := json.Marshal(b.snapshot())
			if err != nil {
				continue
			}
			b.broadcast(sseFrame{event: "stats", data: data})
			lastConnected = connected
			lastPush = now
		}
	}
}

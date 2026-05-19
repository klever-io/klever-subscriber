// Stream events from a Klever node and print a short summary per event.
//
//	go run ./examples/stream
//
// Override the node target with KLEVER_NODE / KLEVER_WSS.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func main() {
	host := envOr("KLEVER_NODE", "localhost:8080")
	scheme := "ws"
	if os.Getenv("KLEVER_WSS") != "" {
		scheme = "wss"
	}

	sub := subscriber.New(host,
		[]subscriber.EventType{
			subscriber.EventBlocks,
			subscriber.EventTransactions,
		},
		subscriber.WithScheme(scheme),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go sub.Start(ctx)

	fmt.Fprintf(os.Stderr, "streaming from %s — press Ctrl+C to exit\n", sub.URL())

	for evt := range sub.Events() {
		fmt.Println(evt.Type)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Stream events with connect / disconnect / error callbacks. The subscriber
// reconnects automatically; this example shows how to observe those
// transitions so your service can update health checks, metrics, etc.
//
//	go run ./examples/reconnect
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func main() {
	host := envOr("KLEVER_NODE", "localhost:8080")
	scheme := "ws"
	if os.Getenv("KLEVER_WSS") != "" {
		scheme = "wss"
	}

	sub := subscriber.New(host,
		[]subscriber.EventType{subscriber.EventBlocks},
		subscriber.WithScheme(scheme),
		subscriber.WithReconnectInterval(2*time.Second),
		subscriber.WithOnConnect(func() {
			fmt.Fprintln(os.Stderr, "[connected]")
		}),
		subscriber.WithOnDisconnect(func() {
			fmt.Fprintln(os.Stderr, "[disconnected]")
		}),
		subscriber.WithOnError(func(err error) {
			fmt.Fprintf(os.Stderr, "[error] %v\n", err)
		}),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go sub.Start(ctx)

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

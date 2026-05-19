// One-shot request/response over the subscriber WebSocket — no event
// subscription. Useful when you want to reuse the same connection for both
// streaming and lookups, or when you only need lookups.
//
//	go run ./examples/query --hash <tx-hash>
//	go run ./examples/query --block 12345
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func main() {
	txHash := flag.String("hash", "", "Transaction hash to look up")
	blockNonce := flag.Uint64("block", 0, "Block nonce to look up")
	withResults := flag.Bool("with-results", false, "Include execution results")
	flag.Parse()

	if *txHash == "" && *blockNonce == 0 {
		fmt.Fprintln(os.Stderr, "usage: query --hash <tx-hash> | --block <nonce>")
		os.Exit(2)
	}

	host := envOr("KLEVER_NODE", "localhost:8080")
	scheme := "ws"
	if os.Getenv("KLEVER_WSS") != "" {
		scheme = "wss"
	}

	connected := make(chan struct{})
	sub := subscriber.New(host, nil,
		subscriber.WithScheme(scheme),
		subscriber.WithQueryOnly(),
		subscriber.WithOnConnect(func() { close(connected) }),
		subscriber.WithOnError(func(err error) {
			fmt.Fprintf(os.Stderr, "[subscriber error] %v\n", err)
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go sub.Start(ctx)

	select {
	case <-connected:
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "connect timeout: %v\n", ctx.Err())
		os.Exit(1)
	}

	var (
		data []byte
		err  error
	)
	if *txHash != "" {
		data, err = sub.GetTransaction(ctx, *txHash, *withResults)
	} else {
		nonce := *blockNonce
		data, err = sub.GetBlock(ctx, subscriber.GetBlockParams{Nonce: &nonce})
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(data))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

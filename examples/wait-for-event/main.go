// Wait for a specific block or transaction to appear on chain.
//
// Subscriptions only deliver future events, so to handle the common case
// where the user broadcasts a tx and *then* runs this command, we:
//
//  1. Subscribe to the relevant event stream so we don't miss anything.
//  2. Query the node for the target once. If it already exists, return.
//  3. Otherwise keep reading the subscription until the target arrives
//     or the timeout fires.
//
// Steps 1 and 2 are in that order on purpose: subscribing before querying
// closes the race where the target lands between the query and the
// subscription.
//
//	go run ./examples/wait-for-event --block-nonce 12345
//	go run ./examples/wait-for-event --block-hash af42...
//	go run ./examples/wait-for-event --tx-hash 9ab1...
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func main() {
	blockNonce := flag.Uint64("block-nonce", 0, "Block nonce to wait for")
	blockHash := flag.String("block-hash", "", "Block hash to wait for")
	txHash := flag.String("tx-hash", "", "Transaction hash to wait for")
	timeout := flag.Duration("timeout", 2*time.Minute, "Maximum time to wait")
	debug := flag.Bool("debug", false, "Print every incoming event to stderr")
	flag.Parse()

	var blockNonceSet bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "block-nonce" {
			blockNonceSet = true
		}
	})

	target, err := pickTarget(blockNonceSet, *blockNonce, *blockHash, *txHash)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, "usage: wait-for-event --block-nonce N | --block-hash H | --tx-hash H")
		os.Exit(2)
	}

	host := envOr("KLEVER_NODE", "localhost:8080")
	scheme := "ws"
	if os.Getenv("KLEVER_WSS") != "" {
		scheme = "wss"
	}

	connected := make(chan struct{})
	sub := subscriber.New(host,
		[]subscriber.EventType{target.eventType},
		subscriber.WithScheme(scheme),
		subscriber.WithOnConnect(func() { close(connected) }),
		subscriber.WithOnError(func(err error) {
			fmt.Fprintf(os.Stderr, "[subscriber error] %v\n", err)
		}),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, *timeout)
	defer cancel()

	go sub.Start(ctx)

	select {
	case <-connected:
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "connect timeout: %v\n", ctx.Err())
		os.Exit(1)
	}

	// Already-on-chain check. Runs after we are subscribed so we cannot
	// miss the target if it lands between the lookup and the subscribe.
	if data, err := target.lookup(ctx, sub); err == nil {
		printJSON(data)
		return
	} else if !errors.Is(err, subscriber.ErrRequestFailed) {
		fmt.Fprintf(os.Stderr, "lookup failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "not on chain yet — waiting up to %s for %s…\n", *timeout, target.label)

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "timed out: %v\n", ctx.Err())
			os.Exit(1)
		case evt, ok := <-sub.Events():
			if !ok {
				fmt.Fprintln(os.Stderr, "event stream closed")
				os.Exit(1)
			}
			if *debug {
				fmt.Fprintf(os.Stderr, "[event] type=%q hash=%q raw=%s\n", evt.Type, evt.Hash, string(evt.Raw))
			}
			if evt.Type != target.eventType || !target.match(evt) {
				continue
			}
			pretty, err := json.MarshalIndent(evt, "", "  ")
			if err != nil {
				fmt.Println(string(evt.Raw))
				return
			}
			fmt.Println(string(pretty))
			return
		}
	}
}

type target struct {
	eventType subscriber.EventType
	label     string
	match     func(subscriber.Event) bool
	lookup    func(context.Context, *subscriber.Subscriber) (json.RawMessage, error)
}

func pickTarget(nonceSet bool, nonce uint64, blockHash, txHash string) (target, error) {
	switch {
	case nonceSet:
		want := nonce
		return target{
			eventType: subscriber.EventBlocks,
			label:     fmt.Sprintf("block nonce=%d", want),
			match: func(evt subscriber.Event) bool {
				probe, err := decodeBlockData(evt.Raw)
				return err == nil && probe.Nonce == want
			},
			lookup: func(ctx context.Context, s *subscriber.Subscriber) (json.RawMessage, error) {
				n := want
				return s.GetBlock(ctx, subscriber.GetBlockParams{Nonce: &n})
			},
		}, nil

	case blockHash != "":
		want := strings.ToLower(blockHash)
		return target{
			eventType: subscriber.EventBlocks,
			label:     "block hash=" + blockHash,
			match: func(evt subscriber.Event) bool {
				probe, err := decodeBlockData(evt.Raw)
				return err == nil && strings.EqualFold(probe.Hash, want)
			},
			lookup: func(ctx context.Context, s *subscriber.Subscriber) (json.RawMessage, error) {
				return s.GetBlock(ctx, subscriber.GetBlockParams{Hash: blockHash})
			},
		}, nil

	case txHash != "":
		want := strings.ToLower(txHash)
		return target{
			eventType: subscriber.EventTransactions,
			label:     "tx hash=" + txHash,
			match: func(evt subscriber.Event) bool {
				if strings.EqualFold(evt.Hash, want) {
					return true
				}
				for _, h := range decodeTxHashes(evt.Raw) {
					if strings.EqualFold(h, want) {
						return true
					}
				}
				return false
			},
			lookup: func(ctx context.Context, s *subscriber.Subscriber) (json.RawMessage, error) {
				return s.GetTransaction(ctx, txHash, false)
			},
		}, nil
	}
	return target{}, fmt.Errorf("no target specified")
}

// The streaming event format flattens block fields under `data` (lowercase
// keys), unlike the `get_block` response which uses the chain's internal
// CamelCase representation. Mirror the wire shape here so the example
// actually matches incoming events.
type blockData struct {
	Hash  string `json:"hash"`
	Nonce uint64 `json:"nonce"`
}

func decodeBlockData(raw []byte) (blockData, error) {
	var probe struct {
		Data blockData `json:"data"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return blockData{}, err
	}
	return probe.Data, nil
}

// A transactions event carries an array of txs (the block's worth), with
// each entry exposing the hash under a top-level "hash" key:
//
//	{"type":"transactions","data":[{"hash":"…",…},{"hash":"…",…}]}
//
// We also fall back to the top-level "hash" and a single-object "data" shape
// so the example tolerates protocol drift across node versions.
func decodeTxHashes(raw []byte) []string {
	var probe struct {
		Hash string          `json:"hash"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil
	}
	var out []string
	if probe.Hash != "" {
		out = append(out, probe.Hash)
	}
	if len(probe.Data) == 0 {
		return out
	}

	type hashOnly struct {
		Hash string `json:"hash"`
	}
	// Array shape (the live transactions stream).
	var arr []hashOnly
	if err := json.Unmarshal(probe.Data, &arr); err == nil {
		for _, e := range arr {
			if e.Hash != "" {
				out = append(out, e.Hash)
			}
		}
		return out
	}
	// Single-object shape (defensive fallback).
	var single hashOnly
	if err := json.Unmarshal(probe.Data, &single); err == nil && single.Hash != "" {
		out = append(out, single.Hash)
	}
	return out
}

func printJSON(raw json.RawMessage) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		fmt.Println(string(raw))
		return
	}
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Println(string(raw))
		return
	}
	fmt.Println(string(pretty))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

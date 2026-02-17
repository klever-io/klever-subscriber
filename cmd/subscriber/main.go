package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/klever-io/klever-subscriber/subscriber"
	"github.com/klever-io/klever-subscriber/web"
	"github.com/spf13/cobra"
)

var (
	nodeURL   string
	useWss    bool
	types     []string
	addresses []string
	pretty    bool
	raw       bool
	webAddr   string
	quiet     bool
)

var rootCmd = &cobra.Command{
	Use:   "subscriber",
	Short: "Klever Node WebSocket Subscriber",
	Long: `Connects to a Klever node's WebSocket subscription endpoint and
prints received events to stdout. Optionally serves a live web dashboard.

Available event types:
  blocks            - New block events
  transactions      - All transaction events
  user_transaction  - Address-specific transaction events
  accounts          - Address-specific account update events

Examples:
  # Subscribe to all block events
  subscriber --types blocks

  # Subscribe to transactions for specific addresses
  subscriber --types user_transaction,accounts --addresses klv1abc...,klv1def...

  # Subscribe to everything on a custom node
  subscriber --node 10.0.0.1:8080 --types blocks,transactions

  # Pretty-print decoded events
  subscriber --node 10.0.0.1:8080 --types blocks --pretty

  # Start with web dashboard (types optional, configurable via UI)
  subscriber --web :3000

  # Start with web dashboard and initial subscription
  subscriber --types blocks --web :3000

  # Web mode with types but no console output
  subscriber --types blocks --web :3000 --quiet`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// When --web is set, --types becomes optional (configurable via dashboard).
		if len(types) == 0 && webAddr == "" {
			return fmt.Errorf("at least one event type is required (use --types), or use --web for dashboard mode")
		}

		valid := subscriber.ValidEventTypes()
		var eventTypes []subscriber.EventType
		for _, t := range types {
			et := subscriber.EventType(t)
			if !valid[et] {
				return fmt.Errorf("unknown event type %q (valid: blocks, transactions, user_transaction, accounts)", t)
			}
			eventTypes = append(eventTypes, et)
		}
		return run(eventTypes)
	},
}

func init() {
	rootCmd.Flags().StringVar(&nodeURL, "node", "localhost:8080", "Node API address (host:port)")
	rootCmd.Flags().BoolVar(&useWss, "wss", false, "Use wss:// instead of ws://")
	rootCmd.Flags().StringSliceVar(&types, "types", nil, "Event types to subscribe to (blocks,transactions,user_transaction,accounts)")
	rootCmd.Flags().StringSliceVar(&addresses, "addresses", nil, "Addresses to watch (required for user_transaction and accounts types)")
	rootCmd.Flags().BoolVar(&pretty, "pretty", false, "Pretty-print JSON output")
	rootCmd.Flags().BoolVar(&raw, "raw", false, "Print raw messages without decoding base64 data")
	rootCmd.Flags().StringVar(&webAddr, "web", "", "Start web dashboard on the given address (e.g. :3000)")
	rootCmd.Flags().BoolVar(&quiet, "quiet", false, "Suppress stdout event output and stderr status messages (implied in web-only mode)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(eventTypes []subscriber.EventType) error {
	// In web-only mode (no types), default to quiet.
	if webAddr != "" && len(eventTypes) == 0 {
		quiet = true
	}

	logf := func(format string, a ...any) {
		if !quiet {
			fmt.Fprintf(os.Stderr, format, a...)
		}
	}

	scheme := "ws"
	if useWss {
		scheme = "wss"
	}

	var opts []subscriber.Option
	opts = append(opts, subscriber.WithScheme(scheme))
	if len(addresses) > 0 {
		opts = append(opts, subscriber.WithAddresses(addresses))
	}
	opts = append(opts, subscriber.WithOnConnect(func() {
		logf("Connected, waiting for events...\n")
	}))
	opts = append(opts, subscriber.WithOnDisconnect(func() {
		logf("Disconnected\n")
	}))
	opts = append(opts, subscriber.WithOnError(func(err error) {
		logf("Error: %v\n", err)
		logf("Reconnecting in %s...\n", subscriber.DefaultReconnectInterval)
	}))

	sub := subscriber.New(nodeURL, eventTypes, opts...)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logf("Connecting to %s\n", sub.URL())
	if len(types) > 0 {
		logf("Subscribed types: %s\n", strings.Join(types, ", "))
	}
	if len(addresses) > 0 {
		logf("Watching addresses: %s\n", strings.Join(addresses, ", "))
	}

	if webAddr != "" {
		srv := web.NewServer(webAddr, sub)
		go srv.Start(ctx)
		// Always print the dashboard URL so you know where to go.
		fmt.Fprintf(os.Stderr, "Web dashboard at http://%s\n", webAddr)
		if len(eventTypes) == 0 {
			fmt.Fprintln(os.Stderr, "No event types specified — configure via dashboard")
		}
	}

	logf("Press Ctrl+C to exit\n")
	logf("---\n")

	go sub.Start(ctx)

	if quiet {
		// No stdout output — just wait for context cancellation.
		<-ctx.Done()
	} else {
		for evt := range sub.Events() {
			if raw {
				if len(evt.Raw) > 0 {
					fmt.Println(string(evt.Raw))
				}
				continue
			}

			var output []byte
			var err error
			if pretty {
				output, err = json.MarshalIndent(evt, "", "  ")
			} else {
				output, err = json.Marshal(evt)
			}
			if err != nil {
				if len(evt.Raw) > 0 {
					fmt.Println(string(evt.Raw))
				}
				continue
			}
			fmt.Println(string(output))
		}
	}

	logf("\nShutting down...\n")
	return nil
}

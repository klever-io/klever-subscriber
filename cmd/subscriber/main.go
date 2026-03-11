package main

import (
	"fmt"
	"os"

	"github.com/klever-io/klever-subscriber/subscriber"
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
  blocks             - New block events
  transactions       - All transaction events
  user_transactions  - Address-specific transaction events
  accounts           - Address-specific account update events

Examples:
  # Subscribe to all block events
  subscriber --types blocks

  # Subscribe to transactions for specific addresses
  subscriber --types user_transactions,accounts --addresses klv1abc...,klv1def...

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
		if len(types) == 0 && webAddr == "" {
			return fmt.Errorf("at least one event type is required (use --types), or use --web for dashboard mode")
		}

		valid := subscriber.ValidEventTypes()
		var eventTypes []subscriber.EventType
		for _, t := range types {
			et := subscriber.EventType(t)
			if !valid[et] {
				return fmt.Errorf("unknown event type %q (valid: blocks, transactions, user_transactions, accounts)", t)
			}
			eventTypes = append(eventTypes, et)
		}
		return run(eventTypes)
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&nodeURL, "node", "localhost:8080", "Node API address (host:port)")
	rootCmd.PersistentFlags().BoolVar(&useWss, "wss", false, "Use wss:// instead of ws://")
	rootCmd.PersistentFlags().BoolVar(&pretty, "pretty", false, "Pretty-print JSON output")

	rootCmd.Flags().StringSliceVar(&types, "types", nil, "Event types to subscribe to (blocks,transactions,user_transactions,accounts)")
	rootCmd.Flags().StringSliceVar(&addresses, "addresses", nil, "Addresses to watch (required for user_transactions and accounts types)")
	rootCmd.Flags().BoolVar(&raw, "raw", false, "Print raw messages without decoding base64 data")
	rootCmd.Flags().StringVar(&webAddr, "web", "0.0.0.0:3000", "Start web dashboard on the given address (e.g. :3000)")
	rootCmd.Flags().BoolVar(&quiet, "quiet", false, "Suppress stdout event output and stderr status messages (implied in web-only mode)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

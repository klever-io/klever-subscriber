package main

import (
	"context"
	"fmt"

	"github.com/klever-io/klever-subscriber/subscriber"
	"github.com/spf13/cobra"
)

var withResults bool

var getTxCmd = &cobra.Command{
	Use:   "get-tx <hash>",
	Short: "Look up a transaction by hash",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hash := args[0]
		scheme := "ws"
		if useWss {
			scheme = "wss"
		}

		connected := make(chan struct{})
		sub := subscriber.New(nodeURL, nil,
			subscriber.WithScheme(scheme),
			subscriber.WithQueryOnly(),
			subscriber.WithOnConnect(func() { close(connected) }),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go sub.Start(ctx)

		select {
		case <-connected:
		case <-ctx.Done():
			return fmt.Errorf("connection timed out")
		}

		data, err := sub.GetTransaction(ctx, hash, withResults)
		if err != nil {
			return err
		}

		return printJSON(data)
	},
}

func init() {
	getTxCmd.Flags().BoolVar(&withResults, "with-results", false, "Include execution results")
	rootCmd.AddCommand(getTxCmd)
}

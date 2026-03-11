package main

import (
	"context"
	"fmt"

	"github.com/klever-io/klever-subscriber/subscriber"
	"github.com/spf13/cobra"
)

var (
	blockNonce uint64
	blockHash  string
	withTxs    bool
)

var getBlockCmd = &cobra.Command{
	Use:   "get-block",
	Short: "Look up a block by nonce or hash",
	RunE: func(cmd *cobra.Command, args []string) error {
		if blockNonce == 0 && blockHash == "" {
			return fmt.Errorf("must provide --nonce or --hash")
		}

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

		params := subscriber.GetBlockParams{
			Hash:    blockHash,
			WithTxs: withTxs,
		}
		if blockNonce > 0 {
			params.Nonce = &blockNonce
		}

		data, err := sub.GetBlock(ctx, params)
		if err != nil {
			return err
		}

		return printJSON(data)
	},
}

func init() {
	getBlockCmd.Flags().Uint64Var(&blockNonce, "nonce", 0, "Block nonce (height)")
	getBlockCmd.Flags().StringVar(&blockHash, "hash", "", "Block hash")
	getBlockCmd.Flags().BoolVar(&withTxs, "with-txs", false, "Include full transaction details")
	rootCmd.AddCommand(getBlockCmd)
}

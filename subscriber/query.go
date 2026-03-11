package subscriber

import (
	"context"
	"encoding/json"
)

type GetBlockParams struct {
	Nonce   *uint64 `json:"nonce,omitempty"`
	Hash    string  `json:"hash,omitempty"`
	WithTxs bool    `json:"withTxs,omitempty"`
}

func (s *Subscriber) GetTransaction(ctx context.Context, hash string, withResults bool) (json.RawMessage, error) {
	resp, err := s.sendRequest(ctx, "get_transaction", map[string]any{
		"hash":        hash,
		"withResults": withResults,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *Subscriber) GetBlock(ctx context.Context, params GetBlockParams) (json.RawMessage, error) {
	resp, err := s.sendRequest(ctx, "get_block", params)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

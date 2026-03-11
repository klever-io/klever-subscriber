package handler

import (
	"encoding/json"
	"net/http"

	"github.com/klever-io/klever-subscriber/subscriber"
)

type QueryHandler struct {
	sub *subscriber.Subscriber
}

func NewQueryHandler(sub *subscriber.Subscriber) *QueryHandler {
	return &QueryHandler{sub: sub}
}

type getTransactionRequest struct {
	Hash        string `json:"hash"`
	WithResults bool   `json:"withResults"`
}

func (h *QueryHandler) HandleGetTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req getTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Hash == "" {
		http.Error(w, "hash is required", http.StatusBadRequest)
		return
	}

	data, err := h.sub.GetTransaction(r.Context(), req.Hash, req.WithResults)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

type getBlockRequest struct {
	Nonce   *uint64 `json:"nonce"`
	Hash    string  `json:"hash"`
	WithTxs bool    `json:"withTxs"`
}

func (h *QueryHandler) HandleGetBlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req getBlockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Nonce == nil && req.Hash == "" {
		http.Error(w, "nonce or hash is required", http.StatusBadRequest)
		return
	}

	params := subscriber.GetBlockParams{
		Nonce:   req.Nonce,
		Hash:    req.Hash,
		WithTxs: req.WithTxs,
	}

	data, err := h.sub.GetBlock(r.Context(), params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

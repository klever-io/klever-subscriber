package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/klever-io/klever-subscriber/subscriber"
)

type SubscriptionHandler struct {
	sub *subscriber.Subscriber
}

func NewSubscriptionHandler(sub *subscriber.Subscriber) *SubscriptionHandler {
	return &SubscriptionHandler{sub: sub}
}

type subscriptionPayload struct {
	Types     []subscriber.EventType `json:"types"`
	Addresses []string               `json:"addresses"`
}

func (h *SubscriptionHandler) HandleSubscription(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		resp := subscriptionPayload{
			Types:     h.sub.Types(),
			Addresses: h.sub.Addresses(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)

	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var req subscriptionPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		valid := subscriber.ValidEventTypes()
		for _, t := range req.Types {
			if !valid[t] {
				http.Error(w, fmt.Sprintf("unknown event type %q", t), http.StatusBadRequest)
				return
			}
		}

		for _, addr := range req.Addresses {
			if len(addr) == 0 || len(addr) > 128 {
				http.Error(w, fmt.Sprintf("invalid address length: %q", addr), http.StatusBadRequest)
				return
			}
		}

		h.sub.Reconfigure(req.Types, req.Addresses)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(req)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *SubscriptionHandler) HandleDynamicSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req subscriptionPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	valid := subscriber.ValidEventTypes()
	for _, t := range req.Types {
		if !valid[t] {
			http.Error(w, fmt.Sprintf("unknown event type %q", t), http.StatusBadRequest)
			return
		}
	}

	if err := h.sub.AddSubscriptions(r.Context(), req.Types, req.Addresses); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subscriptionPayload{
		Types:     h.sub.Types(),
		Addresses: h.sub.Addresses(),
	})
}

func (h *SubscriptionHandler) HandleDynamicUnsubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req subscriptionPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	valid := subscriber.ValidEventTypes()
	for _, t := range req.Types {
		if !valid[t] {
			http.Error(w, fmt.Sprintf("unknown event type %q", t), http.StatusBadRequest)
			return
		}
	}

	if err := h.sub.RemoveSubscriptions(r.Context(), req.Types, req.Addresses); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subscriptionPayload{
		Types:     h.sub.Types(),
		Addresses: h.sub.Addresses(),
	})
}

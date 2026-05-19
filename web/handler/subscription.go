package handler

import (
	"encoding/json"
	"fmt"
	"io"
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

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid JSON: unexpected trailing data", http.StatusBadRequest)
		return false
	}
	return true
}

func validatePayload(req subscriptionPayload) error {
	valid := subscriber.ValidEventTypes()
	for _, t := range req.Types {
		if !valid[t] {
			return fmt.Errorf("unknown event type %q", t)
		}
	}
	for _, addr := range req.Addresses {
		if len(addr) == 0 || len(addr) > 128 {
			return fmt.Errorf("invalid address length: %q", addr)
		}
	}
	return nil
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
		var req subscriptionPayload
		if !decodeJSON(w, r, &req) {
			return
		}
		if err := validatePayload(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
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

	var req subscriptionPayload
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validatePayload(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

	var req subscriptionPayload
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validatePayload(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

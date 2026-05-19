package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func TestHandleDynamicSubscribe_MethodNotAllowed(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, nil)

	req := httptest.NewRequest(http.MethodGet, "/subscription/subscribe", nil)
	rec := httptest.NewRecorder()
	h.HandleDynamicSubscribe(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleDynamicSubscribe_InvalidType(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, nil)

	body := strings.NewReader(`{"types":["bad_type"],"addresses":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/subscription/subscribe", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleDynamicSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleDynamicUnsubscribe_MethodNotAllowed(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, nil)

	req := httptest.NewRequest(http.MethodGet, "/subscription/unsubscribe", nil)
	rec := httptest.NewRecorder()
	h.HandleDynamicUnsubscribe(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleDynamicUnsubscribe_InvalidType(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, nil)

	body := strings.NewReader(`{"types":["invalid"],"addresses":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/subscription/unsubscribe", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleDynamicUnsubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestHandleSubscription_FiresOnChange locks in the contract that the
// onChange callback runs after a successful POST /subscription, so SSE
// fan-out to other tabs cannot silently regress.
func TestHandleSubscription_FiresOnChange(t *testing.T) {
	var called atomic.Bool
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, func() { called.Store(true) })

	body := strings.NewReader(`{"types":["blocks"],"addresses":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/subscription", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleSubscription(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !called.Load() {
		t.Error("onChange not fired on successful POST /subscription")
	}
}

// TestHandleSubscription_NoOnChangeOnBadRequest ensures we do not fan out
// a non-mutation: a 400 reply must not invoke the callback.
func TestHandleSubscription_NoOnChangeOnBadRequest(t *testing.T) {
	var called atomic.Bool
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub, func() { called.Store(true) })

	body := strings.NewReader(`{"types":["bad_type"],"addresses":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/subscription", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleSubscription(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if called.Load() {
		t.Error("onChange fired despite 400 response")
	}
}

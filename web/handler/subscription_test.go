package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func TestHandleDynamicSubscribe_MethodNotAllowed(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub)

	req := httptest.NewRequest(http.MethodGet, "/subscription/subscribe", nil)
	rec := httptest.NewRecorder()
	h.HandleDynamicSubscribe(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleDynamicSubscribe_InvalidType(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub)

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
	h := NewSubscriptionHandler(sub)

	req := httptest.NewRequest(http.MethodGet, "/subscription/unsubscribe", nil)
	rec := httptest.NewRecorder()
	h.HandleDynamicUnsubscribe(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleDynamicUnsubscribe_InvalidType(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewSubscriptionHandler(sub)

	body := strings.NewReader(`{"types":["invalid"],"addresses":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/subscription/unsubscribe", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleDynamicUnsubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

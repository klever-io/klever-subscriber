package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klever-io/klever-subscriber/subscriber"
)

func TestHandleGetTransaction_MethodNotAllowed(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewQueryHandler(sub)

	req := httptest.NewRequest(http.MethodGet, "/api/transaction", nil)
	rec := httptest.NewRecorder()
	h.HandleGetTransaction(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleGetTransaction_MissingHash(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewQueryHandler(sub)

	body := strings.NewReader(`{"withResults": true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/transaction", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleGetTransaction(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleGetBlock_MethodNotAllowed(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewQueryHandler(sub)

	req := httptest.NewRequest(http.MethodGet, "/api/block", nil)
	rec := httptest.NewRecorder()
	h.HandleGetBlock(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleGetBlock_MissingParams(t *testing.T) {
	sub := subscriber.New("unused:8080", nil)
	h := NewQueryHandler(sub)

	body := strings.NewReader(`{"withTxs": true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/block", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.HandleGetBlock(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

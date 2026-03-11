package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/gorilla/websocket"
)

var (
	ErrNotConnected  = errors.New("not connected")
	ErrRequestFailed = errors.New("request failed")
)

type Request struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type Response struct {
	ID    string          `json:"id,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

func (s *Subscriber) sendRequest(ctx context.Context, method string, params any) (*Response, error) {
	id := strconv.FormatInt(s.nextID.Add(1), 10)
	req := Request{
		ID:     id,
		Method: method,
		Params: params,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	ch := make(chan *Response, 1)
	s.pendingMu.Lock()
	s.pending[id] = ch
	s.pendingMu.Unlock()

	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, id)
		s.pendingMu.Unlock()
	}()

	if err := s.writeMessage(websocket.TextMessage, payload); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		if resp.Error != "" {
			return nil, fmt.Errorf("%w: %s", ErrRequestFailed, resp.Error)
		}
		return resp, nil
	}
}

func (s *Subscriber) routeResponse(resp *Response) {
	s.pendingMu.Lock()
	ch, ok := s.pending[resp.ID]
	s.pendingMu.Unlock()
	if ok {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (s *Subscriber) failAllPending() {
	s.pendingMu.Lock()
	for id, ch := range s.pending {
		select {
		case ch <- &Response{ID: id, Error: "connection closed"}:
		default:
		}
	}
	s.pending = make(map[string]chan *Response)
	s.pendingMu.Unlock()
}

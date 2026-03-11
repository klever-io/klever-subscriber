package subscriber

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSubscriber_Reconfigure(t *testing.T) {
	var connCount atomic.Int32

	mock := &mockServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
	mock.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := mock.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req subscribeRequest
		json.Unmarshal(msg, &req)

		n := connCount.Add(1)
		var evt rawEvent
		if n == 1 {
			evt = makeEvent("blocks", "h1", `{"n":1}`)
		} else {
			evt = makeEvent("transactions", "tx1", `{"n":2}`)
		}

		data, _ := json.Marshal(evt)
		conn.WriteMessage(websocket.TextMessage, data)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var r struct {
				ID     string `json:"id"`
				Method string `json:"method"`
			}
			if json.Unmarshal(msg, &r) == nil && r.ID != "" {
				errResp, _ := json.Marshal(map[string]string{"id": r.ID, "error": "not supported"})
				conn.WriteMessage(websocket.TextMessage, errResp)
			}
		}
	}))
	defer mock.srv.Close()

	host := strings.TrimPrefix(mock.srv.URL, "http://")
	sub := New(host, []EventType{EventBlocks},
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go sub.Start(ctx)

	evt1 := <-sub.Events()
	if evt1.Type != EventBlocks {
		t.Errorf("first event type = %q, want blocks", evt1.Type)
	}

	sub.Reconfigure([]EventType{EventTransactions}, nil)

	evt2 := <-sub.Events()
	if evt2.Type != EventTransactions {
		t.Errorf("second event type = %q, want transactions", evt2.Type)
	}

	cancel()
}

func TestSubscriber_StartWithNoTypes(t *testing.T) {
	events := []rawEvent{
		makeEvent("blocks", "h1", `{}`),
	}
	mock := newMockServer(events)
	defer mock.close()

	sub := New(mock.host(), nil,
		WithReconnectInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go sub.Start(ctx)

	select {
	case <-sub.Events():
		t.Fatal("should not receive events before Reconfigure")
	case <-time.After(200 * time.Millisecond):
	}

	sub.Reconfigure([]EventType{EventBlocks}, nil)

	evt := <-sub.Events()
	if evt.Type != EventBlocks {
		t.Errorf("type = %q, want blocks", evt.Type)
	}
	cancel()
}

package subscriber

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestDecodeEvent(t *testing.T) {
	tests := []struct {
		name      string
		input     rawEvent
		wantType  EventType
		wantHash  string
		wantAddr  string
		wantData  any
		wantErr   bool
	}{
		{
			name: "valid base64 json data",
			input: rawEvent{
				Type: "blocks",
				Hash: "abc123",
				Data: base64.StdEncoding.EncodeToString([]byte(`{"height":100}`)),
			},
			wantType: EventBlocks,
			wantHash: "abc123",
			wantData: map[string]any{"height": float64(100)},
		},
		{
			name: "valid base64 non-json data",
			input: rawEvent{
				Type: "transactions",
				Hash: "tx123",
				Data: base64.StdEncoding.EncodeToString([]byte("plain text data")),
			},
			wantType: EventTransactions,
			wantHash: "tx123",
			wantData: "plain text data",
		},
		{
			name: "invalid base64 fallback to raw string",
			input: rawEvent{
				Type: "accounts",
				Hash: "acct1",
				Data: "not-valid-base64!!!",
			},
			wantType: EventAccounts,
			wantHash: "acct1",
			wantData: "not-valid-base64!!!",
		},
		{
			name: "empty data field",
			input: rawEvent{
				Type: "blocks",
				Hash: "h1",
				Data: "",
			},
			wantType: EventBlocks,
			wantHash: "h1",
			wantData: "",
		},
		{
			name: "with address",
			input: rawEvent{
				Type:    "user_transaction",
				Hash:    "utx1",
				Address: "klv1abc",
				Data:    base64.StdEncoding.EncodeToString([]byte(`{"amount":50}`)),
			},
			wantType: EventUserTransaction,
			wantHash: "utx1",
			wantAddr: "klv1abc",
			wantData: map[string]any{"amount": float64(50)},
		},
		{
			name: "base64 json array",
			input: rawEvent{
				Type: "blocks",
				Hash: "arr1",
				Data: base64.StdEncoding.EncodeToString([]byte(`[1,2,3]`)),
			},
			wantType: EventBlocks,
			wantHash: "arr1",
			wantData: []any{float64(1), float64(2), float64(3)},
		},
		{
			name: "nested json object",
			input: rawEvent{
				Type: "transactions",
				Hash: "nested1",
				Data: base64.StdEncoding.EncodeToString([]byte(`{"tx":{"sender":"klv1a","receiver":"klv1b"}}`)),
			},
			wantType: EventTransactions,
			wantHash: "nested1",
			wantData: map[string]any{
				"tx": map[string]any{
					"sender":   "klv1a",
					"receiver": "klv1b",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("marshal input: %v", err)
			}

			evt, err := DecodeEvent(msg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if evt.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", evt.Type, tt.wantType)
			}
			if evt.Hash != tt.wantHash {
				t.Errorf("Hash = %q, want %q", evt.Hash, tt.wantHash)
			}
			if evt.Address != tt.wantAddr {
				t.Errorf("Address = %q, want %q", evt.Address, tt.wantAddr)
			}

			gotJSON, _ := json.Marshal(evt.Data)
			wantJSON, _ := json.Marshal(tt.wantData)
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("Data = %s, want %s", gotJSON, wantJSON)
			}

			if len(evt.Raw) == 0 {
				t.Error("Raw should be set to the original message bytes")
			}
		})
	}
}

func TestDecodeEvent_MalformedJSON(t *testing.T) {
	_, err := DecodeEvent([]byte(`not json at all`))
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestDecodeEvent_EmptyMessage(t *testing.T) {
	_, err := DecodeEvent([]byte(``))
	if err == nil {
		t.Fatal("expected error for empty message, got nil")
	}
}

func TestValidEventTypes(t *testing.T) {
	valid := ValidEventTypes()

	expected := []EventType{EventBlocks, EventTransactions, EventUserTransaction, EventAccounts}
	for _, et := range expected {
		if !valid[et] {
			t.Errorf("expected %q to be valid", et)
		}
	}

	if valid[EventType("unknown")] {
		t.Error("expected 'unknown' to be invalid")
	}

	if len(valid) != len(expected) {
		t.Errorf("expected %d valid types, got %d", len(expected), len(valid))
	}
}

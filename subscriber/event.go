package subscriber

import (
	"encoding/json"
)

// EventType represents a subscribable event type.
type EventType string

const (
	EventBlocks           EventType = "blocks"
	EventTransactions     EventType = "transactions"
	EventUserTransactions EventType = "user_transactions"
	EventAccounts         EventType = "accounts"
)

// ValidEventTypes returns the set of known event types.
func ValidEventTypes() map[EventType]bool {
	return map[EventType]bool{
		EventBlocks:           true,
		EventTransactions:     true,
		EventUserTransactions: true,
		EventAccounts:         true,
	}
}

// Event is the decoded representation of a node event.
type Event struct {
	Type    EventType `json:"type"`
	Address string    `json:"address,omitempty"`
	Hash    string    `json:"hash,omitempty"`
	Data    any       `json:"data"`
	Raw     []byte    `json:"-"`
}

// rawEvent represents the wire format from the node.
type rawEvent struct {
	Type    string          `json:"type"`
	Address string          `json:"address"`
	Hash    string          `json:"hash"`
	Data    json.RawMessage `json:"data"`
}

// DecodeEvent decodes a raw WebSocket message into an Event. The message
// slice is copied so callers can hold Raw safely after the underlying
// read buffer is reused.
func DecodeEvent(message []byte) (Event, error) {
	var raw rawEvent
	if err := json.Unmarshal(message, &raw); err != nil {
		return Event{}, err
	}

	evt := Event{
		Type:    EventType(raw.Type),
		Address: raw.Address,
		Hash:    raw.Hash,
		Raw:     append([]byte(nil), message...),
	}

	var jsonData any
	if err := json.Unmarshal(raw.Data, &jsonData); err != nil {
		evt.Data = string(raw.Data)
	} else {
		evt.Data = jsonData
	}

	return evt, nil
}

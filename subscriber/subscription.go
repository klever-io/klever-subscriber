package subscriber

import (
	"context"
	"time"
)

func (s *Subscriber) Reconfigure(types []EventType, addresses []string) {
	s.mu.Lock()
	oldTypes := make([]EventType, len(s.types))
	copy(oldTypes, s.types)
	oldAddrs := make([]string, len(s.addresses))
	copy(oldAddrs, s.addresses)
	s.types = types
	s.addresses = addresses
	cancel := s.connCancel
	s.mu.Unlock()

	if s.connected.Load() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()

		addT := diffTypes(types, oldTypes)
		removeT := diffTypes(oldTypes, types)
		addA := diffStrings(addresses, oldAddrs)
		removeA := diffStrings(oldAddrs, addresses)

		ok := true
		if len(addT) > 0 || len(addA) > 0 {
			typeStrs := make([]string, len(addT))
			for i, t := range addT {
				typeStrs[i] = string(t)
			}
			_, err := s.sendRequest(ctx, "subscribe", map[string]any{
				"types":     typeStrs,
				"addresses": addA,
			})
			if err != nil {
				ok = false
			}
		}

		if ok && (len(removeT) > 0 || len(removeA) > 0) {
			typeStrs := make([]string, len(removeT))
			for i, t := range removeT {
				typeStrs[i] = string(t)
			}
			_, err := s.sendRequest(ctx, "unsubscribe", map[string]any{
				"types":     typeStrs,
				"addresses": removeA,
			})
			if err != nil {
				ok = false
			}
		}

		if ok {
			return
		}
	}

	if cancel != nil {
		cancel()
	}
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

func (s *Subscriber) AddSubscriptions(ctx context.Context, types []EventType, addresses []string) error {
	typeStrs := make([]string, len(types))
	for i, t := range types {
		typeStrs[i] = string(t)
	}
	addrs := addresses
	if addrs == nil {
		addrs = []string{}
	}
	_, err := s.sendRequest(ctx, "subscribe", map[string]any{
		"types":     typeStrs,
		"addresses": addrs,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.types = mergeTypes(s.types, types)
	s.addresses = mergeStrings(s.addresses, addresses)
	s.mu.Unlock()
	return nil
}

func (s *Subscriber) RemoveSubscriptions(ctx context.Context, types []EventType, addresses []string) error {
	typeStrs := make([]string, len(types))
	for i, t := range types {
		typeStrs[i] = string(t)
	}
	addrs := addresses
	if addrs == nil {
		addrs = []string{}
	}
	_, err := s.sendRequest(ctx, "unsubscribe", map[string]any{
		"types":     typeStrs,
		"addresses": addrs,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.types = removeTypes(s.types, types)
	s.addresses = removeStrings(s.addresses, addresses)
	s.mu.Unlock()
	return nil
}

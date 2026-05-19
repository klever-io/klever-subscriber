package subscriber

import (
	"context"
	"errors"
	"time"
)

func (s *Subscriber) Reconfigure(types []EventType, addresses []string) {
	s.reconfigureMu.Lock()
	defer s.reconfigureMu.Unlock()

	s.mu.RLock()
	oldTypes := append([]EventType(nil), s.types...)
	oldAddrs := append([]string(nil), s.addresses...)
	s.mu.RUnlock()

	if s.connected.Load() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()

		addT := diffTypes(types, oldTypes)
		removeT := diffTypes(oldTypes, types)
		addA := diffStrings(addresses, oldAddrs)
		removeA := diffStrings(oldAddrs, addresses)

		ok := true
		if len(addT) > 0 || len(addA) > 0 {
			typeStrs := make([]string, len(types))
			for i, t := range types {
				typeStrs[i] = string(t)
			}
			_, err := s.sendRequest(ctx, "subscribe", map[string]any{
				"types":     typeStrs,
				"addresses": addresses,
			})
			if err != nil {
				ok = false
			}
		}

		// Send removals with only the dimension that actually changed.
		// Mixing types and addresses in the same unsubscribe is ambiguous —
		// for global event types like "blocks" the node treats it as
		// "drop the type subscription entirely" and stops delivering them.
		if ok && len(removeA) > 0 {
			_, err := s.sendRequest(ctx, "unsubscribe", map[string]any{
				"addresses": removeA,
			})
			if err != nil {
				ok = false
			}
		}

		if ok && len(removeT) > 0 {
			removeTypeStrs := make([]string, len(removeT))
			for i, t := range removeT {
				removeTypeStrs[i] = string(t)
			}
			_, err := s.sendRequest(ctx, "unsubscribe", map[string]any{
				"types": removeTypeStrs,
			})
			if err != nil {
				ok = false
			}
		}

		if ok {
			s.mu.Lock()
			s.types = types
			s.addresses = addresses
			s.mu.Unlock()
			return
		}
	}

	// Slow path: persist the desired state and bounce the connection so
	// the next handshake carries the new subscription set.
	s.mu.Lock()
	s.types = types
	s.addresses = addresses
	cancel := s.connCancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

func (s *Subscriber) AddSubscriptions(ctx context.Context, types []EventType, addresses []string) error {
	s.reconfigureMu.Lock()
	defer s.reconfigureMu.Unlock()

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
		if errors.Is(err, ErrNotConnected) {
			s.mu.Lock()
			s.types = mergeTypes(s.types, types)
			s.addresses = mergeStrings(s.addresses, addresses)
			cancel := s.connCancel
			s.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			select {
			case s.reconnectCh <- struct{}{}:
			default:
			}
			return nil
		}
		return err
	}
	s.mu.Lock()
	s.types = mergeTypes(s.types, types)
	s.addresses = mergeStrings(s.addresses, addresses)
	s.mu.Unlock()
	return nil
}

func (s *Subscriber) RemoveSubscriptions(ctx context.Context, types []EventType, addresses []string) error {
	s.reconfigureMu.Lock()
	defer s.reconfigureMu.Unlock()

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
		if errors.Is(err, ErrNotConnected) {
			s.mu.Lock()
			s.types = removeTypes(s.types, types)
			s.addresses = removeStrings(s.addresses, addresses)
			s.mu.Unlock()
			return nil
		}
		return err
	}
	s.mu.Lock()
	s.types = removeTypes(s.types, types)
	s.addresses = removeStrings(s.addresses, addresses)
	s.mu.Unlock()
	return nil
}

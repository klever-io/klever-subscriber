package subscriber

import (
	"context"
	"errors"
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

		if ok && len(removeA) > 0 {
			oldTypeStrs := make([]string, len(oldTypes))
			for i, t := range oldTypes {
				oldTypeStrs[i] = string(t)
			}
			_, err := s.sendRequest(ctx, "unsubscribe", map[string]any{
				"types":     oldTypeStrs,
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
				"types":     removeTypeStrs,
				"addresses": oldAddrs,
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

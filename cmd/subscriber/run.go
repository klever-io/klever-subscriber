package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/klever-io/klever-subscriber/subscriber"
	"github.com/klever-io/klever-subscriber/web"
)

func run(eventTypes []subscriber.EventType) error {
	if webAddr != "" && len(eventTypes) == 0 {
		quiet = true
	}

	logf := func(format string, a ...any) {
		if !quiet {
			fmt.Fprintf(os.Stderr, format, a...)
		}
	}

	scheme := "ws"
	if useWss {
		scheme = "wss"
	}

	var opts []subscriber.Option
	opts = append(opts, subscriber.WithScheme(scheme))
	if len(addresses) > 0 {
		opts = append(opts, subscriber.WithAddresses(addresses))
	}
	opts = append(opts, subscriber.WithOnConnect(func() {
		logf("Connected, waiting for events...\n")
	}))
	opts = append(opts, subscriber.WithOnDisconnect(func() {
		logf("Disconnected\n")
	}))
	opts = append(opts, subscriber.WithOnError(func(err error) {
		logf("Error: %v\n", err)
		logf("Reconnecting in %s...\n", subscriber.DefaultReconnectInterval)
	}))

	sub := subscriber.New(nodeURL, eventTypes, opts...)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logf("Connecting to %s\n", sub.URL())
	if len(types) > 0 {
		logf("Subscribed types: %s\n", strings.Join(types, ", "))
	}
	if len(addresses) > 0 {
		logf("Watching addresses: %s\n", strings.Join(addresses, ", "))
	}

	if webAddr != "" {
		srv := web.NewServer(webAddr, sub)
		go func() {
			if err := srv.Start(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Web server error: %v\n", err)
				cancel()
			}
		}()
		fmt.Fprintf(os.Stderr, "Web dashboard at http://%s\n", webAddr)
		if len(eventTypes) == 0 {
			fmt.Fprintln(os.Stderr, "No event types specified — configure via dashboard")
		}
	}

	logf("Press Ctrl+C to exit\n")
	logf("---\n")

	go sub.Start(ctx)

	if quiet {
		<-ctx.Done()
	} else {
		for evt := range sub.Events() {
			if raw {
				if len(evt.Raw) > 0 {
					fmt.Println(string(evt.Raw))
				}
				continue
			}

			var output []byte
			var err error
			if pretty {
				output, err = json.MarshalIndent(evt, "", "  ")
			} else {
				output, err = json.Marshal(evt)
			}
			if err != nil {
				if len(evt.Raw) > 0 {
					fmt.Println(string(evt.Raw))
				}
				continue
			}
			fmt.Println(string(output))
		}
	}

	logf("\nShutting down...\n")
	return nil
}

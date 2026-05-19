# klever-subscriber

A reference implementation of a Klever node WebSocket client. Ships as:

- a **Go library** (`subscriber` package) for embedding in your own service
- a **CLI** for streaming events to stdout
- an optional **local web dashboard** for inspecting what the subscriber is doing

This project is intended as a starting point and debugging aid for teams building on
Klever. The dashboard is single-tenant by design — it reflects the live state of one
subscriber process, with no per-user filtering or authentication. Embed the
`subscriber` package in your service when you need that kind of isolation; see
[`examples/`](./examples) for common patterns.

## Installation

```bash
go install github.com/klever-io/klever-subscriber/cmd/subscriber@latest
```

Or build from source:

```bash
git clone https://github.com/klever-io/klever-subscriber.git
cd klever-subscriber
make build
```

The binary will be at `bin/subscriber`.

## CLI Usage

```bash
subscriber --types <event_types> [flags]
```

### Event Types

| Type               | Description                              |
|--------------------|------------------------------------------|
| `blocks`           | New block events                         |
| `transactions`     | All transaction events                   |
| `user_transaction` | Address-specific transaction events      |
| `accounts`         | Address-specific account update events   |

### Flags

| Flag          | Default          | Description                                       |
|---------------|------------------|---------------------------------------------------|
| `--node`      | `localhost:8080` | Node API address (host:port)                      |
| `--types`     | *(required)*     | Event types to subscribe to (comma-separated)     |
| `--addresses` |                  | Addresses to watch (for user_transaction/accounts) |
| `--wss`       | `false`          | Use `wss://` instead of `ws://`                   |
| `--pretty`    | `false`          | Pretty-print JSON output                          |
| `--raw`       | `false`          | Print raw messages without decoding base64 data   |
| `--web`       |                  | Start web dashboard on the given address           |

### Examples

Subscribe to block events:

```bash
subscriber --types blocks
```

Subscribe to transactions for specific addresses:

```bash
subscriber --types user_transaction,accounts --addresses klv1abc...,klv1def...
```

Connect to a custom node with pretty output:

```bash
subscriber --node 10.0.0.1:8080 --types blocks --pretty
```

Use secure WebSocket:

```bash
subscriber --wss --node node.klever.io:443 --types blocks
```

Start with the web dashboard:

```bash
subscriber --types blocks --web :3000
```

Then open `http://localhost:3000` in your browser.

## Web Dashboard

When started with `--web :3000`, the subscriber serves a live dashboard that shows:

- **Connection status** — green/red badge
- **Stats bar** — events/sec, total count, per-type counters
- **Type filters** — toggle visibility of each event type
- **Event list** — scrolling list with JSON syntax highlighting

The dashboard uses Server-Sent Events (SSE) for real-time streaming and requires no external dependencies.

## Library Usage

See [`examples/`](./examples) for runnable patterns (streaming, reconnect
handling, one-shot queries). A minimal embed looks like this:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os/signal"
    "syscall"

    "github.com/klever-io/klever-subscriber/subscriber"
)

func main() {
    sub := subscriber.New("localhost:8080",
        []subscriber.EventType{subscriber.EventBlocks},
        subscriber.WithScheme("wss"),
        subscriber.WithAddresses([]string{"klv1abc..."}),
        subscriber.WithOnConnect(func() { log.Println("connected") }),
    )

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
    defer cancel()

    go sub.Start(ctx)

    for evt := range sub.Events() {
        fmt.Println(evt.Type, evt.Hash)
    }
}
```

### Key API

| Function / Method             | Description                                       |
|-------------------------------|---------------------------------------------------|
| `subscriber.New(host, types, ...Option)` | Create a new subscriber                |
| `sub.Start(ctx)`             | Connect and stream events (blocks until cancelled) |
| `sub.Events()`               | Pre-registered event channel                       |
| `sub.Subscribe()`            | Create an independent event channel (fan-out)      |
| `sub.Connected()`            | Whether the WebSocket is currently connected       |
| `sub.URL()`                  | The full WebSocket URL                             |

### Options

| Option                        | Description                         |
|-------------------------------|-------------------------------------|
| `WithScheme("wss")`          | Use `wss://` instead of `ws://`     |
| `WithAddresses([]string{})` | Filter events by address            |
| `WithReconnectInterval(d)`   | Delay between reconnection attempts |
| `WithPingInterval(d)`        | WebSocket ping frequency            |
| `WithOnConnect(fn)`          | Callback on connection              |
| `WithOnDisconnect(fn)`       | Callback on disconnection           |
| `WithOnError(fn)`            | Callback on errors                  |

## How It Works

1. Connects to the node's `/subscribe` WebSocket endpoint
2. Sends a subscription request with the specified event types and addresses
3. Receives events and decodes the base64-encoded `data` field into readable JSON
4. Fans out events to all registered consumers (CLI stdout, web dashboard, custom subscribers)
5. Automatically reconnects on connection loss
6. Sends periodic pings to keep the connection alive through proxies

## License

MIT

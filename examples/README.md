# Examples

Four minimal programs showing how to embed the `subscriber` package.

| Directory | Pattern |
|---|---|
| [`stream/`](./stream) | Subscribe to events and process them in a loop |
| [`reconnect/`](./reconnect) | Same, with connect/disconnect/error callbacks wired up |
| [`query/`](./query) | Use the subscriber as a one-shot request client (no event subscription) |
| [`wait-for-event/`](./wait-for-event) | Subscribe and wait for a specific block (by nonce/hash) or transaction (by hash) |

Each example is a standalone `main.go`. Run from the repo root:

```bash
go run ./examples/stream
go run ./examples/reconnect
go run ./examples/query --hash <tx-hash>
go run ./examples/wait-for-event --block-nonce 12345
go run ./examples/wait-for-event --tx-hash <tx-hash>
```

All examples default to `localhost:8080` over plain `ws://`. Override with the
`KLEVER_NODE` and `KLEVER_WSS` environment variables:

```bash
KLEVER_NODE=node.testnet.klever.org:443 KLEVER_WSS=1 go run ./examples/stream
```

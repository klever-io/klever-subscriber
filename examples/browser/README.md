# Browser WebSocket Example

A vanilla-JS demo that talks **directly** to a Klever node's `/subscribe`
WebSocket endpoint from the browser. No server, no build step, no
dependencies.

## Files

| File | What it is | Copy into your project? |
|---|---|---|
| `klever-subscriber.js` | **Reusable client class** with a small, JSDoc'd API: `connect`, `request`, `getBlock`, `getTransaction`, `on(event)`, `close`. ~140 lines. | **Yes** — this is the part you'd drop into your dApp/game. |
| `app.js` | Demo glue: DOM manipulation, stats counters, tab switching. One possible consumer of `klever-subscriber.js`. | No — write your own UI on top of the library. |
| `style.css` | Demo's visual styles. | No. |
| `index.html` | Markup only — loads the two scripts. | No. |

## Run

```bash
# Open directly:
open examples/browser/index.html

# Or serve over HTTP:
python3 -m http.server 8000
# then http://localhost:8000/examples/browser/
```

Use `wss://` instead of `ws://` when the page is served over HTTPS — browsers
block mixed content.

## The 30-second copy-paste

`await` only works at the top level inside an ES module — wrap in an async
IIFE for a classic script tag, or use `<script type="module">`.

```html
<script src="klever-subscriber.js"></script>
<script>
  (async () => {
    const sub = new KleverSubscriber("ws://localhost:8080/subscribe");

    sub.on("event", (evt) => console.log(evt.type, evt));
    sub.on("open",  () => console.log("connected"));
    sub.on("close", () => console.log("disconnected"));
    sub.on("error", (msg) => console.warn(msg));

    await sub.connect({ types: ["blocks"], addresses: [] });

    // One-shot request over the same socket:
    const block = await sub.getBlock({ nonce: 12345 });
    console.log(block);
  })();
</script>
```

## Caveats consumers should know

- **`connect()` resolves on the WebSocket handshake, not on a subscribe ack.**
  If the server rejects the subscribe payload it'll close shortly after; listen
  for `"close"` / `"error"` to detect that.
- **No automatic reconnect.** If the socket closes, call `connect()` again —
  wrap in a backoff loop if you need resilience.
- **JSON pretty-print is depth-capped at 32** to defend against pathological
  payloads. Raw access via `evt` is still unbounded.

## What this demonstrates

- The exact wire format of the subscribe handshake
  (`{addresses: […], subscribed_types: […]}`).
- Receiving streamed events and pulling hashes out of payloads (the shapes
  differ — block events have a single `data` object with lowercase keys;
  transaction events have `data` as an **array** of txs).
- **Request / response over the same WebSocket** — `get_block` and
  `get_transaction` requests share the socket with the event stream. Send a
  request with an `id`, match the response by `id`.
- **Live subscribe / unsubscribe** on the open socket — the "Subscribe more"
  and "Unsubscribe" buttons send in-band `subscribe` / `unsubscribe` requests
  using the current form values, so you can mutate the active subscription
  without dropping the connection. See `KleverSubscriber.subscribeMore()` and
  `unsubscribe()` in `klever-subscriber.js`.
- The **subscribe-then-query** pattern for "wait for this block/tx to appear":
  query first to catch the already-on-chain case, then keep listening on the
  subscription for the future case.

## Wire-format notes for dApp builders

- **Subscribe must include at least one type.** The node rejects an empty
  `subscribed_types` (`"subscribed_types must not be empty"`) and closes the
  connection. If you only want request/response, send a placeholder subscribe
  like `{ types: ["accounts"], addresses: [] }` — `accounts` is
  address-scoped, so with no addresses configured it delivers zero events
  but the connection stays open.
- **Block events** carry `data` as a single object with **lowercase** keys
  (`data.hash`, `data.nonce`). This is different from the `get_block`
  response, which uses the chain's internal CamelCase (`data.Header.Nonce`).
- **Transaction events** carry `data` as an **array** of objects, one per
  transaction in the block. Each entry has `hash`, `blockNum`, `sender`,
  etc.
- The top-level `evt.hash` field is empty for blocks and transactions; pull
  hashes from `data.hash` (blocks) or iterate `data[].hash` (txs).
- **`unsubscribe` semantics are dimension-sensitive.** Sending
  `unsubscribe {types: ["blocks"], addresses: ["klv1…"]}` is treated by the
  node as "drop the `blocks` subscription entirely" — the address filter is
  effectively ignored for global event types. To remove an address without
  killing a type subscription, send only the `addresses` field; to remove a
  type, send only `types`. `klever-subscriber.js`'s `unsubscribe()` splits a
  mixed call into two requests to preserve the right semantics.

## Manual test checklist

Run through these against a local Klever node before publishing changes to
the example or the library:

- [ ] Connect with `blocks` checked — block events arrive in the live feed.
- [ ] Connect with `blocks` unchecked — page rejects with "select at least
      one event type" (the node rejects empty subscribed_types).
- [ ] "Subscribe more" with `transactions` checked → tx events start
      arriving without reconnect.
- [ ] "Unsubscribe" with only `transactions` checked → tx events stop, but
      `blocks` continue (verifies the dimension-split fix).
- [ ] Lookup tab: `get_block {"nonce": 1}` returns the block JSON.
- [ ] Lookup tab: `get_block {}` (empty params) — page shows a clear
      "nonce or hash is required" error before sending.
- [ ] Wait-for-event tab: a known-existing block nonce returns immediately
      via the upfront query.
- [ ] Wait-for-event tab: a future tx hash matches via the live
      subscription when broadcast.
- [ ] Kill the node mid-session — UI flips to "disconnected", no console
      errors thrown.

## Why this is separate from the Go examples

The Go examples in `examples/stream`, `examples/reconnect`, `examples/query`,
and `examples/wait-for-event` embed the `subscriber` package. This one is
intentionally **plain-vanilla browser JS** so frontend developers can drop
`klever-subscriber.js` into any page and have it work — no toolchain.

The wire protocol is the same on both sides, so anything you learn here
transfers directly to the Go side and vice versa.

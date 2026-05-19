/**
 * KleverSubscriber — minimal browser client for the Klever node's
 * WebSocket subscription endpoint.
 *
 * Two responsibilities on a single connection:
 *   1. Streaming event subscription (blocks, transactions, …).
 *   2. JSON-RPC-style request/response (get_block, get_transaction).
 *
 * Usage:
 *   const sub = new KleverSubscriber("ws://localhost:8080/subscribe");
 *   sub.on("event", (e) => console.log(e.type, e));
 *   sub.on("error", (msg) => console.warn(msg));
 *   await sub.connect({ types: ["blocks"], addresses: [] });
 *   const block = await sub.request("get_block", { nonce: 12345 });
 *   sub.close();
 *
 * No build step, no dependencies. Drop this file into your project and
 * include it with <script src="klever-subscriber.js"></script>.
 *
 * Caveats / limitations consumers should know:
 *   - connect() resolves on the WebSocket handshake, NOT on the node's
 *     subscribe ack. If the server rejects the subscribe payload it'll
 *     close the socket shortly after; listen for "close" / "error" to
 *     detect that.
 *   - No automatic reconnect. If the socket closes, call connect() again.
 *     Wrap in a backoff loop if you need resilience.
 */
const _KLEVER_EVENT_NAMES = ["event", "open", "close", "error"];

class KleverSubscriber {
  /**
   * @param {string} url Full WebSocket URL, e.g. "ws://node:8080/subscribe".
   * @param {object} [opts]
   * @param {number} [opts.requestTimeoutMs=30000] Per-request timeout.
   */
  constructor(url, opts = {}) {
    this.url = url;
    this.requestTimeoutMs = opts.requestTimeoutMs || 30000;
    this._ws = null;
    this._listeners = { event: [], open: [], close: [], error: [] };
    this._pending = new Map(); // id -> {resolve, reject, timer}
    this._nextId = 0;
  }

  /**
   * Open the WebSocket and send the initial subscribe frame.
   *
   * Note: the node rejects an empty `subscribed_types` and closes the
   * connection. If you only want request/response, pass a placeholder
   * (e.g. types: ["accounts"], addresses: []) — address-scoped types
   * deliver no events without addresses.
   *
   * @param {object} opts
   * @param {string[]} opts.types Event types: blocks | transactions | user_transactions | accounts.
   * @param {string[]} [opts.addresses] Addresses to filter on.
   * @returns {Promise<void>} resolves when the socket is open.
   */
  connect({ types, addresses = [] }) {
    if (!types || types.length === 0) {
      return Promise.reject(new Error("types must be non-empty"));
    }
    return new Promise((resolve, reject) => {
      let ws;
      try {
        ws = new WebSocket(this.url);
      } catch (e) {
        reject(e);
        return;
      }
      this._ws = ws;

      ws.onopen = () => {
        ws.send(JSON.stringify({ addresses, subscribed_types: types }));
        this._emit("open");
        resolve();
      };

      ws.onmessage = (e) => this._onMessage(e.data);

      ws.onerror = () => {
        this._emit("error", "websocket error (check URL and that the node is reachable)");
      };

      ws.onclose = () => {
        this._failPending(new Error("connection closed"));
        this._ws = null;
        this._emit("close");
      };
    });
  }

  /** Close the underlying WebSocket. Pending requests reject. */
  close() {
    if (this._ws) this._ws.close();
  }

  /** @returns {boolean} true once the WebSocket handshake has completed. */
  isOpen() {
    return !!this._ws && this._ws.readyState === WebSocket.OPEN;
  }

  /**
   * Register an event handler. Valid names:
   *   on("event", (evt) => ...)   per node-stream event (has .type)
   *   on("open",  () => ...)      socket opened
   *   on("close", () => ...)      socket closed (also if it never opened)
   *   on("error", (msg) => ...)   transport error or server error frame
   *
   * Throws on an unknown event name — typos like on("evnet", …) would
   * otherwise register a handler that never fires.
   */
  on(name, fn) {
    if (!_KLEVER_EVENT_NAMES.includes(name)) {
      throw new Error(
        `unknown event "${name}" (valid: ${_KLEVER_EVENT_NAMES.join(", ")})`
      );
    }
    this._listeners[name].push(fn);
  }

  off(name, fn) {
    const arr = this._listeners[name];
    if (!arr) return;
    const i = arr.indexOf(fn);
    if (i >= 0) arr.splice(i, 1);
  }

  /**
   * Send a JSON-RPC-style request over the same socket and resolve with
   * the matching response payload.
   *
   * @param {string} method Method name the node understands (e.g. "get_block").
   * @param {object} params Parameter object.
   * @returns {Promise<any>} Response `data` field on success.
   *                          Rejects with Error on server error or timeout.
   */
  request(method, params) {
    return new Promise((resolve, reject) => {
      if (!this.isOpen()) {
        reject(new Error("not connected"));
        return;
      }
      const id = String(++this._nextId);
      const timer = setTimeout(() => {
        if (this._pending.has(id)) {
          this._pending.delete(id);
          reject(new Error("request timed out"));
        }
      }, this.requestTimeoutMs);
      this._pending.set(id, { resolve, reject, timer });
      this._ws.send(JSON.stringify({ id, method, params }));
    });
  }

  // --- convenience wrappers ---

  /** @param {{nonce?: number, hash?: string, withTxs?: boolean}} params */
  getBlock(params = {}) {
    if (params.nonce == null && !params.hash) {
      return Promise.reject(new Error("getBlock: nonce or hash is required"));
    }
    return this.request("get_block", params);
  }

  /** @param {string} hash @param {boolean} [withResults] */
  getTransaction(hash, withResults = false) {
    if (!hash) return Promise.reject(new Error("getTransaction: hash is required"));
    return this.request("get_transaction", { hash, withResults });
  }

  /**
   * Add more types and/or addresses to the active subscription without
   * dropping the connection. Additive — does not remove anything.
   *
   * @param {{types?: string[], addresses?: string[]}} params
   * @returns {Promise<any>} node ack payload
   */
  subscribeMore({ types = [], addresses = [] } = {}) {
    if (types.length === 0 && addresses.length === 0) {
      return Promise.reject(new Error("subscribeMore: types or addresses must be non-empty"));
    }
    return this.request("subscribe", { types, addresses });
  }

  /**
   * Remove types and/or addresses from the active subscription.
   *
   * The Klever node treats `unsubscribe {types: [t], addresses: [a]}` as
   * "remove type t entirely" for global event types like `blocks`. To avoid
   * killing a type subscription while only meaning to drop an address, this
   * method splits a mixed call into two separate requests — one for the
   * address removal and one for the type removal.
   *
   * @param {{types?: string[], addresses?: string[]}} params
   * @returns {Promise<any[]>} array of node ack payloads, in [addresses, types] order.
   */
  unsubscribe({ types = [], addresses = [] } = {}) {
    if (types.length === 0 && addresses.length === 0) {
      return Promise.reject(new Error("unsubscribe: types or addresses must be non-empty"));
    }
    const ops = [];
    if (addresses.length) ops.push(this.request("unsubscribe", { addresses }));
    if (types.length)     ops.push(this.request("unsubscribe", { types }));
    return Promise.all(ops);
  }

  // --- internals ---

  _onMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // Response to a request we issued.
    if (msg && msg.id && this._pending.has(msg.id)) {
      const { resolve, reject, timer } = this._pending.get(msg.id);
      this._pending.delete(msg.id);
      clearTimeout(timer);
      if (msg.error) reject(new Error(msg.error));
      else resolve(msg.data);
      return;
    }

    // Top-level server error (no .id, no .type).
    if (msg && msg.error && !msg.type) {
      this._emit("error", msg.error);
      return;
    }

    // Otherwise it's a streamed event.
    if (msg && msg.type) {
      this._emit("event", msg);
    }
  }

  _emit(name, payload) {
    const arr = this._listeners[name];
    if (!arr) return;
    for (const fn of arr) {
      try { fn(payload); } catch (e) { console.error(e); }
    }
  }

  _failPending(err) {
    for (const { reject, timer } of this._pending.values()) {
      clearTimeout(timer);
      try { reject(err); } catch (e) { console.error(e); }
    }
    this._pending.clear();
  }
}

// Expose the class for both classic <script> tags and CommonJS-style
// consumers. For ES modules, replace this block with
// `export { KleverSubscriber };` and load via <script type="module">.
if (typeof window !== "undefined") window.KleverSubscriber = KleverSubscriber;
if (typeof module !== "undefined" && module.exports) module.exports = { KleverSubscriber };

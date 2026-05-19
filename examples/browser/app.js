// Demo glue — wires KleverSubscriber (klever-subscriber.js) to the page UI.
// This file is intentionally NOT what you'd copy into your dApp; it's just
// one possible consumer of the client class.

(function () {
  "use strict";

  const MAX_EVENTS = 200;
  const MAX_JSON_DEPTH = 32;
  const KNOWN_TYPES = ["blocks", "transactions", "user_transactions", "accounts"];

  // ---------- element refs ----------
  const $ = (id) => document.getElementById(id);
  const urlEl = $("url");
  const addrsEl = $("addresses");
  const connectBtn = $("connect-btn");
  const disconnectBtn = $("disconnect-btn");
  const subMoreBtn = $("sub-more-btn");
  const unsubBtn = $("unsub-btn");
  const mutationStatusEl = $("mutation-status");
  const statusDot = $("status-dot");
  const statusText = $("status-text");
  const wireEl = $("wire");
  const feedEl = $("feed");

  // ---------- state ----------
  let sub = null;             // current KleverSubscriber instance
  let waiter = null;          // {tryMatch, succeed, fail, cancel} or null
  const counts = { total: 0, blocks: 0, transactions: 0, user_transactions: 0, accounts: 0 };
  const recentTimestamps = [];

  // ---------- tabs ----------
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => {
        t.classList.remove("active");
        t.setAttribute("aria-selected", "false");
      });
      document.querySelectorAll(".pane").forEach((p) => {
        p.classList.remove("active");
        p.hidden = true;
      });
      tab.classList.add("active");
      tab.setAttribute("aria-selected", "true");
      const pane = $("pane-" + tab.dataset.tab);
      pane.classList.add("active");
      pane.hidden = false;
    });
  });

  // ---------- helpers ----------
  function setStatus(state, label) {
    statusDot.className = "dot " + (state || "");
    statusText.textContent = label;
  }
  function selectedTypes() {
    return [...document.querySelectorAll('.checks input[type="checkbox"]:checked')].map((cb) => cb.value);
  }
  function parsedAddresses() {
    return addrsEl.value.split(",").map((s) => s.trim()).filter(Boolean);
  }
  // pretty serializes obj as indented JSON with a recursion cap so a
  // pathological node payload can't OOM the tab.
  function pretty(obj) {
    try {
      return JSON.stringify(capDepth(obj, MAX_JSON_DEPTH), null, 2);
    } catch {
      return String(obj);
    }
  }

  function capDepth(value, depth) {
    if (depth <= 0) return "…";
    if (value === null || typeof value !== "object") return value;
    if (Array.isArray(value)) return value.map((v) => capDepth(v, depth - 1));
    const out = {};
    for (const k of Object.keys(value)) out[k] = capDepth(value[k], depth - 1);
    return out;
  }
  function badgeClass(t) { return KNOWN_TYPES.includes(t) ? t : ""; }
  function clearChildren(el) { while (el.firstChild) el.removeChild(el.firstChild); }

  // ---------- stats panel ----------
  function recordEvent(type) {
    counts.total++;
    if (counts[type] !== undefined) counts[type]++;
    const now = Date.now();
    recentTimestamps.push(now);
    const cutoff = now - 10000;
    while (recentTimestamps.length && recentTimestamps[0] < cutoff) recentTimestamps.shift();
    renderStats();
  }
  function renderStats() {
    $("stat-total").textContent = counts.total;
    $("stat-rate").textContent = (recentTimestamps.length / 10).toFixed(1);
    $("stat-blocks").textContent = counts.blocks;
    $("stat-txs").textContent = counts.transactions;
    $("stat-utxs").textContent = counts.user_transactions;
    $("stat-accts").textContent = counts.accounts;
  }

  // ---------- event feed ----------
  function setFeedPlaceholder(text) {
    clearChildren(feedEl);
    const div = document.createElement("div");
    div.className = "event muted";
    div.textContent = text;
    feedEl.appendChild(div);
  }
  setFeedPlaceholder("Connect to start receiving events.");
  $("clear-feed").addEventListener("click", () => setFeedPlaceholder("Cleared."));

  function appendEvent(evt) {
    const first = feedEl.firstChild;
    if (first && first.classList && first.classList.contains("muted")) clearChildren(feedEl);

    const div = document.createElement("div");
    div.className = "event";

    const typ = String(evt.type || "unknown");
    const badge = document.createElement("span");
    badge.className = "badge " + badgeClass(typ);
    badge.textContent = typ;
    div.appendChild(badge);

    const hash = extractHash(evt);
    if (hash) {
      const hashSpan = document.createElement("span");
      hashSpan.className = "muted";
      hashSpan.textContent = hash;
      div.appendChild(hashSpan);
    }

    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "view payload";
    details.appendChild(summary);
    const pre = document.createElement("pre");
    pre.textContent = pretty(evt);
    details.appendChild(pre);
    div.appendChild(details);

    feedEl.insertBefore(div, feedEl.firstChild);
    while (feedEl.children.length > MAX_EVENTS) feedEl.removeChild(feedEl.lastChild);
  }

  // Block events keep .data as an object; transactions events keep .data as
  // an array of {hash, …}. user_transactions / accounts surface a top-level
  // hash. Try each shape in turn. For tx arrays, show the first hash + a
  // count so a 100-tx block doesn't render a kilobyte-long row.
  function extractHash(evt) {
    if (evt.hash) return evt.hash;
    if (evt.data && typeof evt.data === "object") {
      if (Array.isArray(evt.data)) {
        const hashes = evt.data.map((e) => e && e.hash).filter(Boolean);
        if (hashes.length === 0) return "";
        if (hashes.length === 1) return hashes[0];
        return `${hashes[0]} (+${hashes.length - 1} more)`;
      }
      if (evt.data.hash) return evt.data.hash;
    }
    return "";
  }

  // ---------- connect / disconnect ----------
  connectBtn.addEventListener("click", connect);
  disconnectBtn.addEventListener("click", () => sub && sub.close());
  subMoreBtn.addEventListener("click", () => mutate("subscribe"));
  unsubBtn.addEventListener("click", () => mutate("unsubscribe"));

  function setMutationButtons(enabled) {
    subMoreBtn.disabled = !enabled;
    unsubBtn.disabled = !enabled;
  }

  async function connect() {
    const url = urlEl.value.trim();
    if (!url) return;
    const types = selectedTypes();
    if (types.length === 0) {
      setStatus("err", "select at least one event type");
      return;
    }
    const addresses = parsedAddresses();
    wireEl.textContent = pretty({ addresses, subscribed_types: types });

    setStatus("warn", "connecting…");
    connectBtn.disabled = true;
    disconnectBtn.disabled = false;

    sub = new KleverSubscriber(url);
    sub.on("open", () => {
      setStatus("ok", "connected");
      setMutationButtons(true);
    });
    sub.on("error", (msg) => setStatus("err", String(msg)));
    sub.on("close", () => {
      setStatus("", "disconnected");
      connectBtn.disabled = false;
      disconnectBtn.disabled = true;
      setMutationButtons(false);
      sub = null;
    });
    sub.on("event", (evt) => {
      recordEvent(evt.type);
      appendEvent(evt);
      if (waiter) waiter.tryMatch(evt);
    });

    try {
      await sub.connect({ types, addresses });
    } catch (err) {
      setStatus("err", err.message);
    }
  }

  // Send an in-band subscribe/unsubscribe using the current form values.
  // Subscribe is additive; unsubscribe is subtractive. The form is just a
  // transient delta — to see what's currently subscribed on the node side,
  // check the server logs or use the dashboard.
  async function mutate(action) {
    if (!sub || !sub.isOpen()) return;
    const types = selectedTypes();
    const addresses = parsedAddresses();
    if (types.length === 0 && addresses.length === 0) {
      mutationStatusEl.textContent = "select a type or address first";
      mutationStatusEl.className = "err-text";
      return;
    }

    wireEl.textContent = pretty({ method: action, params: { types, addresses } });
    mutationStatusEl.textContent = action + " sending…";
    mutationStatusEl.className = "muted";
    setMutationButtons(false);
    try {
      if (action === "subscribe") await sub.subscribeMore({ types, addresses });
      else await sub.unsubscribe({ types, addresses });
      mutationStatusEl.textContent = action + " ok";
    } catch (err) {
      mutationStatusEl.textContent = action + " failed: " + err.message;
      mutationStatusEl.className = "err-text";
    } finally {
      setMutationButtons(sub && sub.isOpen());
    }
  }

  // ---------- lookup tab ----------
  $("lookup-btn").addEventListener("click", async () => {
    const method = $("lookup-method").value;
    const statusEl = $("lookup-status");
    const resultEl = $("lookup-result");

    let params;
    try {
      params = JSON.parse($("lookup-params").value.trim() || "{}");
    } catch (e) {
      statusEl.textContent = "params: " + e.message;
      statusEl.className = "err-text";
      return;
    }

    if (!sub || !sub.isOpen()) {
      statusEl.textContent = "not connected";
      statusEl.className = "err-text";
      return;
    }

    statusEl.textContent = "sending…";
    statusEl.className = "muted";
    resultEl.classList.add("hidden");

    try {
      const data = await sub.request(method, params);
      statusEl.textContent = "ok";
      resultEl.classList.remove("hidden");
      resultEl.textContent = pretty(data);
    } catch (err) {
      statusEl.textContent = err.message;
      statusEl.className = "err-text";
      resultEl.classList.remove("hidden");
      resultEl.textContent = err.message;
    }
  });

  // ---------- wait-for-event tab ----------
  $("wait-btn").addEventListener("click", () => {
    const kind = $("wait-target").value;
    const value = $("wait-value").value.trim();
    if (!value) return;
    if (!sub || !sub.isOpen()) {
      $("wait-status").textContent = "not connected";
      $("wait-status").className = "err-text";
      return;
    }
    startWaiter(kind, value);
  });

  function startWaiter(kind, value) {
    if (waiter) waiter.cancel();

    const resultEl = $("wait-result");
    const statusEl = $("wait-status");
    resultEl.classList.add("hidden");
    statusEl.textContent = "looking up…";
    statusEl.className = "muted";

    const deadline = setTimeout(() => waiter && waiter.fail(new Error("timed out (2m)")), 120000);
    waiter = {
      tryMatch(evt) { if (matches(evt, kind, value)) waiter.succeed(evt); },
      succeed(data) {
        clearTimeout(deadline);
        waiter = null;
        statusEl.textContent = "matched";
        statusEl.className = "muted";
        resultEl.classList.remove("hidden");
        resultEl.textContent = pretty(data);
      },
      fail(err) {
        clearTimeout(deadline);
        waiter = null;
        statusEl.textContent = err.message;
        statusEl.className = "err-text";
      },
      cancel() { clearTimeout(deadline); waiter = null; },
    };

    // Subscribe-then-query: ask the node if it already has the target so we
    // don't sit listening for an event that already happened.
    // Capture the current waiter so a stale lookup result can't resolve a
    // newer waiter created by a rapid second click.
    const myWaiter = waiter;
    const lookup =
      kind === "block-nonce" ? sub.getBlock({ nonce: Number(value) }) :
      kind === "block-hash"  ? sub.getBlock({ hash: value }) :
                               sub.getTransaction(value);

    lookup.then(
      (data) => { if (waiter === myWaiter) myWaiter.succeed(data); },
      () => { /* not found — keep listening on the subscription */ }
    );
  }

  function matches(evt, kind, value) {
    const want = String(value).toLowerCase();
    if (kind === "tx-hash") {
      if (evt.type !== "transactions") return false;
      if (evt.hash && evt.hash.toLowerCase() === want) return true;
      if (Array.isArray(evt.data)) {
        return evt.data.some((e) => e && e.hash && e.hash.toLowerCase() === want);
      }
      return false;
    }
    if (kind === "block-hash") {
      return evt.type === "blocks" && evt.data && evt.data.hash && evt.data.hash.toLowerCase() === want;
    }
    if (kind === "block-nonce") {
      return evt.type === "blocks" && evt.data && Number(evt.data.nonce) === Number(value);
    }
    return false;
  }
})();

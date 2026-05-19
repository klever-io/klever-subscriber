(function () {
  "use strict";

  var MAX_EVENTS = 500;
  var MAX_JSON_DEPTH = 32;
  var KNOWN_TYPES = {
    blocks: true,
    transactions: true,
    user_transactions: true,
    accounts: true,
  };

  function safeTypeClass(t) {
    return KNOWN_TYPES[t] ? t : "";
  }

  var statusDot = document.getElementById("status-dot");
  var statusText = document.getElementById("status");
  var statusDotStats = document.getElementById("status-dot-stats");
  var statusStats = document.getElementById("status-stats");
  var nodeUrlEl = document.getElementById("node-url");
  var epsEl = document.getElementById("eps");
  var totalEl = document.getElementById("total");
  var eventListEl = document.getElementById("event-list");
  var applyBtn = document.getElementById("sub-apply");
  var addBtn = document.getElementById("sub-add");
  var removeBtn = document.getElementById("sub-remove");
  var addrInput = document.getElementById("sub-addr");
  var activeTypesEl = document.getElementById("active-types");
  var activeAddrsEl = document.getElementById("active-addresses");
  var configJsonEl = document.getElementById("config-json");
  var clearEventsBtn = document.getElementById("clear-events-btn");
  var copyConfigBtn = document.getElementById("copy-config-btn");
  var refreshBtn = document.getElementById("refresh-btn");

  var countEls = {
    blocks: document.getElementById("count-blocks"),
    transactions: document.getElementById("count-transactions"),
    user_transactions: document.getElementById("count-user_transactions"),
    accounts: document.getElementById("count-accounts"),
  };

  var filters = {
    blocks: true,
    transactions: true,
    user_transactions: true,
    accounts: true,
  };

  var serverTypes = [];
  var serverAddresses = [];

  var toolTabs = document.querySelectorAll(".tool-tab");
  var panes = document.querySelectorAll(".pane");

  toolTabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      toolTabs.forEach(function (t) { t.classList.remove("active"); });
      panes.forEach(function (p) { p.classList.remove("active"); });
      tab.classList.add("active");
      document.getElementById("pane-" + tab.dataset.tab).classList.add("active");
    });
  });

  var subCheckboxes = document.querySelectorAll('input[name="sub-type"]');

  function getSelectedTypes() {
    var types = [];
    subCheckboxes.forEach(function (cb) {
      if (cb.checked) types.push(cb.value);
    });
    return types;
  }

  function getAddresses() {
    var val = addrInput.value.trim();
    if (!val) return [];
    return val
      .split(",")
      .map(function (a) { return a.trim(); })
      .filter(function (a) { return a.length > 0; });
  }

  function checkDirty() {
    var types = getSelectedTypes();
    var addrs = getAddresses();
    var dirty =
      JSON.stringify(types) !== JSON.stringify(serverTypes) ||
      JSON.stringify(addrs) !== JSON.stringify(serverAddresses);
    var hasSelection = types.length > 0 || addrs.length > 0;
    applyBtn.disabled = !dirty;
    addBtn.disabled = !hasSelection;
    removeBtn.disabled = !hasSelection;
  }

  subCheckboxes.forEach(function (cb) {
    cb.addEventListener("change", checkDirty);
  });
  addrInput.addEventListener("input", checkDirty);

  function escapeHTML(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function updateConfigDisplay() {
    var config = { types: serverTypes, addresses: serverAddresses };
    configJsonEl.innerHTML = highlightJSON(config);
  }

  function updateSubscriptionDisplay() {
    if (serverTypes.length === 0) {
      activeTypesEl.innerHTML = '<div class="empty-msg">None</div>';
    } else {
      activeTypesEl.innerHTML = serverTypes
        .map(function (t) {
          return '<div class="card-item"><span class="card-dot ' + safeTypeClass(t) + '"></span><span class="card-label">' + escapeHTML(t) + "</span></div>";
        })
        .join("");
    }

    if (serverAddresses.length === 0) {
      activeAddrsEl.innerHTML = '<div class="empty-msg">None</div>';
    } else {
      activeAddrsEl.innerHTML = serverAddresses
        .map(function (a) {
          return '<div class="card-item"><span class="card-label mono">' + escapeHTML(a) + "</span></div>";
        })
        .join("");
    }

    updateConfigDisplay();
  }

  function syncCheckboxes() {
    subCheckboxes.forEach(function (cb) {
      cb.checked = serverTypes.indexOf(cb.value) !== -1;
    });
    addrInput.value = serverAddresses.join(", ");
  }

  function handleSubscriptionResponse(resp) {
    serverTypes = resp.types || [];
    serverAddresses = resp.addresses || [];
    syncCheckboxes();
    checkDirty();
    updateSubscriptionDisplay();
    if (serverTypes.length > 0) {
      var empty = eventListEl.querySelector(".empty-state");
      if (empty) empty.textContent = "Waiting for events\u2026";
    }
  }

  applyBtn.addEventListener("click", function () {
    applyBtn.disabled = true;
    fetch("/subscription", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ types: getSelectedTypes(), addresses: getAddresses() }),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(handleSubscriptionResponse)
      .catch(function (err) {
        console.error("subscription error:", err);
        checkDirty();
      });
  });

  addBtn.addEventListener("click", function () {
    addBtn.disabled = true;
    fetch("/subscription/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ types: getSelectedTypes(), addresses: getAddresses() }),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(handleSubscriptionResponse)
      .catch(function (err) {
        console.error("subscribe error:", err);
        checkDirty();
      });
  });

  removeBtn.addEventListener("click", function () {
    removeBtn.disabled = true;
    fetch("/subscription/unsubscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ types: getSelectedTypes(), addresses: getAddresses() }),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(handleSubscriptionResponse)
      .catch(function (err) {
        console.error("unsubscribe error:", err);
        checkDirty();
      });
  });

  function loadSubscription() {
    fetch("/subscription")
      .then(function (r) { return r.json(); })
      .then(function (s) {
        serverTypes = s.types || [];
        serverAddresses = s.addresses || [];
        syncCheckboxes();
        updateSubscriptionDisplay();
        if (serverTypes.length > 0) {
          var empty = eventListEl.querySelector(".empty-state");
          if (empty) empty.textContent = "Waiting for events\u2026";
        }
        checkDirty();
      })
      .catch(function () {});
  }

  refreshBtn.addEventListener("click", function () {
    loadSubscription();
  });

  clearEventsBtn.addEventListener("click", function () {
    eventListEl.innerHTML = '<div class="empty-state">No events.</div>';
  });

  copyConfigBtn.addEventListener("click", function () {
    var config = JSON.stringify({ types: serverTypes, addresses: serverAddresses }, null, 2);
    navigator.clipboard.writeText(config).catch(function () {});
  });

  document.querySelectorAll(".copy-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var targetId = btn.dataset.target;
      var el = document.getElementById(targetId);
      if (el) {
        var pre = el.querySelector("pre");
        if (pre) navigator.clipboard.writeText(pre.textContent).catch(function () {});
      }
    });
  });

  document.getElementById("query-tx-btn").addEventListener("click", function () {
    var hash = document.getElementById("query-tx-hash").value.trim();
    if (!hash) return;
    var withRes = document.getElementById("query-tx-results").checked;
    var resultEl = document.getElementById("query-tx-result");

    resultEl.className = "col-body result-body loading";
    resultEl.textContent = "Loading\u2026";

    fetch("/api/transaction", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ hash: hash, withResults: withRes }),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(function (data) {
        resultEl.className = "col-body result-body";
        resultEl.innerHTML = "<pre>" + highlightJSON(data) + "</pre>";
      })
      .catch(function (err) {
        resultEl.className = "col-body result-body error";
        resultEl.textContent = err.message;
      });
  });

  document.getElementById("query-block-btn").addEventListener("click", function () {
    var nonceVal = document.getElementById("query-block-nonce").value.trim();
    var hashVal = document.getElementById("query-block-hash").value.trim();
    if (!nonceVal && !hashVal) return;
    var withT = document.getElementById("query-block-txs").checked;

    var payload = { withTxs: withT };
    if (nonceVal) payload.nonce = parseInt(nonceVal, 10);
    if (hashVal) payload.hash = hashVal;

    var resultEl = document.getElementById("query-block-result");
    resultEl.className = "col-body result-body loading";
    resultEl.textContent = "Loading\u2026";

    fetch("/api/block", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(function (data) {
        resultEl.className = "col-body result-body";
        resultEl.innerHTML = "<pre>" + highlightJSON(data) + "</pre>";
      })
      .catch(function (err) {
        resultEl.className = "col-body result-body error";
        resultEl.textContent = err.message;
      });
  });

  document.querySelectorAll("#filters input").forEach(function (cb) {
    cb.addEventListener("change", function () {
      filters[this.value] = this.checked;
      applyFilters();
    });
  });

  function applyFilters() {
    var items = eventListEl.querySelectorAll(".event-item");
    items.forEach(function (el) {
      el.classList.toggle("hidden", !filters[el.dataset.type]);
    });
  }

  function highlightJSON(obj, indent) {
    if (indent === undefined) indent = 0;
    if (indent > MAX_JSON_DEPTH) return '<span class="j-null">…</span>';
    var pad = "  ".repeat(indent);
    if (obj === null) return '<span class="j-null">null</span>';
    if (typeof obj === "boolean")
      return '<span class="j-bool">' + obj + "</span>";
    if (typeof obj === "number")
      return '<span class="j-num">' + obj + "</span>";
    if (typeof obj === "string")
      return '<span class="j-str">"' + escapeHTML(obj) + '"</span>';

    if (Array.isArray(obj)) {
      if (obj.length === 0) return "[]";
      var items = obj.map(function (v) {
        return pad + "  " + highlightJSON(v, indent + 1);
      });
      return "[\n" + items.join(",\n") + "\n" + pad + "]";
    }

    var keys = Object.keys(obj);
    if (keys.length === 0) return "{}";
    var entries = keys.map(function (k) {
      return (
        pad +
        '  <span class="j-key">"' +
        escapeHTML(k) +
        '"</span>: ' +
        highlightJSON(obj[k], indent + 1)
      );
    });
    return "{\n" + entries.join(",\n") + "\n" + pad + "}";
  }

  function normalizeEvent(evt) {
    if (typeof evt.data === "string") {
      try {
        evt.data = JSON.parse(evt.data);
      } catch (e) {}
    }

    if (
      evt.data &&
      typeof evt.data === "object" &&
      evt.data.type &&
      evt.data.data !== undefined
    ) {
      if (!evt.type) evt.type = evt.data.type;
      if (!evt.hash && evt.data.hash) evt.hash = evt.data.hash;
      if (!evt.address && evt.data.address) evt.address = evt.data.address;
      evt.data = evt.data.data;
    }

    if (typeof evt.data === "string") {
      try {
        evt.data = JSON.parse(evt.data);
      } catch (e) {}
    }

    return evt;
  }

  function addEvent(evt) {
    evt = normalizeEvent(evt);

    var empty = eventListEl.querySelector(".empty-state");
    if (empty) empty.remove();

    var div = document.createElement("div");
    div.className = "event-item";
    div.dataset.type = evt.type || "";

    var header = document.createElement("div");
    header.className = "event-header";

    var badge =
      '<span class="ev-badge ' +
      safeTypeClass(evt.type) +
      '">' +
      escapeHTML(evt.type || "unknown") +
      "</span>";
    var hashSpan = "";
    if (evt.hash) {
      hashSpan = '<span class="ev-hash">' + escapeHTML(evt.hash) + "</span>";
    }
    var arrow = '<span class="ev-arrow">&#9654;</span>';

    header.innerHTML =
      badge + hashSpan + '<span class="ev-spacer"></span>' + arrow;
    div.appendChild(header);

    if (evt.data !== undefined && evt.data !== null) {
      var body = document.createElement("div");
      body.className = "event-body";
      body.innerHTML = "<pre>" + highlightJSON(evt.data) + "</pre>";
      div.appendChild(body);
    }

    header.addEventListener("click", function () {
      div.classList.toggle("expanded");
    });

    if (!filters[evt.type]) {
      div.classList.add("hidden");
    }

    eventListEl.insertBefore(div, eventListEl.firstChild);

    while (eventListEl.children.length > MAX_EVENTS) {
      eventListEl.removeChild(eventListEl.lastChild);
    }
  }

  function setStatus(connected) {
    var label = connected ? "Connected" : "Disconnected";
    var cls = "status-dot " + (connected ? "connected" : "disconnected");
    statusText.textContent = label;
    statusDot.className = cls;
    statusDotStats.className = cls;
    statusStats.textContent = label;
  }

  function applyStats(s) {
    if (!s) return;
    setStatus(!!s.connected);
    epsEl.textContent = (s.eventsPerSec || 0).toFixed(1);
    totalEl.textContent = s.total || 0;

    if (s.url) {
      nodeUrlEl.textContent = s.url;
      nodeUrlEl.title = s.url;
    }

    var types = ["blocks", "transactions", "user_transactions", "accounts"];
    types.forEach(function (t) {
      if (countEls[t]) {
        countEls[t].textContent = (s.counts && s.counts[t]) || 0;
      }
    });
  }

  function connectSSE() {
    var es = new EventSource("/events");

    // Don't preemptively claim "Connected" on SSE open — the broker
    // sends an initial stats frame within milliseconds that reflects
    // the real upstream-node state (which is what the user cares about).

    es.onmessage = function (e) {
      try {
        addEvent(JSON.parse(e.data));
      } catch (err) {}
    };

    es.addEventListener("stats", function (e) {
      try {
        applyStats(JSON.parse(e.data));
      } catch (err) {}
    });

    es.addEventListener("subscription", function (e) {
      try {
        handleSubscriptionResponse(JSON.parse(e.data));
      } catch (err) {}
    });

    es.onerror = function () {
      // SSE dropped — we no longer know the node state, so reflect
      // "Disconnected" across both the top badge and the Stats panel.
      setStatus(false);
    };
  }

  loadSubscription();
  connectSSE();
})();

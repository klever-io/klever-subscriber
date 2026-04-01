(function () {
  "use strict";

  var MAX_EVENTS = 500;

  var statusEl = document.getElementById("status");
  var epsEl = document.getElementById("eps");
  var totalEl = document.getElementById("total");
  var eventListEl = document.getElementById("event-list");
  var applyBtn = document.getElementById("sub-apply");
  var addBtn = document.getElementById("sub-add");
  var removeBtn = document.getElementById("sub-remove");
  var addrInput = document.getElementById("sub-addr");

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

  eventListEl.innerHTML =
    '<div class="empty-state">Configure subscription above to start receiving events.</div>';

  // --- Subscription panel ---

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
    applyBtn.disabled = !dirty;
    addBtn.disabled = !dirty;
    removeBtn.disabled = !dirty;
  }

  subCheckboxes.forEach(function (cb) {
    cb.addEventListener("change", checkDirty);
  });
  addrInput.addEventListener("input", checkDirty);

  applyBtn.addEventListener("click", function () {
    applyBtn.disabled = true;
    var payload = {
      types: getSelectedTypes(),
      addresses: getAddresses(),
    };

    fetch("/subscription", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(function (resp) {
        serverTypes = resp.types || [];
        serverAddresses = resp.addresses || [];
        syncCheckboxes();
        checkDirty();
        if (serverTypes.length > 0) {
          var empty = eventListEl.querySelector(".empty-state");
          if (empty) empty.textContent = "Waiting for events...";
        }
      })
      .catch(function (err) {
        console.error("subscription error:", err);
        checkDirty();
      });
  });

  addBtn.addEventListener("click", function () {
    addBtn.disabled = true;
    var payload = {
      types: getSelectedTypes(),
      addresses: getAddresses(),
    };

    fetch("/subscription/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(function (resp) {
        serverTypes = resp.types || [];
        serverAddresses = resp.addresses || [];
        syncCheckboxes();
        checkDirty();
        if (serverTypes.length > 0) {
          var empty = eventListEl.querySelector(".empty-state");
          if (empty) empty.textContent = "Waiting for events...";
        }
      })
      .catch(function (err) {
        console.error("subscribe error:", err);
        checkDirty();
      });
  });

  removeBtn.addEventListener("click", function () {
    removeBtn.disabled = true;
    var payload = {
      types: getSelectedTypes(),
      addresses: getAddresses(),
    };

    fetch("/subscription/unsubscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
        return r.json();
      })
      .then(function (resp) {
        serverTypes = resp.types || [];
        serverAddresses = resp.addresses || [];
        syncCheckboxes();
        checkDirty();
      })
      .catch(function (err) {
        console.error("unsubscribe error:", err);
        checkDirty();
      });
  });

  function syncCheckboxes() {
    subCheckboxes.forEach(function (cb) {
      cb.checked = serverTypes.indexOf(cb.value) !== -1;
    });
    addrInput.value = serverAddresses.join(", ");
  }

  function loadSubscription() {
    fetch("/subscription")
      .then(function (r) { return r.json(); })
      .then(function (s) {
        serverTypes = s.types || [];
        serverAddresses = s.addresses || [];
        syncCheckboxes();

        if (serverTypes.length > 0) {
          var empty = eventListEl.querySelector(".empty-state");
          if (empty) empty.textContent = "Waiting for events...";
        }

        checkDirty();
      })
      .catch(function () {});
  }

  // --- Query panel ---

  var tabBtns = document.querySelectorAll(".tab-btn");
  var queryTabs = document.querySelectorAll(".query-tab");
  var queryResult = document.getElementById("query-result");

  tabBtns.forEach(function (btn) {
    btn.addEventListener("click", function () {
      tabBtns.forEach(function (b) { b.classList.remove("active"); });
      queryTabs.forEach(function (t) { t.classList.remove("active"); });
      btn.classList.add("active");
      document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
      queryResult.style.display = "none";
    });
  });

  document.getElementById("query-tx-btn").addEventListener("click", function () {
    var hash = document.getElementById("query-tx-hash").value.trim();
    if (!hash) return;
    var withRes = document.getElementById("query-tx-results").checked;

    queryResult.style.display = "block";
    queryResult.className = "query-result loading";
    queryResult.textContent = "Loading...";

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
        queryResult.className = "query-result";
        queryResult.innerHTML = '<pre>' + highlightJSON(data) + '</pre>';
      })
      .catch(function (err) {
        queryResult.className = "query-result error";
        queryResult.textContent = err.message;
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

    queryResult.style.display = "block";
    queryResult.className = "query-result loading";
    queryResult.textContent = "Loading...";

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
        queryResult.className = "query-result";
        queryResult.innerHTML = '<pre>' + highlightJSON(data) + '</pre>';
      })
      .catch(function (err) {
        queryResult.className = "query-result error";
        queryResult.textContent = err.message;
      });
  });

  // --- Display filter checkboxes ---

  document.querySelectorAll("#filters input").forEach(function (cb) {
    cb.addEventListener("change", function () {
      filters[this.value] = this.checked;
      applyFilters();
    });
  });

  function applyFilters() {
    var items = eventListEl.querySelectorAll(".event-item");
    items.forEach(function (el) {
      var evtType = el.dataset.type;
      el.style.display = filters[evtType] ? "" : "none";
    });
  }

  // --- JSON syntax highlighting ---

  function highlightJSON(obj, indent) {
    if (indent === undefined) indent = 0;
    var pad = "  ".repeat(indent);
    if (obj === null) return '<span class="json-null">null</span>';
    if (typeof obj === "boolean")
      return '<span class="json-bool">' + obj + "</span>";
    if (typeof obj === "number")
      return '<span class="json-number">' + obj + "</span>";
    if (typeof obj === "string")
      return '<span class="json-string">"' + escapeHTML(obj) + '"</span>';

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
        '  <span class="json-key">"' +
        escapeHTML(k) +
        '"</span>: ' +
        highlightJSON(obj[k], indent + 1)
      );
    });
    return "{\n" + entries.join(",\n") + "\n" + pad + "}";
  }

  function escapeHTML(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function normalizeEvent(evt) {
    // If data is a JSON string, parse it into an object.
    if (typeof evt.data === "string") {
      try {
        evt.data = JSON.parse(evt.data);
      } catch (e) {
        // keep as-is if not valid JSON
      }
    }

    // If the parsed data contains a nested structure with type/data,
    // lift the fields up (e.g. { type: "", data: { type: "blocks", data: {...} } }).
    if (evt.data && typeof evt.data === "object" && evt.data.type && evt.data.data !== undefined) {
      if (!evt.type) evt.type = evt.data.type;
      if (!evt.hash && evt.data.hash) evt.hash = evt.data.hash;
      if (!evt.address && evt.data.address) evt.address = evt.data.address;
      evt.data = evt.data.data;
    }

    // If data is still a JSON string after first parse, parse again.
    if (typeof evt.data === "string") {
      try {
        evt.data = JSON.parse(evt.data);
      } catch (e) {
        // keep as-is
      }
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

    var typeSpan =
      '<span class="event-type ' +
      escapeHTML(evt.type || "") +
      '">' +
      escapeHTML(evt.type || "unknown") +
      "</span>";

    var hashSpan = "";
    if (evt.hash) {
      hashSpan =
        '<span class="event-hash">' + escapeHTML(evt.hash) + "</span>";
    }

    var dataHtml = "";
    if (evt.data !== undefined && evt.data !== null) {
      dataHtml =
        '<span class="event-data">' + highlightJSON(evt.data) + "</span>";
    }

    div.innerHTML = typeSpan + hashSpan + dataHtml;

    if (!filters[evt.type]) {
      div.style.display = "none";
    }

    eventListEl.insertBefore(div, eventListEl.firstChild);

    while (eventListEl.children.length > MAX_EVENTS) {
      eventListEl.removeChild(eventListEl.lastChild);
    }
  }

  // --- SSE connection ---

  function connectSSE() {
    var es = new EventSource("/events");

    es.onopen = function () {
      statusEl.textContent = "Connected";
      statusEl.className = "badge connected";
    };

    es.onmessage = function (e) {
      try {
        var evt = JSON.parse(e.data);
        addEvent(evt);
      } catch (err) {
        // Ignore parse errors.
      }
    };

    es.onerror = function () {
      statusEl.textContent = "Disconnected";
      statusEl.className = "badge disconnected";
    };
  }

  // --- Poll stats ---

  function pollStats() {
    fetch("/stats")
      .then(function (r) { return r.json(); })
      .then(function (s) {
        if (s.connected) {
          statusEl.textContent = "Connected";
          statusEl.className = "badge connected";
        } else if (serverTypes.length > 0) {
          statusEl.textContent = "Disconnected";
          statusEl.className = "badge disconnected";
        }
        epsEl.textContent = (s.eventsPerSec || 0).toFixed(1);
        totalEl.textContent = s.total || 0;
        var types = ["blocks", "transactions", "user_transactions", "accounts"];
        types.forEach(function (t) {
          if (countEls[t]) {
            countEls[t].textContent = (s.counts && s.counts[t]) || 0;
          }
        });
      })
      .catch(function () {});
  }

  // --- Init ---

  loadSubscription();
  connectSSE();
  pollStats();
  setInterval(pollStats, 2000);
})();

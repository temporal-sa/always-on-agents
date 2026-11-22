// Briefing Assistant frontend. Plain ES module, no build step.
// Every view reads live state from the server, which reads it from Temporal
// (Workflow queries, Activity and Schedule descriptions) and the app's store.

const SAFE_ID = /^[A-Za-z0-9_-]{1,48}$/;
const FAULT_POINTS = ["model_call", "after_model_call", "write_report", "deliver", "publish_memory", "provision_vm", "snapshot_workspace"];
const SUGGESTIONS = [
  'Send me "hello" in 30 seconds',
  "Prepare my briefing in 2 minutes",
  "Send me my briefing every Monday at 8am",
  "Share my latest report with dana@example.com",
  "Next time, lead with the decisions that need my attention.",
];

const state = {
  user: localStorage.getItem("aoa.user") || "alice",
  config: null,
  cleanup: [],
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const $ = (sel, root = document) => root.querySelector(sel);

// Like node.replaceChildren(), but skips null/false (the DOM would print them as text).
function fill(node, ...children) {
  node.replaceChildren(...children.flat(Infinity).filter((c) => c != null && c !== false));
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value == null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "html") node.innerHTML = value; // only for markdown(), which escapes its input
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (key in node && typeof value !== "string") node[key] = value;
    else node.setAttribute(key, value === true ? "" : value);
  }
  for (const child of children.flat(Infinity)) {
    if (child == null || child === false) continue;
    node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

async function api(path, { method = "GET", body } = {}) {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { detail: text };
  }
  if (!res.ok) {
    const detail = data?.detail;
    throw new Error(typeof detail === "string" ? detail : detail ? JSON.stringify(detail) : res.statusText);
  }
  return data;
}

function toast(message, kind = "") {
  const node = el("div", { class: `toast ${kind}` }, message);
  $("#toasts").append(node);
  setTimeout(() => node.remove(), kind === "bad" ? 6000 : 3500);
}

function every(ms, fn) {
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    try {
      await fn();
    } catch (err) {
      console.warn(err);
    }
  };
  tick();
  const id = setInterval(tick, ms);
  state.cleanup.push(() => {
    stopped = true;
    clearInterval(id);
  });
}

const onCleanup = (fn) => state.cleanup.push(fn);
const uid = () => (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2)).replace(/-/g, "").slice(0, 12);

function fmtTime(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  const sameDay = d.toDateString() === new Date().toDateString();
  return sameDay
    ? d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })
    : d.toLocaleString([], { weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function ago(iso) {
  if (!iso) return "";
  const s = Math.round((Date.now() - Date.parse(iso)) / 1000);
  if (s < 60) return `${Math.max(s, 0)}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
}

function countdown(ms) {
  if (ms <= 0) return "0:00";
  const total = Math.ceil(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = String(total % 60).padStart(2, "0");
  return h ? `${h}:${String(m).padStart(2, "0")}:${s}` : `${m}:${s}`;
}

function wfLink(workflowId, label = "Open in Temporal UI") {
  const c = state.config;
  if (!c) return null;
  const href = `${c.temporal_ui}/namespaces/${encodeURIComponent(c.namespace)}/workflows/${encodeURIComponent(workflowId)}`;
  return el("a", { href, target: "_blank", rel: "noopener" }, `${label} ↗`);
}

function badge(text, kind = "", { pulse = false } = {}) {
  return el("span", { class: `badge ${kind}` }, pulse ? el("span", { class: "dot pulse" }) : null, text);
}

function statusBadge(status) {
  const map = {
    RUNNING: ["Running", "info", true],
    COMPLETED: ["Completed", "ok"],
    FAILED: ["Failed", "bad"],
    TERMINATED: ["Terminated", "bad"],
    CANCELED: ["Canceled", "warn"],
    TIMED_OUT: ["Timed out", "bad"],
    CONTINUED_AS_NEW: ["Continued", "accent"],
  };
  const [text, kind, pulse] = map[status] || [status || "–", ""];
  return badge(text, kind, { pulse });
}

function card(title, ...children) {
  const head = Array.isArray(title)
    ? el("div", { class: "card-head" }, el("h2", {}, title[0]), title[1])
    : el("div", { class: "card-head" }, el("h2", {}, title));
  return el("section", { class: "card" }, head, ...children);
}

function pageHead(title, text, right = null) {
  return el("div", { class: "page-head" }, el("div", {}, el("h1", {}, title), el("p", {}, text)), right);
}

// Small, safe Markdown renderer for reports and replies: escapes first.
function inline(text) {
  return esc(text)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|[^*\w])\*([^*\s][^*]*?)\*(?!\*)/g, "$1<em>$2</em>");
}

function markdown(src) {
  const lines = String(src ?? "").replace(/\r/g, "").split("\n");
  let html = "";
  let para = [];
  const lists = [];
  const flush = () => {
    if (para.length) html += `<p>${inline(para.join(" "))}</p>`;
    para = [];
  };
  const close = (indent = -1) => {
    while (lists.length && lists.at(-1).indent > indent) html += `</li></${lists.pop().type}>`;
  };
  for (const raw of lines) {
    if (!raw.trim()) {
      flush();
      close();
      continue;
    }
    const heading = raw.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      flush();
      close();
      const level = Math.min(heading[1].length, 3);
      html += `<h${level}>${inline(heading[2])}</h${level}>`;
      continue;
    }
    const item = raw.match(/^(\s*)([-*]|\d+\.)\s+(.*)$/);
    if (item) {
      flush();
      const indent = item[1].replace(/\t/g, "  ").length;
      const type = /\d/.test(item[2]) ? "ol" : "ul";
      close(indent);
      if (lists.length && lists.at(-1).indent === indent) html += "</li><li>";
      else {
        html += `<${type}><li>`;
        lists.push({ type, indent });
      }
      html += inline(item[3]);
      continue;
    }
    if (lists.length) {
      html += " " + inline(raw.trim());
      continue;
    }
    para.push(raw.trim());
  }
  flush();
  close();
  return html;
}

// ---------------------------------------------------------------------------
// shell
// ---------------------------------------------------------------------------

const VIEWS = {
  assistant: AssistantView,
  briefings: BriefingsView,
  memory: MemoryView,
  workspace: WorkspaceView,
  operations: OperationsView,
};

function route() {
  state.cleanup.splice(0).forEach((fn) => fn());
  const name = VIEWS[location.hash.slice(1)] ? location.hash.slice(1) : "assistant";
  document.querySelectorAll(".nav a").forEach((a) => a.classList.toggle("active", a.dataset.view === name));
  const root = $("#view");
  fill(root);
  root.scrollTop = 0;
  VIEWS[name](root);
}

function renderEnv() {
  const c = state.config;
  const env = $("#env");
  if (!c) {
    fill(env, el("div", { class: "row" }, badge("Server unreachable", "bad")));
    return;
  }
  const model = badge(`Claude · ${c.claude_model}`, "accent");
  fill(env, 
    el("div", { class: "row" }, model),
    el("div", { class: "row" }, "Namespace ", el("code", {}, c.namespace)),
    el(
      "div",
      { class: "row" },
      el("a", { href: `${c.temporal_ui}/namespaces/${encodeURIComponent(c.namespace)}/workflows`, target: "_blank", rel: "noopener" }, "Temporal UI ↗"),
    ),
  );
}

async function boot() {
  const userInput = $("#user");
  userInput.value = state.user;
  userInput.addEventListener("change", () => {
    const value = userInput.value.trim();
    if (!SAFE_ID.test(value)) {
      toast("User ids are letters, digits, '-' or '_'.", "bad");
      userInput.value = state.user;
      return;
    }
    state.user = value;
    localStorage.setItem("aoa.user", value);
    route();
  });
  try {
    state.config = await api("/api/config");
  } catch (err) {
    toast(`Cannot reach the server: ${err.message}`, "bad");
  }
  renderEnv();
  window.addEventListener("hashchange", route);
  route();
}

// ---------------------------------------------------------------------------
// Assistant: one session Workflow, streamed over SSE
// ---------------------------------------------------------------------------

function AssistantView(root) {
  const user = state.user;
  const storageKey = `aoa.session.${user}`;
  let sessionId = localStorage.getItem(storageKey);
  let source = null;
  let lastSeq = 0;
  let connState = "off";
  let status = null;
  const approvals = new Map();
  const pending = new Map();

  root.append(
    pageHead(
      "Assistant",
      "Chat with the agent. The session is one Temporal Workflow: messages are accepted right away, actions that reach other people wait for your approval, and closing this page stops nothing.",
    ),
  );

  const typingText = el("span", {}, "Working…");
  const typing = el("div", { class: "typing", hidden: true }, el("span", { class: "dots" }, el("i"), el("i"), el("i")), typingText);
  const log = el("div", { class: "chat-log", role: "log", "aria-live": "polite" }, typing);
  const input = el("textarea", { placeholder: "Message the assistant…  (Enter to send, Shift+Enter for a new line)", rows: 2, "aria-label": "Message" });
  const mode = el(
    "select",
    { "aria-label": "How to deliver this message", title: "If a turn is already running, a message can join it or wait for the next turn" },
    el("option", { value: "auto" }, "Start or join a turn"),
    el("option", { value: "queue" }, "Queue for the next turn"),
  );
  const sendBtn = el("button", { class: "btn primary", onclick: send }, "Send");
  const conn = el("span", { class: "conn" });
  const suggestions = el(
    "div",
    { class: "chips" },
    SUGGESTIONS.map((s) => el("button", { class: "chip", onclick: () => ((input.value = s), input.focus()) }, s)),
  );
  const side = el("aside", { class: "stack" });
  root.append(
    el(
      "div",
      { class: "chat-wrap" },
      el("section", { class: "chat" }, log, el("div", { class: "composer" }, suggestions, input, el("div", { class: "bar" }, conn, el("div", { class: "row" }, mode, sendBtn)))),
      side,
    ),
  );

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  });

  const add = (node) => {
    log.insertBefore(node, typing);
    log.scrollTop = log.scrollHeight;
  };
  const sys = (text, kind = "") => el("div", { class: `sys ${kind}` }, text);

  function emptyState(note = "") {
    fill(log, 
      el(
        "div",
        { class: "sys", style: "margin:auto; flex-direction:column; text-align:center; gap:10px" },
        el("div", { style: "font-size:15px; color:var(--text); font-weight:600" }, "No session yet"),
        note ? el("div", {}, note) : null,
        el("div", {}, "Start one, or just send a message."),
        el("button", { class: "btn primary", onclick: () => ensureSession() }, "Start a session"),
      ),
      typing,
    );
  }

  function resetLog() {
    fill(log, typing);
    lastSeq = 0;
    approvals.clear();
    pending.clear();
  }

  const DISPOSITION = {
    started: ["started a turn", "info"],
    joined: ["joined the running turn", "accent"],
    queued: ["queued for the next turn", "warn"],
    duplicate: ["duplicate, ignored", ""],
  };

  function userMsg(text, disposition, isPending = false) {
    const [label, kind] = DISPOSITION[disposition] || ["sending…", ""];
    return el("div", { class: `msg user${isPending ? " pending" : ""}` }, el("div", { class: "bubble" }, text), el("div", { class: "meta" }, badge(label, kind)));
  }

  function assistantMsg(text) {
    return el("div", { class: "msg assistant" }, el("div", { class: "bubble md", html: markdown(text) }), el("div", { class: "meta" }, "Assistant"));
  }

  function approvalCard(event) {
    const d = event.data;
    const params = Object.entries(d.input || {});
    const statusEl = badge("Waiting for you", "warn", { pulse: true });
    const timer = el("span", { class: "fine" });
    const error = el("span", { class: "fine", style: "color:var(--bad)" });
    const approveBtn = el("button", { class: "btn good small", onclick: () => decide(true) }, "Approve");
    const denyBtn = el("button", { class: "btn small", onclick: () => decide(false) }, "Deny");
    const node = el(
      "div",
      { class: "approval" },
      el("div", { class: "approval-head" }, el("span", { class: "approval-title" }, `Approve ${humanTool(d.tool)}?`), statusEl),
      el("div", { class: "fine" }, "The agent wants to run this action. Approving covers exactly these parameters, and the action re-checks it when it runs."),
      params.length
        ? el("dl", { class: "kv params" }, params.map(([k, v]) => [el("dt", {}, k), el("dd", {}, typeof v === "string" ? v : JSON.stringify(v))]))
        : null,
      el("div", { class: "actions" }, approveBtn, denyBtn, timer, error),
      el("div", { class: "fine mono" }, `approval ${d.approval_id} · digest ${d.digest}`),
    );
    const entry = { node, statusEl, timer, buttons: [approveBtn, denyBtn], expires: Date.parse(d.expires_at), state: "pending" };
    approvals.set(d.approval_id, entry);

    async function decide(approve) {
      entry.buttons.forEach((b) => (b.disabled = true));
      error.textContent = "";
      try {
        await api(`/api/sessions/${encodeURIComponent(sessionId)}/approvals`, {
          method: "POST",
          body: { approval_id: d.approval_id, digest: d.digest, approve },
        });
      } catch (err) {
        error.textContent = err.message;
        entry.buttons.forEach((b) => (b.disabled = false));
      }
    }
    return node;
  }

  function settleApproval(id, outcome) {
    const entry = approvals.get(id);
    if (!entry) return;
    entry.state = outcome;
    entry.node.className = `approval ${outcome}`;
    entry.buttons.forEach((b) => b.remove());
    entry.timer.textContent = outcome === "expired" ? "Nothing was done; you were notified." : "";
    const [text, kind] = { approved: ["Approved", "ok"], denied: ["Denied", ""], expired: ["Expired", "bad"] }[outcome];
    entry.statusEl.replaceWith((entry.statusEl = badge(text, kind)));
  }

  every(1000, () => {
    for (const entry of approvals.values()) {
      if (entry.state !== "pending") continue;
      const left = entry.expires - Date.now();
      entry.timer.textContent = left > 0 ? `Expires in ${countdown(left)}` : "Expiring…";
    }
  });

  function renderEvent(event) {
    switch (event.kind) {
      case "session_started":
        // Always first, even if the opening message's bubble was drawn before the stream connected.
        log.prepend(sys("Session started"));
        break;
      case "message_accepted": {
        const node = userMsg(event.text, event.data.disposition);
        const waiting = pending.get(event.data.message_id);
        if (waiting) {
          waiting.replaceWith(node);
          pending.delete(event.data.message_id);
        } else add(node);
        break;
      }
      case "turn_started":
        break;
      case "assistant":
        add(assistantMsg(event.text));
        break;
      case "approval_requested":
        add(approvalCard(event));
        break;
      case "approval_approved":
      case "approval_denied":
      case "approval_expired":
        settleApproval(event.data.approval_id, event.kind.replace("approval_", ""));
        break;
      case "timer_set":
        add(sys(`⏰ Timer set for ${fmtTime(event.data.fires_at)}: ${event.text}`));
        break;
      case "timer_fired":
        add(sys(`⏰ Timer fired: ${event.text}`));
        break;
      case "turn_failed":
        add(sys(`The turn failed: ${event.text}`, "error"));
        break;
      default:
        add(sys(event.text));
    }
  }

  function applyStatus() {
    if (!status) return;
    const waitingForYou = status.pending_approvals.length > 0;
    typing.hidden = !status.turn_running;
    typingText.textContent = waitingForYou ? "Waiting for your approval…" : "Working…";
    renderSide();
  }

  const CONN = {
    connecting: ["Connecting…", ""],
    live: ["Live", "live"],
    reconnecting: ["Reconnecting…", "off"],
    off: ["Disconnected (the session keeps running)", "off"],
    waiting: ["Waiting for a worker…", "off"],
    closed: ["Session closed", ""],
    none: ["No session", ""],
  };

  function setConn(value) {
    connState = value;
    const [text, kind] = CONN[value];
    conn.className = `conn ${kind}`;
    fill(conn, el("span", { class: `dot${value === "live" ? " pulse" : ""}` }), text);
    renderSide();
  }

  function connect(since = lastSeq) {
    if (!sessionId) return;
    source?.close();
    setConn("connecting");
    // Each SSE event id is the session's event number. If the connection
    // drops, the browser reconnects with Last-Event-ID and resumes there.
    source = new EventSource(`/api/sessions/${encodeURIComponent(sessionId)}/events?since=${since}`);
    source.onopen = () => setConn("live");
    source.addEventListener("session", (e) => {
      const event = JSON.parse(e.data);
      if (event.seq <= lastSeq) return;
      lastSeq = event.seq;
      renderEvent(event);
    });
    source.addEventListener("unavailable", () => setConn("waiting"));
    source.addEventListener("status", (e) => {
      if (connState !== "live") setConn("live");
      status = JSON.parse(e.data);
      applyStatus();
      if (status.closed && !status.turn_running) {
        source.close();
        setConn("closed");
      }
    });
    source.addEventListener("gone", () => {
      source.close();
      localStorage.removeItem(storageKey);
      sessionId = null;
      status = null;
      setConn("none");
      emptyState("The previous session no longer exists on this Temporal server.");
    });
    source.onerror = () => {
      if (connState === "off" || connState === "closed") return;
      setConn(source.readyState === EventSource.CLOSED ? "off" : "reconnecting");
    };
  }
  onCleanup(() => source?.close());

  async function ensureSession() {
    if (sessionId) return sessionId;
    const result = await api("/api/sessions", { method: "POST", body: { user } });
    sessionId = result.session_id;
    localStorage.setItem(storageKey, sessionId);
    resetLog();
    connect(0);
    return sessionId;
  }

  async function send() {
    const text = input.value.trim();
    if (!text) return;
    sendBtn.disabled = true;
    const clientId = uid();
    let node = null;
    try {
      await ensureSession();
      node = userMsg(text, null, true);
      pending.set(clientId, node);
      add(node);
      input.value = "";
      await api(`/api/sessions/${encodeURIComponent(sessionId)}/messages`, {
        method: "POST",
        body: { text, mode: mode.value, client_message_id: clientId },
      });
    } catch (err) {
      node?.remove();
      pending.delete(clientId);
      input.value = text;
      toast(err.message, "bad");
    } finally {
      sendBtn.disabled = false;
      input.focus();
    }
  }

  function renderSide() {
    const s = status;
    const sessionCard = sessionId
      ? card(
          "Session",
          el(
            "dl",
            { class: "kv" },
            el("dt", {}, "Id"),
            el("dd", {}, el("code", {}, sessionId)),
            el("dt", {}, "State"),
            el(
              "dd",
              { class: "chips" },
              !s ? badge("…") : s.closed ? badge("Closed") : s.turn_running ? badge("Turn running", "info", { pulse: true }) : badge("Idle", "ok"),
              s && s.pending_approvals.length ? badge(`${s.pending_approvals.length} awaiting approval`, "warn") : null,
              s && s.queued_messages ? badge(`${s.queued_messages} queued`, "warn") : null,
              s && s.pending_timers.length ? badge(`${s.pending_timers.length} timer${s.pending_timers.length === 1 ? "" : "s"} pending`, "accent") : null,
            ),
            s && s.pending_timers.length ? el("dt", {}, "Timers") : null,
            s && s.pending_timers.length
              ? el("dd", {}, s.pending_timers.map((t) => el("div", { class: "fine" }, `${fmtTime(t.fires_at)} · ${t.note}`)))
              : null,
            el("dt", {}, "Stream"),
            el("dd", {}, CONN[connState][0]),
            el("dt", {}, "Last event"),
            el("dd", {}, `#${lastSeq}`),
          ),
          el("p", { class: "hint", style: "margin-top:10px" }, wfLink(`session-${sessionId}`)),
          el(
            "div",
            { class: "row" },
            connState === "off"
              ? el("button", { class: "btn small primary", onclick: () => (connect(lastSeq), toast(`Resuming from event #${lastSeq}`)) }, "Reconnect")
              : el(
                  "button",
                  {
                    class: "btn small",
                    disabled: connState === "closed",
                    onclick: () => {
                      source?.close();
                      setConn("off");
                    },
                  },
                  "Disconnect",
                ),
            el(
              "button",
              {
                class: "btn small",
                onclick: async () => {
                  source?.close();
                  localStorage.removeItem(storageKey);
                  sessionId = null;
                  status = null;
                  resetLog();
                  await ensureSession();
                },
              },
              "New session",
            ),
            el(
              "button",
              {
                class: "btn small danger",
                disabled: !s || s.closed,
                onclick: async () => {
                  await api(`/api/sessions/${encodeURIComponent(sessionId)}/close`, { method: "POST" });
                  toast("Close requested. Queued messages still finish.");
                },
              },
              "Close",
            ),
          ),
        )
      : card("Session", el("p", { class: "hint" }, "No session for this user yet."), el("button", { class: "btn primary", onclick: () => ensureSession() }, "Start a session"));

    fill(side, 
      sessionCard,
      card(
        "Things to try",
        el(
          "ul",
          { class: "hint", style: "padding-left:18px; margin:0; display:flex; flex-direction:column; gap:6px" },
          el("li", {}, 'Ask it to do something later, like "Send me hello in 30 seconds". Its timer is a Standalone Activity with a start delay; when it fires, it wakes this session.'),
          el("li", {}, "Ask for a briefing now or later. It runs in the background and lands in the outbox (Briefings view)."),
          el("li", {}, "Ask for a briefing every week, like \"every Monday at 8am Pacific\". It sets up a Temporal Schedule you can move or cancel the same way (Briefings view)."),
          el("li", {}, "Tell it how you like your briefings. What you say feeds memory (Memory view) and shapes the next briefing."),
          el("li", {}, "Ask it to share your latest report with someone. You get an approval card: approve, deny, or let it expire."),
          el("li", {}, "While it waits for approval, send another message. It joins the running turn, or queue it instead."),
          el("li", {}, "Press Disconnect, send nothing, then Reconnect: the stream resumes from the last event you saw."),
          el("li", {}, "Restart the worker while an approval is pending. The approval is still there."),
        ),
      ),
    );
  }

  renderSide();
  if (sessionId) {
    resetLog();
    connect(0);
  } else {
    setConn("none");
    emptyState();
  }
}

function humanTool(name) {
  return {
    share_report: "sharing your report",
    write_report: "writing a report",
    set_weekly_briefing: "setting your weekly briefing",
    get_weekly_briefing: "checking your weekly briefing",
    cancel_weekly_briefing: "cancelling your weekly briefing",
  }[name] || (name || "this action").replace(/_/g, " ");
}

// ---------------------------------------------------------------------------
// Briefings (read-only): what was scheduled, what ran, what was delivered
// ---------------------------------------------------------------------------

function BriefingsView(root) {
  const user = state.user;
  root.append(
    pageHead(
      "Briefings",
      "What the assistant has scheduled and delivered. To get a briefing, ask the assistant, for example \"Prepare my briefing in 2 minutes\". Every delivery appears in the outbox exactly once.",
    ),
  );

  // -- scheduled jobs
  const jobsBody = el("div", {}, el("div", { class: "empty" }, "Loading…"));
  const jobsCard = card(
    [
      "Scheduled briefings",
      state.config
        ? el("a", { href: `${state.config.temporal_ui}/namespaces/${encodeURIComponent(state.config.namespace)}/activities`, target: "_blank", rel: "noopener" }, "Standalone Activities ↗")
        : null,
    ],
    el("p", { class: "hint" }, "Each is a Standalone Activity with a start delay. Temporal holds it through the wait, even if every worker is down, then runs the whole turn as one job."),
    jobsBody,
  );
  let jobs = [];
  async function refreshJobs() {
    jobs = await api(`/api/users/${user}/scheduled`);
    renderJobs();
  }
  function renderJobs() {
    if (!jobs.length) return fill(jobsBody, el("div", { class: "empty" }, "Nothing scheduled yet. Ask the assistant for a briefing."));
    fill(jobsBody, 
      el(
        "table",
        {},
        el("thead", {}, el("tr", {}, el("th", {}, "Briefing"), el("th", {}, "Status"), el("th", {}, "Attempt"), el("th", {}, "Submitted"))),
        el(
          "tbody",
          {},
          jobs.map((j) => {
            const startsAt = Date.parse(j.scheduled) + j.start_delay_seconds * 1000;
            let st;
            if (j.status === "RUNNING" && j.run_state === "SCHEDULED") {
              const left = startsAt - Date.now();
              st = left > 0 ? badge(`Waiting · starts in ${countdown(left)}`, "warn", { pulse: true }) : badge("Waiting for a worker", "warn", { pulse: true });
            } else if (j.status === "RUNNING") st = badge("Running", "info", { pulse: true });
            else if (j.status === "COMPLETED") st = badge(j.result?.delivered ? "Delivered" : "Completed", "ok");
            else st = statusBadge(j.status);
            return el(
              "tr",
              {},
              el(
                "td",
                { class: "mono" },
                j.activity_id,
                j.result?.report_id ? el("div", {}, el("button", { class: "btn link small", onclick: () => openReport(j.result.report_id) }, "Open report")) : null,
              ),
              el("td", {}, st),
              el("td", { class: j.attempt > 1 ? "attempt-repeat" : "" }, j.attempt),
              el("td", {}, fmtTime(j.scheduled)),
            );
          }),
        ),
      ),
    );
  }
  every(2500, refreshJobs);
  every(1000, () => jobs.some((j) => j.run_state === "SCHEDULED") && renderJobs());

  // -- weekly schedule
  const weeklyBody = el("div");
  const weeklyCard = card("Weekly briefing", el("p", { class: "hint" }, "A Temporal Schedule starts one briefing Workflow per run. Delivery is its own Activity with its own retries."), weeklyBody);
  async function refreshWeekly() {
    const w = await api(`/api/users/${user}/weekly`);
    if (!w.exists) {
      return fill(weeklyBody, el("div", { class: "empty" }, "No weekly briefing. Ask the assistant, for example \"Send me my briefing every Monday at 8am\"."));
    }
    fill(weeklyBody, 
      el(
        "dl",
        { class: "kv" },
        el("dt", {}, "Schedule"),
        el("dd", {}, el("code", {}, w.schedule_id)),
        el("dt", {}, "Runs"),
        el("dd", {}, w.when || "–", ` (${w.time_zone})`, w.paused ? badge("paused", "warn") : null),
        el("dt", {}, "Next runs"),
        el("dd", {}, (w.next_runs || []).map(fmtTime).join(" · ") || "–"),
        el("dt", {}, "Runs so far"),
        el("dd", {}, String(w.runs_so_far)),
      ),
    );
  }
  every(10000, refreshWeekly);

  // -- recent runs
  const runsBody = el("div");
  const runsCard = card("Recent runs", runsBody);
  async function refreshRuns() {
    const runs = await api(`/api/users/${user}/turns`);
    if (!runs.length) return fill(runsBody, el("div", { class: "empty" }, "No briefing Workflows yet. The weekly Schedule starts one per run."));
    fill(runsBody, 
      el(
        "table",
        {},
        el(
          "tbody",
          {},
          runs.map((r) =>
            el(
              "tr",
              {},
              el("td", { class: "mono" }, r.turn_id),
              el("td", {}, statusBadge(r.status)),
              el("td", { style: "white-space:nowrap; color:var(--faint)" }, ago(r.started)),
              el("td", { style: "white-space:nowrap" }, wfLink(r.turn_id, "Temporal")),
            ),
          ),
        ),
      ),
    );
  }
  every(3000, refreshRuns);

  // -- reports
  const reportList = el("div", { class: "list" });
  const reportView = el("div", { class: "report-body md" }, el("div", { class: "empty" }, "Select a report."));
  let selected = null;
  const reportsCard = card("Reports", el("div", { class: "report-layout" }, reportList, reportView));
  async function refreshReports() {
    const reports = await api(`/api/users/${user}/reports`);
    if (!reports.length) return fill(reportList, el("div", { class: "empty" }, "No reports yet."));
    fill(reportList, 
      ...reports.map((r) =>
        el(
          "button",
          { class: r.report_id === selected ? "active" : "", onclick: () => openReport(r.report_id) },
          el("span", {}, r.title),
          el("span", { class: "sub mono" }, r.report_id),
          el("span", { class: "sub" }, fmtTime(r.updated_at)),
        ),
      ),
    );
    if (!selected) openReport(reports[0].report_id, false);
  }
  async function openReport(reportId, scroll = true) {
    selected = reportId;
    const r = await api(`/api/reports/${encodeURIComponent(reportId)}`);
    reportView.innerHTML = markdown(r.body);
    refreshReports();
    if (scroll) reportsCard.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  every(5000, refreshReports);

  // -- outbox
  const outboxBody = el("div");
  const outboxCard = card("Outbox", el("p", { class: "hint" }, "Every message that left the system. Each has an idempotency key, so a retried delivery never sends twice."), outboxBody);
  async function refreshOutbox() {
    const items = await api(`/api/users/${user}/outbox`);
    if (!items.length) return fill(outboxBody, el("div", { class: "empty" }, "Nothing delivered yet."));
    fill(outboxBody, 
      ...items.map((m) =>
        el(
          "details",
          { class: "outbox-item" },
          el(
            "summary",
            {},
            badge(m.channel.startsWith("share:") ? `shared → ${m.channel.slice(6)}` : m.channel, m.channel.startsWith("share:") ? "accent" : "info"),
            el("code", {}, m.message_id),
            el("span", { style: "color:var(--faint)" }, fmtTime(m.created_at)),
          ),
          el("pre", {}, m.body),
        ),
      ),
    );
  }
  every(4000, refreshOutbox);

  root.append(el("div", { class: "stack" }, jobsCard, el("div", { class: "grid two" }, weeklyCard, runsCard), reportsCard, outboxCard));
}

// ---------------------------------------------------------------------------
// Memory (read-only): curated notes and the consolidation Workflow
// ---------------------------------------------------------------------------

function MemoryView(root) {
  const user = state.user;
  root.append(
    pageHead(
      "Memory",
      "What the assistant remembers about how you like your briefings. What you tell the assistant is the source material: messages are batched, a background Workflow prepares a revision with the model and saves it, then publishes it in one transaction. The next briefing reads the new notes.",
    ),
  );

  const notesBody = el("div");
  const notesCard = card("What the assistant remembers", notesBody);

  const wfBody = el("div");
  const wfCard = card(
    "Consolidation",
    el("p", { class: "hint" }, "Each chat message notifies this Workflow (signal-with-start). It runs one model pass per batch, not one per message."),
    wfBody,
  );

  const STEPS = ["batching", "preparing", "publishing", "idle"];
  const STEP_LABEL = { batching: "Batching", preparing: "Preparing (model pass)", publishing: "Publishing", idle: "Published" };

  async function refresh() {
    const m = await api(`/api/users/${user}/memory`);
    fill(notesBody, 
      el("div", { class: "row", style: "margin-bottom:10px" }, badge(`Revision ${m.revision}`, "accent"), badge(`${m.unprocessed} unprocessed`, m.unprocessed ? "warn" : "")),
      m.notes.length
        ? el(
            "ul",
            { class: "notes" },
            m.notes.map((n) => el("li", {}, n.text, el("div", { class: "src" }, "from ", n.sources.map((s) => el("code", {}, s)).flatMap((c, i) => (i ? [", ", c] : [c]))))),
          )
        : el("div", { class: "empty" }, "Nothing yet. Tell the assistant how you like your briefings; notes show up here once a batch is published."),
    );

    const wf = m.workflow;
    const st = wf?.state;
    const current = wf?.status === "RUNNING" ? st?.status : wf ? "idle" : null;
    const stuck = current === "waiting_for_operator";
    const idx = STEPS.indexOf(stuck ? "publishing" : current);
    fill(wfBody, 
      el("dl", { class: "kv" }, el("dt", {}, "Workflow"), el("dd", {}, el("code", {}, m.workflow_id), " ", wf ? statusBadge(wf.status) : badge("not started"))),
      wf
        ? el(
            "div",
            { class: "steps" },
            STEPS.map((s, i) =>
              el("span", { class: `step ${stuck && s === "publishing" ? "stuck" : i < idx || (s === "idle" && current === "idle") ? "done" : i === idx ? "now" : ""}` }, STEP_LABEL[s]),
            ),
          )
        : null,
      st
        ? el("p", { class: "hint", style: "margin-top:8px" }, `${st.pending} pending · ${st.batches} batch${st.batches === 1 ? "" : "es"} · published revisions: ${st.published_revisions.join(", ") || "none yet"}`)
        : null,
      stuck
        ? el(
            "div",
            { class: "approval", style: "margin-top:8px" },
            el("div", { class: "approval-title" }, "Waiting for an operator"),
            el(
              "div",
              { class: "fine" },
              "Publication ran out of retries. The saved proposal is kept and later briefings still read the previous revision. An operator fixes the dependency, then retries from ",
              el("a", { href: "#operations" }, "Operations"),
              ". No new model call is needed.",
            ),
          )
        : null,
      wf ? el("div", { class: "row", style: "margin-top:12px" }, wfLink(m.workflow_id)) : null,
    );
  }
  every(1500, refresh);

  root.append(el("div", { class: "grid side" }, notesCard, wfCard));
}

// ---------------------------------------------------------------------------
// Workspace: hot pool + save-and-release lease
// ---------------------------------------------------------------------------

function WorkspaceView(root) {
  const user = state.user;
  root.append(
    pageHead(
      "Workspace",
      "A controller Workflow keeps sandbox VMs warm. A workspace lease claims one, restores saved files, and when the workspace goes idle it saves the files, records that, and only then releases the VM. (VMs here are local folders.)",
    ),
  );

  const poolBody = el("div");
  const poolCard = card("VM pool", poolBody);
  async function refreshPool() {
    const p = await api("/api/pool");
    if (!p.running) {
      const target = el("input", { type: "number", min: 0, max: 10, value: 2 });
      const tick = el("input", { type: "number", min: 1, max: 600, value: 10 });
      fill(poolBody, 
        el("p", { class: "hint" }, "The pool controller is not running."),
        el(
          "div",
          { class: "row" },
          el("label", { class: "field", style: "width:120px" }, "Keep ready", target),
          el("label", { class: "field", style: "width:140px" }, "Check every (s)", tick),
          el(
            "button",
            {
              class: "btn primary",
              onclick: async () => {
                await api("/api/pool", { method: "POST", body: { target_ready: Number(target.value), tick_seconds: Number(tick.value) } });
                refreshPool();
              },
            },
            "Start pool",
          ),
        ),
      );
      return;
    }
    const claimed = Object.entries(p.claimed || {});
    fill(poolBody, 
      el("div", { class: "row", style: "margin-bottom:10px" }, badge(`${p.ready.length} ready`, "ok"), badge(`${claimed.length} claimed`, "accent"), p.waiting_claims ? badge(`${p.waiting_claims} waiting`, "warn") : null, badge(`${p.creates_in_window} created this minute`)),
      el(
        "div",
        { class: "vms" },
        p.ready.map((vm) => el("div", { class: "vm ready" }, el("code", {}, vm), el("div", { class: "src", style: "color:var(--ok)" }, "ready"))),
        claimed.map(([vm, ws]) => el("div", { class: "vm claimed" }, el("code", {}, vm), el("div", { style: "color:var(--accent)" }, `claimed by ${ws}`))),
      ),
      el("p", { class: "hint", style: "margin-top:10px" }, wfLink(state.config?.pool_workflow_id || "workspace-pool")),
    );
  }
  every(2000, refreshPool);

  const wsKey = `aoa.workspace.${user}`;
  const wsId = el("input", { value: localStorage.getItem(wsKey) || `${user}-ws`, "aria-label": "Workspace id" });
  const idle = el("input", { type: "number", min: 5, value: 60, "aria-label": "Idle seconds" });
  const leaseBody = el("div");
  const filesBody = el("div");
  const fileName = el("input", { value: "notes.md", "aria-label": "File name" });
  const fileText = el("textarea", { rows: 3, placeholder: "Write something the agent would keep between turns…", "aria-label": "File contents" });

  const leaseCard = card(
    "Workspace lease",
    el(
      "div",
      { class: "row" },
      el("label", { class: "field grow" }, "Workspace", wsId),
      el("label", { class: "field", style: "width:150px" }, "Release after idle (s)", idle),
      el(
        "button",
        {
          class: "btn primary",
          onclick: async () => {
            const id = wsId.value.trim();
            if (!SAFE_ID.test(id)) return toast("Workspace ids are letters, digits, '-' or '_'.", "bad");
            localStorage.setItem(wsKey, id);
            try {
              await api(`/api/workspaces/${id}/open`, { method: "POST", body: { idle_seconds: Number(idle.value) } });
              toast(`Opened ${id}`);
              refreshLease();
            } catch (err) {
              toast(err.message, "bad");
            }
          },
        },
        "Open",
      ),
    ),
    leaseBody,
    // The editor lives outside the refreshing body so polling never steals focus while you type.
    el(
      "div",
      { class: "lease-editor", hidden: true, style: "margin-top:12px" },
      el("div", { class: "row" }, el("label", { class: "field", style: "width:160px" }, "File", fileName), el("div", { class: "grow" })),
      fileText,
      el(
        "div",
        { class: "row", style: "margin-top:8px" },
        el(
          "button",
          {
            class: "btn small primary",
            onclick: async () => {
              try {
                await api(`/api/workspaces/${wsId.value.trim()}/files`, { method: "POST", body: { name: fileName.value.trim(), text: fileText.value } });
                toast(`Saved ${fileName.value} (idle timer reset)`);
                refreshLease();
              } catch (err) {
                toast(err.message, "bad");
              }
            },
          },
          "Write file",
        ),
        el(
          "button",
          {
            class: "btn small",
            onclick: async () => {
              await api(`/api/workspaces/${wsId.value.trim()}/finish`, { method: "POST" });
              toast("Saving and releasing now");
            },
          },
          "Save and release now",
        ),
      ),
    ),
    filesBody,
  );
  const editor = $(".lease-editor", leaseCard);

  const STAGES = ["claiming", "restoring", "active", "saving", "releasing", "released"];
  async function refreshLease() {
    const id = wsId.value.trim();
    if (!SAFE_ID.test(id)) return;
    const w = await api(`/api/workspaces/${id}`);
    const active = w.exists && w.stage === "active";
    editor.hidden = !active;
    if (!w.exists) {
      fill(leaseBody, el("p", { class: "hint", style: "margin-top:12px" }, "Not opened yet.", w.latest_snapshot ? ` Last saved snapshot: ${w.latest_snapshot}.` : ""));
      fill(filesBody);
      return;
    }
    const idx = STAGES.indexOf(w.stage);
    fill(
      leaseBody,
      el("div", { class: "steps", style: "margin-top:12px" }, STAGES.map((s, i) => el("span", { class: `step ${i < idx || w.stage === "released" ? "done" : i === idx ? "now" : ""}` }, s))),
      el(
        "dl",
        { class: "kv", style: "margin-top:8px" },
        el("dt", {}, "VM"),
        el("dd", {}, el("code", {}, w.vm_id || "–")),
        el("dt", {}, "Last snapshot"),
        el("dd", {}, el("code", {}, w.latest_snapshot || "none yet")),
        el("dt", {}, "Workflow"),
        el("dd", {}, wfLink(w.workflow_id)),
      ),
      active ? null : el("p", { class: "hint", style: "margin-top:10px" }, w.stage === "released" ? "Saved and released. Open it again to restore the files on a fresh VM." : "Working…"),
    );
    fill(
      filesBody,
      active
        ? w.files.length
          ? w.files.map((f) => el("div", { class: "file" }, el("code", {}, f.name), el("pre", {}, f.text)))
          : el("div", { class: "empty" }, "No files yet. Write one, let the workspace go idle, then open it again: the file comes back from the snapshot.")
        : null,
    );
  }
  every(1500, refreshLease);

  root.append(el("div", { class: "stack" }, poolCard, leaseCard));
}

// ---------------------------------------------------------------------------
// Operations: the article's recovery drills
// ---------------------------------------------------------------------------

function OperationsView(root) {
  root.append(
    pageHead(
      "Operations",
      "Break things on purpose, then check what woke, what repeated, and what reached the outside world. Faults apply to the next calls at that point, across all users.",
    ),
  );

  const point = el("select", { "aria-label": "Fault point" }, FAULT_POINTS.map((p) => el("option", { value: p }, p)));
  const count = el("input", { type: "number", min: 1, value: 2, "aria-label": "Count" });
  const mode = el("select", { "aria-label": "Mode" }, el("option", { value: "error" }, "fail with an error (retryable)"), el("option", { value: "crash" }, "crash the worker process"));
  const faultsBody = el("div");
  const faultCard = card(
    "Inject a fault",
    el("p", { class: "hint" }, "Try write_report ×2, then ask the assistant for a briefing: it runs as one coarse Activity, so the whole turn repeats. Or publish_memory ×3, then send the assistant three messages: publication retries from the saved proposal (×8 exhausts the retries and raises an alert). Crash mode kills the worker; restart it to recover."),
    el(
      "div",
      { class: "row" },
      el("label", { class: "field grow" }, "Point", point),
      el("label", { class: "field", style: "width:80px" }, "Next", count),
      el("label", { class: "field grow" }, "Mode", mode),
      el(
        "button",
        {
          class: "btn primary",
          onclick: async () => {
            if (mode.value === "crash" && !confirm("This will kill the worker process the next time that point runs. Continue?")) return;
            await api("/api/faults", { method: "POST", body: { point: point.value, count: Number(count.value), mode: mode.value } });
            refreshFaults();
          },
        },
        "Add",
      ),
    ),
    faultsBody,
  );

  async function refreshFaults() {
    const faults = await api("/api/faults");
    fill(faultsBody, 
      faults.length
        ? el(
            "div",
            { class: "row", style: "margin-top:10px; align-items:center" },
            faults.map((f) => badge(`${f.name} × ${f.remaining} (${f.mode})`, f.mode === "crash" ? "bad" : "warn")),
            el(
              "button",
              {
                class: "btn small",
                onclick: async () => {
                  await api("/api/faults", { method: "DELETE" });
                  refreshFaults();
                },
              },
              "Clear all",
            ),
          )
        : el("div", { class: "empty" }, "No active faults."),
    );
  }
  every(2000, refreshFaults);

  const alertsBody = el("div");
  const alertsCard = card("Operator alerts", alertsBody);
  every(3000, async () => {
    const alerts = await api("/api/alerts");
    fill(alertsBody, 
      alerts.length
        ? el(
            "div",
            { class: "stack", style: "gap:8px" },
            alerts.map((a) =>
              el(
                "div",
                { class: "approval" },
                el("div", { class: "approval-head" }, el("span", { class: "approval-title" }, a.kind), el("span", { class: "fine" }, fmtTime(a.created_at))),
                el("code", {}, a.subject),
                el("div", { class: "fine" }, a.detail),
                // Offered only while the run that raised the alert is still waiting.
                a.can_retry
                  ? el(
                      "div",
                      { class: "actions" },
                      el(
                        "button",
                        {
                          class: "btn small primary",
                          onclick: async (e) => {
                            e.target.disabled = true;
                            try {
                              await api(`/api/alerts/${a.id}/retry-publication`, { method: "POST" });
                              toast("Retry requested");
                            } catch (err) {
                              toast(err.message, "bad");
                              e.target.disabled = false;
                            }
                          },
                        },
                        "Retry publication",
                      ),
                    )
                  : null,
              ),
            ),
          )
        : el("div", { class: "empty" }, "No alerts."),
    );
  });

  const filter = el("input", { placeholder: "Filter, e.g. a turn id or model_call", "aria-label": "Filter operations" });
  const opsBody = el("div");
  const opsCard = card(
    ["Operation log", el("div", { style: "width:280px" }, filter)],
    el("p", { class: "hint" }, "Every model call, tool call, delivery, and provider call, with its attempt number. A repeated operation shows attempt 2 or more."),
    opsBody,
  );
  let ops = [];
  function renderOps() {
    const q = filter.value.trim().toLowerCase();
    const rows = ops.filter((o) => !q || `${o.op} ${o.key} ${o.detail}`.toLowerCase().includes(q)).reverse();
    fill(opsBody, 
      rows.length
        ? el(
            "table",
            {},
            el("thead", {}, el("tr", {}, el("th", {}, "Time"), el("th", {}, "Operation"), el("th", {}, "Attempt"), el("th", {}, "Key"), el("th", {}, "Detail"))),
            el(
              "tbody",
              {},
              rows.map((o) =>
                el(
                  "tr",
                  {},
                  el("td", { style: "white-space:nowrap; color:var(--faint)" }, fmtTime(o.at)),
                  el("td", {}, el("code", {}, o.op)),
                  el("td", { class: o.attempt > 1 ? "attempt-repeat" : "" }, o.attempt),
                  el("td", { class: "mono" }, o.key),
                  el("td", { class: "mono", style: "color:var(--faint)" }, o.detail),
                ),
              ),
            ),
          )
        : el("div", { class: "empty" }, "No operations yet."),
    );
  }
  filter.addEventListener("input", renderOps);
  every(1500, async () => {
    ops = await api("/api/ops?limit=150");
    renderOps();
  });

  root.append(el("div", { class: "stack" }, el("div", { class: "grid two" }, faultCard, alertsCard), opsCard));
}

boot();

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
// agy keeps a per-conversation SQLite trajectory (one row per turn / tool call)
// and appends to it LIVE during a run — verified 2026-09-11 by polling it while
// agy worked. That table is the only progress signal that sees sub-second tool
// calls, which the /proc sampler structurally cannot (see readNewSteps).
let DatabaseSync = null;
try { ({ DatabaseSync } = require('node:sqlite')); } catch (e) { /* older node: step signal off, /proc only */ }

const PORT = parseInt(process.env.PROXY_PORT || '8000', 10);
// agy print mode may run web searches and tools for complex queries, which
// legitimately takes minutes. The proxy owns the timeout; agy gets a longer
// internal --print-timeout (AGY_PRINT_TIMEOUT) so it never self-terminates
// first (which would look like a failure and trigger a pointless retry).
// Some agentic tasks (e.g. investigating Home Assistant over sshfs) legitimately
// run ~9 min and stay silent on stdout — agy --print buffers everything until
// the end. But agy's private glog file (--log-file) DOES record live activity:
// each "streamGenerateContent" line is one model round of the agent loop. The
// proxy tails that file, so the timeout can be adaptive instead of flat:
//   SOFT — past this, keep going as long as agy shows recent activity
//   IDLE — no model round AND no stdout for this long => agy is stuck, kill
//   HARD — absolute ceiling regardless of activity (runaway protection)
// SSE heartbeats still keep the client connection alive during silence.
//
// A model round is NOT the only kind of legitimate activity, and its glog line
// (streamGenerateContent) is written ONCE at the START of the round — a single
// slow round (Gemini Pro High, thinking, on a huge prompt) can then generate for
// 8-12 min with no further log line and no stdout. A long tool call (a script
// reading a big .docx over Nextcloud, an sshfs walk) is similar: minutes of
// silence. Both used to trip the IDLE timeout and kill a perfectly healthy run.
// Two mitigations:
//   1. The watchdog treats an active tool SUBPROCESS as activity — agy spawns
//      shell tools into its own process group, so any live descendant means
//      "busy running a tool", not "stuck" (hasActiveToolChild).
//   2. IDLE is generous enough to sit through one slow model round (no child,
//      no log line) without killing. Background HTTP noise (quota/auth refresh)
//      has no subprocess, so it still can't by itself defeat the timeout.
// HARD stays the authoritative ceiling for a genuinely runaway/stuck process.
const SOFT_TIMEOUT_MS = 720000;     // 12 min — extend past this only if agy is active
const IDLE_TIMEOUT_MS = 900000;     // 15 min with no round, no stdout AND no live tool subprocess — long enough for one slow Pro round
const HARD_TIMEOUT_MS = 1800000;    // 30 min — absolute kill (authoritative)
// Loop guard: a model round resets the idle clock (it IS activity), so an agy
// spinning rounds fast — e.g. asked for something it has no tool/permission for
// — never trips IDLE and rides HARD to the full 30 min (then OpenClaw re-runs
// the turn, doubling it). STUCK catches that: many model rounds, ZERO answer
// tokens, and no live tool subprocess = looping without progress. A single slow
// round can't trip it (needs several rounds); a legit long tool run can't
// (hasActiveToolChild is true, and a blocked-on-tool agy emits no new rounds).
// NOTE this is a budget on TIME-TO-FIRST-TOKEN (total since attempt start), not
// a stall timer — a fast-spinning loop keeps emitting rounds, so "no new round"
// would never fire and only elapsed time discriminates. That makes the value a
// straight bet on how long a healthy tool-heavy run may stay silent.
// Raised 7 -> 12 min on 2026-08-07 after measuring the real workload: the
// ha_watchdog_v2 cron normally answers in 30-90s, but two consecutive runs
// during a live incident needed far longer (12:00 finished OK at TTFT=394s,
// 51 rounds — 26s under the old 420s wire; 11:00 was killed at 420s with 28
// rounds and produced nothing). The watchdog gets slowest exactly when
// something is wrong, so a tight budget cuts hardest during an incident.
// Round RATE does not separate the two cases (the healthy run span rounds
// FASTER: 7.8/min vs 4/min), so raising the ceiling is the only honest fix.
// Still well under HARD (30 min), and the permission-denial loop that
// originally motivated STUCK now exits on its own via the glog signature,
// so it no longer depends on this timer.
// Explicitly set by the operator, or null. Kept separate from the effective
// value because a per-model override must not silently defeat it (see
// timeoutsFor): AGY_STUCK_MS=40000 used to be ignored for every Flash model.
const ENV_STUCK_MS = process.env.AGY_STUCK_MS ? Number(process.env.AGY_STUCK_MS) : null;
const STUCK_NO_OUTPUT_MS = ENV_STUCK_MS || 720000;
const STUCK_MIN_ROUNDS = 3;         // require several rounds so one slow first round isn't mistaken for a loop
// A live tool subprocess keeps the run alive (see hasActiveToolChild), but ONLY
// for this long past the last real progress (model round or stdout). Without a
// cap, a genuinely IO-bound runaway (a `find /` stuck in NFS rpc_wait, state D)
// counts as "active tool" indefinitely and rides HARD to 30 min. Past the grace,
// a lingering child no longer masks the idle/stuck detectors.
const TOOL_GRACE_MS = 600000;       // 10 min of tool-only activity past last real round/stdout
const AGY_PRINT_TIMEOUT = '35m';    // agy's own budget, kept above HARD
const WATCHDOG_MS = 15000;          // adaptive-timeout check cadence
const LOG_POLL_MS = 2000;           // how often the agy log file is tailed
const HEARTBEAT_MS = 15000;         // visible progress-tick cadence during silence
const STATUS_MIN_GAP_MS = parseInt(process.env.AGY_STATUS_GAP_MS || '60000', 10); // min spacing of visible status lines (append-only UI); env-tunable so the rationing can be exercised in tests
const PROGRESS_MAX_GAP_MS = 180000; // max spacing of ANY visible tick — kept under OpenClaw's ~400s stall/abort so a silent long tool never looks stalled to the client
const MAX_ATTEMPTS = 2;             // one automatic retry on a transient agy failure
// On ANY error termination (timeout, spawn error, non-zero exit, empty output,
// permission denial) agy's private glog is preserved in tmpdir instead of
// deleted — it's the only record of what the failed run actually touched (which
// file/tool a denial hit). Prefix encodes the cause: agy-timeout-/agy-denied-/
// agy-fail-. Saved glogs older than this are swept on startup so /tmp can't grow
// without bound.
const GLOG_RETENTION_DAYS = parseInt(process.env.AGY_GLOG_RETENTION_DAYS || '7', 10);

// agy retries a RESOURCE_EXHAUSTED (429) quota error internally, on its own
// growing backoff (seen: 4s, 8s, 14s, ... up to ~3.5min between attempts,
// 8 attempts before it gives up) — for a genuinely exhausted quota (reset
// hours away) that whole loop is pointless and ties up a concurrency slot
// for 15-25 min, starving the queue for every other request (observed
// 2026-09-10: agy/claude-opus quota exhausted with ~3h reset, four separate
// runs each held a slot 20-25 min, so a third request queued behind them hit
// QUEUE_TIMEOUT_MS and died with "Przekroczono limit oczekiwania w
// kolejce"). agy has no --no-retry flag, so the fix lives here: the glog
// carries the quota line (with the reset ETA) the moment the FIRST attempt
// fails, well before agy's own loop gives up — watchAgyLog surfaces it live
// so runAgy can kill the child immediately instead of waiting agy out.
// A reset far enough out that waiting can't help gets killed right away;
// a reset within this window is left to agy's own short backoff (may still
// succeed on the next attempt).
const QUOTA_FAIL_FAST_MS = parseInt(process.env.AGY_QUOTA_FAIL_FAST_MS || '90000', 10);

// How long a conversation step keeps counting as "agy is making progress".
// A new row in agy's trajectory table (a model turn or a tool call) is hard
// evidence of work, unlike the /proc scan which only catches a tool child that
// happens to be in R/D at sample time. Measured failure this fixes (2026-09-10
// 23:07): a run with 48 successful `run_command` steps was killed as STUCK
// because every command finished well inside the 15s watchdog gap, so the
// sampler saw an idle tree and "no output for 12 min" looked like a loop.
const STEP_PROGRESS_WINDOW_MS = parseInt(process.env.AGY_STEP_WINDOW_MS || '120000', 10);
const QUOTA_RESET_RE = /RESOURCE_EXHAUSTED.*?Resets in (?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?/;

// Parse agy's glog "RESOURCE_EXHAUSTED ... Resets in 2h55m31s" line. Returns
// the reset delay in ms, or null if the line isn't a quota-exhaustion line.
function matchQuotaReset(line) {
    const m = line.match(QUOTA_RESET_RE);
    if (!m) return null;
    const h = parseInt(m[1] || 0, 10), mi = parseInt(m[2] || 0, 10), se = parseInt(m[3] || 0, 10);
    return (h * 3600 + mi * 60 + se) * 1000;
}

// Human-readable reset ETA for user-facing messages ("2h 55m" / "45s").
function fmtDuration(ms) {
    if (ms == null) return 'nieznany czas';
    const totalSec = Math.round(ms / 1000);
    const h = Math.floor(totalSec / 3600), m = Math.floor((totalSec % 3600) / 60), s = totalSec % 60;
    if (h) return `${h}h ${m}m`;
    if (m) return `${m}m ${s}s`;
    return `${s}s`;
}

// Ring buffer of recent console output, so /logs gives a live-ish debug view
// from a browser without an SSH session + journalctl on the Pi.
const LOG_BUFFER_MAX = parseInt(process.env.AGY_LOG_BUFFER_LINES || '400', 10);
const logBuffer = [];
function captureLog(args) {
    try {
        const text = args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' ');
        // The request logger already stamps its own line; don't double it.
        const line = /^\[\d{4}-\d{2}-\d{2}T/.test(text) ? text : `[${new Date().toISOString()}] ${text}`;
        logBuffer.push(line);
        if (logBuffer.length > LOG_BUFFER_MAX) logBuffer.shift();
    } catch (e) {}
}
const origConsoleLog = console.log.bind(console);
const origConsoleError = console.error.bind(console);
console.log = (...args) => { origConsoleLog(...args); captureLog(args); };
console.error = (...args) => { origConsoleError(...args); captureLog(args); };

// Per-model timeout overrides: keyed by a lowercase substring match against
// the agy model name (see MODEL_MAP). Flat global timeouts used to fight
// Pro-High-thinking (legitimately slow, needs headroom) against a runaway
// Flash loop (should fail fast) — see the long comment block at the top of
// this file. Only override what differs; anything unlisted falls through to
// the global default.
const MODEL_TIMEOUT_OVERRIDES = [
    { match: 'pro (high)', soft: 900000, idle: 1200000 },              // Gemini 3.1 Pro (High) — slow single rounds are normal, give more room
    { match: 'flash',      idle: 600000, stuck: 480000 },              // Flash: cheap/fast model, a stuck loop should die sooner
];
function timeoutsFor(agyModel) {
    const base = { soft: SOFT_TIMEOUT_MS, idle: IDLE_TIMEOUT_MS, hard: HARD_TIMEOUT_MS, stuck: STUCK_NO_OUTPUT_MS };
    const lower = (agyModel || '').toLowerCase();
    const rule = agyModel ? MODEL_TIMEOUT_OVERRIDES.find(r => lower.includes(r.match)) : null;
    const t = rule ? { ...base, soft: rule.soft ?? base.soft, idle: rule.idle ?? base.idle, hard: rule.hard ?? base.hard, stuck: rule.stuck ?? base.stuck } : { ...base };
    if (ENV_STUCK_MS) t.stuck = ENV_STUCK_MS;   // an explicit operator setting outranks the per-model default
    return t;
}

// ---- Concurrency limiter -------------------------------------------------
// Host is a Raspberry Pi; each agy spawn is hundreds of MB plus its own MCP
// child processes. Without a cap, OpenClaw multiagent (sessions_spawn) can fire
// several sessions at once and OOM the host or crash agy auth. Semaphore +
// bounded FIFO queue: at most MAX_CONCURRENT agy run at once, up to MAX_QUEUE
// more wait, anything past that is rejected fast (429 / visible ⚠️) instead of
// piling on. A waiter that outlives QUEUE_TIMEOUT_MS is failed, not left hanging.
const MAX_CONCURRENT   = parseInt(process.env.AGY_MAX_CONCURRENT   || '2', 10);
const MAX_QUEUE        = parseInt(process.env.AGY_MAX_QUEUE        || '4', 10);
const QUEUE_TIMEOUT_MS = parseInt(process.env.AGY_QUEUE_TIMEOUT_MS || '600000', 10); // 10 min

// Async counting semaphore with a bounded waiter queue. A "slot" is one unit of
// `active`. acquire() either hands a slot out immediately, queues the caller, or
// refuses (queue full). The slot count is conserved on hand-off: when a holder
// releases, the freed slot is transferred straight to the next live waiter
// (active stays put) rather than decremented-then-reincremented. release() is
// created only at the moment a slot is actually granted, so a waiter that never
// gets promoted has no release to mis-fire. Every release is idempotent.
const sem = {
    max: MAX_CONCURRENT,
    maxQueue: MAX_QUEUE,
    active: 0,
    waiters: [],
    get queued() { return this.waiters.length; },
    _mkRelease() {
        let done = false;
        return () => { if (done) return; done = true; this._next(); };
    },
    // { ok:true, slot:Promise<release> } — slot resolves now (free) or later
    // (queued). { ok:false } — queue full, caller must reject the request.
    acquire() {
        if (this.active < this.max) {
            this.active++;
            return { ok: true, slot: Promise.resolve(this._mkRelease()) };
        }
        if (this.waiters.length >= this.maxQueue) return { ok: false };
        const waiter = { cancelled: false, enqueuedAt: Date.now() };
        waiter.slot = new Promise((resolve, reject) => {
            waiter._promote = () => resolve(this._mkRelease());
            waiter.timer = setTimeout(() => {
                this._removeWaiter(waiter);
                reject(new Error('queue_timeout'));
            }, QUEUE_TIMEOUT_MS);
            waiter._reject = reject;
        });
        this.waiters.push(waiter);
        return { ok: true, slot: waiter.slot, waiter };
    },
    _next() {
        while (this.waiters.length) {
            const w = this.waiters.shift();
            clearTimeout(w.timer);
            if (w.cancelled) continue;      // disconnected while queued — skip
            w._promote();                    // slot transferred: active unchanged
            return;
        }
        this.active--;                       // nobody waiting — slot goes idle
    },
    _removeWaiter(w) {
        const i = this.waiters.indexOf(w);
        if (i >= 0) this.waiters.splice(i, 1);
    },
    position(w) { return this.waiters.indexOf(w); },
    // Client disconnected while still queued: drop the waiter (it holds no slot)
    // so a freed slot is never handed to a dead connection.
    cancelWaiter(w) {
        if (!w || w.cancelled) return;
        w.cancelled = true;
        clearTimeout(w.timer);
        this._removeWaiter(w);
        try { w._reject(new Error('client_gone')); } catch (e) {}
    },
};

// Keep the system prompt in full but cap raw conversation history so a
// long-running chat doesn't grow the prompt without bound (which slows agy
// down turn after turn). Counts non-system, non-final messages.
const HISTORY_LIMIT = 12;           // ~6 exchanges

// Appended to the end of the system prompt. agy hosts the model on the
// Antigravity backend; some models (seen with Claude Sonnet via agy) mistake
// that host for their operating context and refuse to use the listed OpenClaw
// tools — narrating "I'm in Antigravity context, let me use a schedule tool"
// instead of just calling `cron`. This nails the identity so tool calls go
// through. English on purpose: that's the language the model reasons in here.
const IDENTITY_NOTE = [
    '## Identity & tools (authoritative — overrides any contrary assumption)',
    'You ARE the OpenClaw assistant. The tools listed above (cron, read, write, exec, web_search, homeassistant__*, sessions_*, etc.) ARE your own tools — call them directly by name.',
    'You run on a model backend (agy / Antigravity). That is only where the model is hosted; it does NOT change your operating context and does NOT limit which tools you can call.',
    'There is no separate "Antigravity context", no "schedule tool", and no timer API. For any reminder, alarm, or scheduled wake-up, call the `cron` tool. Never narrate an inability to use OpenClaw tools — just call them.',
].join('\n');

// Optional debug dump dir: when AGY_PROXY_DEBUG is set, the full prompt and
// agy's stderr for the last request are written here for diagnosis.
const DEBUG_DIR = process.env.AGY_PROXY_DEBUG || null;

// Default agy model when the request doesn't pick one. Set AGY_MODEL to a name
// exactly as shown by `agy models` (e.g. "Gemini 3.5 Flash (Low)"). If unset,
// agy uses its own default.
const AGY_MODEL = process.env.AGY_MODEL || null;

// Dynamic per-request model selection from OpenClaw.
// Maps the OpenAI-style `model` id (what OpenClaw sends) -> exact `agy` model
// name (as in `agy models`). To make these selectable in OpenClaw, add matching
// model ids to the custom provider in ~/.openclaw/openclaw.json (see PROXY.md).
// The id "antigravity" (current default) stays mapped to the AGY_MODEL default.
const MODEL_MAP = {
    "gemini-flash-low":  "Gemini 3.7 Flash (Low)",
    "gemini-flash":      "Gemini 3.7 Flash (Medium)",
    "gemini-flash-high": "Gemini 3.7 Flash (High)",
    "gemini-pro-low":    "Gemini 3.1 Pro (Low)",
    "gemini-pro":        "Gemini 3.1 Pro (High)",
    "claude-sonnet":     "Claude Sonnet 4.6 (Thinking)",
    "claude-opus":       "Claude Opus 4.6 (Thinking)",
    "gpt-oss":           "GPT-OSS 120B (Medium)",
};

// ---- Permission roles ----------------------------------------------------
// agy calls Home Assistant (and Nextcloud, etc.) through its OWN mcp_config,
// never through OpenClaw's tool layer, so OpenClaw's tools.deny can't reach it.
// The only real control point is agy's own config, selected here by HOME:
//   full — the process HOME (production profile), --dangerously-skip-permissions
//          auto-approves every tool (current behaviour, watchdog path).
//   ro   — an isolated HOME (~/.agy-profiles/ro) whose settings.json has NO
//          skip-permissions and a narrow permissions.allow of read-only tools.
//          Verified 2026-08-06 (agy 1.1.10): a tool NOT on the allowlist is
//          auto-DENIED in --print mode (clean, ~9s, no hang), reads still work,
//          and matching is EXACT tool name (no substring, no glob). See
//          PLAN_ACL_LIMITS.md §7. Optional --mode plan (AGY_RO_PLAN=1) adds an
//          orthogonal write-block layer that still permits reads.
// The role is picked by a `-ro` suffix on the model id (see resolveAgyModel):
// registering `*-ro` ids in openclaw.json makes OpenClaw's own model allowlist
// the gate for which agent may reach the full role at all.
const ROLES = {
    full: { home: null, args: ['--dangerously-skip-permissions'] },
    ro:   {
        home: process.env.AGY_RO_HOME || path.join(os.homedir(), '.agy-profiles/ro'),
        args: process.env.AGY_RO_PLAN === '1' ? ['--mode', 'plan'] : [],
    },
};
const DEFAULT_ROLE = (process.env.AGY_DEFAULT_ROLE === 'ro') ? 'ro' : 'full'; // backward compatible
// Live per-role slot occupancy, surfaced by /healthz.
const roleActive = { full: 0, ro: 0 };

// Resolve the requested OpenAI model id to an agy model name + permission role.
// Accepts our short ids, a "provider/id" form, or an exact agy model name.
// A trailing `-ro` on the id selects the read-only role; the base id is then
// resolved as usual. "antigravity" / empty -> the AGY_MODEL default (may be
// null = agy default). Unknown ids are an ERROR (not a silent fallback): a typo
// like "gmeini-flash" used to silently route to the default, undebuggable.
function resolveAgyModel(requested) {
    if (!requested) return { model: AGY_MODEL, role: DEFAULT_ROLE };
    let id = requested.includes('/') ? requested.split('/').pop() : requested;
    let role = DEFAULT_ROLE;
    if (id.endsWith('-ro')) { role = 'ro'; id = id.slice(0, -3); }
    if (id === 'antigravity') return { model: AGY_MODEL, role };
    if (MODEL_MAP[id]) return { model: MODEL_MAP[id], role };
    if (Object.values(MODEL_MAP).includes(requested)) return { model: requested, role }; // exact agy name
    return { error: `Nieznany model "${requested}". Dostępne id: antigravity, ${Object.keys(MODEL_MAP).join(', ')} (sufiks -ro = rola read-only)` };
}

// agy >= 1.1.0 swallows backend errors in --print mode: exit code 0, empty
// stdout, empty stderr. The only trace is its glog log file, so each request
// runs with a private --log-file and we mine it for the real error.
// glog error lines look like: "E0708 11:44:42.620606 98256 log.go:398] msg"
// Lines that describe the CAUSE of a failure, whenever one is present.
const GLOG_CAUSE_RE = /RESOURCE_EXHAUSTED|permission check failed|quota reached|user denied permission/i;
// Lines that are consequences of the run ending — several of them are produced
// by our OWN kill, so reporting them as "the error" is circular. Measured
// 2026-09-11 on preserved glogs: a run killed by the STUCK watchdog reported
// "error running grep: signal: killed" (the grep we killed) and two quota
// failures reported "You are not logged into Antigravity" (torn-down auth
// during shutdown) instead of the quota line that actually caused them.
const GLOG_TEARDOWN_RE = /signal: killed|app root path missing|Language server shutting down|Language server shutdown timed out|skipping empty or temp file|store manager shutting down/i;

function extractAgyError(logText) {
    if (!logText) return null;
    const strip = l => l.replace(/^[EIWF]\d{4} \S+\s+\d+ \S+\]\s*/, '').trim();
    const all = logText.split('\n');
    const errLines = all.filter(l => /^E\d{4} /.test(l)).map(strip).filter(Boolean);
    // A cause can be logged below error level: since the quota fail-fast kills
    // agy on its FIRST 429, agy never reaches its own E-level "agent executor
    // error: ... RESOURCE_EXHAUSTED" summary, and the only trace left is the
    // I-level "Run: attempt N failed (RESOURCE_EXHAUSTED ...)". So look for
    // causes across all levels, and only fall back to E-lines for the rest.
    const causes = all.filter(l => GLOG_CAUSE_RE.test(l)).map(strip).filter(Boolean);
    if (!errLines.length && !causes.length) return null;
    // Prefer a causal line; else the newest E-line that isn't teardown noise;
    // else fall back to the newest E-line at all, so something is always reported.
    const nonNoise = errLines.filter(l => !GLOG_TEARDOWN_RE.test(l));
    const pool = causes.length ? causes : (nonNoise.length ? nonNoise : errLines);
    let msg = pool[pool.length - 1];
    // agy doubles the message as "X: X" — collapse the repeat.
    const dup = msg.match(/^(.*): \1$/s);
    if (dup) msg = dup[1];
    return msg;
}

// Turn a raw agy error into a short, user-facing description (Polish, since
// that's what lands in the chat). Known cases get a friendly form; anything
// else is passed through truncated.
function summarizeAgyError(raw) {
    if (!raw) return null;
    if (/RESOURCE_EXHAUSTED|code 429/.test(raw)) {
        const reset = raw.match(/Resets in ([\dhms]+)/);
        return `limit (quota) modelu wyczerpany — kod 429${reset ? `, reset za ${reset[1]}` : ''}`;
    }
    return raw.length > 300 ? raw.slice(0, 300) + '…' : raw;
}

// Classify a permission denial from a finished run. Two signals, both in text
// the caller already has:
//   1. errOut/stdout carry agy's headless auto-deny notice — the reliable
//      DETECTOR: `a tool required the "<class>" permission that headless mode
//      cannot prompt for`.
//   2. The glog (proxy's --print run puts --print last, so --log-file IS
//      populated) carries the precise line, with class AND target together:
//      `permission_manager.go:954] permission check failed for <class> "<target>"`.
// Prefer (2) for a self-consistent class+target; fall back to (1)'s class with a
// null target. Returns {permClass, target} or null if this was not a denial.
function classifyDenial(logText, errOut, out) {
    const glog = (logText || '').match(/permission check failed for (\S+)\s+"([^"]+)"/);
    if (glog) return { permClass: glog[1], target: glog[2] };
    const notice = `${errOut || ''}\n${out || ''}\n${logText || ''}`
        .match(/required the "?(mcp|command|read_file)"? permission that headless mode cannot prompt for/);
    if (notice) return { permClass: notice[1], target: null };
    // Third signature, and on 2026-09-11 the ONLY one present in the glog of a
    // real RO denial: agy's own "soft-deny" line. It names the tool, not the
    // permission class, so map it; the concrete target comes from the
    // trajectory DB (readDenialDetail).
    const soft = (logText || '').match(/soft-denying tool confirmation "([^"]+)"/);
    if (soft) {
        const tool = soft[1];
        const permClass = /^run/i.test(tool) ? 'command'
            : /^(view|read)/i.test(tool) ? 'read_file'
            : /mcp/i.test(tool) ? 'mcp'
            : tool;
        return { permClass, target: null };
    }
    return null;
}

// The exact thing agy was denied, read from the conversation trajectory: the
// glog records only the tool NAME ("RunCommand"), while the DB row carries the
// full command line / file path plus agy's error text. Without this the
// operator was told "Cel nieustalony" and had nothing to put on the allowlist.
function readDenialDetail(dbPath) {
    if (!DatabaseSync || !dbPath) return null;
    let db;
    try {
        db = new DatabaseSync(dbPath, { readOnly: true });
        const rows = db.prepare('select idx, metadata, error_details from steps where error_details is not null order by idx desc limit 1').all();
        if (!rows.length) return null;
        const meta = rows[0].metadata ? Buffer.from(rows[0].metadata).toString('utf8') : '';
        const err = rows[0].error_details ? Buffer.from(rows[0].error_details).toString('utf8') : '';
        const cmd = meta.match(/"CommandLine":"((?:[^"\\]|\\.){0,400})"/);
        const file = meta.match(/"AbsolutePath":"([^"]{0,300})"/);
        const label = meta.match(/"toolAction":"([^"]{0,80})"/);
        const target = cmd ? cmd[1].replace(/\\"/g, '"').replace(/\\\\/g, '\\') : (file ? file[1] : null);
        // error_details is a protobuf blob; keep only its readable run of text.
        const msg = err.replace(/[^\x20-\x7E -ɏ]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 200) || null;
        return { target, label: label ? label[1] : null, msg };
    } catch (e) {
        return null;
    } finally {
        try { db && db.close(); } catch (e) {}
    }
}

// User-facing message for a permission denial. Always names the role and the
// profile's settings.json so the operator knows exactly which allowlist to edit;
// names the concrete target when it could be recovered from agy's log.
function buildPermissionError(role, permClass, target, settingsPath, label) {
    const where = settingsPath ? ` Dodaj regułę w ${settingsPath}.` : '';
    const what = label ? ` (krok agy: "${label}")` : '';
    if (target) {
        return `agy odmówił uprawnienia: narzędzie klasy '${permClass}' na cel "${target}" nie jest w allowliście roli '${role}'${what}. Dopisz '${permClass}(${target})' do allowlisty.${where}`;
    }
    return `agy odmówił uprawnienia klasy '${permClass}' (rola '${role}')${what}. Celu nie udało się ustalić ani z logu, ani z trajektorii — dodaj regułę '${permClass}(<cel>)' w allowliście.${where}`;
}

// Machine-readable label for a failed run. Kept as one function so the stream
// and buffered paths cannot drift apart — they previously disagreed (buffered
// collapsed every timeout into 'agy_failed'), which made proxy logs and cron
// history describe the same failure differently.
// Minutes for humans: STUCK_NO_OUTPUT_MS is env-tunable, so a raw /60000
// printed things like "0.16666666666666666 min" in the user-facing message.
const mins = ms => Number((ms / 60000).toFixed(1));

function failureCode({ spawnErr, timedOut, code, emptyFail, quotaExhausted }) {
    if (quotaExhausted) return 'quota_exhausted';
    if (spawnErr) return 'spawn_failed';
    if (timedOut === 'stuck') return 'stuck_no_progress';
    if (timedOut === 'idle') return 'idle_timeout';
    if (timedOut === 'hard') return 'hard_timeout';
    if (code !== 0) return 'agy_failed';
    if (emptyFail) return 'empty_response';
    return 'agy_failed';
}

// Live-tail agy's private glog file and report agent-loop activity. Each
// "streamGenerateContent" line = one model round (the reliable live marker in
// the log; tool executions are not logged here — those are caught separately by
// hasActiveToolChild in the watchdog). Raw file growth is NOT treated as
// activity — glog writes periodic noise (quota refresh etc.) even when the
// agent loop is stuck, and that must not defeat the idle timeout.
// Returns a stop() function; safe if the file doesn't exist yet.
// onQuota (optional) fires ONCE, with the parsed reset delay in ms, the first
// time a RESOURCE_EXHAUSTED line with a reset ETA past QUOTA_FAIL_FAST_MS is
// seen — this is what lets runAgy kill a quota-exhausted attempt immediately
// instead of riding out agy's own multi-minute internal retry loop.
function watchAgyLog(logFile, onRound, onQuota, onConversation) {
    let offset = 0;
    let leftover = '';
    let quotaFired = false;
    let lastConv = null;
    const timer = setInterval(() => {
        let fd;
        try {
            const size = fs.statSync(logFile).size;
            if (size <= offset) return;
            fd = fs.openSync(logFile, 'r');
            const len = size - offset;
            const buf = Buffer.alloc(len);
            fs.readSync(fd, buf, 0, len, offset);
            offset = size;
            const lines = (leftover + buf.toString('utf8')).split('\n');
            leftover = lines.pop();
            for (const line of lines) {
                if (line.includes('streamGenerateContent')) onRound();
                if (onConversation) {
                    // "Streaming conversation <uuid>" names the trajectory DB for
                    // this run; agy can switch conversations mid-run, so react to
                    // the latest one seen, not just the first.
                    const conv = line.match(/Streaming conversation ([0-9a-f-]{36})/);
                    if (conv && conv[1] !== lastConv) { lastConv = conv[1]; onConversation(conv[1]); }
                }
                if (!quotaFired && onQuota && line.includes('RESOURCE_EXHAUSTED')) {
                    const resetMs = matchQuotaReset(line);
                    if (resetMs !== null && resetMs >= QUOTA_FAIL_FAST_MS) {
                        quotaFired = true;
                        onQuota(resetMs);
                    }
                }
            }
        } catch (e) {
            // log file not created yet, or vanished — ignore
        } finally {
            if (fd !== undefined) { try { fs.closeSync(fd); } catch (e) {} }
        }
    }, LOG_POLL_MS);
    return () => clearInterval(timer);
}

// Is agy currently running a tool? agy spawns tool commands (shell scripts,
// curl, ssh, xdg-open) as processes in its own process group (pgid == agy pid,
// since it was spawned detached). A live descendant CAN mean agy is busy
// executing a tool — legitimate activity that produces no glog
// "streamGenerateContent" line and no stdout for minutes. We read /proc directly
// (no subprocess spawn): field 5 of /proc/<pid>/stat is the process-group id;
// comm (field 2) may hold spaces/parens, so we parse from just after the final ')'.
//
// Only a child in state R (running) or D (uninterruptible IO) counts. agy keeps
// long-lived MCP servers (e.g. mcp-nextcloud-rag) as children that sit idle in
// state S the whole run; counting those made hasActiveToolChild permanently true,
// which silently defeated BOTH the idle and stuck detectors so every wedged run
// rode HARD to 30 min. An idle background server is S and must be ignored; a
// foreground tool doing work is R or D.
//
// Walks the PPID tree, NOT the process group: verified empirically
// (2026-09-10, `ps --forest` while a bash tool ran) that agy puts every tool
// subprocess in its OWN new process group (setpgid/setsid on spawn) — a tool
// child's pgrp is its own pid, never agy's. A pgid-equality check (the
// original approach) therefore never matches a real tool child; it only
// happened to look correct because it also never false-positived. PPID is
// the only link back to agy that survives that regrouping, so this walks
// descendants (any depth — a tool can itself fork, e.g. bash -> find) via
// parent-pid, not process-group membership.
//
// Returns { active, comm } — comm is just the binary name (e.g. "curl",
// "bash"), never the full argv, so it's safe to show a viewer: no path, no
// flags, no secrets that a command's arguments might carry (a token in a curl
// -H, a password in a URL). If more than one descendant is busy, the first
// R/D hit wins — good enough for a "what's it doing" status line, not an audit.
function hasActiveToolChild(rootPid) {
    let procs;
    try { procs = fs.readdirSync('/proc'); } catch (e) { return { active: false, comm: null }; }
    const childrenOf = new Map(); // ppid -> [{pid, state, comm}]
    for (const name of procs) {
        if (!/^\d+$/.test(name)) continue;
        const pid = parseInt(name, 10);
        if (pid === rootPid) continue;
        let stat;
        try { stat = fs.readFileSync(`/proc/${name}/stat`, 'utf8'); } catch (e) { continue; }
        const lp = stat.indexOf('(');
        const rp = stat.lastIndexOf(')');
        if (lp === -1 || rp === -1) continue;
        const comm = stat.slice(lp + 1, rp);
        // after ') ' come: state(3) ppid(4) ...
        const fields = stat.slice(rp + 2).trim().split(/\s+/);
        const ppid = parseInt(fields[1], 10);
        const state = fields[0];
        if (!childrenOf.has(ppid)) childrenOf.set(ppid, []);
        childrenOf.get(ppid).push({ pid, state, comm });
    }
    const queue = [rootPid];
    const seen = new Set(queue);
    while (queue.length) {
        const pid = queue.shift();
        for (const c of childrenOf.get(pid) || []) {
            if (seen.has(c.pid)) continue;
            seen.add(c.pid);
            if (c.state === 'R' || c.state === 'D') return { active: true, comm: c.comm }; // busy; keep scanning past idle (S) descendants
            queue.push(c.pid);
        }
    }
    return { active: false, comm: null };
}

// Broad, safe-to-display category for a tool child's binary name — no args,
// so nothing a command's arguments might carry (tokens, paths, secrets) ever
// reaches this. Unrecognized binaries still get a label (the name itself is
// just a program name, not sensitive).
const TOOL_CATEGORIES = [
    { match: /^(bash|sh|zsh|dash|python3?|node|ruby|perl|php)\d*$/, label: 'wykonuje kod/skrypt' },
    { match: /^(curl|wget)$/, label: 'pobiera dane z sieci' },
    { match: /^(ssh|sshfs|scp|rsync)$/, label: 'łączy się z innym hostem' },
    { match: /^git$/, label: 'operacja git' },
    { match: /^(grep|rg|find|fd|cat|awk|sed|ls)$/, label: 'przeszukuje/czyta pliki' },
    { match: /^(chromium|chrome|xdg-open)$/i, label: 'otwiera przeglądarkę' },
];
function classifyTool(comm) {
    if (!comm) return null;
    const rule = TOOL_CATEGORIES.find(r => r.match.test(comm));
    return rule ? rule.label : `wykonuje narzędzie (${comm})`;
}

// Where agy stores the trajectory for one conversation. The RO role runs under
// its own HOME, so the path follows the role's HOME, not the proxy's.
function convDbPath(home, convId) {
    return path.join(home || os.homedir(), '.gemini/antigravity-cli/conversations', `${convId}.db`);
}

// Rows agy appended to the trajectory since `sinceIdx`. Read-only, and safe to
// call while agy is writing: the DB runs in WAL mode, so a reader never blocks
// the writer (validated live — 40+ polls during an active run, zero lock
// errors). `step_payload` is deliberately NOT selected: it holds the full
// prompt/response (100KB+ per row), while `metadata` is a couple of KB and
// already carries the tool name and agy's own human-readable action label.
// Any failure (file not created yet, torn write, schema change) returns [] —
// the caller then just falls back to the other progress signals.
function readNewSteps(dbPath, sinceIdx) {
    if (!DatabaseSync || !dbPath) return [];
    let db;
    try {
        db = new DatabaseSync(dbPath, { readOnly: true });
        return db.prepare('select idx, status, metadata from steps where idx > ? order by idx').all(sinceIdx)
            .map(r => {
                const meta = r.metadata ? Buffer.from(r.metadata).toString('utf8') : '';
                const label = meta.match(/"toolAction":"([^"]{0,80})"/);
                return { idx: r.idx, status: r.status, label: label ? label[1] : null };
            });
    } catch (e) {
        return [];
    } finally {
        try { db && db.close(); } catch (e) {}
    }
}

// Extract plain text from an OpenAI message `content` field, which may be a
// string or an array of parts like [{type:"text", text:"..."}].
function contentToText(content) {
    if (typeof content === 'string') return content;
    if (Array.isArray(content)) {
        return content
            .filter(c => c && c.type === 'text')
            .map(c => c.text)
            .join('\n');
    }
    if (content == null) return '';
    return JSON.stringify(content);
}

// Strip OpenClaw metadata timestamps like "[Fri 2026-07-03 19:15 GMT+2] ".
function stripTimestamp(text) {
    return text.replace(/^\[.*?\]\s*/, '').trim();
}

// Exact-match control sentinels that OpenClaw cron/heartbeat contracts compare
// verbatim (e.g. "if reply === NO_REPLY, suppress delivery"). Our own
// "— model: X" footer below used to be appended unconditionally, which broke
// every one of these checks silently: the werdykt was still "NO_REPLY" to a
// human eye but no longer matched byte-for-byte, so it got delivered as if it
// were a real report. Discovered 2026-09-11 (Bartek: "napraw by no reply nie
// przychodziło jako alert"). Prefix-based contracts (HEARTBEAT_OK) mostly
// survived this bug already; kept here too so they can't regress the same way.
const CONTROL_SENTINELS = ['NO_REPLY', 'HEARTBEAT_OK'];
const CONTROL_PREFIXES = ['WATCHDOG_FAIL', 'CANARY-'];
function isControlSentinel(text) {
    const t = text.trim();
    return CONTROL_SENTINELS.includes(t) || CONTROL_PREFIXES.some(p => t.startsWith(p));
}

// Remove our own progress-tick segments (⏳/⌛ …) and error notices
// (⚠️ [proxy] …) so they don't accumulate in the conversation history fed
// back to agy later.
function stripProgress(text) {
    return text
        .replace(/\r?[⏳⌛][^\r\n]*/g, '')
        .replace(/⚠️?\s*\[proxy\][^\r\n]*/g, '')
        .replace(/\n\n— model: [^\r\n]*$/, '') // our own "which model answered" footer
        .replace(/\r/g, '')
        .replace(/^\n+/, '')
        .trim();
}

// agy is stateless between invocations, so we fold the OpenAI-style
// conversation (system + capped history + latest turn) into a single prompt.
function buildPrompt(messages) {
    const systemParts = [];
    const history = [];
    let lastUser = '';

    messages.forEach((msg, idx) => {
        const role = (msg.role || 'user').toLowerCase();
        const text = stripProgress(stripTimestamp(contentToText(msg.content)));
        if (!text) return;

        if (role === 'system') {
            systemParts.push(text);
        } else if (idx === messages.length - 1 && role === 'user') {
            lastUser = text;
        } else {
            history.push(`[${role.toUpperCase()}]: ${text}`);
        }
    });

    // Only the most recent turns matter for context; drop older ones.
    const cappedHistory = history.slice(-HISTORY_LIMIT);

    // Simple, common case: a single user turn with no system prompt.
    if (!systemParts.length && !cappedHistory.length) {
        return lastUser;
    }

    const sections = [];
    if (systemParts.length) {
        sections.push('Instrukcje systemowe:\n' + systemParts.join('\n') + '\n\n' + IDENTITY_NOTE);
    }
    if (cappedHistory.length) {
        sections.push('Kontekst poprzednich wiadomości:\n' + cappedHistory.join('\n'));
    }
    if (lastUser) {
        sections.push('Aktualne zapytanie użytkownika:\n' + lastUser);
    }
    return sections.join('\n\n');
}

const server = http.createServer((req, res) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);

    // Models endpoint: advertise the default plus every selectable id.
    if (req.url === '/v1/models' || req.url === '/models') {
        res.setHeader('Content-Type', 'application/json');
        const baseIds = ["antigravity", ...Object.keys(MODEL_MAP)];
        // Advertise each id in both roles: bare = full (or AGY_DEFAULT_ROLE),
        // `-ro` = read-only. Register the ones you want selectable in openclaw.json.
        const ids = [...baseIds, ...baseIds.map(id => `${id}-ro`)];
        res.end(JSON.stringify({
            object: "list",
            data: ids.map(id => ({ id, object: "model", owned_by: "local" }))
        }));
        return;
    }

    // Liveness/concurrency probe. Consumed by the hourly ha_watchdog so it can
    // skip a proxy that is already saturated instead of piling another spawn on.
    if (req.url === '/healthz') {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({
            active: sem.active,
            queued: sem.queued,
            maxConcurrent: MAX_CONCURRENT,
            maxQueue: MAX_QUEUE,
            roles: { full: roleActive.full, ro: roleActive.ro },
            roHome: ROLES.ro.home,
            defaultRole: DEFAULT_ROLE,
            uptimeSec: Math.round(process.uptime()),
        }));
        return;
    }

    // Recent console output (ring buffer, last LOG_BUFFER_MAX lines) — quick
    // debug from a browser/curl on the Pi without an SSH session + journalctl.
    // ?n=N caps how many of the most recent lines come back (default: all).
    if (req.url === '/logs' || req.url.startsWith('/logs?')) {
        const reqUrl = new URL(req.url, 'http://localhost');
        const n = parseInt(reqUrl.searchParams.get('n'), 10);
        const lines = (Number.isFinite(n) && n > 0) ? logBuffer.slice(-n) : logBuffer;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ lines, total: logBuffer.length, max: LOG_BUFFER_MAX }));
        return;
    }

    if (req.method === 'POST' && (req.url === '/v1/chat/completions' || req.url === '/chat/completions')) {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });

        req.on('end', () => {
            let data;
            try {
                data = JSON.parse(body);
            } catch (e) {
                res.statusCode = 400;
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ error: "Błędny format JSON" }));
                return;
            }

            const messages = data.messages || [];
            const isStream = data.stream === true;
            const modelName = data.model || "antigravity";
            const resolved = resolveAgyModel(data.model);

            if (resolved.error) {
                console.error(`✗ ${resolved.error}`);
                if (isStream) {
                    // For streams, a visible chunk is the only thing the user
                    // actually sees — an HTTP error becomes a generic
                    // "couldn't generate a response" in OpenClaw.
                    res.setHeader('Content-Type', 'text/event-stream');
                    res.write(`data: ${JSON.stringify({
                        id: `chatcmpl-${Date.now()}`,
                        object: "chat.completion.chunk",
                        created: Math.floor(Date.now() / 1000),
                        model: modelName,
                        choices: [{ delta: { content: `⚠️ [proxy] ${resolved.error}` }, index: 0, finish_reason: 'stop' }]
                    })}\n\n`);
                    res.write('data: [DONE]\n\n');
                    res.end();
                } else {
                    res.statusCode = 400;
                    res.setHeader('Content-Type', 'application/json');
                    res.end(JSON.stringify({ error: { message: resolved.error, type: 'invalid_request_error', code: 'unknown_model' } }));
                }
                return;
            }
            const agyModel = resolved.model;
            const role = resolved.role || DEFAULT_ROLE;
            const roleCfg = ROLES[role] || ROLES.full;

            if (!messages.length) {
                res.statusCode = 400;
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ error: "Brak wiadomości w zapytaniu" }));
                return;
            }

            const prompt = buildPrompt(messages);
            const startedAt = Date.now();
            let firstByteAt = 0;
            console.log(`→ agy | stream=${isStream} | rola=${role} | model=${modelName} → agy:${agyModel || '(domyślny)'} | prompt=${prompt.length} zn. | "${prompt.substring(0, 80).replace(/\n/g, ' ')}..."`);

            if (DEBUG_DIR) {
                try { fs.writeFileSync(path.join(DEBUG_DIR, 'last_prompt.txt'), prompt); } catch (e) {}
            }

            // Track the live child so a client disconnect can kill whichever
            // attempt is currently running.
            let currentChild = null;
            let clientGone = false;
            // Concurrency slot: `queueWaiter` is set only while this request is
            // parked in the queue; `heldRelease` is set only once it actually
            // holds a slot. freeSlot() releases exactly once (idempotent) and is
            // called on every terminal path — success, error, timeout, disconnect.
            let queueWaiter = null;
            let heldRelease = null;
            const holdSlot = (release) => { heldRelease = release; roleActive[role]++; };
            const freeSlot = () => { if (heldRelease) { const r = heldRelease; heldRelease = null; roleActive[role]--; r(); } };
            // Kill agy together with its whole process group (detached spawn):
            // grandchildren (tool commands, xdg-open/browser) would otherwise
            // survive and keep the stdio pipes open forever.
            const killTree = (child) => {
                try { process.kill(-child.pid, 'SIGKILL'); } catch (e) {
                    try { child.kill('SIGKILL'); } catch (e2) {}
                }
            };
            res.on('close', () => {
                clientGone = true;
                if (currentChild && currentChild.exitCode === null && currentChild.signalCode === null) {
                    killTree(currentChild);
                }
                // Queued but never started: drop the waiter so its slot isn't
                // handed to a dead connection. Holding a slot: free it after the
                // kill above so a waiter can take over.
                if (queueWaiter) { sem.cancelWaiter(queueWaiter); queueWaiter = null; }
                freeSlot();
            });

            const elapsed = () => ((Date.now() - startedAt) / 1000).toFixed(2);

            // Spawn agy for one attempt. `onChunk` (if given) receives stdout
            // live; otherwise stdout is buffered. `cb` gets the final result.
            // `onStatus` (if given) receives live agent-loop events:
            //   {type:'round', rounds}            — one more model round seen in the log
            //   {type:'extended', rounds}         — SOFT limit crossed, agy active, kept alive
            function runAgy(onChunk, cb, onStatus) {
                let out = "";
                let errOut = "";
                let timedOut = false;   // false | 'idle' | 'hard' | 'stuck'
                let spawnErr = null;
                let quotaExhausted = null;   // { resetMs } once a fail-fast-worthy quota line is seen
                let done = false;
                let rounds = 0;
                // Trajectory-DB progress signal: path resolves once the glog names
                // the conversation, then each poll reports rows appended since.
                let convDb = null;
                let lastStepIdx = -1;
                let lastStepAt = 0;
                const T = timeoutsFor(agyModel);
                let lastActivityAt = Date.now();
                // Real progress = a model round or a stdout byte. Distinct from
                // lastActivityAt (which a live tool child also refreshes): a tool
                // child only keeps the run alive for TOOL_GRACE_MS past this.
                let lastRealActivityAt = Date.now();
                const attemptStart = Date.now();

                // Private glog file per attempt: agy >= 1.1.0 reports backend
                // errors ONLY here (stdout/stderr stay empty, exit code 0).
                const logFile = path.join(os.tmpdir(), `agy-proxy-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.log`);

                // agy >= 1.1.1: `--print` REQUIRES the prompt as its argument
                // (stdin is no longer read; a flag following `--print` would be
                // consumed as the prompt itself). spawn() passes argv without a
                // shell, so no escaping issues; Linux per-arg limit (MAX_ARG_STRLEN)
                // is far above our prompt sizes.
                // Role decides the permission posture: full = skip-permissions
                // (auto-approve), ro = no skip + an isolated HOME whose
                // settings.json allowlist is authoritative (see ROLES).
                const agyArgs = [...roleCfg.args, '--print-timeout', AGY_PRINT_TIMEOUT, '--log-file', logFile];
                if (agyModel) agyArgs.push('--model', agyModel);
                agyArgs.push('--print', prompt);
                const spawnEnv = { ...process.env, NO_COLOR: '1' };
                if (roleCfg.home) spawnEnv.HOME = roleCfg.home;   // isolated RO profile
                // detached: own process group, so a timeout kill also reaps
                // grandchildren agy may spawn (tools, xdg-open, browsers).
                const child = spawn('agy', agyArgs, {
                    stdio: ['ignore', 'pipe', 'pipe'],
                    detached: true,
                    env: spawnEnv
                });
                currentChild = child;

                // Live activity feed from agy's own log: one event per model
                // round of the agent loop. Feeds both the user-facing status
                // line and the adaptive-timeout watchdog.
                const stopLogWatch = watchAgyLog(logFile, () => {
                    rounds++;
                    lastActivityAt = Date.now();
                    lastRealActivityAt = lastActivityAt;
                    if (onStatus) onStatus({ type: 'round', rounds });
                }, (resetMs) => {
                    // Quota exhausted far enough out that agy's own internal
                    // retry loop can't help — kill now instead of holding the
                    // slot for its full ~15-25 min backoff dance.
                    quotaExhausted = { resetMs };
                    console.log(`⏱ agy QUOTA wyczerpana po ${elapsed()}s — reset za ${fmtDuration(resetMs)}, zabijam zamiast czekać na wewnętrzny retry agy`);
                    clearInterval(watchdog);
                    killTree(child);
                    setTimeout(() => finish(-1), 2000).unref();
                }, (convId) => {
                    convDb = convDbPath(roleCfg.home || process.env.HOME, convId);
                    lastStepIdx = -1;   // new conversation: re-read its steps from the start
                });

                // Adaptive timeout. Flat 12-min kill used to cut off tasks agy
                // was still actively working on; now SOFT only marks the point
                // past which continued life requires visible activity (recent
                // model round or stdout). IDLE catches a genuinely stuck agy
                // much earlier than the old flat limit; HARD stays authoritative.
                let extendedNotified = false;
                const watchdog = setInterval(() => {
                    const now = Date.now();
                    const total = now - attemptStart;
                    // A live tool subprocess (R/D state) counts as activity: agy
                    // is busy executing a tool that emits no round line and no
                    // stdout — BUT only within TOOL_GRACE_MS past the last real
                    // progress, so a wedged/runaway child (e.g. find stuck in NFS)
                    // can't mask the detectors forever. Computed once and reused
                    // by both the idle refresh and the stuck check.
                    const tool = (now - lastRealActivityAt < TOOL_GRACE_MS) && child.pid
                        ? hasActiveToolChild(child.pid)
                        : { active: false, comm: null };
                    const toolActive = tool.active;
                    if (now - lastActivityAt >= WATCHDOG_MS && toolActive) {
                        lastActivityAt = now;
                    }
                    // Rows appended to agy's trajectory since the last tick are the
                    // authoritative progress signal: they capture sub-second tool
                    // calls that the /proc sampler above structurally misses.
                    const newSteps = convDb ? readNewSteps(convDb, lastStepIdx) : [];
                    if (newSteps.length) {
                        lastStepIdx = newSteps[newSteps.length - 1].idx;
                        lastStepAt = now;
                        lastActivityAt = now;
                        lastRealActivityAt = now;
                    }
                    // What to show the user. agy writes its own human-readable
                    // label per step ("Reading WATCHDOG.md", "Sending canary
                    // message") — always better than our guess from a process
                    // name, and it also covers tools that spawn no process at all
                    // (file reads), which /proc can never see. Fall back to the
                    // process-name category only when no labelled step is new.
                    const labelled = [...newSteps].reverse().find(s => s.label);
                    if (onStatus && (labelled || toolActive)) {
                        onStatus({ type: 'tool', label: labelled ? labelled.label : classifyTool(tool.comm) });
                    }
                    const stepsRecent = lastStepAt > 0 && (now - lastStepAt) < STEP_PROGRESS_WINDOW_MS;
                    const idle = now - lastActivityAt;
                    if (!firstByteAt && rounds >= STUCK_MIN_ROUNDS && total >= T.stuck
                        && !toolActive && !stepsRecent) {
                        timedOut = 'stuck';
                        console.log(`⏱ agy STUCK po ${elapsed()}s — ${rounds} rund modelu, 0 tokenów odpowiedzi, brak żywego narzędzia i brak nowych kroków trajektorii od ${Math.round(STEP_PROGRESS_WINDOW_MS / 1000)}s (kroków łącznie: ${lastStepIdx + 1}); pętla bez postępu, zabijam`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (total >= T.hard) {
                        timedOut = 'hard';
                        console.log(`⏱ agy HARD timeout (${T.hard / 60000} min) po ${elapsed()}s — zabijam proces (rund: ${rounds})`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (idle >= T.idle) {
                        timedOut = 'idle';
                        console.log(`⏱ agy IDLE timeout (${T.idle / 60000} min bez aktywności) po ${elapsed()}s — zabijam proces (rund: ${rounds})`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (total >= T.soft && !extendedNotified) {
                        extendedNotified = true;
                        console.log(`⏱ SOFT limit (${T.soft / 60000} min) minięty po ${elapsed()}s, agy aktywny (rund: ${rounds}) — przedłużam do max ${T.hard / 60000} min`);
                        if (onStatus) onStatus({ type: 'extended', rounds });
                    }
                }, WATCHDOG_MS);

                const finish = (code) => {
                    if (done) return;
                    done = true;
                    clearInterval(watchdog);
                    stopLogWatch();
                    let logErr = null;
                    let logText = '';
                    try { logText = fs.readFileSync(logFile, 'utf8'); } catch (e) {}
                    try { logErr = extractAgyError(logText); } catch (e) {}
                    // Permission denial is its own failure mode: agy auto-denies a
                    // tool whose permission class is not on the role's allowlist.
                    // classifyDenial gets the class (+ target, when the glog logged
                    // it) from text we already hold. settingsPath points the
                    // operator at the exact allowlist to edit for this role.
                    const denial = classifyDenial(logText, errOut, out);
                    const permissionClass = denial ? denial.permClass : null;
                    let permissionTarget = null, settingsPath = null, permissionLabel = null;
                    if (denial) {
                        permissionTarget = denial.target;
                        // The glog names the tool but usually not what it tried to
                        // touch; the trajectory row has the exact command/path.
                        if (!permissionTarget && convDb) {
                            const detail = readDenialDetail(convDb);
                            if (detail) {
                                permissionTarget = detail.target;
                                permissionLabel = detail.label;
                            }
                        }
                        const agyHome = (roleCfg && roleCfg.home) || process.env.HOME || os.homedir();
                        settingsPath = path.join(agyHome, '.gemini/antigravity-cli/settings.json');
                    }
                    // Preserve the glog on ANY error termination — not just timeouts.
                    // It is the only record of which file/tool the failed run
                    // touched. A success (clean exit WITH real output) is deleted as
                    // before. Prefix encodes the cause for later triage.
                    const hadOutput = !!firstByteAt;
                    let errKind = null;
                    if (quotaExhausted)          errKind = 'quota';
                    else if (timedOut)           errKind = 'timeout';
                    else if (spawnErr)           errKind = 'fail';
                    else if (permissionClass)    errKind = 'denied';
                    else if (code !== 0)         errKind = 'fail';
                    else if (!hadOutput)         errKind = 'fail'; // clean exit, empty output
                    let savedGlog = null;
                    if (errKind && logText) {
                        savedGlog = path.join(os.tmpdir(), `agy-${errKind}-${process.pid}-${Date.now()}.log`);
                        try { fs.writeFileSync(savedGlog, logText); } catch (e) { savedGlog = null; }
                        const tail = logText.trimEnd().split('\n').slice(-25).join('\n');
                        console.log(`⛏ agy ${errKind} glog zachowany: ${savedGlog}\n----- ogon glog -----\n${tail}\n----- koniec -----`);
                    }
                    try { fs.unlinkSync(logFile); } catch (e) {}
                    cb({ code, out, errOut, timedOut, spawnErr, logErr, rounds, permissionClass, permissionTarget, settingsPath, permissionLabel, savedGlog, quotaExhausted, T, steps: lastStepIdx + 1 });
                };

                child.stdout.on('data', (c) => {
                    const s = c.toString();
                    lastActivityAt = Date.now();
                    lastRealActivityAt = lastActivityAt;
                    if (!firstByteAt) {
                        firstByteAt = Date.now();
                        console.log(`  ↳ pierwszy token po ${elapsed()}s`);
                    }
                    if (onChunk) onChunk(s); else out += s;
                });
                child.stderr.on('data', (c) => {
                    errOut += c.toString();
                    if (DEBUG_DIR) {
                        try { fs.appendFileSync(path.join(DEBUG_DIR, 'last_stderr.txt'), c); } catch (e) {}
                    }
                });
                child.on('error', (err) => { spawnErr = err; finish(-1); });
                // 'exit', not 'close': 'close' waits for stdio pipes to drain,
                // and a grandchild inheriting the pipe (e.g. a browser opened
                // via xdg-open) can hold it open forever — watchdog then spams
                // kills for hours. 'exit' fires as soon as agy itself dies;
                // a short delay lets already-buffered stdout flush first.
                child.on('exit', (code) => setTimeout(() => finish(code), 250));
            }

            const requestId = `chatcmpl-${Date.now()}`;
            const createdTime = Math.floor(Date.now() / 1000);

            function sendDelta(content, finishReason = null) {
                res.write(`data: ${JSON.stringify({
                    id: requestId,
                    object: "chat.completion.chunk",
                    created: createdTime,
                    model: modelName,
                    choices: [{ delta: content ? { content } : {}, index: 0, finish_reason: finishReason }]
                })}\n\n`);
            }

            function finalLog(code, rounds, steps) {
                const ttft = firstByteAt ? ((firstByteAt - startedAt) / 1000).toFixed(2) : 'n/a';
                console.log(`← agy zakończone | kod=${code} | całość=${elapsed()}s | TTFT=${ttft}s | rund=${rounds ?? '?'} | kroków=${steps ?? '?'}`);
            }

            if (isStream) {
                // Real live streaming: forward agy's stdout to the client as it
                // arrives. Retry is still possible as long as nothing has been
                // sent yet (a fast failure before the first token).
                res.setHeader('Content-Type', 'text/event-stream');
                res.setHeader('Cache-Control', 'no-cache');
                res.setHeader('Connection', 'keep-alive');

                let sentAny = false;
                let fullOut = '';   // mirrors `out` in the buffered path; onChunk below never touches `out` itself (see runAgy), so the sentinel check needs its own accumulator

                // Visible progress while agy is silent. agy --print emits
                // nothing on stdout until the very end of heavy tasks, so the
                // live agent-loop events from the log watcher (model rounds)
                // are the only real progress signal. OpenClaw's renderer does
                // not honor \r — every visible line stacks permanently — so
                // visible lines are rationed: first "working" notice at the
                // first heartbeat tick, then round updates at most every
                // STATUS_MIN_GAP_MS, plus one "extending timeout" notice when
                // the SOFT limit is crossed. Between visible lines the
                // connection stays alive with invisible SSE comments. All
                // visible lines start with ⏳ so stripProgress removes them
                // from future conversation history. Everything stops once real
                // output begins.
                let lastVisibleAt = 0;
                let lastShownRounds = 0;
                let lastShownTool = null;
                const elapsedMin = () => Math.max(1, Math.round((Date.now() - startedAt) / 60000));

                const showStatus = (text) => {
                    if (res.writableEnded || sentAny) return;
                    sendDelta(text);
                    lastVisibleAt = Date.now();
                };

                const heartbeat = setInterval(() => {
                    if (res.writableEnded || sentAny) return;
                    // Still parked in the queue: keep the client's stall detector
                    // fed (it aborts after ~400s of no stream progress) with a
                    // rationed visible position tick, invisible keepalives between.
                    if (queueWaiter) {
                        if (!lastVisibleAt || Date.now() - lastVisibleAt >= PROGRESS_MAX_GAP_MS) {
                            showStatus(`⏳ [kolejka] czekam na wolny slot agy (pozycja ${sem.position(queueWaiter) + 1})…\n`);
                        } else {
                            sendDelta(null);
                            res.write(`: queued ${elapsed()}s\n\n`);
                        }
                        return;
                    }
                    if (!lastVisibleAt) {
                        showStatus('⏳ Pracuję nad zadaniem, może potrwać kilka minut…\n');
                    } else if (Date.now() - lastVisibleAt >= PROGRESS_MAX_GAP_MS) {
                        // OpenClaw's stall detector aborts a run after ~400s with
                        // no stream progress (invisible SSE comments do NOT count).
                        // Rationed round-updates go silent during a long single
                        // tool call, so force a visible tick well under that
                        // threshold — a real content delta the client counts as
                        // progress. Starts with ⏳ so stripProgress drops it later.
                        showStatus(`⏳ [${elapsedMin()} min] pracuję nad zadaniem…\n`);
                    } else {
                        // Between visible ticks: an empty-delta chunk still reads
                        // as stream progress for the client but renders nothing,
                        // plus an invisible SSE comment as a plain keepalive.
                        sendDelta(null);
                        res.write(`: keepalive ${elapsed()}s\n\n`);
                    }
                }, HEARTBEAT_MS);

                // Latest step label agy reported but that hasn't been rendered yet.
                // Round ticks arrive far more often than trajectory labels, so
                // without this the generic "runda N" would always win the
                // rationing window and the informative line would never show.
                let pendingTool = null;
                const onStatus = (ev) => {
                    if (ev.type === 'extended') {
                        showStatus(`⏳ [${elapsedMin()} min] agy wciąż aktywnie pracuje (runda ${ev.rounds}) — przedłużam limit czasu…\n`);
                    } else if (ev.type === 'round'
                        && ev.rounds > lastShownRounds
                        && lastVisibleAt
                        && Date.now() - lastVisibleAt >= STATUS_MIN_GAP_MS) {
                        lastShownRounds = ev.rounds;
                        if (pendingTool) {   // a concrete step beats a bare round counter
                            const label = pendingTool;
                            pendingTool = null;
                            lastShownTool = label;
                            showStatus(`⏳ [${elapsedMin()} min] agy ${label.replace(/\s+/g, ' ').trim().slice(0, 80)}…\n`);
                        } else {
                            showStatus(`⏳ [${elapsedMin()} min] agy pracuje — runda ${ev.rounds} zapytań do modelu…\n`);
                        }
                    } else if (ev.type === 'tool' && ev.label && ev.label !== lastShownTool
                        && !(lastVisibleAt && Date.now() - lastVisibleAt >= STATUS_MIN_GAP_MS)) {
                        pendingTool = ev.label;   // too soon to render — remember it for the next window
                    } else if (ev.type === 'tool'
                        && ev.label
                        && ev.label !== lastShownTool
                        && lastVisibleAt
                        && Date.now() - lastVisibleAt >= STATUS_MIN_GAP_MS) {
                        // What agy is doing, from its own per-step label when the
                        // trajectory has one, else the coarse process-name category.
                        // Neither is raw argv: classifyTool reads only the binary
                        // name, and the label is agy's own short summary of the
                        // step — the same voice as the answer this user already
                        // receives, so it adds no exposure a command line would.
                        // Still normalised before rendering: one line, bounded.
                        lastShownTool = ev.label;
                        pendingTool = null;
                        const shown = ev.label.replace(/\s+/g, ' ').trim().slice(0, 80);
                        showStatus(`⏳ [${elapsedMin()} min] agy ${shown}…\n`);
                    }
                };

                const streamAttempt = (n) => {
                    runAgy(
                        (chunk) => { sentAny = true; fullOut += chunk; sendDelta(chunk); },
                        (result) => {
                            if (clientGone) { clearInterval(heartbeat); freeSlot(); return; }
                            const { code, errOut, timedOut, spawnErr, logErr, rounds, permissionClass, permissionTarget, settingsPath, permissionLabel, quotaExhausted, T } = result;
                            // Permission denial: a distinct, DETERMINISTIC failure —
                            // agy auto-denied a tool not on the role's allowlist.
                            // Never retried (a retry re-denies, wasting a spawn), and
                            // surfaced as an ERROR event, not a content delta: a
                            // content delta is exactly what made OpenClaw read the
                            // failed run as a success.
                            const permissionDenied = !!permissionClass && !sentAny;
                            // IDLE is retried like a transient failure: it fires on a
                            // genuinely stalled model call (network hang), and retrying
                            // inside proxy — once, only if nothing reached the client yet —
                            // beats surfacing an error and letting OpenClaw re-run the whole
                            // turn externally (which doubled the wait, see STUCK comment
                            // above). HARD and STUCK stay authoritative/non-retried.
                            // Explicitly excludes a denial (code is 0 there anyway) and a
                            // quota kill (retrying hits the same exhausted quota).
                            const transientFail = !spawnErr && !permissionDenied && !quotaExhausted && (!timedOut || timedOut === 'idle') && code !== 0;
                            // agy >= 1.1.0 error signature: clean exit, zero
                            // output. The real cause sits in logErr. Not
                            // retried: the dominant case (quota 429) won't
                            // clear on retry and OpenClaw retries once anyway.
                            const emptyFail = !spawnErr && !permissionDenied && !quotaExhausted && !timedOut && code === 0 && !sentAny;

                            if (transientFail && !sentAny && n < MAX_ATTEMPTS) {
                                console.log(`↻ agy próba ${n} nieudana (kod ${code}${timedOut ? `, timedOut=${timedOut}` : ''}) — ponawiam. stderr: ${errOut.trim().slice(0, 200)}`);
                                return streamAttempt(n + 1);
                            }

                            clearInterval(heartbeat);
                            if (quotaExhausted) {
                                const full = `Kwota (limit) modelu ${modelName} wyczerpana — reset za ${fmtDuration(quotaExhausted.resetMs)}. Wybierz inny model lub poczekaj.`;
                                console.error(`✗ [kwota] ${full}`);
                                res.write(`data: ${JSON.stringify({ error: { message: full, type: 'agy_error', code: 'quota_exhausted', role, resetMs: quotaExhausted.resetMs } })}\n\n`);
                                res.write('data: [DONE]\n\n');
                                res.end();
                                finalLog(code, rounds, result.steps);
                                freeSlot();
                                return;
                            }
                            if (permissionDenied) {
                                const full = buildPermissionError(role, permissionClass, permissionTarget, settingsPath, permissionLabel);
                                console.error(`✗ [odmowa uprawnienia] ${full}`);
                                // OpenAI-style error event — NOT a content delta, and
                                // NOT a finish_reason:'stop' delta. The 200 headers
                                // already went out (heartbeat starts before the slot),
                                // so the status can't change; the error object is the
                                // only in-band way to signal a failed turn.
                                res.write(`data: ${JSON.stringify({ error: { message: full, type: 'agy_error', code: 'permission_denied', permission: permissionClass, role } })}\n\n`);
                                res.write('data: [DONE]\n\n');
                                res.end();
                                finalLog(code, rounds, result.steps);
                                freeSlot();
                                return;
                            }
                            if (spawnErr || timedOut || code !== 0 || emptyFail) {
                                const msg = spawnErr
                                    ? `Nie udało się uruchomić agy: ${spawnErr.message}`
                                    : timedOut === 'stuck'
                                        ? `agy utknął bez pierwszego tokenu — ${rounds} rund do modelu i zero odpowiedzi przez ${mins(T.stuck)} min, bez żywego narzędzia i bez nowych kroków trajektorii. Możliwa pętla (brak narzędzia/uprawnienia) — sprawdź glog i rozważ AGY_STUCK_MS. Przerwane.`
                                    : timedOut === 'idle'
                                        ? `agy przerwany — brak aktywności przez ${T.idle / 60000} min (po ${elapsed()}s, rund: ${rounds})`
                                        : timedOut === 'hard'
                                            ? `Przekroczono absolutny limit czasu (${T.hard / 60000} min, rund: ${rounds})`
                                            : code !== 0
                                                ? `agy zakończył się błędem (kod ${code})`
                                                : `agy nie zwrócił treści`;
                                const detail = summarizeAgyError(logErr) || errOut.trim().slice(0, 300) || null;
                                const full = detail ? `${msg}: ${detail}` : msg;
                                console.error(`✗ ${full}`);
                                // Error EVENT, not a content delta — same reason as the
                                // permission branch above. A delta with finish_reason:'stop'
                                // is indistinguishable from a normal answer upstream, so
                                // every failure here was booked as a successful turn.
                                // Measured cost, 2026-08-07 11:07: a STUCK kill (28 rounds,
                                // zero output, 420s) was written to cron history as
                                // `status = ok`, consecutiveErrors stayed 0 and failureAlert
                                // never fired — one hour of HA monitoring vanished and the
                                // ⚠️ text was delivered to the channel as if it were the
                                // watchdog's report. Known trade-off: OpenClaw retries an
                                // error object ~4x, so a genuinely stuck run now costs
                                // several attempts instead of one. Capping that belongs to
                                // the cron/gateway config, not here — a silent success is
                                // the worse failure mode.
                                res.write(`data: ${JSON.stringify({ error: { message: full, type: 'agy_error', code: failureCode({ spawnErr, timedOut, code, emptyFail }), role } })}\n\n`);
                            } else {
                                // Which model actually answered — agyModel is the
                                // resolved Antigravity display name (e.g. "Gemini
                                // 3.1 Pro (High)"); modelName is the raw id when
                                // agyModel is null (unset AGY_MODEL default).
                                // Skip our own footer for exact-match control sentinels
                                // (NO_REPLY, HEARTBEAT_OK, ...) — appending it breaks the
                                // byte-exact comparison OpenClaw's suppression logic does.
                                // Free-form reports still get the footer; it's harmless there.
                                if (!isControlSentinel(fullOut)) {
                                    sendDelta(`\n\n— model: ${agyModel || modelName}`);
                                }
                                sendDelta(null, 'stop');
                            }
                            res.write('data: [DONE]\n\n');
                            res.end();
                            finalLog(code, rounds, result.steps);
                            freeSlot();
                        },
                        onStatus
                    );
                };

                // Slot acquisition (ordering per plan §3.3): SSE headers and the
                // heartbeat are already live above, so the client keeps seeing
                // progress even while we sit in the queue. Only now do we take a
                // slot. Queue full → immediate visible refusal. Granted (now or
                // after a wait) → start the attempt. Queue timeout / disconnect
                // while waiting → clean close, no slot held.
                const acq = sem.acquire();
                if (!acq.ok) {
                    clearInterval(heartbeat);
                    console.error(`✗ agy_busy — ${sem.active} aktywnych, ${sem.queued} w kolejce (limit ${MAX_CONCURRENT}+${MAX_QUEUE})`);
                    sendDelta(`⚠️ [proxy] Wszystkie sloty agy zajęte (${sem.active} aktywnych, ${sem.queued} w kolejce) — spróbuj za chwilę.`, 'stop');
                    res.write('data: [DONE]\n\n');
                    res.end();
                    return;
                }
                queueWaiter = acq.waiter || null;
                acq.slot.then((release) => {
                    queueWaiter = null;
                    if (clientGone) { release(); clearInterval(heartbeat); return; }
                    holdSlot(release);
                    streamAttempt(1);
                }).catch(() => {
                    // queue_timeout or client_gone: no slot was ever held.
                    queueWaiter = null;
                    clearInterval(heartbeat);
                    if (!clientGone && !res.writableEnded) {
                        sendDelta(`\n⚠️ [proxy] Przekroczono limit oczekiwania w kolejce (${Math.round(QUEUE_TIMEOUT_MS / 60000)} min) — spróbuj ponownie.`, 'stop');
                        res.write('data: [DONE]\n\n');
                        res.end();
                    }
                });
            } else {
                const bufAttempt = (n) => {
                    runAgy(null, (result) => {
                        if (clientGone) { freeSlot(); return; }
                        const { code, out, errOut, timedOut, spawnErr, logErr, rounds, permissionClass, permissionTarget, settingsPath, permissionLabel, quotaExhausted, T } = result;
                        // Permission denial: deterministic, never retried. Distinct
                        // HTTP 403 + code permission_denied so the caller can tell it
                        // apart from a transient failure and NOT retry it.
                        const permissionDenied = !!permissionClass && !out.trim();
                        // See streaming path above: IDLE is retried in-proxy (buffered mode
                        // never sent anything to the client mid-flight, so it's always safe).
                        // Excludes a quota kill too — retrying hits the same exhausted quota.
                        const transientFail = !spawnErr && !permissionDenied && !quotaExhausted && (!timedOut || timedOut === 'idle') && code !== 0;
                        // Same empty-output error signature as in stream mode.
                        const emptyFail = !spawnErr && !permissionDenied && !quotaExhausted && !timedOut && code === 0 && !out.trim();

                        if (transientFail && n < MAX_ATTEMPTS) {
                            console.log(`↻ agy próba ${n} nieudana (kod ${code}${timedOut ? `, timedOut=${timedOut}` : ''}) — ponawiam. stderr: ${errOut.trim().slice(0, 200)}`);
                            return bufAttempt(n + 1);
                        }

                        if (res.writableEnded) return;

                        if (quotaExhausted) {
                            const full = `Kwota (limit) modelu ${modelName} wyczerpana — reset za ${fmtDuration(quotaExhausted.resetMs)}. Wybierz inny model lub poczekaj.`;
                            console.error(`✗ [kwota] ${full}`);
                            res.statusCode = 429;
                            res.setHeader('Retry-After', String(Math.ceil(quotaExhausted.resetMs / 1000)));
                            res.setHeader('Content-Type', 'application/json');
                            res.end(JSON.stringify({ error: { message: full, type: 'agy_error', code: 'quota_exhausted', role, resetMs: quotaExhausted.resetMs } }));
                            finalLog(code, rounds, result.steps);
                            freeSlot();
                            return;
                        }

                        if (permissionDenied) {
                            const full = buildPermissionError(role, permissionClass, permissionTarget, settingsPath, permissionLabel);
                            console.error(`✗ [odmowa uprawnienia] ${full}`);
                            res.statusCode = 403;
                            res.setHeader('Content-Type', 'application/json');
                            res.end(JSON.stringify({ error: { message: full, type: 'agy_error', code: 'permission_denied', permission: permissionClass, role } }));
                            finalLog(code, rounds, result.steps);
                            freeSlot();
                            return;
                        }

                        if (spawnErr || timedOut || code !== 0 || emptyFail) {
                            const msg = spawnErr
                                ? `Nie udało się uruchomić agy: ${spawnErr.message}`
                                : timedOut === 'stuck'
                                    ? `agy utknął bez pierwszego tokenu — ${rounds} rund do modelu i zero odpowiedzi przez ${mins(T.stuck)} min, bez żywego narzędzia i bez nowych kroków trajektorii. Możliwa pętla (brak narzędzia/uprawnienia) — sprawdź glog i rozważ AGY_STUCK_MS. Przerwane.`
                                : timedOut === 'idle'
                                    ? `agy przerwany — brak aktywności przez ${T.idle / 60000} min (po ${elapsed()}s, rund: ${rounds})`
                                    : timedOut === 'hard'
                                        ? `Przekroczono absolutny limit czasu (${T.hard / 60000} min, rund: ${rounds})`
                                        : code !== 0
                                            ? `agy zakończył się błędem (kod ${code})`
                                            : `agy nie zwrócił treści`;
                            const detail = summarizeAgyError(logErr) || errOut.trim().slice(0, 300) || null;
                            const full = detail ? `${msg}: ${detail}` : msg;
                            console.error(`✗ ${full}`);
                            res.statusCode = spawnErr ? 502 : timedOut ? 504 : 500;
                            res.setHeader('Content-Type', 'application/json');
                            res.end(JSON.stringify({ error: { message: full, type: 'agy_error', code: failureCode({ spawnErr, timedOut, code, emptyFail }), role } }));
                            finalLog(code, rounds, result.steps);
                            freeSlot();
                            return;
                        }

                        res.setHeader('Content-Type', 'application/json');
                        res.statusCode = 200;
                        // See streaming path: don't corrupt an exact-match control
                        // sentinel (NO_REPLY, ...) with our own footer.
                        const trimmedOut = out.trim();
                        const finalContent = isControlSentinel(trimmedOut)
                            ? trimmedOut
                            : `${trimmedOut}\n\n— model: ${agyModel || modelName}`;
                        res.end(JSON.stringify({
                            id: requestId,
                            object: "chat.completion",
                            created: createdTime,
                            model: modelName,
                            choices: [{ message: { role: "assistant", content: finalContent }, finish_reason: "stop", index: 0 }]
                        }));
                        finalLog(code, rounds, result.steps);
                        freeSlot();
                    });
                };

                // Buffered path: no stream to keep alive, so just take a slot.
                // Queue full → 429 with Retry-After now. Granted → run. Queue
                // timeout / disconnect while waiting → error out / drop quietly.
                const acq = sem.acquire();
                if (!acq.ok) {
                    console.error(`✗ agy_busy (buffered) — ${sem.active} aktywnych, ${sem.queued} w kolejce`);
                    res.statusCode = 429;
                    res.setHeader('Retry-After', '60');
                    res.setHeader('Content-Type', 'application/json');
                    res.end(JSON.stringify({ error: { message: `Wszystkie sloty agy zajęte (${sem.active} aktywnych, ${sem.queued} w kolejce)`, type: 'agy_error', code: 'agy_busy' } }));
                    return;
                }
                queueWaiter = acq.waiter || null;
                acq.slot.then((release) => {
                    queueWaiter = null;
                    if (clientGone) { release(); return; }
                    holdSlot(release);
                    bufAttempt(1);
                }).catch(() => {
                    queueWaiter = null;
                    if (!clientGone && !res.writableEnded) {
                        res.statusCode = 503;
                        res.setHeader('Retry-After', '60');
                        res.setHeader('Content-Type', 'application/json');
                        res.end(JSON.stringify({ error: { message: `Przekroczono limit oczekiwania w kolejce (${Math.round(QUEUE_TIMEOUT_MS / 60000)} min)`, type: 'agy_error', code: 'queue_timeout' } }));
                    }
                });
            }
        });
    } else {
        res.statusCode = 404;
        res.end("Not Found");
    }
});

// Sweep preserved error glogs older than the retention window so /tmp does not
// grow without bound. Matches only the proxy's own saved glogs (agy-timeout-/
// agy-denied-/agy-fail-), never the live per-request agy-proxy-* files.
function sweepOldGlogs() {
    const dir = os.tmpdir();
    const maxAgeMs = GLOG_RETENTION_DAYS * 86400000;
    const now = Date.now();
    let removed = 0;
    try {
        for (const name of fs.readdirSync(dir)) {
            // Match ANY agy-<kind>- glog, not an enumerated prefix list. The
            // old list was (timeout|denied|fail) and silently stopped sweeping
            // when a prefix was renamed: on 2026-08-07 /tmp still held 23 files,
            // oldest from 07-20 (18 days at a 7-day retention), all named
            // agy-stuck-* / agy-bis-* from earlier proxy versions.
            // Live per-request files (agy-proxy-*) are safe to include here:
            // they can only be minutes old, and the mtime test below is days.
            if (!/^agy-[a-z]+-.*\.log$/.test(name)) continue;
            const p = path.join(dir, name);
            try {
                if (now - fs.statSync(p).mtimeMs > maxAgeMs) { fs.unlinkSync(p); removed++; }
            } catch (e) {}
        }
    } catch (e) {}
    if (removed) console.log(`⛏ sprzątanie: skasowano ${removed} zachowanych glogów starszych niż ${GLOG_RETENTION_DAYS} dni`);
}

server.listen(PORT, '127.0.0.1', () => {
    console.log(`Lokalny serwer Proxy Node.js (live streaming + retry + telemetria) działa na porcie ${PORT}`);
    sweepOldGlogs();
});
process.stdin.resume();

server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`Port ${PORT} already in use – shutting down.`);
    } else {
        console.error('Server error:', err);
    }
    process.exit(1);
});

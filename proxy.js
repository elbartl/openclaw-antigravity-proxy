const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const PORT = 8000;
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
const STUCK_NO_OUTPUT_MS = 420000;  // 7 min of rounds-but-no-stdout-and-no-tool => stuck, kill fast
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
const STATUS_MIN_GAP_MS = 60000;    // min spacing of visible round-update status lines (append-only UI)
const PROGRESS_MAX_GAP_MS = 180000; // max spacing of ANY visible tick — kept under OpenClaw's ~400s stall/abort so a silent long tool never looks stalled to the client
const MAX_ATTEMPTS = 2;             // one automatic retry on a transient agy failure

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
    "gemini-flash-low":  "Gemini 3.5 Flash (Low)",
    "gemini-flash":      "Gemini 3.5 Flash (Medium)",
    "gemini-flash-high": "Gemini 3.5 Flash (High)",
    "gemini-pro-low":    "Gemini 3.1 Pro (Low)",
    "gemini-pro":        "Gemini 3.1 Pro (High)",
    "claude-sonnet":     "Claude Sonnet 4.6 (Thinking)",
    "claude-opus":       "Claude Opus 4.6 (Thinking)",
    "gpt-oss":           "GPT-OSS 120B (Medium)",
};

// Resolve the requested OpenAI model id to an agy model name.
// Accepts our short ids, a "provider/id" form, or an exact agy model name.
// "antigravity" / empty -> the AGY_MODEL default (may be null = agy default).
// Unknown ids are an ERROR (not a silent fallback): a typo like "gmeini-flash"
// used to silently route to the default model, which made failures undebuggable.
function resolveAgyModel(requested) {
    if (!requested) return { model: AGY_MODEL };
    const id = requested.includes('/') ? requested.split('/').pop() : requested;
    if (id === 'antigravity') return { model: AGY_MODEL };
    if (MODEL_MAP[id]) return { model: MODEL_MAP[id] };
    if (Object.values(MODEL_MAP).includes(requested)) return { model: requested }; // exact agy name
    return { error: `Nieznany model "${requested}". Dostępne id: antigravity, ${Object.keys(MODEL_MAP).join(', ')}` };
}

// agy >= 1.1.0 swallows backend errors in --print mode: exit code 0, empty
// stdout, empty stderr. The only trace is its glog log file, so each request
// runs with a private --log-file and we mine it for the real error.
// glog error lines look like: "E0708 11:44:42.620606 98256 log.go:398] msg"
function extractAgyError(logText) {
    if (!logText) return null;
    const errLines = logText.split('\n').filter(l => /^E\d{4} /.test(l));
    if (!errLines.length) return null;
    let msg = errLines[errLines.length - 1].replace(/^E\d{4} \S+\s+\d+ \S+\]\s*/, '').trim();
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

// Live-tail agy's private glog file and report agent-loop activity. Each
// "streamGenerateContent" line = one model round (the reliable live marker in
// the log; tool executions are not logged here — those are caught separately by
// hasActiveToolChild in the watchdog). Raw file growth is NOT treated as
// activity — glog writes periodic noise (quota refresh etc.) even when the
// agent loop is stuck, and that must not defeat the idle timeout.
// Returns a stop() function; safe if the file doesn't exist yet.
function watchAgyLog(logFile, onRound) {
    let offset = 0;
    let leftover = '';
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
function hasActiveToolChild(pgid) {
    let procs;
    try { procs = fs.readdirSync('/proc'); } catch (e) { return false; }
    for (const name of procs) {
        if (!/^\d+$/.test(name)) continue;
        const pid = parseInt(name, 10);
        if (pid === pgid) continue;
        let stat;
        try { stat = fs.readFileSync(`/proc/${name}/stat`, 'utf8'); } catch (e) { continue; }
        const rp = stat.lastIndexOf(')');
        if (rp === -1) continue;
        // after ') ' come: state(3) ppid(4) pgrp(5) ...
        const fields = stat.slice(rp + 2).trim().split(/\s+/);
        if (parseInt(fields[2], 10) !== pgid) continue;
        const state = fields[0];
        if (state === 'R' || state === 'D') return true; // busy; keep scanning past idle (S) children
    }
    return false;
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

// Remove our own progress-tick segments (⏳/⌛ …) and error notices
// (⚠️ [proxy] …) so they don't accumulate in the conversation history fed
// back to agy later.
function stripProgress(text) {
    return text
        .replace(/\r?[⏳⌛][^\r\n]*/g, '')
        .replace(/⚠️?\s*\[proxy\][^\r\n]*/g, '')
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
        const ids = ["antigravity", ...Object.keys(MODEL_MAP)];
        res.end(JSON.stringify({
            object: "list",
            data: ids.map(id => ({ id, object: "model", owned_by: "local" }))
        }));
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

            if (!messages.length) {
                res.statusCode = 400;
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ error: "Brak wiadomości w zapytaniu" }));
                return;
            }

            const prompt = buildPrompt(messages);
            const startedAt = Date.now();
            let firstByteAt = 0;
            console.log(`→ agy | stream=${isStream} | model=${modelName} → agy:${agyModel || '(domyślny)'} | prompt=${prompt.length} zn. | "${prompt.substring(0, 80).replace(/\n/g, ' ')}..."`);

            if (DEBUG_DIR) {
                try { fs.writeFileSync(path.join(DEBUG_DIR, 'last_prompt.txt'), prompt); } catch (e) {}
            }

            // Track the live child so a client disconnect can kill whichever
            // attempt is currently running.
            let currentChild = null;
            let clientGone = false;
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
                let done = false;
                let rounds = 0;
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
                const agyArgs = ['--dangerously-skip-permissions', '--print-timeout', AGY_PRINT_TIMEOUT, '--log-file', logFile];
                if (agyModel) agyArgs.push('--model', agyModel);
                agyArgs.push('--print', prompt);
                // detached: own process group, so a timeout kill also reaps
                // grandchildren agy may spawn (tools, xdg-open, browsers).
                const child = spawn('agy', agyArgs, {
                    stdio: ['ignore', 'pipe', 'pipe'],
                    detached: true,
                    env: { ...process.env, NO_COLOR: '1' }
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
                    const toolActive = (now - lastRealActivityAt < TOOL_GRACE_MS)
                        && child.pid && hasActiveToolChild(child.pid);
                    if (now - lastActivityAt >= WATCHDOG_MS && toolActive) {
                        lastActivityAt = now;
                    }
                    const idle = now - lastActivityAt;
                    if (!firstByteAt && rounds >= STUCK_MIN_ROUNDS && total >= STUCK_NO_OUTPUT_MS
                        && !toolActive) {
                        timedOut = 'stuck';
                        console.log(`⏱ agy STUCK po ${elapsed()}s — ${rounds} rund modelu, 0 tokenów odpowiedzi, brak żywego narzędzia; pętla bez postępu, zabijam`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (total >= HARD_TIMEOUT_MS) {
                        timedOut = 'hard';
                        console.log(`⏱ agy HARD timeout (${HARD_TIMEOUT_MS / 60000} min) po ${elapsed()}s — zabijam proces (rund: ${rounds})`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (idle >= IDLE_TIMEOUT_MS) {
                        timedOut = 'idle';
                        console.log(`⏱ agy IDLE timeout (${IDLE_TIMEOUT_MS / 60000} min bez aktywności) po ${elapsed()}s — zabijam proces (rund: ${rounds})`);
                        clearInterval(watchdog);
                        killTree(child);
                        setTimeout(() => finish(-1), 5000).unref();
                    } else if (total >= SOFT_TIMEOUT_MS && !extendedNotified) {
                        extendedNotified = true;
                        console.log(`⏱ SOFT limit (${SOFT_TIMEOUT_MS / 60000} min) minięty po ${elapsed()}s, agy aktywny (rund: ${rounds}) — przedłużam do max ${HARD_TIMEOUT_MS / 60000} min`);
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
                    // On a timeout kill, agy looked "stuck" — preserve its glog and
                    // dump the tail so we can see what it was actually doing during
                    // the silent window (waiting on a model round vs a hung tool).
                    if (timedOut && logText) {
                        const saved = path.join(os.tmpdir(), `agy-stuck-${process.pid}-${Date.now()}.log`);
                        try { fs.writeFileSync(saved, logText); } catch (e) {}
                        const tail = logText.trimEnd().split('\n').slice(-25).join('\n');
                        console.log(`⛏ agy ${timedOut}-timeout glog zachowany: ${saved}\n----- ogon glog -----\n${tail}\n----- koniec -----`);
                    }
                    try { fs.unlinkSync(logFile); } catch (e) {}
                    cb({ code, out, errOut, timedOut, spawnErr, logErr, rounds });
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

            function finalLog(code, rounds) {
                const ttft = firstByteAt ? ((firstByteAt - startedAt) / 1000).toFixed(2) : 'n/a';
                console.log(`← agy zakończone | kod=${code} | całość=${elapsed()}s | TTFT=${ttft}s | rund=${rounds ?? '?'}`);
            }

            if (isStream) {
                // Real live streaming: forward agy's stdout to the client as it
                // arrives. Retry is still possible as long as nothing has been
                // sent yet (a fast failure before the first token).
                res.setHeader('Content-Type', 'text/event-stream');
                res.setHeader('Cache-Control', 'no-cache');
                res.setHeader('Connection', 'keep-alive');

                let sentAny = false;

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
                const elapsedMin = () => Math.max(1, Math.round((Date.now() - startedAt) / 60000));

                const showStatus = (text) => {
                    if (res.writableEnded || sentAny) return;
                    sendDelta(text);
                    lastVisibleAt = Date.now();
                };

                const heartbeat = setInterval(() => {
                    if (res.writableEnded || sentAny) return;
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

                const onStatus = (ev) => {
                    if (ev.type === 'extended') {
                        showStatus(`⏳ [${elapsedMin()} min] agy wciąż aktywnie pracuje (runda ${ev.rounds}) — przedłużam limit czasu…\n`);
                    } else if (ev.type === 'round'
                        && ev.rounds > lastShownRounds
                        && lastVisibleAt
                        && Date.now() - lastVisibleAt >= STATUS_MIN_GAP_MS) {
                        lastShownRounds = ev.rounds;
                        showStatus(`⏳ [${elapsedMin()} min] agy pracuje — runda ${ev.rounds} zapytań do modelu…\n`);
                    }
                };

                const streamAttempt = (n) => {
                    runAgy(
                        (chunk) => { sentAny = true; sendDelta(chunk); },
                        (result) => {
                            if (clientGone) { clearInterval(heartbeat); return; }
                            const { code, errOut, timedOut, spawnErr, logErr, rounds } = result;
                            // IDLE is retried like a transient failure: it fires on a
                            // genuinely stalled model call (network hang), and retrying
                            // inside proxy — once, only if nothing reached the client yet —
                            // beats surfacing an error and letting OpenClaw re-run the whole
                            // turn externally (which doubled the wait, see STUCK comment
                            // above). HARD and STUCK stay authoritative/non-retried.
                            const transientFail = !spawnErr && (!timedOut || timedOut === 'idle') && code !== 0;
                            // agy >= 1.1.0 error signature: clean exit, zero
                            // output. The real cause sits in logErr. Not
                            // retried: the dominant case (quota 429) won't
                            // clear on retry and OpenClaw retries once anyway.
                            const emptyFail = !spawnErr && !timedOut && code === 0 && !sentAny;

                            if (transientFail && !sentAny && n < MAX_ATTEMPTS) {
                                console.log(`↻ agy próba ${n} nieudana (kod ${code}${timedOut ? `, timedOut=${timedOut}` : ''}) — ponawiam. stderr: ${errOut.trim().slice(0, 200)}`);
                                return streamAttempt(n + 1);
                            }

                            clearInterval(heartbeat);
                            if (spawnErr || timedOut || code !== 0 || emptyFail) {
                                const msg = spawnErr
                                    ? `Nie udało się uruchomić agy: ${spawnErr.message}`
                                    : timedOut === 'stuck'
                                        ? `agy utknął w pętli bez postępu — ${rounds} rund do modelu i ani jednej odpowiedzi przez ${STUCK_NO_OUTPUT_MS / 60000} min. Zwykle znaczy brak potrzebnego narzędzia lub uprawnień do zadania. Przerwane.`
                                    : timedOut === 'idle'
                                        ? `agy przerwany — brak aktywności przez ${IDLE_TIMEOUT_MS / 60000} min (po ${elapsed()}s, rund: ${rounds})`
                                        : timedOut === 'hard'
                                            ? `Przekroczono absolutny limit czasu (${HARD_TIMEOUT_MS / 60000} min, rund: ${rounds})`
                                            : code !== 0
                                                ? `agy zakończył się błędem (kod ${code})`
                                                : `agy nie zwrócił treści`;
                                const detail = summarizeAgyError(logErr) || errOut.trim().slice(0, 300) || null;
                                const full = detail ? `${msg}: ${detail}` : msg;
                                console.error(`✗ ${full}`);
                                sendDelta(`\n⚠️ [proxy] ${full}`, 'stop');
                            } else {
                                sendDelta(null, 'stop');
                            }
                            res.write('data: [DONE]\n\n');
                            res.end();
                            finalLog(code, rounds);
                        },
                        onStatus
                    );
                };
                streamAttempt(1);
            } else {
                const bufAttempt = (n) => {
                    runAgy(null, (result) => {
                        if (clientGone) return;
                        const { code, out, errOut, timedOut, spawnErr, logErr, rounds } = result;
                        // See streaming path above: IDLE is retried in-proxy (buffered mode
                        // never sent anything to the client mid-flight, so it's always safe).
                        const transientFail = !spawnErr && (!timedOut || timedOut === 'idle') && code !== 0;
                        // Same empty-output error signature as in stream mode.
                        const emptyFail = !spawnErr && !timedOut && code === 0 && !out.trim();

                        if (transientFail && n < MAX_ATTEMPTS) {
                            console.log(`↻ agy próba ${n} nieudana (kod ${code}${timedOut ? `, timedOut=${timedOut}` : ''}) — ponawiam. stderr: ${errOut.trim().slice(0, 200)}`);
                            return bufAttempt(n + 1);
                        }

                        if (res.writableEnded) return;

                        if (spawnErr || timedOut || code !== 0 || emptyFail) {
                            const msg = spawnErr
                                ? `Nie udało się uruchomić agy: ${spawnErr.message}`
                                : timedOut === 'stuck'
                                    ? `agy utknął w pętli bez postępu — ${rounds} rund do modelu i ani jednej odpowiedzi przez ${STUCK_NO_OUTPUT_MS / 60000} min. Zwykle znaczy brak potrzebnego narzędzia lub uprawnień do zadania. Przerwane.`
                                : timedOut === 'idle'
                                    ? `agy przerwany — brak aktywności przez ${IDLE_TIMEOUT_MS / 60000} min (po ${elapsed()}s, rund: ${rounds})`
                                    : timedOut === 'hard'
                                        ? `Przekroczono absolutny limit czasu (${HARD_TIMEOUT_MS / 60000} min, rund: ${rounds})`
                                        : code !== 0
                                            ? `agy zakończył się błędem (kod ${code})`
                                            : `agy nie zwrócił treści`;
                            const detail = summarizeAgyError(logErr) || errOut.trim().slice(0, 300) || null;
                            const full = detail ? `${msg}: ${detail}` : msg;
                            console.error(`✗ ${full}`);
                            res.statusCode = spawnErr ? 502 : timedOut ? 504 : 500;
                            res.setHeader('Content-Type', 'application/json');
                            res.end(JSON.stringify({ error: { message: full, type: 'agy_error', code: emptyFail ? 'empty_response' : 'agy_failed' } }));
                            finalLog(code, rounds);
                            return;
                        }

                        res.setHeader('Content-Type', 'application/json');
                        res.statusCode = 200;
                        res.end(JSON.stringify({
                            id: requestId,
                            object: "chat.completion",
                            created: createdTime,
                            model: modelName,
                            choices: [{ message: { role: "assistant", content: out.trim() }, finish_reason: "stop", index: 0 }]
                        }));
                        finalLog(code, rounds);
                    });
                };
                bufAttempt(1);
            }
        });
    } else {
        res.statusCode = 404;
        res.end("Not Found");
    }
});

server.listen(PORT, '127.0.0.1', () => {
    console.log(`Lokalny serwer Proxy Node.js (live streaming + retry + telemetria) działa na porcie ${PORT}`);
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

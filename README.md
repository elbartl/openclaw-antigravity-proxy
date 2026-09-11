# openclaw-antigravity-proxy

A tiny, zero-dependency Node.js proxy that exposes Google **Antigravity** (the `agy` CLI)
as an **OpenAI-compatible API**, so that [OpenClaw](https://openclaw.ai) — or any other
OpenAI-API client — can use Antigravity's agentic models as a chat backend.

```
OpenClaw gateway
      │  POST /v1/chat/completions   (OpenAI format, streaming or not)
      ▼
proxy.js  (127.0.0.1:8000)           ← systemd user service
      │  spawn per request:
      │  agy --dangerously-skip-permissions --print-timeout 35m
      │      --log-file <private> [--model X] --print "<prompt>"
      ▼
agy  (Antigravity CLI)               ← its own agent loop: Gemini/Claude backend,
      │                                shell, browser, web tools…
      ▼
response → back to the client as chat.completion(.chunk)
```

**One file, no npm dependencies** — plain `node:http`. The interesting part is not the
translation itself but the production hardening around a CLI that was never designed to
be a server backend: adaptive timeouts, live progress reporting, error mining from log
files, process-tree cleanup, and per-request model routing.

## Why

`agy` is a fully agentic CLI: given a single prompt it will happily run web searches,
shell commands and multi-step investigations that take **minutes** and print *nothing*
until the very end. Wrapping that in a chat API that streams, times out sanely, retries
transient failures and reports progress requires solving a few non-obvious problems —
documented below and in the comments throughout `proxy.js`.

## Features

- **OpenAI-compatible endpoints**: `POST /v1/chat/completions` (stream and non-stream),
  `GET /v1/models`, plus `GET /healthz` (slot occupancy) and `GET /logs?n=N` (recent
  console output).
- **Stateless by design** — each request spawns a fresh `agy` process; the conversation
  (system prompt + capped history + latest turn) is folded into a single prompt.
  Every request is reproducible, retryable and debuggable in isolation.
- **Per-request model routing** — the OpenAI `model` field is mapped through `MODEL_MAP`
  to an exact `agy` model name (Gemini Flash/Pro, Claude Sonnet/Opus, GPT-OSS…).
  Unknown ids are a hard error, not a silent fallback.
- **Adaptive timeouts, tuned per model** (SOFT / IDLE / HARD / STUCK): a genuinely
  *active* agent is allowed to run long, a *stuck* one is killed early. Gemini Pro
  (High) gets more headroom for slow thinking rounds; Flash dies sooner when it loops.
  An explicit `AGY_STUCK_MS` outranks the per-model default.
- **Progress read from agy's own trajectory** — agy appends a row per turn and per tool
  call to a per-conversation SQLite database, and does so *live*. The proxy tails that
  table, which is the only signal that sees sub-second tool calls; a `/proc` scan of
  agy's descendants is kept as a fallback for tools that run long without new rows.
- **Quota fail-fast** — on a `RESOURCE_EXHAUSTED` (429) with a reset far enough out,
  the run is killed immediately instead of waiting out agy's own 15-25 min internal
  retry loop, which used to hold a concurrency slot and starve every queued request.
- **Concurrency limit** — a semaphore plus bounded FIFO queue (`AGY_MAX_CONCURRENT`,
  `AGY_MAX_QUEUE`); each `agy` spawn costs hundreds of MB, so on a Raspberry Pi an
  unbounded fan-out from a multi-agent client would OOM the host. Overflow is refused
  fast (HTTP 429 / a visible stream notice), never piled on.
- **Permission roles** — a model id ending in `-ro` runs agy under an isolated `HOME`
  whose allowlist permits read-only tools only; anything else is auto-denied. A denial
  is reported as an error naming the exact command that was refused, read from the
  trajectory, so the operator knows what to put on the allowlist.
- **Live progress + SSE keepalive** — while `agy` is silent, the client sees rationed
  status lines carrying agy's own step labels (`⏳ … agy Reading WATCHDOG.md …`), and
  invisible SSE comments keep the connection alive. Progress lines are stripped from
  future conversation history automatically.
- **Error mining** — `agy ≥ 1.1.0` swallows backend errors in `--print` mode
  (exit 0, empty stdout/stderr). The proxy gives each request a private `--log-file`
  and extracts the real error (e.g. quota `RESOURCE_EXHAUSTED 429`) from it.
- **One automatic retry** on transient failures — but never after a timeout, never
  after the first streamed token, and never for empty-output errors (quota won't
  clear on retry).
- **Process-tree hygiene** — `agy` is spawned detached in its own process group and
  killed with `kill(-pid)`, so grandchildren (tool commands, `xdg-open`, browsers)
  don't survive and hold pipes open. Completion is detected on `exit`, not `close`
  (a grandchild inheriting stdout can block `close` forever).
- **Telemetry** — every request logs prompt size, model routing, TTFT, total time,
  exit code, agent-loop rounds and trajectory steps. `GET /healthz` reports live slot
  occupancy, `GET /logs` returns the last N console lines from an in-memory ring buffer
  (debugging without an SSH session), and each answer ends with a `— model: …` footer
  naming the model that produced it.

## Requirements

- Node.js ≥ 18 (any version with `node:http`; no npm packages needed)
- [Antigravity CLI](https://antigravity.google) (`agy`) installed and authenticated
- Linux (uses process groups and `systemd --user`; adaptable elsewhere)

## Quick start

```bash
node proxy.js
# → listens on 127.0.0.1:8000

curl -s http://127.0.0.1:8000/v1/models | jq .

curl -s http://127.0.0.1:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gemini-flash","messages":[{"role":"user","content":"Hello"}]}'
```

## Running as a systemd user service

Create `~/.config/systemd/user/openclaw-proxy.service`:

```ini
[Unit]
Description=Antigravity OpenAI Proxy for OpenClaw (agy -> :8000)
After=network-online.target
Wants=network-online.target
StartLimitBurst=5
StartLimitIntervalSec=60

[Service]
ExecStart=/usr/bin/node %h/projects/openclaw/proxy.js
WorkingDirectory=%h/projects/openclaw
Restart=always
RestartSec=5
TimeoutStopSec=30
TimeoutStartSec=30
SuccessExitStatus=0 143
KillMode=control-group
Environment=HOME=%h
Environment=TMPDIR=/tmp
# PATH must include the directory containing `agy`, or spawn fails with ENOENT.
Environment=PATH=/usr/bin:%h/.local/bin:/usr/local/bin:/bin
Environment=NO_COLOR=1
# Optional default model (name EXACTLY as printed by `agy models`):
# Environment=AGY_MODEL=Gemini 3.5 Flash (Low)

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now openclaw-proxy
journalctl --user -u openclaw-proxy -f        # live telemetry
loginctl enable-linger $USER                   # start at boot without login
```

Restart the service after **any** edit to `proxy.js` — and don't run `node proxy.js`
manually while the service is up (port conflict, `EADDRINUSE`).

## Configuration

All knobs are constants at the top of `proxy.js`:

| Constant | Default | Meaning |
|---|---|---|
| `PORT` / `PROXY_PORT` (env) | `8000` | Listen port (bound to `127.0.0.1` only). |
| `SOFT_TIMEOUT_MS` | 12 min | Past this, the request survives only while `agy` shows activity. |
| `IDLE_TIMEOUT_MS` | 15 min | No round, no stdout, no live tool and no new trajectory step → SIGKILL. |
| `HARD_TIMEOUT_MS` | 30 min | Absolute ceiling regardless of activity. |
| `STUCK_NO_OUTPUT_MS` / `AGY_STUCK_MS` (env) | 12 min | Model rounds but zero answer tokens and no progress → loop, killed. An explicit env value outranks any per-model rule. |
| `MODEL_TIMEOUT_OVERRIDES` | — | Per-model tuning of the four budgets above, matched on the agy model name. |
| `STEP_PROGRESS_WINDOW_MS` / `AGY_STEP_WINDOW_MS` (env) | 2 min | How long a new trajectory row keeps counting as progress. `0` disables the signal. |
| `AGY_QUOTA_FAIL_FAST_MS` (env) | 90 s | Quota resetting further out than this is killed at once instead of waiting for agy's internal retry. |
| `TOOL_GRACE_MS` | 10 min | How long a live tool subprocess may stand in for real progress. |
| `AGY_PRINT_TIMEOUT` | `35m` | agy's own budget — deliberately **above** HARD so the proxy owns the timeout. |
| `HISTORY_LIMIT` | `12` | How many recent history messages go into the prompt (system prompt always full). |
| `MAX_ATTEMPTS` | `2` | One automatic retry on transient failure — never on quota or a permission denial. |
| `STATUS_MIN_GAP_MS` / `AGY_STATUS_GAP_MS` (env) | 60 s | Minimum spacing of visible status lines. |
| `PROGRESS_MAX_GAP_MS` | 3 min | Maximum spacing of any visible tick, kept under OpenClaw's ~400 s stall detector. |
| `AGY_MAX_CONCURRENT` (env) | `2` | Parallel `agy` spawns allowed. |
| `AGY_MAX_QUEUE` (env) | `4` | Requests allowed to wait beyond that; the rest are refused fast. |
| `AGY_QUEUE_TIMEOUT_MS` (env) | 10 min | Maximum wait in the queue before giving up. |
| `AGY_LOG_BUFFER_LINES` (env) | `400` | Size of the ring buffer served by `GET /logs`. |
| `AGY_GLOG_RETENTION_DAYS` (env) | `7` | How long preserved error glogs are kept in `$TMPDIR`. |
| `AGY_RO_HOME` (env) | `~/.agy-profiles/ro` | `HOME` of the read-only profile used by the `ro` role. |
| `AGY_DEFAULT_ROLE` (env) | `full` | Role for ids without the `-ro` suffix. |
| `AGY_RO_PLAN` (env) | unset | `1` adds `--mode plan` to the `ro` role. |
| `MODEL_MAP` | — | OpenAI model id → exact `agy` model name. |
| `AGY_MODEL` (env) | unset | Default `agy` model when the request doesn't pick one. |
| `AGY_PROXY_DEBUG` (env) | unset | Directory for `last_prompt.txt` / `last_stderr.txt` dumps. |

## OpenClaw integration notes

Two gotchas that cost real debugging time:

1. **Model aliases shadow provider/model pairs.** An alias like
   `gemini-flash → google/gemini-3-flash-preview` silently routes *around* the proxy.
   Fix: `openclaw models aliases add gemini-flash agy/gemini-flash`.
2. **A provider without auth is skipped.** The gateway requires an `apiKey` even for a
   local proxy that ignores it — any dummy value works (`"apiKey": "agy-local-proxy"`).
   Each model must also appear in **both** `models.providers.agy.models[]` *and* the
   `agents.defaults.models` allowlist.

## Known limitations

- Concurrency is capped, not elastic — two parallel `agy` processes by default, then a
  short queue, then refusal. Raising it on a small host trades latency for RAM.
- Progress detection is good but not total: a tool that runs for minutes without
  writing a trajectory row, without CPU/IO activity and without stdout still looks
  idle. The `/proc` half of the signal only sees descendants in state R or D, so a tool
  blocked on a network socket is invisible to it.
- No token accounting. agy does not persist usage counters anywhere the proxy can read
  them — not in the trajectory, not in the glog, not in its state files — so the proxy
  cannot report how much context a turn burned.
- Statelessness means heavy tasks re-discover their environment every turn.
- `agy` auto-updates and has broken its own CLI contract before (1.1.1 changed how
  `--print` consumes the prompt). If behavior changes overnight, check `agy --version`
  and the binary's mtime first. The trajectory schema is likewise undocumented and read
  defensively: any failure there falls back to the other progress signals.

## Repository layout

| File | What it is |
|---|---|
| `proxy.js` | The proxy. The only file you need. |
| `PROXY.md` | The long-form design document: every mechanism, why it exists, and the incident that motivated it. |
| `antigravity_proxy.py` | Abandoned first prototype. It assumed a native Python SDK, which turned out to exist (`google-antigravity`, official, Apache-2.0) — but it authenticates with a Gemini API key or Vertex project, not the Antigravity subscription this proxy rides, so the CLI wrapper stays. Kept for history; not used. |
| `requirements.txt` | Belongs to the abandoned prototype. |

## License

MIT — see [LICENSE](LICENSE).

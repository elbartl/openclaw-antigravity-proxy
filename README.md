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
  `GET /v1/models`.
- **Stateless by design** — each request spawns a fresh `agy` process; the conversation
  (system prompt + capped history + latest turn) is folded into a single prompt.
  Every request is reproducible, retryable and debuggable in isolation.
- **Per-request model routing** — the OpenAI `model` field is mapped through `MODEL_MAP`
  to an exact `agy` model name (Gemini Flash/Pro, Claude Sonnet/Opus, GPT-OSS…).
  Unknown ids are a hard error, not a silent fallback.
- **Adaptive three-stage timeout** (SOFT 12 min / IDLE 5 min / HARD 30 min):
  a genuinely *active* agent is allowed to run long, a *stuck* one is killed early.
  Activity is detected by live-tailing agy's private glog file — each
  `streamGenerateContent` line is one model round of the agent loop.
- **Live progress + SSE keepalive** — while `agy` is silent, the client sees rationed
  status lines (`⏳ … round 7 …`) and invisible SSE comments keep the connection alive.
  Progress lines are stripped from future conversation history automatically.
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
  exit code and the number of agent-loop rounds.

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
| `PORT` | `8000` | Listen port (bound to `127.0.0.1` only). |
| `SOFT_TIMEOUT_MS` | 12 min | Past this, the request survives only while `agy` shows activity. |
| `IDLE_TIMEOUT_MS` | 5 min | No model round and no stdout for this long → SIGKILL. |
| `HARD_TIMEOUT_MS` | 30 min | Absolute ceiling regardless of activity. |
| `AGY_PRINT_TIMEOUT` | `35m` | agy's own budget — deliberately **above** HARD so the proxy owns the timeout. |
| `HISTORY_LIMIT` | `12` | How many recent history messages go into the prompt (system prompt always full). |
| `MAX_ATTEMPTS` | `2` | One automatic retry on transient failure. |
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

- No concurrency limit — each parallel request is a heavy `agy` process; on small
  hosts (Raspberry Pi) many simultaneous agentic tasks can exhaust RAM.
- Model rounds are the only live activity signal; a single tool execution longer than
  `IDLE_TIMEOUT_MS` with no new round would be killed as idle (measured max gap so
  far: ~47 s, so there is >6× headroom).
- Statelessness means heavy tasks re-discover their environment every turn.
- `agy` auto-updates and has broken its own CLI contract before (1.1.1 changed how
  `--print` consumes the prompt). If behavior changes overnight, check `agy --version`
  and the binary's mtime first.

## Repository layout

| File | What it is |
|---|---|
| `proxy.js` | The proxy. The only file you need. |
| `antigravity_proxy.py` | Abandoned first prototype (FastAPI + hypothetical SDK). Kept for history; not used. |
| `requirements.txt` | Belongs to the abandoned prototype. |

## License

MIT — see [LICENSE](LICENSE).

# Technical reference

Looking to install and start Piko? Use the [quick start](../README.md).
This page covers provider settings, runtime limits, MCP, computer use, protocol,
security, development and benchmark details.

A quiet, ACP-native coding-agent runtime in Rust. No terminal chat UI, banners,
spinners, or unsolicited status text. Start the process; drive it from your ACP
client. Human-readable output is limited to explicit help, version, and login.

**An initial implementation, not a claim of market-leading performance.** Model
latency, account quotas, cache residency, and code quality remain upstream and
workload dependent. Benchmarks must separate local overhead from model inference.

## Installation and maintenance

Install the published command with `pnpm add -g @erzenx/piko`, then run
`piko login`. The launcher requires Node.js 22.15+; the native executable
itself does not require Node.js. Only your OS/CPU package is installed. Keep
optional dependencies enabled; reinstall if they were omitted. There is no
fallback network download. `ripgrep` is recommended for faster search; a bounded
Rust fallback is built in.

For a standalone installation, download your platform's archive from
[Releases](https://github.com/ErzenXz/piko/releases), verify its SHA-256
against `SHA256SUMS`, then put the executable on PATH.

```sh
pnpm add -g @erzenx/piko@latest
piko logout
pnpm remove -g @erzenx/piko
```

Restart your ACP agent after updating. If you launch the native path printed by
`piko --print-binary-path`, refresh that path after updating. Logout
removes local login credentials; retained conversations remain in the state
directory. On Unix state defaults to `$XDG_STATE_HOME/piko` or `~/.local/state/piko`; on
Windows it is `%LOCALAPPDATA%\piko`. Existing `codex-harness` state directories
are reused automatically, keeping your login and sessions without moving files.
`PIKO_STATE_DIR` overrides the location; `CODEX_HARNESS_STATE_DIR` remains supported
for existing launchers and has lower precedence.

To choose reasoning, concurrency or another provider, copy a
[starter configuration](../examples/) to a trusted user directory and pass
`--config /absolute/path/config.toml` to **both login and the ACP launcher**.
No configuration is read from the project being edited.

Source installation requires Rust 1.88+ and your OS build tools:

```sh
cargo install --git https://github.com/ErzenXz/piko --tag v0.6.1 --locked
```

## ACP launch contract

See the [quick start](../README.md) for installation and sign-in. An ACP launcher
uses the installed binary and an explicitly chosen, trusted user config:

```json
{"command":"/absolute/path/to/piko","args":["--acp","--config","/absolute/path/to/config.toml"],"env":{}}
```

The client must keep stdin open, continuously read stdout, negotiate protocol
version 1, then send `session/new` with an absolute `cwd` and `mcpServers: []`.
All stdout is newline-delimited JSON-RPC, not Content-Length framing. Fatal
startup/transport diagnostics go to stderr. Empty stdin/EOF shuts the agent down.
The launcher owns daemonization; this executable does not fork away from stdio.

## Web, browser, images and interactive tools

File tools, `view_image`, `apply_patch`, interactive terminals and agent messaging
are built in. Web fetching is enabled by default; `web_enabled = false` removes
both web tools. All external requests, browser actions, file mutations and terminal
commands/input require ACP client approval. Denial never retries through another
route. Plan mode omits mutation, browser and terminal tools; approved public web
research and workspace image reading remain available.

### Web search

Set `BRAVE_SEARCH_API_KEY` in your ACP launcher's environment (or secret settings).
`web_search` appears only when its configured key is available; Piko login does
not supply a search key. Obtain your own key and check its plan at
[Brave Search API](https://api-dashboard.search.brave.com/). Search returns URLs,
titles and snippets; `web_fetch` retrieves source text and never executes scripts.
Fetched text is untrusted. Public-page fetch blocks local/private/special networks,
pins validated DNS answers and rechecks redirects. Browser control supports local
development apps separately.

Optional trusted configuration:

```toml
web_enabled = true
web_search_api_key_env = "BRAVE_SEARCH_API_KEY"
# Override only for an explicitly trusted Brave-compatible HTTPS provider:
# web_search_url = "https://api.search.brave.com/res/v1/web/search"
```

Search and fetch share the configured concurrency gate, have a 20-second deadline,
a 1 MiB response cap and bounded serialized output. PDFs, binary downloads and
JavaScript-heavy pages need a suitable browser or MCP tool. Snippets are search
results, not proof that a source supports a claim; fetch and cite the source.

### Browser automation

Use an **installed Chrome or Edge with its own profile**. There is no browser
download, daemon, profile scan or connection to your everyday browser by default.
Start the dedicated browser yourself; keep its debug port on loopback. For example,
on macOS with Chrome:

```sh
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 \
  --user-data-dir="$HOME/.local/share/piko/browser"
```

On Linux use your installed `google-chrome` or `microsoft-edge` executable with
the same flags. In Windows PowerShell, for a standard Chrome installation:

```powershell
& "$env:ProgramFiles\Google\Chrome\Application\chrome.exe" `
  --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 `
  "--user-data-dir=$env:LOCALAPPDATA\Piko\browser"
```

Then add this to the trusted config passed to `piko --config`:

```toml
browser_cdp_url = "http://127.0.0.1:9222"
```

`browser` now appears in Code mode. `navigate` creates a tab and returns `tab_id`;
reuse that ID for `snapshot`, `click`, `type`, `evaluate`, `screenshot` and `close`.
`tabs` lists pages in this explicitly configured profile. Snapshots include text,
links and controls; selectors drive interactions. Type replaces a field's value;
click is a DOM click, so applications requiring trusted pointer events may need a
specialized MCP browser tool. Inspect snapshots and verify actions. Navigation may
access local development servers. Browser JavaScript and pages can act outside
the workspace, within the approved browser's permissions.

Actions have a 30-second deadline and bounded HTTP/WebSocket/image output. Failed
new-tab navigation closes only the tab it created; existing tabs remain yours.
Screenshots cover the top-left viewport area, not a full-page capture. Close idle
tabs and quit the dedicated browser yourself when done. Its memory is separate
from Piko's native-process measurements. Chrome 136+ requires a non-default
profile for remote debugging; see [Chrome's guidance](https://developer.chrome.com/blog/remote-debugging-port).

### Local images and patches

`view_image` reads workspace PNG/JPEG and sends it through provider and ACP image
content. Limits are 96 KiB, 4096 pixels per side and 8 million pixels. Resize or
compress larger files with an approved installed tool first. Piko adds no image
decoder dependency or implicit subprocess.

`apply_patch` accepts `*** Begin Patch` / `*** End Patch`, add/delete/update/move
operations and exact context hunks. It validates every path and original before
approval, then rechecks snapshots. Limits: 32 operations, 256 KiB/file and 1 MiB
aggregate patch/file data. Each file write is atomic; a multi-file patch is **not
transactional**. If I/O fails partway, the error lists applied paths. Inspect
those paths before retrying. Whitespace fuzz and ambiguous contexts are rejected.

### Interactive terminals and agent messages

`terminal_start` runs an approved command in a native Unix PTY (`/bin/sh`) or
Windows ConPTY (`cmd.exe`, UTF-8). Windows commands use batch syntax, preserving
quoted paths and environment expansion. PowerShell is available as an explicitly
invoked installed program; Piko does not load it for every terminal. The tool
returns a task ID; `task_status` reads bounded
rolling output and wakes when terminal output changes. `terminal_write` requires
separate approval because input can execute commands; include a newline to submit.
`terminal_resize` adjusts rows/columns; `terminal_close` or `task_cancel` terminates
its process tree. Lifetime is capped by `command_timeout_secs`, input by 16 KiB and
terminal count by `max_jobs`. Each live terminal uses two small-stack I/O threads;
none exist before a terminal starts. These tools are not a security sandbox.

`message_agent` queues up to eight 8 KiB messages for a running child, delivered
between model requests. It does not interrupt an in-flight model request or tool.
Use `followup_agent` after completion to reuse its bounded context. Only its parent
may steer it; read-only agents cannot steer workers. Cancelled, evicted or full
contexts require spawning a new agent. Child contexts/mailboxes are in memory;
root session recovery records tool outcomes, not resumable child-agent processes.

Opt-in full-desktop control uses native macOS/Windows APIs or installed Linux X11
tools. See [desktop setup](#opt-in-computer-use). Browser automation above uses a
dedicated browser profile and does not require desktop control.

## Authentication and providers

The default model is [`gpt-6-luna`](https://developers.openai.com/api/docs/models/gpt-6-luna).
Override it with `model` in the config or `OPENAI_MODEL` in the launcher environment.
There is no automatic fallback to another model. Availability through subscription
sign-in depends on your ChatGPT plan. API token prices do not describe subscription
quotas. With GPT-6 Luna function calling over Chat Completions, set
`reasoning_effort = "none"`; Responses supports the model's reasoning modes.

### ChatGPT subscription

`piko login` uses the Codex device-code flow. Enable device-code login in
your ChatGPT security settings if necessary, open the displayed OpenAI URL, and
enter the code. An eligible plan and model access are required. Subscription
limits still apply; hundreds of local agents do not bypass them.

Credentials are stored in `auth.json` under the state directory (Unix directory
mode 0700, files 0600), refreshed when needed, and never intentionally logged or
passed to shell jobs. **Tokens are stored unencrypted on disk.** Protect the
account and state directory. `logout` deletes the local file; restart running
harness processes too, since they cache credentials in memory. It does not revoke
already-issued tokens server-side. Prefer one process per credential directory:
refresh is serialized in-process, not across independent processes.

ChatGPT tokens can only be sent to the Codex backend, never a custom provider.
The backend/device flow follows the open-source Codex contract, not a general
subscription entitlement to the OpenAI Platform API. Upstream changes may require
adapter updates. Device sign-in, streamed `gpt-6-luna` low responses, a read-only
file-tool loop, and ten concurrent short ACP turns were verified on 2026-10-19.
Forced token refresh and other providers have not been live-validated.

The ChatGPT example selects low reasoning and caps simultaneous model requests
and subagents at 10. Increase those limits only within your account allowance;
concurrency limits do not impose a total-token or subscription-usage budget.

### OpenAI API key

Skip device login. Create a config outside your project (for example,
`~/.config/piko/openai.toml`):

```toml
auth = "api_key"
model = "gpt-6-luna"
reasoning_effort = "low"
max_model_requests = 10
max_subagents = 10
```

Point your ACP launcher's `--config` at that file's absolute path and supply
`OPENAI_API_KEY` using the launcher's environment/secret settings. API usage is
billed separately from a ChatGPT subscription. No interactive `login` is needed.

Set `OPENAI_API_KEY` in the **launcher environment**, not a repository file.
In `auth = "auto"`, a key takes precedence over ChatGPT credentials. Use explicit
`auth = "chatgpt"` to force subscription access. Unload all sessions before changing authentication. ACP `authenticate` can select an
available method for the running process; it does not start an interactive login.

### Other OpenAI-compatible providers

Pass `--config /absolute/path/config.toml`; examples are in [`examples/`](../examples/).
Responses and Chat Completions SSE/function calling are supported. Native
Anthropic/Gemini wire protocols are **not** implemented. Compatible providers can
differ in tool-schema, usage-reporting, model, and token-limit support.

## Model discovery and reasoning

ChatGPT sign-in discovers the model catalog available to that account from the
Codex backend. This uses the upstream Codex catalog contract, which may change.
API-key and custom providers use `GET /models` at the configured base URL. Clearly
non-agent entries such as embeddings, image generation and audio-only models are
excluded. The configured model remains available for custom aliases and offline
use; provider permissions, quotas and endpoint compatibility still apply.

Discovery starts only when a client requests a catalog or creates/loads a
session. Concurrent sessions share one request and one immutable catalog. A
private, provider/account-scoped cache lasts five minutes across launches; it
contains model metadata, never credentials. Discovery has a five-second deadline
and caps responses at 2 MiB, normalized metadata at 256 KiB, and catalogs at
512 entries. Oversized catalogs fail discovery without silently dropping models.
Network, malformed-response or cache failures leave the configured model usable. No daemon, SDK, bundled catalog or
new dependency is required.

Reasoning choices come from live capability metadata. Exact documented OpenAI
Responses aliases also have a small capability fallback; unknown/custom models
offer `Provider default` when their provider does not describe reasoning support.
A trusted `reasoning_effort` configuration remains usable for its configured
model. Saved reasoning remains selectable when capability metadata is unavailable;
authoritative metadata replaces unsupported saved choices with the model default. The harness does not assume every model supports the same effort values.
Use the Responses wire API for OpenAI reasoning models with tools; Chat
Completions support and tool compatibility vary by model.

ACP clients receive model, reasoning (`thought_level`) and mode selectors through
`configOptions`. Choices are saved independently per session. Changing a model
refreshes its reasoning options, keeping a compatible choice or selecting the
new model's default. Selecting `Provider default` omits the effort parameter;
unsupported values fail before inference. Configuration cannot change during an
active turn. CodeInk's companion update reads per-model reasoning metadata from
`_meta.harness.reasoningEfforts`; older clients can use standard configuration
updates or legacy model selection.

See the [ACP configuration contract](https://agentclientprotocol.com/protocol/v1/session-config-options),
[OpenAI model listing](https://developers.openai.com/api/reference/resources/models/methods/list)
and [reasoning reference](https://developers.openai.com/api/docs/guides/reasoning).

## Agent runtime

- Gather context → act → verify, with repository `AGENTS.md`, `CLAUDE.md`, and
  `.harness/instructions.md` instructions.
- Streamed text, plans, tool progress, and explicit approval requests over ACP.
- Isolated explore/reviewer/worker agents sharing a workspace, not parent history.
  Explore/reviewer agents and all descendants of plan-mode agents are read-only.
- Foreground/background subagents, shell jobs, task output/status, and cancellation.
- Concurrent independent reads/searches/delegations; mutations remain ordered.
- Exact-match edits with a post-approval conflict check and atomic replacement.
- Durable conversations and active-turn checkpoints, with load/replay and whole-turn
  model-context projection. Older saved turns remain on disk when outside the window.
  Active-turn context exhaustion stops safely; this is not semantic compaction.

### Performance and memory

A single current-thread Tokio event loop multiplexes hundreds of model streams;
there is no OS thread or process per model agent. One pooled HTTP client and
in-memory credential cache are shared. Native `rg` runs without a shell or user
config; bounded Rust `ignore`/`regex` search is the fallback if `rg` is absent.
Search respects ignore files, skips hidden files/symlinks, and applies user globs
without overriding ignored-file protections. Native search scans files up to
128 KiB; fallback up to 1 MiB, with a 20,000-entry walk cap. Narrow searches for
large repositories. Native search has a 15-second deadline.

Defaults: 256 loaded sessions, 256 live subagents, 256 concurrent model requests,
32 active top-level turns, 32 shell/PTY jobs, 8 searches, 16 jobs per session, depth 2, 384 KiB model
context per agent, and 32 KiB retained output per job. Model/search/subagent/turn
limits are **global per process**; job and completed-child-history budgets are per
session. Counts are configurable up to 1,024 for sessions, children and jobs.
These limits do not promise provider capacity or start local model instances.

`max_processes = 32` is shared across all sessions' shell and terminal jobs.
Admission fails before starting another job when full; close/cancel one and retry.
Permits stay with real process/I/O cleanup, not retained completed job records.
This counts Piko-started shell/PTY jobs, not every descendant a command may fork;
MCP has its own connection limit. The laptop profile uses eight process slots.
`_harness/stats` exposes current root turns, live children, shell/PTY slots and
their configured process limit alongside model usage counters.

Top-level turns queue before history, project instructions and tools are prepared.
Child model requests use a separate shared queue, so a parent waiting for a child
does not consume the child's top-level turn slot. Live-child admission fails fast
when its limit is reached, avoiding nested lifetime-permit deadlocks. Completed
job records are evicted when slots are needed; collect outputs before starting
more work than `max_jobs` can retain.

Saved history stays durable up to 16 MiB/session. By default, idle transcripts
above `resident_history_bytes = 65536` release their JSON trees and reload only
when a turn is admitted or a picker change needs saving. Set this to zero to make
all nonempty idle transcripts cold, trading disk reads for RAM; 16 MiB retains
all saved history in memory. Preparation failures release hydrated history too.
This threshold counts encoded history bytes, not actual allocator/RSS usage.

Active model windows borrow saved items and own only the new exchange. Historical
images use copy-on-write when omitted, preserving saved originals. Save snapshots
share immutable turns and serialize through a capped 64 KiB file buffer off the
event loop. File work uses up to eight workers at once, and full load/picker-save
preparation has a separate maximum of eight (or fewer with a smaller root-turn
limit). These bounds cover decoded histories waiting to be saved as well as reads.
Snapshots/transcripts up to 256 KiB use a bounded inline serialization/parser
path; larger saves retain the shared-snapshot worker path. Neither the inline
threshold nor the worker count is a bound on total RSS.

Completed child contexts are stored as compact JSON under the aggregate
`retained_agent_context_bytes = 2097152` per-session budget. Oldest contexts expire
first; task outputs remain available until job eviction. Recent children resume
with `followup_agent`; expired children return explicit summary-and-respawn
guidance. Cancelled child histories are released immediately.

Inbound ACP frames have a 1 MiB limit. Queued and executing prompt frames share
`max_pending_prompt_bytes = 67108864` globally; overflow returns an error instructing
the client to retry when work completes. The reader continues handling cancellation
and permission responses. JSON objects, active histories, HTTP/SSE buffers, jobs,
MCP servers, terminals and browsers add memory beyond these encoded-byte budgets.
Slow clients apply output backpressure. These controls do not cap total process RSS.

ACP stdout queues at most 64 compact frames under a 4 MiB encoded-byte budget,
including the current write. One larger valid frame may own that budget
exclusively; the budget is not a maximum valid response size. Ready bursts share
flushes without a timer delay. Cancelling or abandoning a blocked permission
request retires its pending response registration too.

For large logical agent pools, start with [the laptop profile](../examples/laptop-swarm.toml):
1,024 sessions/children, 16 active root turns/model requests, cold idle history,
96 KiB contexts and smaller tool/job buffers. Provider queue time counts toward
the request deadline; tune workload and allowance rather than raising every
limit. A coordinator spawning hundreds of children may need a larger context
to retain its task IDs. Share one Piko process through an ACP client that supports
multiple sessions; launching 1,000 separate Piko processes duplicates runtime
overhead. External models and browsers have their own resource costs.

### Cache reuse, measured honestly

Stable instructions, tool order, append-only exchanges and ordered tool results
preserve reusable prefixes. OpenAI keys remain stable per session; child agents
use separate stable keys. Piko never caches generated answers as fresh responses,
pads prompts, prewarms models, or promises a cache-hit percentage.

For the official OpenAI API, GPT-5.6 and later use
`prompt_cache_options = {mode: "implicit", ttl: "30m"}` on the wire. This keeps
upstream automatic caching enabled; short or changed prefixes may have no hits.
For Responses, optionally set `prompt_cache_ttl = "12h"` in Piko's TOML config.
Chat Completions supports only `"30m"`. Earlier model families use optional
`prompt_cache_retention = "in_memory"` or `"24h"`; the older Piko spelling
`"in-memory"` is accepted and normalized. GPT-5.5 supports only `"24h"`.
Using a legacy lifetime with a modern model fails clearly before the request.
Subscription sign-in leaves cache lifetime to its backend.

Longer retention can cost more for cache writes. Cache reads and writes are
separate counters; increasing retention or concurrency does not guarantee savings.
Keep the automatic defaults until your workload's reported usage supports a
change. [OpenAI cache semantics and lifetime pricing](https://developers.openai.com/api/docs/guides/prompt-caching).

OpenRouter automatically receives a stable `x-session-id` for best-effort sticky
routing. Anthropic models, including `~anthropic/` aliases, also receive the
provider's automatic `cache_control: {type: "ephemeral"}` option. Set
`openrouter_prompt_cache = false` to omit that option; existing automatic provider
caching can still apply. [OpenRouter caching and routing](https://openrouter.ai/docs/guides/best-practices/prompt-caching).

Only exact official endpoints are recognized automatically. For an explicitly
trusted proxy, `provider_profile = "openai"` or `"openrouter"` opts into those
wire fields. The default `"auto"` leaves other compatible endpoints conservative;
`"compatible"` explicitly disables automatic provider detection. This setting
changes wire options, never authentication or where credentials may be sent.
Unsupported explicit lifetime/tier settings fail rather than being silently lost.

`_harness/usage` includes input, output, cached-input and cache-write token counts,
plus the returned `serviceTier` when supplied. `_harness/stats` exposes
`totalInputTokens`, `totalOutputTokens`, `responsesWithUsage` and
`cacheDetailsCoverage` for all recognized usage. `inputTokenCacheHitRate` is a
token-weighted ratio over responses with known cache reads; missing details remain
`null`. Compatibility aliases `inputTokens`/`outputTokens` in the aggregate cover
only those known-cache responses; use `total*` for complete observed totals.
Writes never count as hits. Counters normalize OpenAI/OpenRouter, DeepSeek, and
Anthropic/Gemini metadata shapes; this does not add their native wire protocols.
No dollar cost is guessed from token counts.

### Fast processing and recovery

Set `service_tier = "fast"` in your external Piko config to request OpenAI Fast
processing. `"priority"` is its API alias; subscription Fast is normalized to
`"priority"` on the wire. API-key requests also accept `"auto"`, `"default"`,
`"flex"` and `"ultrafast"`; Ultrafast requires Responses and an eligible model
and account. Omit the setting to use the provider's account/project default.
Fast and Ultrafast are **opt-in** and can change cost or plan usage. A request is
not a guarantee: Piko reports the tier actually returned by the provider.
It does not silently switch models or remove a rejected service tier.
Subscription backend options beyond the documented Codex mapping have not been
live-validated. [Fast](https://developers.openai.com/api/docs/guides/fast-mode),
[Ultrafast](https://developers.openai.com/api/docs/guides/ultrafast-mode),
[Codex tier mapping](https://learn.chatgpt.com/docs/config-file/config-reference).

One total deadline covers queueing, authentication, backoff and streaming.
Transient HTTP failures before streaming receive at most three jittered retries;
`retry-after-ms`, fractional `Retry-After` and HTTP dates are honored. Delays over
30 seconds or outside the remaining deadline return a retry-later error, without
retrying early. Billing/spend-cap errors and `x-should-retry: false` stop immediately.
Error envelopes are limited to 8 KiB and their arbitrary messages are not echoed.
Connection and timeout errors may retry; ambiguous request failures do not.

Streams and completed tools are never automatically replayed. Chat-compatible
providers may end cleanly after a valid finish marker without `[DONE]`; missing,
contradictory or trailing choice data fails before tool execution. Every call's
JSON arguments are validated before executing any call in its batch.
[HTTP retry semantics](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after),
[OpenAI errors](https://developers.openai.com/api/docs/guides/error-codes),
[Claude overload/spend-cap errors](https://platform.claude.com/docs/en/api/errors).

For Ollama's compatible `/v1` endpoints, configure model residency with the
**server's** `OLLAMA_KEEP_ALIVE`; Piko does not inject the native-only `keep_alive`
field. Longer residency improves warm latency but retains model memory. Keep
parallel requests modest because their context memory grows with concurrency.
[Ollama compatibility](https://docs.ollama.com/api/openai-compatibility),
[Ollama memory settings](https://docs.ollama.com/faq).

## MCP servers

Pass MCP servers in `session/new` or `session/load` (see
[`examples/mcp-session.json`](../examples/mcp-session.json)). Supported transports:
newline-delimited **stdio** and **Streamable HTTP** with JSON or SSE replies.
Legacy HTTP+SSE transport, MCP OAuth, resource/prompt discovery, sampling,
elicitation, and resumable/background SSE subscriptions are not implemented.
Use launcher-supplied environment entries or HTTP headers for authentication.
Never commit real credentials in an example or project file.

```json
{
  "cwd": "/absolute/path/project",
  "mcpServers": [
    {"name":"local","command":"/absolute/path/mcp-server","args":["--stdio"],"env":[]},
    {"name":"remote","type":"http","url":"https://mcp.example.com/mcp","headers":[]}
  ]
}
```

Connections start only on the first code-mode turn, not session creation, idle
startup, or plan-mode prompts. Identical workspace/server definitions share one
connection across sessions and worker subagents; different workspaces do not.
No MCP subprocess is created per ordinary model agent. Stdio processes and their
Unix process groups are stopped when their last session/tool snapshot is dropped.
HTTP session expiry remains server-managed. Closed stdio connections and expired
HTTP sessions (404) reconnect on a subsequent turn; interrupted tool calls are
not automatically retried, because they may already have side effects.

Initialization negotiates the protocol version, discovers paginated tools and
caches schemas for up to 60 seconds. Stdio `tools/list_changed` notifications
invalidate that cache for the next turn; tool sets stay fixed within each turn.
Model names use `mcp_<server>_<tool>_<stable-hash>` to prevent namespace collisions.
Descriptions and results are untrusted data. **Every remote tool call requires
ACP permission**, including tools marked read-only by their server. Plan,
explore and reviewer agents receive no MCP tools.

Bounds: 8 servers per session; `max_mcp_connections = 32` distinct pooled
workspace/server definitions by default (configurable 1–256); 32 tools per turn;
4 KiB per schema; 64 KiB request frames; 1 MiB replies; 64 pending stdio requests
per connection; 45-second I/O phase timeouts. Excess work fails explicitly instead
of spawning unbounded processes. Cancellation drops the local wait and sends a
best-effort MCP cancellation notification; remote side effects cannot be undone.

Stdio environments are cleared, then a small runtime allowlist and explicitly
configured entries are forwarded. HTTP requires TLS except on loopback, disables
redirects and keeps supplied credentials out of diagnostics. Configuration/env/
headers are not persisted with conversation history: resupply `mcpServers` on
load. **MCP servers are trusted executable/network integrations, not sandboxes**:
server startup itself can run arbitrary code, and inherited `HOME` still locates
user files. Only the trusted ACP client may configure them.

## Opt-in computer use

Use [`examples/computer.toml`](../examples/computer.toml), or add `computer_use = true`
to an explicitly selected launcher configuration. The bundled
[`computer-use` skill](../skills/computer-use/SKILL.md) is automatically added to
code/worker instructions when enabled. Normal runs load no computer processes,
capture no screenshots and add no computer schema/instructions to model requests.

A vision-capable model/provider is required to interpret images. Platform setup:

- **macOS:** native ScreenCaptureKit/CoreGraphics capture and event APIs. Grant
  **Screen Recording** and **Accessibility** to the app or terminal launching Piko
  in System Settings → Privacy & Security, then restart that app. Screen Recording
  may be named Screen & System Audio Recording. Piko checks permissions and reports
  missing access without opening permission prompts or changing system settings.
- **Windows:** native User32 input, GDI capture and the system JPEG encoder. Run in
  the signed-in, unlocked interactive desktop session. Secure/UAC screens and
  applications at a higher integrity level cannot be controlled; Piko does not
  bypass these Windows restrictions. No PowerShell or clipboard is used.
- **Linux:** X11 with `xdotool`, ImageMagick's `import`, and access through
  `DISPLAY`/`XAUTHORITY`. Set `computer_xdotool`/`computer_import` in trusted config
  for custom executable paths. Native Wayland is not supported.

macOS and Windows capture their primary display. Pointer coordinates always refer
to the most recent returned JPEG, including any adaptive resizing; secondary
monitors are not addressed. Take a fresh screenshot after a display change.
Typing sends literal Unicode keyboard events on macOS/Windows without replacing
clipboard contents. Use `key` for shortcuts: `Ctrl+l`, `Shift+Tab`, `Return`,
`Escape`, `Left` or `PageUp`. `Alt`/`Option` and `Meta`/`Super`/`Command` are accepted;
`Meta` maps to Command on macOS and the Windows key on Windows. Linux also accepts
X11 key names.

| Action | Arguments |
| --- | --- |
| `screenshot` | No other arguments |
| `move`, `click`, `double_click` | `x`, `y`; optional `button`: left/middle/right |
| `type` | `text` (maximum 4096 bytes, passed literally without a shell) |
| `key` | `key`, e.g. `Return`, `Escape`, `ctrl+l` |
| `scroll` | `direction`: up/down/left/right; optional `amount`: 1–20 ticks |

All actions, **including screenshots**, require explicit ACP permission. Plan,
explore and reviewer agents cannot use them. Native pointer input requires a
screenshot from the same agent. Desktop actions are serialized process-wide and have bounded output and deadlines. Cancellation stops subsequent
typing chunks; dispatched input cannot be undone. Use one desktop-driving session
at a time, or separate displays/VMs: serialized actions
do not isolate focus across whole multi-step flows. Input dispatch is not proof
of UI success; inspect a fresh screenshot after each action.

Screenshots are JPEG, at most 1280×960 and 96 KiB; pointer coordinates refer to
that image and are scaled back to the native display. macOS/Windows reduce image
size further when needed to fit the byte limit. Linux images that exceed the
limit fail explicitly. MCP tool results support bounded text, structured content,
embedded text resources and one validated PNG/JPEG image (96 KiB, at most 4096 per
dimension and 8 million pixels). Resource links are described, never fetched
automatically. Audio, blobs and unsupported/oversized images are omitted with an
explicit marker. Responses receives image tool-output blocks; Chat Completions
receives paired text tool results followed by image user content. Both ACP live
updates and session replay include retained images. Older tool images are replaced
with an explicit omission marker when they exceed half the context-byte budget.
Schemas, instructions and image data all count against the context budget.

**Desktop access is not workspace scoped or sandboxed.** Close private windows
first. Approved screenshots go to the model provider, trusted ACP client and
private, unencrypted session storage. The skill prohibits inspecting credentials,
typing secrets, bypassing denials, or submitting external/destructive actions
without explicit user intent and approval. Do not assume automatic secret
redaction. Let the human perform login and other sensitive steps themselves.

## Safety boundaries

Code mode requires explicit client approval for **every** write/edit/shell call.
Plan mode disables those tools, including in children. Permission denial is
returned to the model; persistent deny policies should also be enforced by the
client. File tools reject traversal, workspace/symlink escapes, credential paths,
and harness state. Edits are not committed automatically.
On Unix, hard-linked regular files are also excluded from file tools/search.
Approval requests include path/operation/size metadata rather than unrelated old
file contents. Model-supplied tool arguments remain visible in ACP tool updates;
the ACP client is trusted, and this is not a general-purpose secret redactor.

**This is not an OS sandbox.** An approved shell command can access the host and
network, read the user's files, and run arbitrary programs. Shell environments are
cleared and a small runtime allowlist forwarded, but `HOME` still locates user
files. Run untrusted work in a container/VM with restricted credentials. Filesystem
checks are defense in depth, not a race-proof capability sandbox. Unix shell task
process groups are killed on cancellation/timeouts; Windows uses Job Objects to
terminate contained descendants. Job assignment follows process creation, so a
very fast child can spawn a descendant before assignment. Neither mechanism is
a security sandbox. Background jobs do not survive process restart.
Prompt cancellation stops that turn and its children, not independent jobs from
earlier completed turns. Cancel retained jobs by task ID, unload their session,
or shut down the process. SIGINT/SIGTERM trigger orderly Unix shutdown.

## Protocol surface

ACP: `initialize`, `authenticate`, `session/new`, `session/load`,
`session/prompt`, `session/cancel`, `session/set_mode`, `session/set_model`,
`session/set_config_option`; notifications via
`session/update`; approvals via `session/request_permission`.

Sessions expose standard ACP `configOptions` for model, reasoning effort and
Code/Plan mode, alongside legacy `models`, `session/set_model` and
`session/set_mode`. `session/set_config_option` returns the complete updated list
and emits `config_option_update`. Invalid values return JSON-RPC `-32602`.
Model changes preserve compatible effort choices and otherwise use the new
model's advertised default. `Provider default` omits the reasoning field.
Selections are isolated per session, persisted on disk and inherited by subagents.
Older sessions migrate when loaded.

Extensions (all stdio JSON-RPC, no extra listener):

| Method | Parameters / purpose |
| --- | --- |
| `_harness/tasks` | `sessionId`: list retained jobs |
| `_harness/task/cancel` | `sessionId`, `taskId`: cancel a job and descendants |
| `_harness/session/unload` | `sessionId`: free an idle loaded session, cancel its jobs |
| `_harness/stats` | `{}`: observed provider cache-token counters |
| `_harness/models` | `{}`: discovered models and reasoning metadata without creating a session |
| `_harness/task` (notification) | task state/output deltas |
| `_harness/usage` (notification) | provider usage for a completed model response |

Text and local `file:` resource-link prompts are supported. Image **tool outputs**
are supported as above; incoming image/audio prompts and client filesystem/terminal
delegation are not implemented. MCP HTTP support is advertised during initialize
(stdio is implicit in ACP); legacy SSE transport is explicitly not advertised.
Model context is a bounded window of complete turns; saved history survives that
projection. Conversation storage stops at 16 MiB per session instead of silently
dropping old turns. Active-turn checkpoints record user intent, pending tools and
completed tool groups. Loading repairs interrupted calls without executing them.
After an interrupted host action, inspect current state before retrying it.
See [execution and recovery design](design.md).

## Development

Maintainers: see [the staged release and approval procedure](releases.md).

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked && python3 tests/stdio.py && python3 tests/reliability.py
```

Ordinary tests use temporary workspaces, mock desktop executables and loopback
model/MCP servers; no paid model calls or credentials are needed. Native Windows
desktop tests opt in with `PIKO_NATIVE_DESKTOP_TEST=1` and use their own temporary
window. macOS permission-denial checks do not capture the desktop or send input;
positive macOS checks require the launcher's system permissions. CI does not
validate live subscription eligibility, model intelligence, real provider caching,
or comparisons against other agents.

Reproduce a release-mode local-overhead/concurrency/search benchmark:

```sh
cargo build --release --locked
python3 tests/benchmark.py --agents 10
```

It reports startup, throughput/latency against a simulated 50 ms loopback model,
Linux process RSS/high-water mark or macOS sampled RSS, and end-to-end native search over 10,000 files.
Small synthetic conversations do not predict memory use for full coding sessions.

### Coding-task validation (2026-10-21)

Piko 0.5.0 used an existing ChatGPT login with `gpt-6-luna` and low reasoning
on a small isolated Python fixture. It read project guidance and source, fixed
inclusive clamping and invalid bounds through one approved exact edit, and ran
`python3 -m unittest -q` through one approved shell call. An independent test
run passed both tests; the unrelated sentinel file was unchanged. The turn used
five model responses and completed in about ten seconds on this Mac.

This checks one real read/edit/verify workflow. It does not establish a coding
benchmark win, broad model quality or production error rate. No credentials were
copied into the fixture or test output.

### Bounded live validation (2026-10-19)

Ten concurrent ACP sessions used ChatGPT subscription authentication with
`gpt-6-luna`, low reasoning, and one model step each. Each answered a short,
distinct source-code question from a provided excerpt; all ten completed with
the expected answer and isolated case identifier. No writes or shell calls were
approved. This was not a ten-agent repository-editing or long-running soak test.

| Measurement | Observed |
| --- | ---: |
| Total wall time | 3.133 s |
| Per-agent latency p50 / p95 (nearest rank) | 1.270 / 3.133 s |
| Idle / ten loaded sessions / peak sampled RSS | 5.07 / 5.30 / 8.46 MiB |
| Maximum observed process threads | 9 |
| Provider input / output tokens | 11,437 / 160 |
| Provider-reported cached input tokens | 0 |

A separate small, same-session read-tool/repeated-context check also reported
zero cached tokens. These observations do not establish a general cache rate.
Earlier 256-agent benchmarks were entirely loopback/synthetic and consumed no
subscription quota. Ten successful live turns do not establish 256-agent provider
capacity, sustained throughput, rate limits, or full-session memory use.

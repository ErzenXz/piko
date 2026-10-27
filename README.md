<p align="center">
  <img src="https://raw.githubusercontent.com/ErzenXz/piko/main/docs/assets/banner.svg" alt="Piko — native Rust, small footprint." width="100%">
</p>

<p align="center">
  A tiny, powerful coding agent.<br>
  macOS · Linux · Windows &nbsp;/&nbsp; arm64 · x64
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@erzenx/piko">npm</a> ·
  <a href="https://github.com/ErzenXz/piko/releases">Downloads</a> ·
  <a href="https://github.com/ErzenXz/piko/blob/main/docs/reference.md">Documentation</a>
</p>

## Quick start

```sh
pnpm add -g @erzenx/piko
piko login
```

Requires pnpm and Node.js 22.15+. Installs only your platform's native binary, with no
compilation or install scripts. [Standalone downloads](https://github.com/ErzenXz/piko/releases)
run without Node.js. Linux uses static musl; Windows runs natively.

Set your ACP app's agent command:

```json
{ "command": "piko", "args": ["--acp"] }
```

Your app provides the interface; Piko communicates over stdin/stdout.
Subscription login uses your plan's model access and limits.
[Other providers and API keys →](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#authentication-and-providers)

Upgrading from `codex-harness`? Install Piko and change your agent command to
`piko`; your existing login and sessions are reused automatically.

Available models and reasoning levels load automatically. Choose them in your
ACP app; selections persist per session. [Discovery and picker details →](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#model-discovery-and-reasoning)

## Performance

Measured locally on **macOS arm64 · v0.7.0**.

| Metric | Result |
| :--- | ---: |
| Native binary | **3.35 MB** |
| ACP startup + initialize | **6.56 ms** |
| Idle memory, sampled RSS | **3.83 MiB** |

Median of three local synthetic runs; warm startup includes fixture setup.
Model inference and browser memory are excluded. Timings and RSS vary by workload.
[Raw results](https://github.com/ErzenXz/piko/blob/main/docs/benchmarks/macos-arm64-0.7.0.json) ·
[Before/after results and reproduction](https://github.com/ErzenXz/piko/blob/main/docs/performance.md)

[Unreleased binary-size measurements and TLS tests →](docs/binary-size.md)

## Built to stay light

One async event-loop thread, shared tool schemas and HTTP connections, lazy MCP and
bounded history, files and tool output. No telemetry, daemon, updater or bundled
browser/model.

Large agent pools can use the [laptop profile](examples/laptop-swarm.toml): shared
runtime, bounded active turns, disk-backed idle history and compact subagent
contexts. [Scale tests and limits →](docs/performance.md)

File/search tools, exact patches, image viewing, interactive terminals, subagents
with messaging, MCP and recoverable sessions are built in. Stable prompt prefixes,
provider-aware caching and bounded retries keep repeated work efficient. OpenAI
Fast/Ultrafast are opt-in; cache reads, writes and actual service tiers are reported.
[Provider options and cache metrics →](docs/reference.md#cache-reuse-measured-honestly)

Approved web fetching
and optional web search/browser automation expand what the agent can verify.
[Tool setup →](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#web-browser-images-and-interactive-tools)
[Design and recovery →](https://github.com/ErzenXz/piko/blob/main/docs/design.md)

For the lowest launch overhead, run `piko --print-binary-path` and use
that executable directly in your ACP app. On macOS/Linux the standard launcher
replaces Node with Rust where `execve` is available; Windows keeps a forwarding
process until you use the native path.

Web search needs your Brave API key; browser automation uses an explicitly
configured installed Chrome/Edge with a dedicated profile. No browser is bundled.
Approved tools can access your host. Opt-in desktop control uses native macOS/Windows
APIs or Linux X11; no desktop automation package is bundled.
[Desktop setup →](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#opt-in-computer-use) · See
[security and limits](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#safety-boundaries).

---

[Configuration](https://github.com/ErzenXz/piko/tree/main/examples) ·
[Install, update & remove](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#installation-and-maintenance) ·
[Development](https://github.com/ErzenXz/piko/blob/main/docs/reference.md#development) ·
[MIT license](https://github.com/ErzenXz/piko/blob/main/LICENSE)

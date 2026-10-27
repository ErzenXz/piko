# Performance

Piko is a single native Rust process with one async event-loop thread,
shared HTTP connections and on-demand blocking workers. It starts no MCP servers,
browsers or models until explicitly needed. Cap concurrency to your machine and
provider allowance; unload idle sessions with `_harness/session/unload` to release
retained history and jobs.

## Current development: active history and runtime resources (2026-10-27)

Compared the previous development binary with this pass in three alternating
pairs on macOS arm64. Both used 32 sessions with 8 MiB of prior history each,
128 KiB prompts, a 384 KiB model window and 32 held mock response streams. The
fixture verifies every original turn, the new exchange, model-window ordering,
reload playback and that omission notices never enter saved history.

| Median across three runs | Previous development | Current development |
| --- | ---: | ---: |
| Native binary bytes | 3,352,912 | 3,403,024 |
| Observed workload maximum RSS | 944,560 KiB | 295,232 KiB |
| RSS after full-history commits | 692,352 KiB | 292,224 KiB |
| Initial history load and replay | 2.592 s | 1.992 s |
| Prepare all model requests | 0.910 s | 0.715 s |
| Release replies through full-history commit | 1.406 s | 0.389 s |
| Durable reload and replay | 2.556 s | 1.984 s |

This workload's sampled maximum is **68.7% lower**; its commit phase is about
**3.6 times faster**, rather than an end-to-end model speed claim. All three
current sampled maxima were below all three previous maxima. Model windows
borrow saved items; only current work is mutable. Image-only copy-on-write
preserves durable screenshots. Save snapshots share turn storage, large files
stream through capped 64 KiB buffers and preparation/file operations are bounded.
Small transcripts up to 256 KiB use bounded inline encoding/parsing after I/O
admission, avoiding a worker buffer where a compact snapshot suffices.

The **short-session workload has a memory tradeoff**. The same three paired runs
of 1,000 sessions, four 32 KiB turns each, 16 mock requests and a 384 KiB context
completed all history, cancellation and reload checks, but sampled peak RSS rose
from 105,440 to 133,296 KiB (**26.4% higher**). Completed-session RSS medians were
64,928 versus 121,056 KiB. First-turn time improved from 2.616 to 2.463 s; the
following three turns improved from 10.355 to 9.130 s (11.8%). Allocator retention,
worker scheduling and allocation patterns affect RSS even after objects are
released; this pass is not uniformly lighter for every workload.

A single 1,000-actual-child fixture passed at **33,744 KiB sampled maximum**,
with 16 simultaneous mock requests and a 1.572 s workload including measurement
holds. Recent followup, older-context expiry, read-only isolation and preserved
results passed. A separate 1,000-session laptop-profile-equivalent run passed
with 81,744 KiB sampled maximum. Neither is a repeated speed comparison.

No runtime dependencies were added. The native binary grows by 50,112 bytes
(1.5%). Global shell/PTY admission prevents independent sessions from each
opening their own full process/terminal allowance. Retirement events replace
repeated mailbox scans; output queues and rolling logs are byte bounded.

[Full samples, hashes, settings and limitations](benchmarks/macos-arm64-active-history-development.json).
These are unpublished native-PID measurements, excluding Python, Node launcher,
inference and external-tool memory. Other host activity was not controlled, and
sampled maxima are not guaranteed absolute peaks. CI now exercises session,
child and long-history fixtures on all six native targets, including Windows
working-set collection; check the workflow result separately from local data.

```sh
python3 -B tests/long_history_benchmark.py --binary /absolute/previous/piko \
  --binary /absolute/current/piko --sessions 32 --history-mib 8
```

Use `.exe` native paths on Windows. The benchmark writes about 256 MiB of
temporary fixture history per binary and removes it afterward. Published npm
0.7.1 does not include either development pass; the earlier pass below is a
different binary, identified separately by hash.

## Earlier development: bounded 1,000-agent pools (2026-10-27)

These changes are **local and unpublished**, based on `f57b53e`. Compared the
public npm 0.7.1 macOS arm64 binary with the development release build in three
alternating pairs, without concurrent task builds/tests. Both used 1,000 sessions,
four 32 KiB prompts per session, a 384 KiB context and 16 mock model requests at
once. Sixteen queued first turns were cancelled; each run completed 3,984 workload
prompts and an additional reload-verification prompt. Every provider request
checked history continuity and session isolation. Native PID samples exclude
Python, inference and external tools; other host activity was not controlled.

| Median across three runs | npm 0.7.1 | Development |
| --- | ---: | ---: |
| Native binary bytes | 3,352,784 | 3,352,912 |
| Idle RSS | 3,952 KiB | 3,984 KiB |
| Observed workload maximum RSS | 202,320 KiB | 83,680 KiB |
| RSS after four turns in 1,000 sessions | 167,008 KiB | 24,992 KiB |
| First-turn completion after fixture release | 2.796 s | 2.796 s |
| Subsequent three turns, including phase samples | 13.777 s | 12.716 s |

The median sampled maximum is **58.6% lower**; sampled memory after the workload
is **85.0% lower**. Individual development completion samples range from 21,472 to
85,920 KiB, so these are not fixed RSS bounds. Idle memory is slightly higher and
the binary grows by 128 bytes. First-turn speed is unchanged at the median; later
turns are about 7.7% faster here, with overlapping timing ranges. This does not
establish a universal speedup or a ranking against another agent.

The new defaults gate 32 root turns before expanding contexts and release idle
transcripts above 64 KiB after saving. ACP prompt parameters and large tool-call
arguments are moved/referenced instead of copied. Provider/auth clones share
configuration. Completed child contexts use compact JSON under an aggregate
per-session byte budget, independently of retained task outputs.

A separate **actual 1,000-child-agent** fixture passed matching results,
read-only/context isolation, recent-child followup, explicit older-context
expiry, preserved results and unload checks. It creates children in batches of
32, uses a 64 KiB retained-context budget and at most 16 simultaneous mock requests.
This single run observed 41,632 KiB native RSS and completed in 2.158 seconds,
including measurement holds and a 2 ms simulated child-model delay. It does not
mean 1,000 model requests were active simultaneously.

A single conservative laptop-settings run also passed 1,000 sessions with 16
active roots, 16 model requests, 96 KiB contexts and fully cold idle history.
Its observed maximum was 36,080 KiB, while first-turn completion took 10.809 s
and the remaining three turns 29.854 s. Aggressive RAM limits can trade away
throughput; tune them on your workload rather than assuming lower limits are
always faster. Real models, terminals and browsers add separate costs.

[Full samples, binary/source hashes and checks](benchmarks/macos-arm64-swarm-development.json).
The existing release version is intentionally unchanged; installing npm 0.7.1
does not include this development patch. Linux/Windows performance was not measured.
The verification workflow includes the new functional regression tests on all
six targets and small scale fixtures on macOS/Linux; adding those steps is not
evidence of a completed remote CI run.

```sh
# Build one native process, then compare with the separately installed old binary.
cargo build --release --locked
python3 -B tests/scalability.py --binary /absolute/old/piko \
  --binary target/release/piko --sessions 1000 --turns 4 \
  --model-concurrency 16 --prompt-kib 32 --context-kib 384
python3 -B tests/subagent_scalability.py --binary target/release/piko --agents 1000
```

Use [the laptop profile](../examples/laptop-swarm.toml) with a development binary
and an ACP client that multiplexes sessions into one process. A thousand separate
CLI processes or local model instances are a different workload. Limits and
failure behavior are described in the [reference](reference.md#performance-and-memory).

Version 0.3 adds lazy model discovery using the existing HTTP pool. Sessions share
one immutable catalog; in-memory and private disk caches last five minutes.
Discovery has a five-second deadline, a 2 MiB input limit and a 256 KiB normalized
metadata limit. It adds no runtime dependencies or background polling.

Version 0.2 removes repeated streamed-argument copies, appends SSE line fragments
in bulk, moves completed output, counts JSON without duplicate buffers and trims
the old rolling history without cloning the result. Version 0.5 preserves durable
history separately from its model-context projection. Built-in grep reuses one bounded file buffer.
Version 0.5 uses size optimization (`z`), full LTO and one codegen unit.
`--help` and `--version` return before creating an async runtime or user state.

The npm launcher installs one exact-version platform binary, with no postinstall
script or network access at launch. On macOS/Linux it calls Node's `execve` when
available, replacing Node with Rust and freeing the launcher memory. On Windows
Node remains as a forwarding process. Use the path from `--print-binary-path`
directly in your ACP client to eliminate launcher overhead on every platform.

Version 0.5 borrows Responses input during serialization, encodes it once and
shares request bytes through retries. Those bytes are freed after successful
headers, before waiting for the response stream. Durable active-turn checkpoints
encode only current work; atomic file operations run in one blocking dispatch.
Extra durability checks have a cost, particularly for very short mock responses.

## Reproduce

```sh
cargo build --release --locked
PIKO_BIN=target/release/piko python3 tests/benchmark.py --agents 10
```

On Windows set `PIKO_BIN` to the `.exe` path in your shell first. The benchmark
uses temporary state/workspaces and loopback mock SSE; no credentials or paid
model calls. It reports native binary bytes, ACP startup, ten simultaneous turns,
process RSS (Linux/macOS) and search over 10,000 files. The default is 256 turns;
`--agents 10` selects the smaller fixture. Startup includes fixture
setup; first-launch OS verification and filesystem cache materially affect it.

Results do not establish a competitor ranking, model intelligence, real provider
throughput or a cache hit rate. RSS varies with platform, workload, active tools,
conversation length and configured concurrency. Context bytes bound serialized
model input; durable history has a separate 16 MiB/session cap. Neither caps total
process RSS. Read the runtime limits in the
[technical reference](reference.md#performance-and-memory).

## Version 0.7.0 native desktop footprint (2026-10-22)

Compared the installed 0.6.2 npm binary with a local 0.7.0 release build on the
same macOS arm64 machine. Both binaries completed a warm-up; three alternating
256-prompt fixtures per version ran without concurrent local builds, tests or
desktop automation. Medians below measure normal sessions with desktop control
disabled, excluding model inference and graphics framework use.

| Measurement | 0.6.2 | 0.7.0 |
| --- | ---: | ---: |
| Native binary bytes | 3,302,528 | 3,352,832 |
| ACP startup + initialize | 6.51 ms | 6.56 ms |
| Startup observed range | 6.27–9.43 ms | 6.55–9.96 ms |
| Idle sampled RSS | 3,952 KiB | 3,920 KiB |
| 256 mock prompts, 50 ms simulated delay | 0.672 s | 0.691 s |
| Sampled RSS after 256 completed prompts | 30,880 KiB | 29,680 KiB |
| 10,000-file search, end-to-end | 328.78 ms | 271.30 ms |

Native desktop support adds **50,304 bytes (1.5%)** on this platform, with no new
runtime dependencies. Startup and idle memory are roughly unchanged in these
samples. Search is faster here; mock turns are slightly slower. Small sample
sizes, caches and scheduling prevent a general speedup or memory-saving claim.

macOS graphics frameworks load only when an approved desktop action reaches the
native driver. Windows loads the system GDI+ encoder on the first approved
capture. Screenshots target a resized surface rather than a full Retina/4K bitmap
on current macOS and Windows. macOS before 14 uses the legacy capture fallback.
Images are at most 1280×960 and 96 KiB; adaptive compression may shrink them
further. Native OS calls use on-demand blocking workers and a shared desktop
gate; normal coding sessions start no desktop helper, capture loop or daemon.
Desktop use loads system libraries and transient image buffers, so these idle
numbers do not describe active capture memory or external WindowServer/browser
memory. Both platforms require the OS permissions described in the reference.

[Raw runs and binary hashes](benchmarks/macos-arm64-0.7.0.json).

## Version 0.6.2 tools and footprint (2026-10-22)

Compared the installed 0.5.0 npm binary against a local 0.6.2 release build on the
same macOS arm64 machine. The earlier 0.5.0 baseline uses the same fixture; three
0.6.2 runs were recorded as a block after a dedicated warm-up, with no concurrent
local agent builds, tests or browser automation. These runs were not randomized.
Numbers below are medians. Startup includes fixture setup and excludes initial OS
verification; timing and RSS remain sensitive to filesystem caches and paging.

| Measurement | 0.5.0 | 0.6.2 |
| --- | ---: | ---: |
| Native binary bytes | 3,051,760 | 3,302,608 |
| ACP startup + initialize | 7.07 ms | 5.91 ms |
| Startup observed range | 6.97–12.34 ms | 5.66–5.92 ms |
| Idle sampled RSS | 3,776 KiB | 3,904 KiB |
| 256 mock prompts, 50 ms simulated delay | 0.650 s | 0.678 s |
| Sampled RSS after 256 completed prompts | 33,888 KiB | 29,920 KiB |
| 10,000-file search, end-to-end | 278.48 ms | 302.99 ms |

The new web, image, patch, PTY and messaging tools add **250,848 bytes (8.2%)** to
the native executable. This release is larger than 0.5.0. It ships no browser,
model, Node tool server or JavaScript runtime dependencies. Browser RAM is
external; each live terminal adds two small-stack I/O threads and OS resources.

Built-in tool schemas are shared across sessions instead of being rebuilt and
copied for every prompt. Responses reuses their encoded bytes across steps. This
removed an allocation regression measured during development. The final fixture's
completed-prompt RSS is lower, while idle RSS is slightly higher. Startup is
faster in these samples; search and the short-turn fixture are slower. This is
**not a general speedup or universal memory-reduction claim**. The expanded tool
catalog also consumes more model context and request bytes.

A separate held-stream fixture consumed every 128 KiB prompt and received all
successful SSE headers before sampling. At 64 concurrent streams, steady sampled
RSS was 47,840 → 46,352 KiB, and observed workload maximum was 53,280 → 51,984 KiB.
The 32-stream result is close (27,040 → 26,032 KiB); one stream is slightly higher
in 0.6.2 (4,960 → 5,104 KiB). Raw phase samples include page reclamation and all runs.

[Raw results, hashes, held-stream samples and validation scope](benchmarks/macos-arm64-0.6.2.json).
A real Chrome smoke test verified a local page through CDP navigation, snapshot,
typing, click, evaluation, screenshot and close with a temporary dedicated profile.
Public web fetching succeeded against agents.md; search uses documented HTTP
fixtures and no paid search key. A live ChatGPT coding check exercised image
viewing, patching and shell verification while preserving other fixture files.
The unpublished [0.6.0](benchmarks/macos-arm64-0.6.0.json) and
[0.6.1](benchmarks/macos-arm64-0.6.1.json) candidate measurements are retained
separately. Windows startup and cross-target license coverage were fixed before
publishing.

## Version 0.5.0 comparison (2026-10-21)

Compared the installed 0.4.1 native binary against a local 0.5.0 release build,
on the same macOS arm64 machine with Rust 1.99. Three alternating runs per binary
used the identical 256-prompt fixture. Both binaries were warmed before these runs;
first-launch OS verification is excluded. Numbers below are medians of those runs.

| Measurement | 0.4.1 | 0.5.0 |
| --- | ---: | ---: |
| Native binary bytes | 4,561,504 | 3,051,760 |
| ACP startup + initialize | 13.36 ms | 8.08 ms |
| Idle sampled RSS | 4,064 KiB | 3,744 KiB |
| 256 mock prompts, 50 ms simulated model delay | 0.621 s | 0.644 s |
| 10,000-file search, end-to-end | 235.73 ms | 254.66 ms |

The binary is **33.1% smaller**. The short-turn timing is roughly flat, not an
end-to-end speedup claim. Search turns now include extra recovery checkpoints;
that durability adds work. Three runs do not establish timing significance.
Startup remains sensitive to filesystem and OS verification caches.

A separate allocation fixture fully consumed each 128 KiB request, sent successful
SSE headers, and held all response bodies. At **64 concurrent streams**, steady
sampled native RSS was **60,928 → 48,000 KiB (21.2% lower)**; the observed workload
peak was **66,256 → 52,960 KiB (20.1% lower)**. Both held phases were stable across
12 samples. These are process samples, not a guaranteed peak-memory bound.

The one- and 32-stream runs are included in the raw data too. Some phases show
page reclamation during sampling; at one stream the old binary's held-phase
median was lower. Small RSS differences and transient drops are not reliable
universal savings. This fixture excludes the Python server and npm launcher.

```sh
python3 tests/memory_benchmark.py --binary /absolute/old/piko \
  --binary /absolute/new/piko --agents 1 32 64 --prompt-kib 128
```

For the development binary, run this held-stream fixture separately with
`--active-turns 64`: every requested stream must be admitted before the fixture
releases any response. Older binaries do not accept that configuration field.
The default fixture now stops at 32 streams; use `scalability.py` to measure queued
work above the root admission limit.

[Raw samples, binary hashes, environment and live coding-task result](benchmarks/macos-arm64-0.5.0.json).
The live task passed its independent tests and preserved an unrelated file;
it is one functional check, not an intelligence benchmark.

## Version 0.4.0 measurement (2026-10-21)

The [six-target Piko verification](https://github.com/ErzenXz/piko/actions/runs/39651456713)
passed on macOS/Linux/Windows, arm64/x64. The installed macOS arm64 binary matched
that CI artifact exactly: 4,561,504 bytes, with 3.97 MiB idle sampled RSS and
16.72 ms through ACP initialization in one local run.

This run used 256 simultaneous mock turns, completing in 0.793 seconds with a
50 ms simulated provider delay. Sampled RSS after those turns was 38.45 MiB.
The concurrency differs from the ten-turn fixtures below, so these are not direct
speed or memory comparisons. Model inference and real provider limits are excluded.

[Raw results and binary hash](benchmarks/macos-arm64-0.4.0.json).

## Version 0.3.1 measurement (2026-10-21)

The [six-target release verification](https://github.com/ErzenXz/piko/actions/runs/39641540994)
passed on native macOS/Linux/Windows runners, each on arm64 and x64. The public
npm install matched the verified macOS arm64 artifact by SHA-256.

One local run of the optimized macOS arm64 release measured a 4,576,720-byte
binary, 8.18 ms through ACP initialization, 4.02 MiB idle sampled RSS and 6.53 MiB
after ten concurrent mock turns. Those turns completed in 72 ms with 50 ms mock
provider delay. Initialization includes temporary fixture and HTTP setup.
These are synthetic overhead measurements, not provider throughput guarantees.

[Raw results and binary hash](benchmarks/macos-arm64-0.3.1.json).

## Version 0.2.0 before/after measurements (2026-10-21)

Same Mac, Rust 1.99, baseline `6f55af7` versus 0.2.0. Byte counts refer to
stripped native release binaries. Startup uses 31 fresh `--version` processes;
streamed-argument timings are the median of nine local runs.

[Raw measurements and individual samples](benchmarks/macos-arm64-0.2.0.json)
include both binaries' hashes, the test environment, available samples and
summary statistics.

| Measurement | Before | After |
| --- | ---: | ---: |
| Native binary bytes | 4,803,360 | 4,475,120 |
| Version startup p50 | 6.01 ms | 5.82 ms |
| Version startup p95 | 10.74 ms | 9.65 ms |
| Append 8,000 × 32-byte argument deltas | 58.24 ms | 2.02 ms |
| Idle sampled RSS | 3.80 MiB | 4.02 MiB |
| RSS after ten synthetic turns | 6.84 MiB | 6.58 MiB |
| Ten concurrent mock turns, wall time | 73 ms | 67 ms |

The binary is 6.8% smaller. The isolated streamed-argument hot path is about 29×
faster for this 256,000-byte fixture; this is **not a 29× end-to-end agent speedup**.
The ten-session figures are one synthetic run per binary with 50 ms mock model
latency, small prompts and filesystem persistence. The idle RSS difference shows
that lower allocation churn does not guarantee lower RSS in every phase.

Reproduce the stream hot path with:

```sh
cargo run --release --locked --example stream_benchmark
```

`--version` startup and file-search results are especially sensitive to OS binary
verification and filesystem caches. Compare repeated runs on your target workload
instead of treating small timing differences as guarantees.

## Platform and installation verification

The [release verification workflow](https://github.com/ErzenXz/piko/actions/runs/39605798717)
passed on macOS, Linux and Windows, each on arm64 and x64. It runs Rust tests,
ACP integration tests, launcher and installation checks, then validates native
packages and standalone archives. Performance numbers above come from macOS;
the other platforms were checked for functionality.

A separate fresh pnpm installation from the public npm registry installed only
the matching macOS arm64 package. Its installed command passed 23 ACP integration
tests; the Linux X11 desktop fixture was skipped. All seven npm payloads matched
the verified release artifacts' SHA-512 integrity values.

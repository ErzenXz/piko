# 0.7.1

- Restore ordinary installation under pnpm’s no-downgrade trust policy by staging all seven packages with provenance and requiring maintainer approval before release.
- Preserve exact staged artifacts for finalization without rebuilding, reject missing approval or mismatched release provenance, and test public-registry installs with trust checks enabled.
- Update release tooling to pnpm 12.15.0. Agent runtime behavior and dependencies are unchanged.

# 0.7.0

- Add native macOS and Windows computer use through system capture, JPEG and input APIs, with no new dependencies, helpers, browser downloads or clipboard changes.
- Bound captures to 1280 × 960 and 96 KiB with adaptive compression. Map pointer coordinates from the actual returned image, preserving Retina and Windows DPI scaling.
- Keep desktop access opt-in, approved through ACP and disabled in read-only mode. Require a same-agent screenshot before native pointer input; reject stale display geometry.
- Run blocking OS calls outside the event loop and serialize desktop access through cancellation and timeout. Explain missing OS permissions and protected-desktop limits.
- Exercise actual Unicode, key chords, pointer actions and scrolling in isolated native desktop fixtures, alongside all six platform release checks.

# 0.6.2

- Add approved web fetch with public-network DNS pinning and bounded text, plus
  optional Brave-compatible search returning source links. Search requires a key.
- Add optional native Chrome/Edge CDP browser control: tabs, navigation, page
  snapshots, selectors, JavaScript and screenshots. Reuse an installed browser
  with a dedicated profile; no bundled browser, Playwright or download.
- Add workspace PNG/JPEG viewing and exact-context apply_patch with full preflight,
  protected paths, stale-content checks and atomic per-file writes.
- Add genuine Unix PTY / Windows ConPTY sessions with approved input, resize,
  rolling output, deadlines, cancellation and process-tree cleanup. Windows uses
  UTF-8 cmd.exe batch scripts with automatic cleanup, preserving quoted paths
  and environment expansion without starting PowerShell for every terminal.
- Add bounded parent-to-child agent messages and follow-up turns preserving child
  context; close the final-reply message race and keep read-only limits intact.
- Share immutable built-in schemas across sessions and encode them once; reuse
  Responses schema bytes across model steps. Keep optional tools lazy and their outputs, queues and lifetimes bounded.
- Answer Windows ConPTY cursor negotiation without model input; propagate active
  parent-turn cancellation to resumed child agents.
- Expand native HTTP/WebSocket/PTY fixtures and end-to-end ACP checks across all
  six release targets; smoke-test an installed Chrome against a local page.
- Include the union of dependency notices across all release targets and check
  notice coverage before packaging. The main packages for 0.6.0 and 0.6.1 were
  never published; interrupted native 0.6.1 uploads are superseded by 0.6.2.

# 0.5.0

- Compact capability-aware prompts: project instruction precedence, evidence-based
  changes, bounded searches, edit recovery and relevant verification.
- Stream large UTF-8 files in bounded numbered pages with intact continuation
  metadata. Reject ambiguous overlapping edits and invalid encodings; recheck
  cancellation, stale contents and paths before atomic replacement.
- Validate whole tool-call batches before executing any action.
- Preserve saved history independently of the model window. Atomically checkpoint
  user intent, pending calls and tool results; repair interrupted calls on reload
  without automatically executing them. Cap storage at 16 MiB per session.
- Retry transient pre-stream provider failures within one cancellable deadline;
  honor server backoff and never replay a started stream or completed tool action.
  Support SSE BOM, CR, LF and split CRLF framing.
- Borrow and encode Responses input once, share retry bytes, release request buffers
  before streaming, and batch checkpoint disk operations off the event loop.
- Use full LTO and size optimization; add durability, paging and retry regressions
  to all six native CI targets. No new runtime dependencies.

# 0.4.1

- Record measured Piko 0.4.0 binary size, startup and memory in the README, with
  reproducible raw results and an explicit 256-session workload description.
- Validate the new npm trusted-publisher connections through a full GitHub OIDC
  release, with provenance for all seven packages. Runtime behavior is unchanged.

# 0.4.0

- Rename Codex Harness to Piko. Install `@erzenx/piko` from npm and run `piko`.
- Rename all six native packages and standalone downloads to Piko; keep the
  `codex-harness` command alias for existing editor configurations.
- Preserve existing sign-in and sessions by reusing an existing legacy state
  directory. `PIKO_STATE_DIR` takes priority over `CODEX_HARNESS_STATE_DIR`.
- Keep ACP extension metadata compatible with existing model/reasoning pickers.

# 0.3.1

- Discover available text models using your existing ChatGPT login or configured
  OpenAI-compatible provider, with names and model-specific reasoning levels.
- Standard ACP model, reasoning and Code/Plan selectors; legacy selection still
  works. Persist choices per session and pass them through to model requests and
  subagents. Reject unsupported values without changing the session.
- Share one lazy catalog fetch across concurrent sessions and cache it privately
  for five minutes. Discovery is bounded to five seconds, 2 MiB input, 256 KiB normalized metadata and 512 models;
  unavailable providers fall back to the configured model. No new dependencies.
- Avoid session-history writes and duplicate notifications when a client
  reselects an unchanged model, reasoning level or mode.
- Add per-model reasoning metadata for CodeInk's model picker, and protect OAuth
  token rotation when a discovery deadline cancels its waiting request.

# 0.2.1

- Fix CodeInk and other ACP clients selecting the advertised model before a
  prompt. `session/set_model` now accepts repeated selection of the session's
  configured model instead of returning "Method not found".
- Reject unavailable models without changing the session; cover selection,
  consecutive prompts and session reload with an ACP regression test.

# 0.2.0

- One-command npm/pnpm installation with only the matching native Rust binary.
  Six native targets: macOS/Linux/Windows, arm64/x64. No installation scripts.
- Smaller release binary; metadata commands avoid starting the async runtime.
- Stream tool arguments in place, parse SSE by fragments, count JSON without
  allocating a duplicate buffer, and trim persisted history without cloning it.
- Reuse search buffers and bound session reads even if a file grows during a read.
- Fix canonical path protection on macOS and Windows, including private-state
  search exclusion and NTFS alternate-stream credential protection.
- Native Windows state paths, environment essentials, shell quoting and Job Object
  process cleanup. Approved shell access remains unsandboxed.
- Native tests and release smoke checks for every supported platform/architecture;
  exact npm version pins, package allowlists, checksummed standalone archives.

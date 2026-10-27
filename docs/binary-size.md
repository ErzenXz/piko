# Native binary size

These are **unreleased development measurements**, not npm release claims.
The comparison baseline is `e584e13b56a6f8730a3da7e200fba98726f17e36`.
All tools, Unicode search, model protocols, TLS 1.2/1.3, desktop control, PTYs,
subagents and recovery remain available.

## Retained candidate

On macOS arm64 with Rust 1.99.0, the native executable fell from **3,419,632 to
3,305,472 bytes**: **114,160 bytes / 3.34%**. This is a modest reduction;
larger reductions from the tested native TLS backend did not preserve compatibility.

- The native entry point delegates to the library, keeping async implementation
  code in one compilation context. There is no extra allocation per ACP request.
- TOML enables parsing without its unused formatting feature.
- Existing transitive logging crates disable dormant dependency diagnostics in
  release builds. Piko installs no logger/subscriber; ACP events and returned
  errors remain enabled. Rust embedders can build with `--no-default-features`
  to retain dependency logging. No new resolved runtime crates were added.
- AArch64 enables LLVM's machine outliner across eligible functions with three
  extra passes. It shares instruction sequences without removing capabilities.
  Normal Cargo size optimization, fat LTO, stripping and panic-abort remain.

The [raw local comparison](benchmarks/macos-arm64-binary-size-development.json)
contains seven alternating pairs, 32 sessions at eight concurrent mock model
requests, 512 read-only control RPCs per run, startup, native RSS and throughput.
It excludes model inference, fixture and external-tool memory. OS caches are warm;
startup excludes fixture creation. Sampled RSS is not a guaranteed production peak.

Local wallclock results varied substantially across repeated experiments. The
recorded sample has lower prompt/control throughput; no universal speedup is
claimed. Read both raw values and paired changes. The clean-runner workflow
produces before/after measurements separately for all six release targets.

## Rejected experiments

| Experiment | macOS arm64 bytes | Result |
| --- | ---: | --- |
| Baseline | 3,419,632 | Existing full-feature development binary |
| Parse-only TOML and dormant release logging | 3,403,072 | Small saving, retained |
| Native TLS | 2,758,144 | 19.3% smaller, but TLS 1.3 fails |
| `opt-level="s"` | 4,053,648 | Larger |
| Thin LTO | 4,916,544 | Larger |
| New TOML 1.1 parser | 3,419,680 | No useful size saving |
| Nightly optimized standard library | 3,270,944 | Different compiler; not adopted |
| Shared dynamic JSON writers / boxed MCP polling | 3,352,704 | Negligible saving versus dispatcher-only experiment; rejected |
| Aggressive inlining limits | 3,323,248 | Larger than the outliner candidate |

[Experiment metadata](benchmarks/macos-arm64-size-experiments.json) records
hashes and configurations. These trials were not published.

`native-tls` uses Secure Transport on macOS, whose backend lacks TLS 1.3. Its
smaller binary passed seven local TLS checks and failed the TLS-1.3-only handshake.
Schannel TLS 1.3 starts with Windows 11 / Server 2022, so a newer Windows runner
passing does not establish Windows 10 compatibility. Linux retains Rustls and
static musl. [Native TLS protocol support](https://docs.rs/native-tls/0.2.18/native_tls/enum.Protocol.html#variant.Tlsv13),
[Windows protocol support](https://learn.microsoft.com/en-us/windows/win32/secauthn/protocols-in-tls-ssl--schannel-ssp-).

Full Unicode and the regex engines used by ignore/globset were preserved. Some
Cargo features are additive across dependencies; narrowing a direct declaration
alone does not remove engines requested elsewhere. No executable packer, smaller
ASCII-only search, TLS downgrade, disabled certificate checks or hidden helper
binary is used.

## Reproduce

With the before/after executables already available:

```sh
python3 -B tests/size_benchmark.py --before /absolute/before/piko \
  --after /absolute/after/piko --rounds 7 --sessions 32 --output /tmp/piko-size.json
```

For clean source builds and per-target provenance:

```sh
python3 -B scripts/size-evaluation.py compare \
  --baseline e584e13b56a6f8730a3da7e200fba98726f17e36 \
  --platform darwin-arm64 --target aarch64-apple-darwin \
  --output /tmp/piko-size-darwin-arm64.json
```

The helper requires a clean committed checkout, builds scratch sources outside
the repository, and deletes its build output. `.github/workflows/size.yml` runs
this on macOS/Linux/Windows x64 and arm64 and stores `runtime-size-*` reports.
Two isolated native TLS jobs store `runtime-native-tls-*` reports; only the known
macOS TLS 1.3 incompatibility is permitted to fail in those experiments. All eight
TLS tests must pass in the production configuration. Python and installed OpenSSL
are required for TLS development tests; Piko itself does not require either.

Compiler settings were checked against the [Cargo profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html)
and [LLVM outliner documentation](https://llvm.org/doxygen/MachineOutliner_8cpp.html).

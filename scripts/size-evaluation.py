"""Build reproducible size comparisons and isolated, unselected native-TLS experiments.

All source snapshots, Cargo targets and candidate binaries live in RUNNER_TEMP.
This helper never edits the checkout's Cargo files or the operating-system trust
store. Comparative performance figures use synthetic loopback responses only.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib


DEFAULT_BASELINE = "e584e13b56a6f8730a3da7e200fba98726f17e36"
NATIVE_TLS_VERSION = "0.2.18"
TARGETS = {
    "linux-x64": "x86_64-unknown-linux-musl",
    "linux-arm64": "aarch64-unknown-linux-musl",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
    "win32-x64": "x86_64-pc-windows-msvc",
    "win32-arm64": "aarch64-pc-windows-msvc",
}
TLS_13_TEST = "tls_13_accepts_explicit_test_root_and_dns_subject_alt_name"
TLS_TESTS = {
    "tls_fixture_bind_never_attempts_reverse_dns",
    "tls_12_accepts_explicit_test_root_and_ip_subject_alt_name",
    TLS_13_TEST,
    "untrusted_certificate_is_rejected",
    "trusted_certificate_with_wrong_hostname_is_rejected",
    "expired_or_not_yet_valid_leaf_is_rejected_with_a_trusted_ca",
    "https_stream_preserves_tool_call_result_pairing_and_split_utf8",
    "https_request_timeout_is_bounded",
    "cancellation_can_drop_a_stalled_authenticated_tls_stream_promptly",
}


def command(args, cwd, env=None, capture=False, check=True):
    result = subprocess.run(
        [str(arg) for arg in args], cwd=cwd, env=env, text=True,
        encoding="utf-8", errors="replace",
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if check and result.returncode:
        if capture:
            print(result.stdout, file=sys.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}): {args[0]}")
    return result


def validate_ref(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,254}", value):
        raise ValueError("Baseline must be a commit SHA, branch, or tag without revision operators")
    return value


def resolve_commit(repo, reference):
    validate_ref(reference)
    args = ["git", "rev-parse", "--verify", reference + "^{commit}"]
    result = command(args, repo, capture=True, check=False)
    if result.returncode:
        command(["git", "fetch", "--no-tags", "--depth=1", "origin", reference], repo)
        result = command(["git", "rev-parse", "--verify", "FETCH_HEAD^{commit}"], repo, capture=True)
    commit = result.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError("Git did not return a full source commit SHA")
    return commit


def archive_source(repo, commit, destination):
    destination.mkdir()
    process = subprocess.Popen(
        ["git", "archive", "--format=tar", commit], cwd=repo,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            archive.extractall(destination, filter="data")
    finally:
        process.stdout.close()
        errors = process.stderr.read().decode("utf-8", errors="replace")
        process.stderr.close()
        returncode = process.wait()
    if returncode:
        raise RuntimeError("Git source archive failed: " + errors[:4096])


def require_clean_checkout(repo):
    status = command(["git", "status", "--porcelain", "--untracked-files=normal"], repo, capture=True)
    if status.stdout.strip():
        raise RuntimeError("Commit the candidate sources before running reproducible CI evaluation")


def file_hash(path):
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_metadata(source, commit, target, env):
    return {
        "sourceCommit": commit,
        "target": target,
        "rustc": command(["rustc", "-Vv"], source, env, capture=True).stdout.strip(),
        "cargo": command(["cargo", "-V"], source, env, capture=True).stdout.strip(),
        "sourceHashes": {str(path): file_hash(source / path) for path in (
            Path("Cargo.toml"), Path("Cargo.lock"), Path(".cargo/config.toml"),
            Path(".cargo/config"), Path("rust-toolchain.toml"), Path("rust-toolchain"))},
        "profileEnvironment": {key: env[key] for key in (
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_PROFILE_RELEASE_OPT_LEVEL",
            "CARGO_PROFILE_RELEASE_LTO", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS",
            "CARGO_PROFILE_RELEASE_STRIP", "CARGO_PROFILE_RELEASE_PANIC") if key in env},
    }


def build(source, target, target_dir, destination, env):
    command(["cargo", "build", "--release", "--locked", "--target", target], source, env)
    binary = target_dir / target / "release" / ("piko.exe" if target.endswith("msvc") else "piko")
    if not binary.is_file():
        raise RuntimeError("Cargo did not produce the requested native binary")
    shutil.copy2(binary, destination)
    return destination


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def emit_summary(title, values):
    text = "## " + title + "\n\n```json\n" + json.dumps(values, indent=2, allow_nan=False) + "\n```\n"
    print(text, flush=True)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as output:
            output.write(text + "\n")


def comparison(args):
    require_clean_checkout(args.repo)
    before_commit = resolve_commit(args.repo, args.baseline)
    after_commit = resolve_commit(args.repo, "HEAD")
    with tempfile.TemporaryDirectory(prefix="piko-size-", dir=os.environ.get("RUNNER_TEMP")) as temporary:
        scratch = Path(temporary)
        before_source = scratch / "before-source"
        archive_source(args.repo, before_commit, before_source)
        target_dir = scratch / "target"
        env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir), CARGO_INCREMENTAL="0", PYTHON=sys.executable)
        suffix = ".exe" if args.target.endswith("msvc") else ""
        before_build = build_metadata(before_source, before_commit, args.target, env)
        before_binary = build(before_source, args.target, target_dir, scratch / ("piko-before" + suffix), env)
        after_build = build_metadata(args.repo, after_commit, args.target, env)
        after_binary = build(args.repo, args.target, target_dir, scratch / ("piko-after" + suffix), env)
        measured = scratch / "comparison.json"
        command([
            sys.executable, "-B", args.repo / "tests/size_benchmark.py",
            "--before", before_binary, "--after", after_binary,
            "--rounds", args.rounds, "--sessions", args.sessions, "--output", measured,
        ], args.repo, env, capture=True)
        report = json.loads(measured.read_text(encoding="utf-8"))
        report["platform"] = args.platform
        report["builds"] = {"before": before_build, "after": after_build}
        report["temporary_build_outputs_retained"] = False
        write_report(args.output, report)
        emit_summary(args.platform + " before/after native measurements", report["summary"])


def patch_native_tls(manifest):
    original = manifest.read_text(encoding="utf-8")
    parsed = tomllib.loads(original)
    reqwest = parsed.get("dependencies", {}).get("reqwest", {})
    if not isinstance(reqwest, dict) or reqwest.get("default-features") is not False:
        raise ValueError("Native experiment requires an explicit minimal reqwest dependency")
    if reqwest.get("features", []).count("rustls-tls") != 1:
        raise ValueError("Native experiment requires exactly one rustls-tls feature")
    if "native-tls" in parsed["dependencies"]:
        raise ValueError("Native TLS is already a direct dependency")
    pattern = r'(?m)^reqwest\s*=\s*\{[^}]*\}'
    matches = list(re.finditer(pattern, original))
    if len(matches) != 1 or matches[0].group().count('"rustls-tls"') != 1:
        raise ValueError("Cannot isolate reqwest TLS feature safely")
    match = matches[0]
    updated = original[:match.start()] + match.group().replace('"rustls-tls"', '"native-tls"') + original[match.end():]
    updated = updated.replace("[dependencies]\n", '[dependencies]\nnative-tls = "=' + NATIVE_TLS_VERSION + '"\n', 1)
    checked = tomllib.loads(updated)
    if checked["dependencies"].get("native-tls") != "=" + NATIVE_TLS_VERSION:
        raise ValueError("Native TLS exact pin was not applied")
    manifest.write_text(updated, encoding="utf-8")


def tls_results(stdout, returncode, platform_name):
    tests = {}
    for line in stdout.splitlines():
        match = re.fullmatch(r"test ([A-Za-z_][A-Za-z0-9_:]*) \.\.\. (ok|FAILED|ignored)", line.strip())
        if match:
            name, outcome = match.groups()
            if name in tests:
                raise ValueError("Duplicate TLS test result")
            tests[name] = outcome
    if set(tests) != TLS_TESTS:
        raise ValueError("Native TLS experiment must execute exactly the nine expected TLS/fixture tests")
    if "ignored" in tests.values():
        raise ValueError("Native TLS experiment may not ignore TLS tests")
    failed = {name for name, outcome in tests.items() if outcome == "FAILED"}
    allowed = {TLS_13_TEST} if platform_name == "darwin-arm64" else set()
    if not failed.issubset(allowed):
        raise ValueError("Unexpected native TLS failure: " + ", ".join(sorted(failed - allowed)))
    if failed and "trusted TLS 1.3 endpoint; do not silently downgrade" not in stdout:
        raise ValueError("Allowed TLS 1.3 failure must reach the actual handshake")
    if failed and "bad protocol version" not in stdout and "protocol version" not in stdout:
        raise ValueError("Native TLS 1.3 failure must report a protocol-version error")
    if returncode != (101 if failed else 0):
        raise ValueError("Native TLS command failed outside the expected TLS test result")
    return {"passed": len(tests) - len(failed), "failed": len(failed), "tests": tests,
            "allowedCompatibilityFailures": sorted(allowed), "returncode": returncode}


def native_tls(args):
    if args.platform not in ("darwin-arm64", "win32-x64"):
        raise ValueError("Native TLS experiment is restricted to macOS arm64 and Windows x64")
    require_clean_checkout(args.repo)
    source_commit = resolve_commit(args.repo, "HEAD")
    with tempfile.TemporaryDirectory(prefix="piko-native-tls-", dir=os.environ.get("RUNNER_TEMP")) as temporary:
        scratch = Path(temporary)
        source = scratch / "source"
        archive_source(args.repo, source_commit, source)
        original_hashes = {name: file_hash(args.repo / name) for name in ("Cargo.toml", "Cargo.lock")}
        patch_native_tls(source / "Cargo.toml")
        target_dir = scratch / "target"
        env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir), CARGO_INCREMENTAL="0", PYTHON=sys.executable)
        # Resolving updates only the isolated lockfile and preserves already locked
        # compatible dependencies. All subsequent builds/tests are locked.
        command(["cargo", "metadata", "--format-version=1", "--filter-platform", args.target], source, env, capture=True)
        lock = tomllib.loads((source / "Cargo.lock").read_text(encoding="utf-8"))
        versions = [package["version"] for package in lock["package"] if package["name"] == "native-tls"]
        if versions != [NATIVE_TLS_VERSION]:
            raise RuntimeError("Native TLS experiment did not resolve the exact verified version")
        metadata = build_metadata(source, source_commit, args.target, env)
        suffix = ".exe" if args.target.endswith("msvc") else ""
        binary = build(source, args.target, target_dir, scratch / ("piko-native-tls" + suffix), env)
        tested = command(["cargo", "test", "--color", "never", "--locked", "--test", "tls", "--target", args.target,
                          "--", "--test-threads=1", "--color", "never"], source, env, capture=True, check=False)
        report = {
            "scope": "Isolated native-TLS size/protocol experiment, never a selected production configuration. Tests use local TLS fixtures and explicit client-only CA roots; no OS trust store is modified.",
            "platform": args.platform,
            "operatingSystem": platform.platform(),
            "build": metadata,
            "nativeTlsVersion": NATIVE_TLS_VERSION,
            "binary": {"bytes": binary.stat().st_size, "sha256": file_hash(binary)},
            "selected": False,
            "selectionReasons": [
                "macOS native-tls Secure Transport lacks TLS 1.3; preserving TLS-1.3-only endpoints is required.",
                "Windows 10 Schannel lacks TLS 1.3; passing newer Windows CI does not establish Windows 10 parity.",
            ],
            "sources": [
                "https://docs.rs/native-tls/0.2.18/native_tls/enum.Protocol.html#variant.Tlsv13",
                "https://learn.microsoft.com/en-us/windows/win32/secauthn/protocols-in-tls-ssl--schannel-ssp-",
            ],
            "tlsTestOutput": tested.stdout,
            "checkoutCargoHashes": original_hashes,
            "temporary_build_outputs_retained": False,
        }
        try:
            report["tlsResults"] = tls_results(tested.stdout, tested.returncode, args.platform)
        except ValueError as error:
            report["validationError"] = str(error)
            write_report(args.output, report)
            print(tested.stdout, file=sys.stderr)
            raise
        if original_hashes != {name: file_hash(args.repo / name) for name in original_hashes}:
            raise RuntimeError("Checkout Cargo files changed during the isolated native experiment")
        write_report(args.output, report)
        emit_summary(args.platform + " native TLS experiment (not selected)",
                     {"binary": report["binary"], "tlsResults": report["tlsResults"],
                      "selected": False, "selectionReasons": report["selectionReasons"]})


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    for mode in ("compare", "native-tls"):
        subparser = modes.add_parser(mode)
        subparser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
        subparser.add_argument("--target", required=True)
        subparser.add_argument("--platform", choices=TARGETS, required=True)
        subparser.add_argument("--output", type=Path, required=True)
        if mode == "compare":
            subparser.add_argument("--baseline", default=DEFAULT_BASELINE)
            subparser.add_argument("--rounds", type=int, default=7)
            subparser.add_argument("--sessions", type=int, default=32)
    args = parser.parse_args(argv)
    args.repo = args.repo.resolve()
    args.output = args.output.resolve()
    if args.target != TARGETS[args.platform]:
        parser.error("target does not match the requested native platform")
    if not (args.repo / "Cargo.toml").is_file() or not (args.repo / ".git").exists():
        parser.error("repo must be a Piko Git checkout")
    if args.output == args.repo or args.repo in args.output.parents:
        parser.error("write reports outside the source checkout")
    if args.mode == "compare":
        try:
            validate_ref(args.baseline)
        except ValueError as error:
            parser.error(str(error))
        if not 1 <= args.rounds <= 25 or not 1 <= args.sessions <= 1024:
            parser.error("rounds must be 1..25 and sessions must be 1..1024")
    elif args.platform not in ("darwin-arm64", "win32-x64"):
        parser.error("native-tls mode supports only darwin-arm64 and win32-x64")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    try:
        comparison(arguments) if arguments.mode == "compare" else native_tls(arguments)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)

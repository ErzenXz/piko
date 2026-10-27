"""Paired native-binary size, warm startup, RSS and loopback throughput comparison.

Example:
  python -B tests/size_benchmark.py --before /tmp/piko-before --after /tmp/piko-after \
      --rounds 5 --sessions 32 --output /tmp/piko-size-comparison.json

No models, credentials, tools or downloads are used. Each run starts a fresh
native process and temporary workspace; executable/OS caches are not flushed.
The HTTP fixture does not measure TLS handshakes or real model performance.
"""
import argparse
import asyncio
import json
import math
import os
from pathlib import Path
import platform
import statistics
import time
from unittest.mock import patch

import stdio
from stdio import Harness, final
from memory_benchmark import binary_metadata, memory, rss_summary, samples


CONCURRENCY = 8
MODEL_DELAY_SECONDS = 0.002
SAMPLE_INTERVAL_SECONDS = 0.01
IDLE_SAMPLES = 3
CONTROL_RPC_COUNT = 512
CONTROL_RPC_BATCH_SIZE = 16


def percent_change(before, after):
    """Signed after-minus-before change; a zero denominator stays unknown."""
    if not math.isfinite(before) or not math.isfinite(after):
        raise ValueError("Benchmark values must be finite")
    if before == 0:
        return None
    return (after - before) / before * 100


def execution_order(round_index):
    return ("before", "after") if round_index % 2 == 0 else ("after", "before")


def paired_summary(runs):
    """Keep per-round paired deltas separate from the medians of raw values."""
    pairs = {}
    for run in runs:
        label = run["variant"]
        if label not in ("before", "after"):
            raise ValueError("Unknown benchmark variant")
        pair = pairs.setdefault(run["round"], {})
        if label in pair:
            raise ValueError("Duplicate variant in a benchmark round")
        pair[label] = run
    if not pairs or any(set(pair) != {"before", "after"} for pair in pairs.values()):
        raise ValueError("Every benchmark round requires before and after runs")

    metrics = {
        "binary_bytes": (lambda run: run["binary"]["bytes"], "bytes", True),
        "startup_and_initialize_ms": (lambda run: run["startup_and_initialize_ms"], "ms", True),
        "idle_rss_kib": (lambda run: run["rss_kib"]["idle"]["median_kib"], "KiB", True),
        "sampled_workload_peak_rss_kib": (lambda run: run["rss_kib"]["workload"]["max_kib"], "KiB", True),
        "workload_ms": (lambda run: run["workload_ms"], "ms", True),
        "requests_per_second": (lambda run: run["requests_per_second"], "requests/s", False),
        "prompt_p50_ms": (lambda run: run["prompt_latency_ms"]["p50"], "ms", True),
    }
    # Older saved reports predate this separate control-path phase. They remain
    # usable as historical data, but never compare mixed measurement protocols.
    control_fields = {"control_rpc_ms", "control_rpc_per_second"}
    controls = [control_fields.issubset(run) for run in runs]
    if any(controls) and not all(controls):
        raise ValueError("Cannot pair runs with and without the control RPC workload")
    if any(any(field in run for field in control_fields) and not present
           for run, present in zip(runs, controls)):
        raise ValueError("Incomplete control RPC measurements")
    if all(controls):
        configurations = {(run.get("control_rpc_count"), run.get("control_rpc_batch_size")) for run in runs}
        if len(configurations) != 1 or any(not isinstance(value, int) or value <= 0
                                            for value in next(iter(configurations))):
            raise ValueError("Control RPC comparisons require matching positive request/batch counts")
        metrics.update({
            "control_rpc_ms": (lambda run: run["control_rpc_ms"], "ms", True),
            "control_rpc_per_second": (lambda run: run["control_rpc_per_second"], "requests/s", False),
        })
    summary = {}
    for name, (read, unit, lower_is_better) in metrics.items():
        values = []
        for index, pair in sorted(pairs.items()):
            before, after = read(pair["before"]), read(pair["after"])
            values.append({"round": index, "before": before, "after": after,
                           "change_percent": percent_change(before, after)})
        changes = [value["change_percent"] for value in values if value["change_percent"] is not None]
        summary[name] = {
            "unit": unit,
            "lower_is_better": lower_is_better,
            "before_median": statistics.median(value["before"] for value in values),
            "after_median": statistics.median(value["after"] for value in values),
            "median_paired_change_percent": statistics.median(changes) if changes else None,
            "pairs": values,
        }
    return summary


class SizeHarness(Harness):
    def __init__(self, binary, sessions):
        super().__init__(self.model, allow=False, extra=(
            f"max_sessions = {sessions}\nmax_model_requests = {CONCURRENCY}\n"
            f"max_active_turns = {CONCURRENCY}\ncontext_bytes = 65536\nrequest_timeout_secs = 60\n"
        ))
        self.binary = binary
        self.expected_labels = {f"size-fixture-session-{index}" for index in range(sessions)}
        self.received_labels = set()
        self.fixture_errors = []
        self.spawn_started = None

    async def __aenter__(self):
        # Harness owns disposable config/server setup. Time only native process
        # creation through initialize, excluding that setup. Runs are sequential,
        # and the subprocess wrapper is restored before any RSS sampling begins.
        create_process = asyncio.create_subprocess_exec

        async def timed_spawn(program, *args, **kwargs):
            if Path(program).resolve() == self.binary:
                self.spawn_started = time.perf_counter()
            return await create_process(program, *args, **kwargs)

        with patch.object(asyncio, "create_subprocess_exec", timed_spawn):
            return await super().__aenter__()

    async def model(self, body):
        try:
            users = [item.get("content") for item in body["input"] if item.get("role") == "user"]
            assert len(users) == 1 and isinstance(users[0], str), "Unexpected/cross-session history"
            label = users[0].strip()
            assert label in self.expected_labels, "Unknown session prompt"
            assert label not in self.received_labels, "Unexpected provider retry"
            self.received_labels.add(label)
            await asyncio.sleep(MODEL_DELAY_SECONDS)
            return final("fixture-result:" + label)
        except Exception as error:
            self.fixture_errors.append(repr(error))
            return 'data: {"type":"error","error":{"message":"fixture validation failed"}}\n\n'


async def control_workload(harness, sessions, count=CONTROL_RPC_COUNT, batch_size=CONTROL_RPC_BATCH_SIZE):
    """Measure read-only stdio/JSON dispatch, without disk writes or model work."""
    provider_requests = len(harness.requests)
    catalog_requests = len(harness.model_list_requests)
    started = time.perf_counter()
    for first in range(0, count, batch_size):
        responses = await asyncio.gather(*(
            harness.rpc("_harness/stats", {}) for _ in range(min(batch_size, count - first))
        ))
        for response in responses:
            runtime = response.get("result", {}).get("runtime", {})
            assert runtime.get("loadedSessions") == sessions, "Control RPC changed/lost loaded sessions"
            assert runtime.get("activeRootTurns") == 0, "Control RPC started a root turn"
            assert runtime.get("liveSubagents") == 0, "Control RPC started a subagent"
            assert runtime.get("toolProcesses") == 0, "Control RPC started a host process"
    milliseconds = (time.perf_counter() - started) * 1000
    assert len(harness.requests) == provider_requests, "Control RPC performed model work"
    assert len(harness.model_list_requests) == catalog_requests, "Control RPC requested a model catalog"
    assert not harness.permissions, "Control RPC requested host tools"
    return {"control_rpc_count": count, "control_rpc_batch_size": batch_size,
            "control_rpc_ms": milliseconds, "control_rpc_per_second": count / (milliseconds / 1000)}


async def benchmark(binary, metadata, sessions):
    stdio.BINARY = binary
    async with SizeHarness(binary, sessions) as harness:
        await harness.initialize()
        assert harness.spawn_started is not None, "Native launch timer did not start"
        startup_ms = (time.perf_counter() - harness.spawn_started) * 1000
        pid = harness.process.pid
        idle = await samples(pid, IDLE_SAMPLES, SAMPLE_INTERVAL_SECONDS)
        creation_started = time.perf_counter()
        session_ids = await asyncio.gather(*(harness.session() for _ in range(sessions)))
        creation_ms = (time.perf_counter() - creation_started) * 1000
        assert len(set(session_ids)) == sessions, "Session identifiers are not isolated"
        # Deliberately separate this CPU/control-path signal from filesystem and
        # synthetic HTTP/model scheduling. No RSS subprocess samples run here.
        control = await control_workload(harness, sessions)

        observed = []
        stopped = asyncio.Event()

        async def monitor():
            while not stopped.is_set():
                observed.append(await memory(pid))
                await asyncio.sleep(SAMPLE_INTERVAL_SECONDS)

        latencies = []

        async def prompt(index, session):
            started = time.perf_counter()
            result = await harness.prompt(session, f"size-fixture-session-{index}")
            latencies.append((time.perf_counter() - started) * 1000)
            assert result.get("result", {}).get("stopReason") == "end_turn", result

        sampling = asyncio.create_task(monitor())
        running = []
        workload_started = time.perf_counter()
        try:
            running = [asyncio.create_task(prompt(index, session)) for index, session in enumerate(session_ids)]
            await asyncio.wait_for(asyncio.gather(*running), 60)
            workload_ms = (time.perf_counter() - workload_started) * 1000
        finally:
            stopped.set()
            try:
                await sampling
            finally:
                for task in running:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*running, return_exceptions=True)

        after = await samples(pid, IDLE_SAMPLES, SAMPLE_INTERVAL_SECONDS)
        assert observed, "No native-process workload RSS samples collected"
        assert not harness.fixture_errors, harness.fixture_errors
        assert harness.received_labels == harness.expected_labels, "Missing session requests"
        assert len(harness.requests) == sessions, "Unexpected request count"
        assert 1 <= harness.peak_active <= CONCURRENCY, "Model concurrency limit exceeded"
        assert not harness.permissions, "Fixture unexpectedly requested host tools"

        chunks = {session: [] for session in session_ids}
        for event in harness.updates:
            if event.get("method") != "session/update":
                continue
            params = event["params"]
            update = params.get("update", {})
            if update.get("sessionUpdate") == "agent_message_chunk":
                chunks[params["sessionId"]].append(update["content"].get("text", ""))
        for index, session in enumerate(session_ids):
            assert "".join(chunks[session]) == f"fixture-result:size-fixture-session-{index}", "Wrong session output"

        ordered = sorted(latencies)
        return {
            "binary": dict(metadata),
            **control,
            "sessions": sessions,
            "configured_model_concurrency": CONCURRENCY,
            "configured_active_root_turns": CONCURRENCY,
            "peak_simultaneous_mock_requests": harness.peak_active,
            "completed_prompts": len(latencies),
            "provider_requests": len(harness.requests),
            "model_catalog_requests": len(harness.model_list_requests),
            "fixture_connections": harness.connections,
            "simulated_model_delay_ms": MODEL_DELAY_SECONDS * 1000,
            "startup_and_initialize_ms": startup_ms,
            "session_creation_ms": creation_ms,
            "workload_ms": workload_ms,
            "requests_per_second": sessions / (workload_ms / 1000),
            "prompt_latency_ms": {"p50": statistics.median(ordered),
                                  "p95": ordered[max(0, math.ceil(len(ordered) * .95) - 1)]},
            "rss_kib": {"idle": rss_summary(idle), "workload": rss_summary(observed),
                        "after_completed": rss_summary(after)},
            "checks": {"all_prompts_completed": True, "request_and_output_isolation": True,
                       "no_automatic_provider_retries": True, "model_concurrency_bounded": True,
                       "no_host_tool_permissions": True, "control_rpc_preserves_sessions": True,
                       "control_rpc_has_no_model_work_or_host_tools": True},
        }


async def main(args):
    binaries = {"before": args.before, "after": args.after}
    # Metadata also checks both executables before measurements begin. Reading
    # and launching each executable makes this a repeated warm-cache comparison.
    metadata = {label: binary_metadata(binary) for label, binary in binaries.items()}
    runs = []
    for index in range(args.rounds):
        for label in execution_order(index):
            run = await benchmark(binaries[label], metadata[label], args.sessions)
            run.update(variant=label, round=index + 1, sequence=len(runs) + 1)
            runs.append(run)
    return {
        "scope": "Repeated warm-cache native-binary comparison using ACP read-only control RPCs and loopback HTTP/SSE only. Control RPC timing includes Python/stdio scheduling but no persistence, models or RSS-sampling subprocesses. Native PID RSS excludes the Python fixture, real models and external tools; sampled RSS is not a guaranteed production peak. No TLS handshake or real inference throughput is measured.",
        "environment": {"platform": platform.platform(), "python": platform.python_version(),
                        "logical_cpus": os.cpu_count()},
        "measurement": {"rounds": args.rounds, "sessions_per_run": args.sessions,
                        "idle_samples": IDLE_SAMPLES, "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
                        "startup_excludes_fixture_setup": True, "workload_excludes_session_creation": True,
                        "workload_includes_fixture_delay_and_rss_sampling": True,
                        "control_rpc_count": CONTROL_RPC_COUNT, "control_rpc_batch_size": CONTROL_RPC_BATCH_SIZE,
                        "control_rpc_after_session_creation_before_model_prompts": True,
                        "control_rpc_excludes_persistence_models_and_rss_sampling": True,
                        "percentage_convention": "(after - before) / before * 100; null when before is zero"},
        "summary": paired_summary(runs),
        "runs": runs,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--sessions", type=int, default=32)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.rounds <= 25:
        parser.error("rounds must be 1..25")
    if not 1 <= args.sessions <= 1024:
        parser.error("sessions must be 1..1024")
    for name in ("before", "after"):
        path = getattr(args, name).resolve()
        if not path.is_file():
            parser.error(f"{name} must be an existing native executable file")
        setattr(args, name, path)
    if args.output:
        args.output = args.output.resolve()
        if args.output in (args.before, args.after):
            parser.error("output must not overwrite an input binary")
        if not args.output.parent.is_dir() or args.output.is_dir():
            parser.error("output requires an existing parent directory and a file path")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    result = asyncio.run(main(arguments))
    text = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if arguments.output:
        arguments.output.write_text(text, encoding="utf-8")
    print(text, end="")

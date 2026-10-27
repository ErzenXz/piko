"""Bounded-concurrency ACP stress fixture; no real models, credentials or tools.

Example (native binary already built):
  python3 tests/scalability.py --binary /absolute/path/piko --sessions 1000 \
      --model-concurrency 16 --output /tmp/piko-scalability.json

Reports logical sessions, actually simultaneous mock model requests, native PID
RSS, cancellation and durable reload separately. This is not a 1,000-model or
production memory/throughput claim. Python fixture and external-tool RAM are
excluded. Temporary state is deleted when the fixture exits.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import platform
import re
import statistics
import time

import stdio
from stdio import Harness, final
from memory_benchmark import binary_metadata, memory, rss_summary, samples


class ScaleHarness(Harness):
    """Consume requests without retaining prompt/schema copies in the fixture."""

    def __init__(self, args):
        super().__init__(None, extra=(
            f"max_sessions = {args.sessions}\n"
            f"max_model_requests = {args.model_concurrency}\n"
            f"context_bytes = {args.context_kib * 1024}\n"
            f"request_timeout_secs = {int(args.timeout)}\n"
            + (f"max_active_turns = {args.active_turns}\n" if args.active_turns else "")
            + (f"resident_history_bytes = {args.resident_history_bytes}\n"
               if args.resident_history_bytes is not None else "")
        ))
        self.args = args
        self.first_wave = asyncio.Event()
        self.release = asyncio.Event()
        self.received = set()
        self.body_bytes = 0
        self.fixture_errors = []
        self.models = {}
        self.history_checks = 0

    def check_history(self, body):
        users = [item["content"].split("\n", 1)[0] for item in body["input"]
                 if item.get("role") == "user"]
        parsed = [re.fullmatch(r"fixture-session-(\d+)-turn-(\d+)", label)
                  for label in users]
        assert all(parsed), users
        session, turn = map(int, parsed[-1].groups())
        assert all(int(match[1]) == session for match in parsed), "Cross-session history leak"
        turns = [int(match[2]) for match in parsed]
        assert turns == sorted(set(turns)), (session, turns)
        omitted = any(item.get("role") == "developer"
                      and "older turns are outside the model window" in item.get("content", "")
                      for item in body["input"])
        if not omitted:
            # A cancelled first turn may have been stopped before admission or
            # persisted after model queue cancellation. Subsequent turns must
            # survive either path and a later cold reload.
            first = 1 if session >= self.args.sessions - self.args.cancel_queued else 0
            expected = set(range(first, turn + 1))
            assert expected.issubset(turns), ("Missing persisted history", session, turn, turns)
        model = body.get("model")
        assert isinstance(model, str) and model, "Missing provider model"
        previous = self.models.setdefault(session, model)
        assert model == previous, ("Model changed across hydration", previous, model)
        self.history_checks += 1
        return users[-1]

    async def rpc(self, method, params):
        ident = f"request-{getattr(self, 'sequence', 0)}"
        self.sequence = getattr(self, "sequence", 0) + 1
        future = asyncio.get_running_loop().create_future()
        self.pending[ident] = future
        try:
            await self.send({"jsonrpc": "2.0", "id": ident, "method": method, "params": params})
            return await asyncio.wait_for(future, self.args.timeout)
        finally:
            self.pending.pop(ident, None)

    async def http(self, reader, writer):
        self.connections += 1
        self.writers.add(writer)
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            while request_line := await reader.readline():
                method, _, _ = request_line.decode().strip().split(" ", 2)
                headers = {}
                while (line := await reader.readline()) != b"\r\n":
                    if not line:
                        return
                    key, value = line.decode().split(":", 1)
                    headers[key.lower()] = value.strip()
                raw = await reader.readexactly(int(headers.get("content-length", "0")))
                if method == "GET":
                    payload = b'{"error":"fixture model catalog unavailable"}'
                    writer.write((
                        "HTTP/1.1 404 Fixture\r\nContent-Type: application/json\r\n"
                        f"Content-Length: {len(payload)}\r\n\r\n"
                    ).encode() + payload)
                    await writer.drain()
                    continue
                assert method == "POST", method
                self.body_bytes += len(raw)
                body = json.loads(raw)
                label = self.check_history(body)
                assert label.startswith("fixture-session-"), label
                assert label not in self.received, f"Unexpected provider retry: {label}"
                self.received.add(label)
                del raw, body
                self.active += 1
                self.peak_active = max(self.active, self.peak_active)
                assert self.active <= self.args.model_concurrency, (
                    "Model request concurrency exceeded", self.active, self.args.model_concurrency)
                payload = final("fixture complete").encode()
                try:
                    writer.write((
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                        f"Content-Length: {len(payload)}\r\n\r\n"
                    ).encode())
                    await writer.drain()
                    if self.active == self.args.model_concurrency:
                        self.first_wave.set()
                    await self.release.wait()
                    await asyncio.sleep(self.args.model_delay)
                    writer.write(payload)
                    await writer.drain()
                finally:
                    self.active -= 1
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.fixture_errors.append(repr(error))
            self.first_wave.set()
        finally:
            self.writers.discard(writer)
            writer.close()
            self.tasks.discard(task)


async def bounded_map(items, limit, operation):
    gate = asyncio.Semaphore(limit)

    async def one(item):
        async with gate:
            return await operation(item)

    return await asyncio.gather(*(one(item) for item in items))


async def benchmark(binary, args):
    stdio.BINARY = binary
    started = time.perf_counter()
    async with ScaleHarness(args) as harness:
        await harness.initialize()
        startup_ms = (time.perf_counter() - started) * 1000
        pid = harness.process.pid
        phases = {"idle": rss_summary(await samples(pid, args.samples, args.interval))}
        started = time.perf_counter()
        sessions = await bounded_map(range(args.sessions), args.control_concurrency,
                                     lambda _: harness.session())
        creation_seconds = time.perf_counter() - started
        phases["loaded_sessions"] = rss_summary(await samples(pid, args.samples, args.interval))
        latencies = []

        async def prompt(index, session, turn=0):
            began = time.perf_counter()
            result = await harness.prompt(session, f"fixture-session-{index}-turn-{turn}\n" + "x" * (args.prompt_kib * 1024))
            latencies.append((time.perf_counter() - began) * 1000)
            return result

        running = [asyncio.create_task(prompt(index, session)) for index, session in enumerate(sessions)]
        observed = []
        stop_monitor = asyncio.Event()

        async def monitor():
            while not stop_monitor.is_set():
                observed.append(await memory(pid))
                await asyncio.sleep(args.interval)

        monitoring = asyncio.create_task(monitor())
        started = time.perf_counter()
        cancelled_indexes = list(range(args.sessions - args.cancel_queued, args.sessions))
        try:
            await asyncio.wait_for(harness.first_wave.wait(), args.timeout)
            assert not harness.fixture_errors, harness.fixture_errors
            # Successful headers have arrived; all response bodies remain held.
            await asyncio.sleep(0.05)
            phases["first_wave_held_with_queue"] = rss_summary(
                await samples(pid, args.samples, args.interval))
            assert len(harness.received) == args.model_concurrency, harness.received
            cancel_started = time.perf_counter()
            for index in cancelled_indexes:
                assert f"fixture-session-{index}-turn-0" not in harness.received, "Cancellation target already running"
                result = await harness.rpc("session/cancel", {"sessionId": sessions[index]})
                assert "error" not in result, result
            cancelled = await asyncio.wait_for(
                asyncio.gather(*(running[index] for index in cancelled_indexes)), args.timeout)
            assert all(result.get("result", {}).get("stopReason") == "cancelled" for result in cancelled), cancelled
            assert len(harness.received) == args.model_concurrency, "Cancelled queue admitted an HTTP request"
            cancellation_ms = (time.perf_counter() - cancel_started) * 1000
            harness.release.set()
            released = time.perf_counter()
            results = await asyncio.wait_for(asyncio.gather(*running), args.timeout)
            release_to_complete_seconds = time.perf_counter() - released
            turn_seconds = time.perf_counter() - started
            for index, result in enumerate(results):
                expected = "cancelled" if index in cancelled_indexes else "end_turn"
                assert result.get("result", {}).get("stopReason") == expected, (index, result)
            expected_count = args.sessions - args.cancel_queued
            assert len(harness.received) == expected_count, (len(harness.received), expected_count)
            assert not harness.fixture_errors, harness.fixture_errors
            assert not harness.permissions, "Fixture unexpectedly requested a host tool permission"
            phases["completed_first_turn"] = rss_summary(await samples(pid, args.samples, args.interval))
            history_started = time.perf_counter()
            for turn in range(1, args.turns):
                results = await asyncio.wait_for(asyncio.gather(*(
                    prompt(index, session, turn) for index, session in enumerate(sessions)
                )), args.timeout)
                assert all(result.get("result", {}).get("stopReason") == "end_turn" for result in results), results
                phases[f"completed_turn_{turn + 1}"] = rss_summary(
                    await samples(pid, args.samples, args.interval))
            history_seconds = time.perf_counter() - history_started
            expected_count = args.sessions * args.turns - args.cancel_queued
            assert len(harness.received) == expected_count, (len(harness.received), expected_count)
            phases["completed_sessions"] = rss_summary(await samples(pid, args.samples, args.interval))
        finally:
            harness.release.set()
            stop_monitor.set()
            await monitoring
            for task in running:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*running, return_exceptions=True)

        async def unload(session):
            result = await harness.rpc("_harness/session/unload", {"sessionId": session})
            assert "error" not in result, result

        started = time.perf_counter()
        await bounded_map(sessions, args.control_concurrency, unload)
        unload_seconds = time.perf_counter() - started
        phases["all_sessions_unloaded"] = rss_summary(await samples(pid, args.samples, args.interval))
        missing = await harness.rpc("_harness/tasks", {"sessionId": sessions[0]})
        assert "error" in missing, "Unloaded session remains addressable"
        saved_count = len(list((harness.root / "state" / "sessions").glob("*.json")))
        assert saved_count == args.sessions, (saved_count, args.sessions)
        replay_start = len(harness.updates)
        loaded = await harness.rpc("session/load", {
            "sessionId": sessions[0], "cwd": str(harness.workspace), "mcpServers": []})
        assert loaded.get("result", {}).get("sessionId") == sessions[0], loaded
        loaded_models = loaded["result"].get("models", {})
        assert loaded_models.get("currentModelId") == harness.models[0], loaded
        assert loaded["result"].get("modes", {}).get("currentModeId") == "code", loaded
        options = {option["id"]: option for option in loaded["result"].get("configOptions", [])}
        assert options.get("model", {}).get("currentValue") == harness.models[0], loaded
        assert any(update.get("params", {}).get("update", {}).get("sessionUpdate") == "user_message_chunk"
                   for update in harness.updates[replay_start:]), "Saved history was not replayed"
        continued = await harness.prompt(sessions[0], f"fixture-session-0-turn-{args.turns}\n" + "x" * (args.prompt_kib * 1024))
        assert continued.get("result", {}).get("stopReason") == "end_turn", continued
        assert len(harness.received) == args.sessions * args.turns - args.cancel_queued + 1
        assert not harness.fixture_errors, harness.fixture_errors
        assert not harness.permissions, "Reload continuation unexpectedly requested a host tool permission"
        await unload(sessions[0])
        phases["observed_during_turns"] = rss_summary(observed)
        ordered = sorted(latencies)
        return {
            "binary": binary_metadata(binary),
            "logical_sessions": args.sessions,
            "turns_per_session": args.turns,
            "configured_active_turns": args.active_turns or "binary default",
            "configured_resident_history_bytes": (
                args.resident_history_bytes if args.resident_history_bytes is not None else "binary default"),
            "configured_model_concurrency": args.model_concurrency,
            "peak_simultaneous_mock_requests": harness.peak_active,
            "completed_prompts": args.sessions * args.turns - args.cancel_queued,
            "cancelled_queued_prompts": args.cancel_queued,
            "provider_requests": len(harness.received),
            "reload_verification_prompts": 1,
            "aggregate_request_body_bytes": harness.body_bytes,
            "prompt_bytes_each_excluding_label": args.prompt_kib * 1024,
            "context_budget_bytes_each": args.context_kib * 1024,
            "simulated_model_delay_ms": args.model_delay * 1000,
            "startup_and_initialize_ms": round(startup_ms, 3),
            "create_sessions_seconds": round(creation_seconds, 3),
            "turn_phase_seconds_including_measurement_hold": round(turn_seconds, 3),
            "release_to_all_completed_seconds": round(release_to_complete_seconds, 3),
            "additional_history_turns_seconds_including_samples": round(history_seconds, 3),
            "queued_cancellation_ms": round(cancellation_ms, 3),
            "unload_sessions_seconds": round(unload_seconds, 3),
            "latency_ms_including_measurement_hold": {
                "p50": round(statistics.median(ordered), 3),
                "p95": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 3),
            },
            "rss_kib": phases,
            "checks": {
                "model_concurrency_bounded": True,
                "queued_cancellation_avoids_http": True,
                "all_sessions_unloaded": True,
                "saved_session_reload_and_replay": True,
                "cold_reload_continues_history": True,
                "model_and_mode_survive_reload": True,
                "provider_history_continuity_checks": harness.history_checks,
                "saved_session_files": saved_count,
                "no_host_tool_permissions": True,
            },
        }


async def main(args):
    runs = []
    for binary in args.binary:
        runs.append(await benchmark(binary.resolve(strict=True), args))
    return {
        "scope": "Synthetic ACP/loopback SSE, one native process. Logical sessions are not simultaneous model inference. Native PID RSS excludes Python, models and external tools; samples are not a guaranteed production peak.",
        "environment": {"platform": platform.platform(), "logical_cpus": os.cpu_count()},
        "results": runs,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--sessions", type=int, default=1000)
    parser.add_argument("--model-concurrency", type=int, default=16)
    parser.add_argument("--active-turns", type=int, default=0,
                        help="Set the new active-turn admission limit; omit for older-binary comparisons")
    parser.add_argument("--resident-history-bytes", type=int,
                        help="Set idle history retention; omit for older-binary comparisons")
    parser.add_argument("--turns", type=int, default=1,
                        help="Repeat prompts to measure retained conversation history")
    parser.add_argument("--cancel-queued", type=int, default=16)
    parser.add_argument("--control-concurrency", type=int, default=32)
    parser.add_argument("--prompt-kib", type=int, default=1)
    parser.add_argument("--context-kib", type=int, default=64)
    parser.add_argument("--model-delay", type=float, default=0.02)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 2 <= args.sessions <= 1024:
        parser.error("sessions must be 2..1024")
    if not 1 <= args.model_concurrency < args.sessions:
        parser.error("model-concurrency must be 1..sessions-1")
    if not 0 <= args.active_turns <= 1024:
        parser.error("active-turns must be 0 (binary default) or 1..1024")
    if args.active_turns and args.active_turns < args.model_concurrency:
        parser.error("active-turns must be >= model-concurrency so the held wave can fill")
    if args.resident_history_bytes is not None and not 0 <= args.resident_history_bytes <= 16 * 1024 * 1024:
        parser.error("resident-history-bytes must be 0..16777216")
    if not 1 <= args.turns <= 20:
        parser.error("turns must be 1..20")
    if not 1 <= args.cancel_queued <= args.sessions - args.model_concurrency:
        parser.error("cancel-queued must be 1..sessions-model-concurrency")
    if not 1 <= args.control_concurrency <= 128:
        parser.error("control-concurrency must be 1..128")
    if not 32 <= args.context_kib <= 4096 or not 1 <= args.prompt_kib < args.context_kib / 2:
        parser.error("context must be 32..4096 KiB and prompt must fit below half of it")
    if not 0 <= args.model_delay <= 1 or not 1 <= args.samples <= 100 or not 0.01 <= args.interval <= 1:
        parser.error("delay must be 0..1 seconds, samples 1..100 and interval 0.01..1 seconds")
    if not 1 <= args.timeout <= 3600:
        parser.error("timeout must be 1..3600 seconds")
    result = asyncio.run(main(args))
    text = json.dumps(result, indent=2) + "\n"
    print(text, end="")
    if args.output:
        args.output.write_text(text)

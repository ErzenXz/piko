"""Synthetic long-history native memory fixture; no real models or credentials.

  python3 -B tests/long_history_benchmark.py --binary /absolute/old/piko \
      --binary /absolute/new/piko --sessions 32 --history-mib 8 \
      --output /tmp/piko-long-history.json

This targets full-history parsing, replay, borrowed context and persistence. Both
binaries must support max_active_turns and resident_history_bytes. Native PID RSS
excludes the Python fixture; sampled maxima are not production guarantees. Large
history is streamed into temporary fixture-owned files only after unloading its
sessions. No browser, desktop action, package install or paid API is involved.
"""
import argparse
import asyncio
import json
from pathlib import Path
import platform
import time

import stdio
from stdio import Harness, final
from memory_benchmark import binary_metadata, memory, rss_summary, samples


def historical_text(session, turn, size):
    marker = f"fixture-history-s{session}-t{turn}\n"
    return marker + "h" * (size - len(marker))


def current_text(session, size):
    marker = f"fixture-current-s{session}\n"
    return marker + "n" * (size - len(marker))


def write_history(path, index, turns, turn_bytes):
    """Keep at most one turn in the Python encoder, not sessions*history MiB."""
    metadata = json.loads(path.read_text())
    assert metadata.pop("turns") == [], "Only unloaded empty fixture sessions may be rewritten"
    with path.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata, separators=(",", ":"))[:-1] + ',"turns":[')
        for turn in range(turns):
            if turn:
                output.write(",")
            json.dump({"items": [
                {"role": "user", "content": historical_text(index, turn, turn_bytes)},
                {"role": "assistant", "content": f"fixture-answer-s{index}-t{turn}"},
            ]}, output, separators=(",", ":"))
        output.write("]}")
    return path.stat().st_size


class LongHistoryHarness(Harness):
    def __init__(self, args):
        super().__init__(None, extra=(
            f"max_sessions = {args.sessions}\nmax_active_turns = {args.sessions}\n"
            f"max_model_requests = {args.sessions}\ncontext_bytes = {args.context_kib * 1024}\n"
            f"request_timeout_secs = {args.timeout}\nresident_history_bytes = 65536\n"
        ))
        self.args = args
        self.turns = args.history_mib * 1024 // args.turn_kib
        self.replayed = {}
        self.notification_count = 0
        self.provider_sessions = set()
        self.body_bytes = 0
        self.all_headers = asyncio.Event()
        self.release = asyncio.Event()
        self.fixture_errors = []

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

    async def read(self):
        # Replay streams hundreds of MiB in the default fixture. Count chunks;
        # never retain their payloads as Harness.updates normally does.
        try:
            while line := await self.process.stdout.readline():
                value = json.loads(line)
                method = value.get("method")
                if method == "session/request_permission":
                    self.permissions.append(value["id"])
                    await self.send({"jsonrpc": "2.0", "id": value["id"],
                                     "result": {"outcome": {"outcome": "cancelled"}}})
                elif method:
                    self.notification_count += 1
                    params = value.get("params", {})
                    if params.get("update", {}).get("sessionUpdate") == "user_message_chunk":
                        ident = params["sessionId"]
                        self.replayed[ident] = self.replayed.get(ident, 0) + 1
                elif value.get("id") in self.pending:
                    future = self.pending.pop(value["id"])
                    if not future.done():
                        future.set_result(value)
                else:
                    raise AssertionError(f"Unexpected ACP response: {value}")
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("ACP stream ended"))

    def validate_input(self, body):
        users = [item["content"] for item in body["input"] if item.get("role") == "user"]
        marker = users[-1].split("\n", 1)[0]
        index = int(marker.removeprefix("fixture-current-s"))
        assert 0 <= index < self.args.sessions, marker
        assert index not in self.provider_sessions, f"Unexpected provider retry for {index}"
        assert users[-1] == current_text(index, self.args.prompt_kib * 1024) + "\n"
        assert len(users) >= 2, "The model window lost all recent history"
        history_turns = []
        for text in users[:-1]:
            label = text.split("\n", 1)[0]
            expected = f"fixture-history-s{index}-t"
            assert label.startswith(expected), "Cross-session history leak"
            turn = int(label.removeprefix(expected))
            assert text == historical_text(index, turn, self.args.turn_kib * 1024)
            history_turns.append(turn)
        assert history_turns == sorted(set(history_turns)), history_turns
        assert history_turns == list(range(history_turns[0], self.turns)), history_turns
        assert history_turns[-1] == self.turns - 1, history_turns
        if len(history_turns) < self.turns:
            assert any(item.get("role") == "developer"
                       and "older turns are outside the model window" in item.get("content", "")
                       for item in body["input"]), "Large history was not explicitly projected"
        self.provider_sessions.add(index)
        return index

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
                    payload = b'{"error":"no fixture model catalog"}'
                    writer.write(("HTTP/1.1 404 Fixture\r\nContent-Type: application/json\r\n"
                                  f"Content-Length: {len(payload)}\r\n\r\n").encode() + payload)
                    await writer.drain()
                    continue
                assert method == "POST", method
                self.body_bytes += len(raw)
                body = json.loads(raw)
                index = self.validate_input(body)
                del raw, body
                self.active += 1
                self.peak_active = max(self.active, self.peak_active)
                payload = final(f"fixture-recorded-s{index}").encode()
                try:
                    writer.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                                  f"Content-Length: {len(payload)}\r\n\r\n").encode())
                    await writer.drain()
                    if self.active == self.args.sessions:
                        self.all_headers.set()
                    await self.release.wait()
                    writer.write(payload)
                    await writer.drain()
                finally:
                    self.active -= 1
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.fixture_errors.append(repr(error))
            self.all_headers.set()
        finally:
            self.writers.discard(writer)
            writer.close()
            self.tasks.discard(task)


async def benchmark(binary, args):
    stdio.BINARY = binary
    async with LongHistoryHarness(args) as harness:
        await harness.initialize()
        sessions = [await harness.session() for _ in range(args.sessions)]

        async def unload(session):
            result = await harness.rpc("_harness/session/unload", {"sessionId": session})
            assert "error" not in result, result

        async def load(session):
            result = await harness.rpc("session/load", {
                "sessionId": session, "cwd": str(harness.workspace), "mcpServers": []})
            assert result.get("result", {}).get("sessionId") == session, result

        await asyncio.gather(*(unload(session) for session in sessions))
        fixture_started = time.perf_counter()
        paths = [harness.root / "state" / "sessions" / f"{session}.json" for session in sessions]
        written = sum(write_history(path, index, harness.turns, args.turn_kib * 1024)
                      for index, path in enumerate(paths))
        fixture_seconds = time.perf_counter() - fixture_started
        pid = harness.process.pid
        phases = {"before_history_load": rss_summary(await samples(pid, args.samples, args.interval))}
        observed = []
        phase_samples = {}
        stopped = asyncio.Event()
        phase = "initial_load"

        async def monitor():
            while not stopped.is_set():
                label = phase
                value = await memory(pid)
                observed.append(value)
                phase_samples.setdefault(label, []).append(value)
                await asyncio.sleep(args.interval)

        monitoring = asyncio.create_task(monitor())
        prompts = []
        try:
            began = time.perf_counter()
            await asyncio.gather(*(load(session) for session in sessions))
            load_seconds = time.perf_counter() - began
            assert all(harness.replayed[session] == harness.turns for session in sessions), harness.replayed
            phases["loaded_cold_histories"] = rss_summary(await samples(pid, args.samples, args.interval))
            phase = "prompt_preparation_and_headers"
            began = time.perf_counter()
            prompts = [asyncio.create_task(harness.prompt(session, current_text(index, args.prompt_kib * 1024)))
                       for index, session in enumerate(sessions)]
            await asyncio.wait_for(harness.all_headers.wait(), args.timeout)
            assert not harness.fixture_errors, harness.fixture_errors
            assert len(harness.provider_sessions) == args.sessions, harness.provider_sessions
            assert all(not prompt.done() for prompt in prompts), "Fixture stream completed before release"
            prepare_seconds = time.perf_counter() - began
            phases["all_model_response_headers_held"] = rss_summary(await samples(pid, args.samples, args.interval))
            phase = "reply_and_full_history_commit"
            began = time.perf_counter()
            harness.release.set()
            results = await asyncio.wait_for(asyncio.gather(*prompts), args.timeout)
            commit_seconds = time.perf_counter() - began
            assert all(result.get("result", {}).get("stopReason") == "end_turn" for result in results), results
            phases["completed_cold_histories"] = rss_summary(await samples(pid, args.samples, args.interval))
            await asyncio.gather(*(unload(session) for session in sessions))
            phase = "durable_reload_and_replay"
            began = time.perf_counter()
            before = harness.replayed.copy()
            await asyncio.gather(*(load(session) for session in sessions))
            reload_seconds = time.perf_counter() - began
            assert all(harness.replayed[session] - before[session] == harness.turns + 1
                       for session in sessions), harness.replayed
            phases["durable_reload_complete"] = rss_summary(await samples(pid, args.samples, args.interval))
            await asyncio.gather(*(unload(session) for session in sessions))
            assert not harness.permissions, "Fixture unexpectedly requested a host permission"
            assert not harness.fixture_errors, harness.fixture_errors
        finally:
            harness.release.set()
            for prompt in prompts:
                if not prompt.done():
                    prompt.cancel()
            await asyncio.gather(*prompts, return_exceptions=True)
            stopped.set()
            await monitoring

        for index, path in enumerate(paths):
            saved = json.loads(path.read_text())
            assert len(saved["turns"]) == harness.turns + 1
            for turn, record in enumerate(saved["turns"][:-1]):
                assert record["items"][0] == {"role": "user", "content": historical_text(index, turn, args.turn_kib * 1024)}
                assert record["items"][1] == {"role": "assistant", "content": f"fixture-answer-s{index}-t{turn}"}
            current = saved["turns"][-1]["items"]
            assert current[0] == {"role": "user", "content": current_text(index, args.prompt_kib * 1024) + "\n"}
            assert len(current) == 2 and current[1]["role"] == "assistant", current
            assert current[1]["content"] == [{"type": "output_text", "text": f"fixture-recorded-s{index}"}]
            assert not any(item.get("role") == "developer" for record in saved["turns"] for item in record["items"]), \
                "Model-only projection notices were persisted"
        missing = await harness.rpc("_harness/tasks", {"sessionId": sessions[0]})
        assert "error" in missing, "Unloaded session remains addressable"
        phases["all_sessions_unloaded"] = rss_summary(await samples(pid, args.samples, args.interval))
        phases["observed_during_workload"] = rss_summary(observed)
        return {
            "binary": binary_metadata(binary), "sessions": args.sessions,
            "historical_turns_per_session": harness.turns,
            "history_content_bytes_per_session": harness.turns * args.turn_kib * 1024,
            "fixture_history_files_bytes": written,
            "history_item_bytes": args.turn_kib * 1024,
            "new_prompt_bytes_excluding_stdio_newline": args.prompt_kib * 1024,
            "model_context_bytes": args.context_kib * 1024,
            "configured_active_turns_and_model_requests": args.sessions,
            "peak_simultaneous_mock_requests": harness.peak_active,
            "provider_requests": len(harness.provider_sessions),
            "provider_request_body_bytes": harness.body_bytes,
            "discarded_acp_notifications": harness.notification_count,
            "timings_seconds": {
                "fixture_history_writes_excluded_from_workload": round(fixture_seconds, 3),
                "load_and_history_replay": round(load_seconds, 3),
                "prepare_prompts_to_all_headers": round(prepare_seconds, 3),
                "release_to_full_history_commit": round(commit_seconds, 3),
                "durable_reload_and_replay": round(reload_seconds, 3),
            },
            "rss_kib": phases,
            "phase_rss_kib": {key: rss_summary(values) for key, values in phase_samples.items()},
            "checks": {"all_headers_received_before_release": True,
                       "projected_history_is_ordered_and_session_isolated": True,
                       "full_original_history_and_new_turn_preserved": True,
                       "projection_notices_not_persisted": True,
                       "durable_reload_replays_all_turns": True,
                       "all_sessions_unloaded": True, "no_host_permissions": True},
        }


async def main(args):
    results = []
    for binary in args.binary:
        results.append(await benchmark(binary.resolve(strict=True), args))
    return {"scope": "Synthetic long-history load/replay, held loopback SSE, and full durable save. Native PID RSS excludes Python, inference and external tools; sampled maxima are not guaranteed production peaks.",
            "environment": {"platform": platform.platform()}, "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--sessions", type=int, default=32)
    parser.add_argument("--history-mib", type=int, default=8)
    parser.add_argument("--turn-kib", type=int, default=128)
    parser.add_argument("--prompt-kib", type=int, default=128)
    parser.add_argument("--context-kib", type=int, default=384)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.sessions <= 64 or not 1 <= args.history_mib <= 14:
        parser.error("sessions must be 1..64 and history-mib 1..14 (each session must remain below16MiB)")
    if not 16 <= args.turn_kib <= 256 or not 1 <= args.prompt_kib <= 1024:
        parser.error("turn-kib must be 16..256 and prompt-kib 1..1024")
    if not 64 <= args.context_kib <= 4096 or args.prompt_kib >= args.context_kib / 2:
        parser.error("context-kib must be 64..4096 and prompt must fit below half of it")
    if args.context_kib < args.turn_kib + args.prompt_kib + 64:
        parser.error("context must leave at least64KiB beyond one old turn and the new prompt")
    if not 1 <= args.samples <= 100 or not .01 <= args.interval <= 1 or not 1 <= args.timeout <= 3600:
        parser.error("samples must be 1..100, interval .01..1 seconds and timeout1..3600 seconds")
    result = asyncio.run(main(args))
    encoded = json.dumps(result, indent=2) + "\n"
    print(encoded, end="")
    if args.output:
        args.output.write_text(encoded)

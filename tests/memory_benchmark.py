"""Synthetic native-process RSS comparison while loopback SSE streams await output.

Uses no models, credentials or downloads. HTTP request bodies are fully consumed
and 200 SSE headers sent before the held-stream samples begin. Results describe
this fixture, never real-model throughput, intelligence or peak production memory.

Example:
  python3 tests/memory_benchmark.py --binary /absolute/old/piko \
      --binary /absolute/new/piko --agents 1 32 64 --prompt-kib 128
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import stdio
from stdio import Harness, final


class HeldStreams(Harness):
    def __init__(self, agents, context_bytes, active_turns=0):
        super().__init__(None, extra=(
            f"max_sessions = {agents}\nmax_model_requests = {agents}\n"
            f"context_bytes = {context_bytes}\nrequest_timeout_secs = 60\n"
            + (f"max_active_turns = {active_turns}\n" if active_turns else "")
        ))
        self.expected = agents
        self.received = 0
        self.body_bytes = 0
        self.all_headers_sent = asyncio.Event()
        self.release = asyncio.Event()
        self.fixture_errors = []

    async def http(self, reader, writer):
        self.connections += 1
        self.writers.add(writer)
        self.tasks.add(asyncio.current_task())
        try:
            while request_line := await reader.readline():
                method, _, _ = request_line.decode().strip().split(" ", 2)
                headers = {}
                while (line := await reader.readline()) != b"\r\n":
                    if not line:
                        return
                    key, value = line.decode().split(":", 1)
                    headers[key.lower()] = value.strip()
                remaining = int(headers.get("content-length", "0"))
                if method == "POST":
                    assert remaining > 0, "Expected an encoded JSON request body"
                    self.body_bytes += remaining
                # Consume bodies incrementally. The fixture deliberately keeps no
                # prompt-sized request copies; only the native PID is measured.
                while remaining:
                    chunk = await reader.readexactly(min(remaining, 65536))
                    remaining -= len(chunk)
                if method == "GET":
                    payload = b'{"error":"no fixture model catalog"}'
                    writer.write((
                        "HTTP/1.1 404 Fixture\r\nContent-Type: application/json\r\n"
                        f"Content-Length: {len(payload)}\r\n\r\n"
                    ).encode() + payload)
                    await writer.drain()
                    continue
                assert method == "POST", method
                self.received += 1
                assert self.received <= self.expected, "Unexpected provider retry"
                self.active += 1
                self.peak_active = max(self.active, self.peak_active)
                payload = final("fixture complete").encode()
                writer.write((
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    f"Content-Length: {len(payload)}\r\n\r\n"
                ).encode())
                await writer.drain()
                if self.received == self.expected:
                    self.all_headers_sent.set()
                await self.release.wait()
                writer.write(payload)
                await writer.drain()
                self.active -= 1
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.fixture_errors.append(repr(error))
            self.all_headers_sent.set()
        finally:
            self.writers.discard(writer)
            writer.close()
            self.tasks.discard(asyncio.current_task())


async def memory(pid):
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        values = {}
        for line in status.read_text().splitlines():
            key, _, value = line.partition(":")
            if key in ("VmRSS", "VmHWM", "Threads"):
                values[key] = int(value.split()[0])
        return values
    if platform.system() == "Darwin":
        process = await asyncio.create_subprocess_exec(
            "ps", "-o", "rss=", "-p", str(pid),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise RuntimeError(stderr.decode())
        return {"VmRSS": int(stdout.strip())}
    if os.name == "nt":
        return windows_memory(pid)
    raise RuntimeError("RSS collection supports macOS, Windows and Linux")


def windows_memory(pid):
    """Native working set counters, no psutil/helper install (Windows 7+ API)."""
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
            (field, ctypes.c_size_t) for field in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                "PagefileUsage", "PeakPagefileUsage")]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessMemoryCounters), wintypes.DWORD]
    kernel.K32GetProcessMemoryInfo.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not kernel.K32GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            raise ctypes.WinError(ctypes.get_last_error())
        return {"VmRSS": counters.WorkingSetSize // 1024, "VmHWM": counters.PeakWorkingSetSize // 1024}
    finally:
        kernel.CloseHandle(handle)


async def samples(pid, count, interval):
    values = []
    for _ in range(count):
        values.append(await memory(pid))
        await asyncio.sleep(interval)
    return values


def rss_summary(values):
    rss = [value["VmRSS"] for value in values]
    return {"median_kib": statistics.median(rss), "max_kib": max(rss), "samples": values}


def binary_metadata(binary):
    digest = hashlib.sha256()
    with binary.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True, check=True)
    return {"path": str(binary), "version": version.stdout.strip(),
            "bytes": binary.stat().st_size, "sha256": digest.hexdigest()}


async def benchmark(binary, agents, prompt_bytes, context_bytes, count, interval, active_turns=0):
    # Harness consults this module constant when starting each subprocess. Runs
    # are sequential, giving both binaries exactly the same server and workload.
    stdio.BINARY = binary
    async with HeldStreams(agents, context_bytes, active_turns) as harness:
        await harness.initialize()
        pid = harness.process.pid
        idle = await samples(pid, count, interval)
        sessions = await asyncio.gather(*(harness.session() for _ in range(agents)))
        loaded = await samples(pid, count, interval)
        prompt = "x" * prompt_bytes
        running = [asyncio.create_task(harness.prompt(session, prompt)) for session in sessions]
        observed = []
        stopped = asyncio.Event()

        async def monitor():
            while not stopped.is_set():
                observed.append(await memory(pid))
                await asyncio.sleep(interval)

        sampling = asyncio.create_task(monitor())
        started = time.monotonic()
        try:
            await asyncio.wait_for(harness.all_headers_sent.wait(), 15)
            assert not harness.fixture_errors, harness.fixture_errors
            assert harness.received == agents, (harness.received, agents)
            # Let the native client consume every already-sent response header.
            await asyncio.sleep(0.15)
            held = await samples(pid, count, interval)
            assert all(not task.done() for task in running), "Streams completed before fixture release"
            held_seconds = time.monotonic() - started
            harness.release.set()
            results = await asyncio.wait_for(asyncio.gather(*running), 15)
            assert all(result.get("result", {}).get("stopReason") == "end_turn" for result in results), results
            after = await samples(pid, count, interval)
        finally:
            harness.release.set()
            stopped.set()
            await sampling
            for task in running:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
        assert not harness.fixture_errors, harness.fixture_errors
        return {
            "agents": agents, "prompt_bytes_each": prompt_bytes,
            "context_budget_bytes_each": context_bytes,
            "received_model_requests": harness.received,
            "peak_simultaneous_fixture_streams": harness.peak_active,
            "aggregate_request_body_bytes": harness.body_bytes,
            "held_phase_seconds": round(held_seconds, 3),
            "rss_kib": {"idle": rss_summary(idle), "loaded_sessions": rss_summary(loaded),
                        "all_streams_held": rss_summary(held), "after_completed": rss_summary(after),
                        "observed_during_workload": rss_summary(observed)},
            "held_minus_loaded_median_kib": statistics.median(value["VmRSS"] for value in held)
                - statistics.median(value["VmRSS"] for value in loaded),
        }


async def main(args):
    results = []
    for binary in args.binary:
        binary = binary.resolve(strict=True)
        runs = []
        for agents in args.agents:
            runs.append(await benchmark(binary, agents, args.prompt_kib * 1024,
                                        args.context_kib * 1024, args.samples, args.interval, args.active_turns))
        results.append({"binary": binary_metadata(binary), "runs": runs})
    return {
        "scope": "Synthetic loopback SSE allocation fixture. Native PID RSS only; excludes model inference and Python fixture memory. Not a real-model or intelligence benchmark.",
        "environment": {"platform": platform.platform(), "logical_cpus": os.cpu_count()},
        "measurement": {"unit": "KiB", "samples_per_phase": args.samples,
                        "sample_interval_seconds": args.interval,
                        "fixture": "consume all POST bodies, send all HTTP 200 SSE headers, hold every response body, sample, release identical final output"},
        "results": results,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--agents", type=int, nargs="+", default=[1, 16, 32])
    parser.add_argument("--active-turns", type=int, default=0,
                        help="New binaries: set at least the largest agents count for this all-streams-held fixture")
    parser.add_argument("--prompt-kib", type=int, default=128)
    parser.add_argument("--context-kib", type=int, default=384)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if any(not 1 <= value <= 1024 for value in args.agents):
        parser.error("agents must be 1..1024")
    if args.active_turns and not max(args.agents) <= args.active_turns <= 1024:
        parser.error("active-turns must be at least the largest agents count and <=1024")
    if not 16 <= args.context_kib <= 4096 or not 1 <= args.prompt_kib < args.context_kib / 2:
        parser.error("context must be 16..4096 KiB and prompt must fit below half of it")
    if not 1 <= args.samples <= 100 or not 0.01 <= args.interval <= 1:
        parser.error("samples must be 1..100 and interval 0.01..1 seconds")
    result = asyncio.run(main(args))
    text = json.dumps(result, indent=2) + "\n"
    print(text, end="")
    if args.output:
        args.output.write_text(text)

"""Exercise actual Piko subagents using a deterministic loopback model fixture.

  python3 -B tests/subagent_scalability.py --binary /absolute/native/piko \
      --agents 1000 --model-concurrency 16 --output /tmp/piko-subagents.json

One root session spawns unique read-only children in batches of up to 32. This
tests logical subagents sharing one process, not 1,000 simultaneous model
inferences. No real models, credentials, host tools or downloads are used.
Native PID RSS excludes Python and external tools; sampled RSS is not a
production peak guarantee. Temporary state is removed on exit.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import platform
import time

import stdio
from stdio import Harness, call, final, response
from memory_benchmark import binary_metadata, memory, rss_summary, samples


ROOT_PROMPT = "ROOT-SUBAGENT-STRESS: delegate exactly the requested fixture tasks"
RETAINED_BYTES = 64 * 1024
CHILD_PADDING_BYTES = 2048


def output_text(item):
    output = item["output"]
    assert isinstance(output, str), output
    return json.loads(output)


class SubagentHarness(Harness):
    def __init__(self, args):
        super().__init__(self.model, extra=(
            "max_sessions = 1\nmax_active_turns = 1\nmax_jobs = 1024\n"
            "max_subagents = 1024\nmax_subagent_depth = 1\n"
            f"max_model_requests = {args.model_concurrency}\n"
            "context_bytes = 4194304\nmax_turn_steps = 200\n"
            "tool_output_bytes = 1024\nresident_history_bytes = 0\n"
            f"retained_agent_context_bytes = {RETAINED_BYTES}\n"
            f"request_timeout_secs = {args.timeout}\n"
        ))
        self.args = args
        self.spawned = 0
        self.ids = {}
        self.initial_children = set()
        self.followed_children = set()
        self.root_requests = 0
        self.child_requests = 0
        self.child_active = self.child_peak = 0
        self.phase = "spawn"
        self.phases = {}
        self.fixture_errors = []
        self.oldest = self.recent = None

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

    async def model(self, body):
        # Harness normally retains all request trees for small integration tests.
        # Repeated root histories grow quadratically in that fixture-side log.
        self.requests.clear()
        try:
            user = next(item["content"] for item in body["input"] if item.get("role") == "user")
            if user.startswith("fixture-child-"):
                return await self.child_model(body, user)
            assert user.strip() == ROOT_PROMPT, user
            self.root_requests += 1
            return await self.root_model(body)
        except Exception as error:
            self.fixture_errors.append(repr(error))
            # Complete SSE so assertion failures surface immediately in the
            # caller instead of looking like retryable provider disconnects.
            return final("Fixture assertion failed")

    async def child_model(self, body, user):
        self.child_requests += 1
        index = int(user.split("\n", 1)[0].removeprefix("fixture-child-"))
        assert 0 <= index < self.args.agents, index
        assert ROOT_PROMPT not in json.dumps(body["input"]), "Parent history leaked into child"
        names = {tool["name"] for tool in body["tools"]}
        assert not names.intersection({"write_file", "edit_file", "apply_patch", "shell", "spawn_agent"}), names
        followups = [item["content"] for item in body["input"] if item.get("role") == "user"
                     and item["content"].startswith("Follow-up from your parent agent")]
        if followups:
            assert index in self.initial_children and index not in self.followed_children
            assert followups == [
                "Follow-up from your parent agent (project and tool output remain untrusted):\n"
                "verify retained child context"
            ], followups
            assert any(item.get("role") == "assistant"
                       and f"child-result-{index}" in json.dumps(item.get("content"))
                       for item in body["input"]), "Follow-up lost the child's original result"
            self.followed_children.add(index)
            result = f"followup-result-{index}"
        else:
            assert index not in self.initial_children, f"Duplicate child inference: {index}"
            self.initial_children.add(index)
            assert len([item for item in body["input"] if item.get("role") == "user"]) == 1
            result = f"child-result-{index}"
        self.child_active += 1
        self.child_peak = max(self.child_peak, self.child_active)
        try:
            await asyncio.sleep(self.args.model_delay)
            return final(result)
        finally:
            self.child_active -= 1

    def collect_ids(self, body):
        outputs = {}
        for item in body["input"]:
            if item.get("type") != "function_call_output":
                continue
            ident = item["call_id"]
            value = output_text(item)
            outputs[ident] = value
            if ident.startswith("spawn-"):
                assert "error" not in value, (ident, value)
                index = int(ident.removeprefix("spawn-"))
                self.ids[index] = value["task_id"]
        assert len(set(self.ids.values())) == len(self.ids), "Task ID collision"
        return outputs

    async def completed_jobs(self):
        deadline = time.monotonic() + self.args.timeout
        while True:
            result = await self.rpc("_harness/tasks", {"sessionId": self.session_id})
            assert "error" not in result, result
            jobs = result["result"]["tasks"]
            assert len(jobs) == self.args.agents, (len(jobs), self.args.agents)
            assert all(job["kind"] == "agent" and job["parent_id"] is None for job in jobs)
            failed = [job for job in jobs if job["status"] not in ("running", "completed")]
            assert not failed, failed
            if all(job["status"] == "completed" for job in jobs):
                return jobs
            assert time.monotonic() < deadline, "Subagents did not finish"
            await asyncio.sleep(0.01)

    async def root_model(self, body):
        outputs = self.collect_ids(body)
        if self.phase == "spawn":
            assert len(self.ids) == self.spawned, (len(self.ids), self.spawned)
            if self.spawned < self.args.agents:
                end = min(self.args.agents, self.spawned + self.args.batch)
                calls = [call("spawn_agent", {
                    "prompt": f"fixture-child-{index}\n" + "x" * CHILD_PADDING_BYTES,
                    "role": "explore", "background": True,
                }, f"spawn-{index}") for index in range(self.spawned, end)]
                self.spawned = end
                return response(calls)
            jobs = await self.completed_jobs()
            by_id = {job["task_id"]: job for job in jobs}
            for index, ident in self.ids.items():
                assert by_id[ident]["output"] == f"child-result-{index}", by_id[ident]
            assert len(self.initial_children) == self.args.agents
            # Completion notifications follow retention. Wait for their delivery
            # before selecting the newest context; polling snapshots alone can
            # observe a finished job before its final ACP notification arrives.
            deadline = time.monotonic() + self.args.timeout
            while True:
                completed = [update["params"]["taskId"] for update in self.updates
                             if update.get("method") == "_harness/task"
                             and update["params"].get("event", {}).get("status") == "completed"]
                if len(completed) == self.args.agents:
                    break
                assert time.monotonic() < deadline, "Missing child completion notifications"
                await asyncio.sleep(0.01)
            self.oldest, self.recent = completed[0], completed[-1]
            assert self.oldest != self.recent
            self.phases["all_children_completed_before_followup"] = rss_summary(
                await samples(self.process.pid, self.args.samples, self.args.interval))
            self.phase = "followup"
            return response([
                call("followup_agent", {"task_id": self.recent, "message": "verify retained child context"}, "follow-recent"),
                call("followup_agent", {"task_id": self.oldest, "message": "verify expired child context"}, "follow-expired"),
            ])
        if self.phase == "followup":
            assert outputs["follow-recent"]["task_id"] == self.recent, outputs["follow-recent"]
            assert outputs["follow-recent"]["status"] == "running", outputs["follow-recent"]
            assert "context expired" in outputs["follow-expired"].get("error", "").lower(), outputs["follow-expired"]
            self.phase = "verify"
            return response([
                call("task_status", {"task_id": self.recent, "wait_ms": 30000}, "status-recent"),
                call("task_status", {"task_id": self.oldest, "wait_ms": 0}, "status-expired"),
            ])
        assert self.phase == "verify", self.phase
        recent_index = next(index for index, ident in self.ids.items() if ident == self.recent)
        oldest_index = next(index for index, ident in self.ids.items() if ident == self.oldest)
        assert outputs["status-recent"]["status"] == "completed", outputs["status-recent"]
        assert outputs["status-recent"]["output"] == f"followup-result-{recent_index}", outputs["status-recent"]
        assert outputs["status-expired"]["status"] == "completed", outputs["status-expired"]
        assert outputs["status-expired"]["output"] == f"child-result-{oldest_index}", outputs["status-expired"]
        assert self.followed_children == {recent_index}, self.followed_children
        self.phase = "done"
        return final(f"verified {self.args.agents} actual subagents")


async def benchmark(binary, args):
    stdio.BINARY = binary
    async with SubagentHarness(args) as harness:
        await harness.initialize()
        harness.session_id = await harness.session()
        harness.phases["idle_root_session"] = rss_summary(
            await samples(harness.process.pid, args.samples, args.interval))
        observed = []
        stopped = asyncio.Event()

        async def monitor():
            while not stopped.is_set():
                observed.append(await memory(harness.process.pid))
                await asyncio.sleep(args.interval)

        monitoring = asyncio.create_task(monitor())
        started = time.perf_counter()
        try:
            result = await harness.prompt(harness.session_id, ROOT_PROMPT)
            seconds = time.perf_counter() - started
            assert result.get("result", {}).get("stopReason") == "end_turn", result
            assert not harness.fixture_errors, harness.fixture_errors
            assert harness.phase == "done", harness.phase
            assert harness.peak_active <= args.model_concurrency, (harness.peak_active, args.model_concurrency)
            assert harness.child_peak > 1, "Fixture did not exercise parallel children"
            assert not harness.permissions, "Fixture requested a host tool permission"
            jobs = await harness.completed_jobs()
            assert len(harness.ids) == len(jobs) == args.agents
            assert harness.child_requests == args.agents + 1, harness.child_requests
            harness.phases["root_and_children_completed"] = rss_summary(
                await samples(harness.process.pid, args.samples, args.interval))
        finally:
            stopped.set()
            await monitoring
        unloaded = await harness.rpc("_harness/session/unload", {"sessionId": harness.session_id})
        assert "error" not in unloaded, unloaded
        missing = await harness.rpc("_harness/tasks", {"sessionId": harness.session_id})
        assert "error" in missing, "Unloaded session still exposes task state"
        harness.phases["root_session_unloaded"] = rss_summary(
            await samples(harness.process.pid, args.samples, args.interval))
        harness.phases["observed_during_workload"] = rss_summary(observed)
        return {
            "binary": binary_metadata(binary),
            "root_sessions": 1,
            "actual_subagents": args.agents,
            "spawn_batch_size": args.batch,
            "configured_model_concurrency": args.model_concurrency,
            "configured_max_subagents": 1024,
            "configured_max_jobs": 1024,
            "configured_retained_agent_context_bytes": RETAINED_BYTES,
            "child_prompt_padding_bytes": CHILD_PADDING_BYTES,
            "root_model_requests": harness.root_requests,
            "child_model_requests_including_followup": harness.child_requests,
            "peak_simultaneous_mock_model_requests": harness.peak_active,
            "peak_simultaneous_child_handlers": harness.child_peak,
            "simulated_child_model_delay_ms": args.model_delay * 1000,
            "workload_seconds_including_measurement_hold": round(seconds, 3),
            "rss_kib": harness.phases,
            "checks": {
                "all_children_completed_with_matching_results": True,
                "explore_children_read_only_and_context_isolated": True,
                "recent_child_followup_keeps_history": True,
                "expired_child_followup_returns_explicit_error": True,
                "expired_context_keeps_task_result": True,
                "model_concurrency_bounded": True,
                "session_unload_releases_task_access": True,
                "no_host_tool_permissions": True,
            },
        }


async def main(args):
    runs = []
    for binary in args.binary:
        runs.append(await benchmark(binary.resolve(strict=True), args))
    return {
        "scope": "Synthetic ACP/loopback SSE with actual Piko child agents. One process, bounded simultaneous mock model requests; excludes real model inference, fixture and external-tool RAM. Sampled native RSS is not a guaranteed production peak.",
        "environment": {"platform": platform.platform(), "logical_cpus": os.cpu_count()},
        "results": runs,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--agents", type=int, default=1000)
    parser.add_argument("--batch", type=int, default=32)
    parser.add_argument("--model-concurrency", type=int, default=16)
    parser.add_argument("--model-delay", type=float, default=0.002)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 64 <= args.agents <= 1024:
        parser.error("agents must be 64..1024 (64 ensures retained-context eviction)")
    if not 1 <= args.batch <= 32 or (args.agents + args.batch - 1) // args.batch + 4 > 200:
        parser.error("batch must be 1..32 and finish within 200 root model steps")
    if not 3 <= args.model_concurrency <= 1024:
        parser.error("model-concurrency must be 3..1024 to exercise parallel children beside the root")
    if not 0 <= args.model_delay <= 1 or not 1 <= args.samples <= 100 or not 0.01 <= args.interval <= 1:
        parser.error("delay must be 0..1 seconds, samples 1..100 and interval 0.01..1 seconds")
    if not 1 <= args.timeout <= 3600:
        parser.error("timeout must be 1..3600 seconds")
    result = asyncio.run(main(args))
    text = json.dumps(result, indent=2) + "\n"
    print(text, end="")
    if args.output:
        args.output.write_text(text)

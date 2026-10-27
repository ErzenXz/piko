"""Durability and retry regressions using only local ACP/HTTP fixtures."""
import asyncio
import json
import os
import time
import unittest

from stdio import Harness, call, final, response, MODEL_CATALOG


STREAM_ERROR = 'data: {"type":"response.failed","response":{"error":{"message":"fixture failure"}}}\n\n'


def items(saved):
    return [item for turn in saved["turns"] for item in turn["items"]]


class ScriptedHarness(Harness):
    """Return scripted HTTP statuses or disconnects before emitting any stream."""
    def __init__(self, actions, **kwargs):
        super().__init__(None, **kwargs)
        self.actions = actions
        self.raw_requests = []

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
                raw = await reader.readexactly(int(headers.get("content-length", "0")))
                if method == "GET":
                    payload = b'{"error":"no fixture catalog"}'
                    writer.write(f"HTTP/1.1 404 Fixture\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n".encode() + payload)
                    await writer.drain()
                    continue
                assert method == "POST", method
                self.raw_requests.append(raw)
                self.requests.append(json.loads(raw))
                self.request_headers.append(headers)
                self.seen.set()
                index = len(self.raw_requests) - 1
                assert index < len(self.actions), "Unexpected retry after scripted responses"
                action = self.actions[index]
                if action == "disconnect":
                    return
                status, extra, body = action
                payload = body.encode()
                header = f"HTTP/1.1 {status} Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {len(payload)}\r\n"
                header += "".join(f"{key}: {value}\r\n" for key, value in extra.items())
                writer.write((header + "\r\n").encode() + payload)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self.writers.discard(writer)
            writer.close()
            self.tasks.discard(asyncio.current_task())


class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_global_process_budget_is_shared_between_acp_sessions_and_reusable(self):
        async def model(body):
            last = body["input"][-1]
            if last.get("type") == "function_call_output":
                return final()
            hold = last.get("content", "").strip() == "hold process"
            command = ("ping -n 30 127.0.0.1 >nul" if os.name == "nt" else "sleep 30") if hold else "echo reusable"
            return response([call("shell", {"command": command, "background": hold, "timeout_secs": 60}, "process")])

        async with Harness(model, extra="max_processes = 1\n") as h:
            await h.initialize()
            first, second = await h.session(), await h.session()
            self.assertEqual((await h.prompt(first, "hold process"))["result"]["stopReason"], "end_turn")
            stats = (await h.rpc("_harness/stats", {}))["result"]["runtime"]
            self.assertEqual(stats["toolProcesses"], 1)
            self.assertEqual(stats["toolProcessLimit"], 1)
            self.assertEqual((await h.prompt(second, "blocked"))["result"]["stopReason"], "end_turn")
            output = json.loads(h.requests[-1]["input"][-1]["output"])
            self.assertIn("process", output["error"].lower())
            self.assertEqual((await h.rpc("_harness/tasks", {"sessionId": second}))["result"]["tasks"], [])
            task = (await h.rpc("_harness/tasks", {"sessionId": first}))["result"]["tasks"][0]
            await h.rpc("_harness/task/cancel", {"sessionId": first, "taskId": task["task_id"]})
            for _ in range(250):
                if (await h.rpc("_harness/stats", {}))["result"]["runtime"]["toolProcesses"] == 0:
                    break
                await asyncio.sleep(.02)
            else:
                self.fail("Cancelled process did not release its global slot")
            self.assertEqual((await h.prompt(second, "retry"))["result"]["stopReason"], "end_turn")
            output = json.loads(h.requests[-1]["input"][-1]["output"])
            self.assertEqual(output["status"], "completed")
            self.assertIn("reusable", output["output"])

    async def test_cold_history_survives_picker_changes_failed_preparation_and_reload(self):
        async def model(body):
            return final("recorded")

        async with Harness(model, extra="resident_history_bytes = 0\n", catalog=MODEL_CATALOG) as h:
            await h.initialize()
            session = await h.session()
            self.assertEqual((await h.prompt(session, "first durable turn"))["result"]["stopReason"], "end_turn")
            saved = h.root / "state" / "sessions" / f"{session}.json"
            before = saved.read_bytes()
            # Invalid guidance fails preparation after history was hydrated.
            (h.workspace / "AGENTS.md").write_bytes(b"invalid\x00guidance")
            failed = await h.prompt(session, "must not lose history")
            self.assertIn("error", failed)
            self.assertEqual(saved.read_bytes(), before)
            (h.workspace / "AGENTS.md").unlink()
            for method, params in [
                ("session/set_model", {"modelId": "fixture-basic"}),
                ("session/set_mode", {"modeId": "plan"}),
                ("session/set_config_option", {"configId": "reasoning_effort", "value": "medium"}),
            ]:
                result = await h.rpc(method, {"sessionId": session, **params})
                self.assertNotIn("error", result)
                self.assertEqual(len(json.loads(saved.read_text())["turns"]), 1)
            self.assertEqual((await h.prompt(session, "second durable turn"))["result"]["stopReason"], "end_turn")
            self.assertEqual([item["content"].strip() for item in h.requests[-1]["input"] if item.get("role") == "user"],
                             ["first durable turn", "second durable turn"])
            self.assertEqual(h.requests[-1]["model"], "fixture-basic")
            self.assertEqual(h.requests[-1]["reasoning"]["effort"], "medium")
            await h.rpc("_harness/session/unload", {"sessionId": session})
            loaded = await h.rpc("session/load", {"sessionId": session, "cwd": str(h.workspace), "mcpServers": []})
            self.assertEqual(loaded["result"]["modes"]["currentModeId"], "plan")
            self.assertEqual((await h.prompt(session, "third durable turn"))["result"]["stopReason"], "end_turn")
            self.assertEqual(len(json.loads(saved.read_text())["turns"]), 3)

    async def test_turn_admission_cancel_and_prompt_byte_overload_remain_responsive(self):
        release = asyncio.Event()
        async def model(body):
            await release.wait()
            return final()

        extra = ("max_active_turns = 1\nmax_model_requests = 8\n"
                 "max_pending_prompt_bytes = 1048576\ncontext_bytes = 1048576\n")
        async with Harness(model, extra=extra) as h:
            await h.initialize()
            sessions = [await h.session() for _ in range(4)]
            running = asyncio.create_task(h.prompt(sessions[0], "x" * (400 * 1024)))
            await asyncio.wait_for(h.seen.wait(), 3)
            queued = asyncio.create_task(h.prompt(sessions[1], "y" * (400 * 1024)))
            await asyncio.sleep(.05)
            rejected = await h.prompt(sessions[2], "z" * (400 * 1024))
            self.assertIn("Pending prompt byte budget", rejected["error"]["message"])
            self.assertEqual(len(h.requests), 1)
            await h.rpc("session/cancel", {"sessionId": sessions[1]})
            self.assertEqual((await asyncio.wait_for(queued, 2))["result"]["stopReason"], "cancelled")
            release.set()
            self.assertEqual((await running)["result"]["stopReason"], "end_turn")
            # Both admission permits and the byte budget are reusable.
            self.assertEqual((await h.prompt(sessions[1], "reused"))["result"]["stopReason"], "end_turn")
            self.assertEqual((await h.prompt(sessions[2], "retried"))["result"]["stopReason"], "end_turn")
            self.assertEqual(len(h.requests), 3)

    async def test_single_root_slot_can_wait_for_nested_subagents(self):
        async def model(body):
            prompt = body["input"][0].get("content", "").strip()
            done = any(item.get("type") == "function_call_output" for item in body["input"])
            if done or prompt == "grandchild":
                return final("complete")
            target = "grandchild" if prompt == "child" else "child"
            return response([call("spawn_agent", {"prompt": target, "role": "explore", "background": False})])

        async with Harness(model, extra="max_active_turns = 1\nmax_model_requests = 1\n") as h:
            await h.initialize()
            result = await asyncio.wait_for(h.prompt(await h.session(), "root"), 5)
            self.assertEqual(result["result"]["stopReason"], "end_turn")
            self.assertEqual(len(h.requests), 5)
            self.assertEqual(h.peak_active, 1)

    async def test_tool_results_are_checkpointed_before_next_provider_request(self):
        prior = "prior-history:" + "x" * 4200

        async def model(body):
            if len(h.requests) == 1:
                return final("prior turn recorded")
            outputs = [item for item in body["input"] if item.get("type") == "function_call_output"]
            if not outputs:
                return response([call("write_file", {"path": "checkpoint.txt", "content": "durable"}, "checkpoint-write")])
            checkpoint = json.loads(active.read_text())
            self.assertEqual(checkpoint["session_id"], session)
            self.assertEqual(checkpoint["base_turns"], 1)
            self.assertNotIn(prior, active.read_text(), "Checkpoint must not rewrite completed history")
            self.assertLess(active.stat().st_size, 2048)
            self.assertTrue(any(item.get("call_id") == "checkpoint-write" and item.get("type") == "function_call_output" for item in checkpoint["items"]))
            self.assertIn(prior, path.read_text(), "Completed history remains in the base session")
            self.assertEqual((h.workspace / "checkpoint.txt").read_text(), "durable")
            return STREAM_ERROR

        async with Harness(model) as h:
            await h.initialize()
            session = await h.session()
            path = h.root / "state" / "sessions" / f"{session}.json"
            active = path.with_name(f"{session}.active.json")
            self.assertNotIn("error", await h.prompt(session, prior))
            self.assertIn("error", await h.prompt(session))
            saved = json.loads(path.read_text())
            self.assertEqual(len(h.requests), 3, "Stream failures must not retry a model step")
            self.assertTrue(any(item.get("type") == "function_call_output" for item in items(saved)))
            self.assertFalse(active.exists(), "Durable base commit should remove the active checkpoint")
            self.assertEqual((h.workspace / "checkpoint.txt").read_text(), "durable")
            self.assertEqual(len(h.permissions), 1)

    async def test_pending_checkpoint_load_repairs_history_without_reexecuting_tools(self):
        snapshot = None
        base_snapshot = None

        async def model(body):
            if len(h.requests) == 1:
                return response([call("write_file", {"path": "once.txt", "content": "write once"}, "write-once")])
            if len(h.requests) == 3:
                outputs = [item for item in body["input"] if item.get("type") == "function_call_output"]
                repaired = next(item for item in outputs if item["call_id"] == "write-once")
                self.assertIn("interrupted", repaired["output"].lower())
            return final()

        async with Harness(model) as h:
            await h.initialize()
            session = await h.session()
            path = h.root / "state" / "sessions" / f"{session}.json"
            active = path.with_name(f"{session}.active.json")

            async def capture_pending(_):
                nonlocal snapshot, base_snapshot
                base_snapshot = path.read_text()
                snapshot = active.read_text()
                checkpoint = json.loads(snapshot)
                self.assertEqual(checkpoint["session_id"], session)
                self.assertEqual(checkpoint["base_turns"], len(json.loads(base_snapshot)["turns"]))
                pending = checkpoint["items"]
                self.assertTrue(any(item.get("call_id") == "write-once" and item.get("type") == "function_call" for item in pending))
                self.assertFalse(any(item.get("type") == "function_call_output" for item in pending))
                self.assertFalse((h.workspace / "once.txt").exists())

            h.permission_hook = capture_pending
            self.assertNotIn("error", await h.prompt(session))
            self.assertIsNotNone(snapshot)
            await h.rpc("_harness/session/unload", {"sessionId": session})
            path.write_text(base_snapshot)
            active.write_text(snapshot)
            active.chmod(0o600)
            loaded = await h.rpc("session/load", {"sessionId": session, "cwd": str(h.workspace), "mcpServers": []})
            self.assertNotIn("error", loaded)
            self.assertNotIn("error", await h.prompt(session, "Inspect after interruption; do not repeat the write."))
            self.assertEqual(len(h.requests), 3)
            self.assertEqual(len(h.permissions), 1)
            self.assertEqual((h.workspace / "once.txt").read_text(), "write once")

    async def test_context_projection_preserves_full_durable_history_after_reload(self):
        async def model(_):
            return final("recorded")

        originals = [f"turn-{i}:" + str(i) * 4200 for i in range(6)]
        async with Harness(model, extra="context_bytes = 16384\n") as h:
            await h.initialize()
            session = await h.session()
            for text in originals:
                self.assertNotIn("error", await h.prompt(session, text))
            projected = [item["content"] for item in h.requests[-1]["input"] if item.get("role") == "user"]
            self.assertLess(len(projected), len(originals), "Older turns must leave the model window")
            path = h.root / "state" / "sessions" / f"{session}.json"
            self.assertEqual([item["content"] for item in items(json.loads(path.read_text())) if item.get("role") == "user"], [text + "\n" for text in originals])
            await h.rpc("_harness/session/unload", {"sessionId": session})
            loaded = await h.rpc("session/load", {"sessionId": session, "cwd": str(h.workspace), "mcpServers": []})
            self.assertNotIn("error", loaded)
            self.assertNotIn("error", await h.prompt(session, "resume"))
            self.assertEqual([item["content"] for item in items(json.loads(path.read_text())) if item.get("role") == "user"], [text + "\n" for text in originals] + ["resume\n"])

    async def test_transient_pre_stream_retries_reuse_body_and_do_not_repeat_tools(self):
        write = response([call("write_file", {"path": "retry.txt", "content": "once"}, "retry-write")])
        actions = [(503, {"Retry-After": "0"}, "busy"), (429, {"Retry-After": "0"}, "busy"), (200, {}, write),
                   (503, {"Retry-After": "0"}, "busy"), (200, {}, final())]
        async with ScriptedHarness(actions) as h:
            await h.initialize()
            result = await h.prompt(await h.session())
            self.assertNotIn("error", result)
            self.assertEqual(len(h.raw_requests), 5)
            self.assertEqual(h.raw_requests[0], h.raw_requests[1])
            self.assertEqual(h.raw_requests[1], h.raw_requests[2])
            self.assertEqual(h.raw_requests[3], h.raw_requests[4])
            self.assertNotEqual(h.raw_requests[2], h.raw_requests[3])
            self.assertEqual(len(h.permissions), 1)
            self.assertEqual((h.workspace / "retry.txt").read_text(), "once")

    async def test_retry_after_limits_and_request_deadline_are_bounded(self):
        for delay, extra in [("90", ""), ("2", "request_timeout_secs = 1\n")]:
            with self.subTest(delay=delay):
                async with ScriptedHarness([(429, {"Retry-After": delay}, "busy")], extra=extra) as h:
                    await h.initialize()
                    session = await h.session()
                    started = time.monotonic()
                    self.assertIn("error", await asyncio.wait_for(h.prompt(session), 3))
                    self.assertLess(time.monotonic() - started, 2.5)
                    self.assertEqual(len(h.requests), 1)

    async def test_cancellation_interrupts_retry_backoff(self):
        async with ScriptedHarness([(429, {"Retry-After": "30"}, "busy")]) as h:
            await h.initialize()
            session = await h.session()
            running = asyncio.create_task(h.prompt(session))
            await h.seen.wait()
            await asyncio.sleep(0.05)
            started = time.monotonic()
            await h.send({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": session}})
            result = await asyncio.wait_for(running, 2)
            self.assertEqual(result["result"]["stopReason"], "cancelled")
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertEqual(len(h.requests), 1)

    async def test_provider_stream_error_is_never_retried(self):
        async with ScriptedHarness([(200, {}, STREAM_ERROR)]) as h:
            await h.initialize()
            self.assertIn("error", await h.prompt(await h.session()))
            self.assertEqual(len(h.requests), 1)
            self.assertFalse(h.permissions)


if __name__ == "__main__":
    unittest.main(verbosity=2)

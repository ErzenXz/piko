"""Math and CLI safety tests for the native size benchmark; no native build needed."""
import asyncio
import contextlib
import io
from pathlib import Path
import tempfile
import unittest

from size_benchmark import control_workload, execution_order, paired_summary, parse_args, percent_change


def run(round_number, variant, value):
    return {"round": round_number, "variant": variant,
            "binary": {"bytes": value}, "startup_and_initialize_ms": value,
            "rss_kib": {"idle": {"median_kib": value}, "workload": {"max_kib": value}},
            "workload_ms": value, "requests_per_second": value,
            "control_rpc_ms": value, "control_rpc_per_second": value,
            "control_rpc_count": 512, "control_rpc_batch_size": 16,
            "prompt_latency_ms": {"p50": value}}


class SummaryTests(unittest.TestCase):
    def test_percentages_are_signed_and_zero_baselines_stay_unknown(self):
        self.assertEqual(percent_change(100, 75), -25)
        self.assertEqual(percent_change(100, 125), 25)
        self.assertEqual(percent_change(100, 100), 0)
        self.assertIsNone(percent_change(0, 100))
        self.assertIsNone(percent_change(0, 0))
        for before, after in [(float("nan"), 1), (1, float("inf"))]:
            with self.assertRaises(ValueError):
                percent_change(before, after)

    def test_execution_order_alternates_without_reordering_pairs(self):
        self.assertEqual(execution_order(0), ("before", "after"))
        self.assertEqual(execution_order(1), ("after", "before"))
        self.assertEqual(execution_order(2), ("before", "after"))

    def test_paired_median_is_not_percentage_of_unpaired_medians(self):
        values = [run(1, "before", 100), run(1, "after", 50),
                  run(2, "after", 200), run(2, "before", 100),
                  run(3, "before", 1000), run(3, "after", 1000)]
        summary = paired_summary(values)
        metric = summary["binary_bytes"]
        self.assertEqual(metric["before_median"], 100)
        self.assertEqual(metric["after_median"], 200)
        self.assertEqual(metric["median_paired_change_percent"], 0)
        self.assertEqual([pair["round"] for pair in metric["pairs"]], [1, 2, 3])
        self.assertTrue(metric["lower_is_better"])
        self.assertFalse(summary["requests_per_second"]["lower_is_better"])
        self.assertTrue(summary["control_rpc_ms"]["lower_is_better"])
        self.assertFalse(summary["control_rpc_per_second"]["lower_is_better"])
        self.assertEqual(summary["control_rpc_ms"]["median_paired_change_percent"], 0)

    def test_zero_pairs_are_excluded_from_relative_median_only(self):
        summary = paired_summary([run(1, "before", 0), run(1, "after", 1),
                                  run(2, "before", 100), run(2, "after", 80)])
        self.assertEqual(summary["binary_bytes"]["before_median"], 50)
        self.assertEqual(summary["binary_bytes"]["median_paired_change_percent"], -20)
        self.assertIsNone(summary["binary_bytes"]["pairs"][0]["change_percent"])
        all_zero = paired_summary([run(1, "before", 0), run(1, "after", 0)])
        self.assertIsNone(all_zero["binary_bytes"]["median_paired_change_percent"])

    def test_incomplete_duplicate_and_unknown_pairs_fail(self):
        for values in [[], [run(1, "before", 1)],
                       [run(1, "before", 1), run(1, "before", 2), run(1, "after", 2)],
                       [run(1, "unknown", 1), run(1, "after", 2)]]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                paired_summary(values)

    def test_historical_reports_remain_separate_from_new_control_measurements(self):
        historical = [run(1, "before", 100), run(1, "after", 90)]
        for value in historical:
            del value["control_rpc_ms"], value["control_rpc_per_second"]
        summary = paired_summary(historical)
        self.assertNotIn("control_rpc_ms", summary)
        self.assertEqual(summary["workload_ms"]["median_paired_change_percent"], -10)
        with self.assertRaises(ValueError):
            paired_summary([historical[0], run(1, "after", 90)])
        incomplete = [run(1, "before", 100), run(1, "after", 90)]
        del incomplete[0]["control_rpc_ms"]
        with self.assertRaises(ValueError):
            paired_summary(incomplete)
        mismatched = [run(1, "before", 100), run(1, "after", 90)]
        mismatched[1]["control_rpc_count"] = 256
        with self.assertRaises(ValueError):
            paired_summary(mismatched)


class FakeControlHarness:
    def __init__(self, sessions, invalid=None):
        self.sessions, self.invalid = sessions, invalid
        self.requests, self.model_list_requests, self.permissions = [], [], []
        self.calls = self.active = self.peak_active = 0

    async def rpc(self, method, params):
        assert method == "_harness/stats" and params == {}
        self.calls += 1
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        try:
            await asyncio.sleep(0)
            runtime = {"loadedSessions": self.sessions, "activeRootTurns": 0,
                       "liveSubagents": 0, "toolProcesses": 0}
            if self.invalid in runtime:
                runtime[self.invalid] += 1
            if self.invalid == "model":
                self.requests.append("unexpected model call")
            if self.invalid == "permission":
                self.permissions.append("unexpected host tool")
            return {"result": {"runtime": runtime}}
        finally:
            self.active -= 1


class ControlWorkloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_control_calls_are_bounded_and_count_the_partial_final_batch(self):
        harness = FakeControlHarness(32)
        measured = await control_workload(harness, 32, count=35, batch_size=16)
        self.assertEqual(harness.calls, 35)
        self.assertEqual(harness.peak_active, 16)
        self.assertEqual(measured["control_rpc_count"], 35)
        self.assertEqual(measured["control_rpc_batch_size"], 16)
        self.assertGreater(measured["control_rpc_ms"], 0)
        self.assertGreater(measured["control_rpc_per_second"], 0)

    async def test_control_signal_rejects_active_work_session_changes_and_side_effects(self):
        for invalid in ["loadedSessions", "activeRootTurns", "liveSubagents", "toolProcesses",
                        "model", "permission"]:
            with self.subTest(invalid=invalid), self.assertRaises(AssertionError):
                await control_workload(FakeControlHarness(32, invalid), 32, count=1)


class ArgumentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.before = self.root / "before fixture.exe"
        self.after = self.root / "after fixture.exe"
        self.before.write_bytes(b"test input")
        self.after.write_bytes(b"test input")
        self.base = ["--before", str(self.before), "--after", str(self.after)]

    def rejected(self, extra):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(self.base + extra)

    def test_defaults_and_explicit_counts(self):
        args = parse_args(self.base)
        self.assertEqual((args.rounds, args.sessions), (5, 32))
        self.assertEqual(args.before, self.before.resolve())
        self.assertIsNone(args.output)
        args = parse_args(self.base + ["--rounds", "3", "--sessions", "8", "--output", str(self.root / "report.json")])
        self.assertEqual((args.rounds, args.sessions), (3, 8))

    def test_bounds_and_missing_binaries(self):
        for key, value in [("--rounds", "0"), ("--rounds", "26"),
                           ("--sessions", "0"), ("--sessions", "1025"),
                           ("--rounds", "1.5"), ("--before", str(self.root / "missing")),
                           ("--after", str(self.root))]:
            self.rejected([key, value])

    def test_output_cannot_destroy_binary_or_target_missing_directory(self):
        for output in [self.before, self.after, self.root, self.root / "missing" / "report.json"]:
            self.rejected(["--output", str(output)])


if __name__ == "__main__":
    unittest.main()

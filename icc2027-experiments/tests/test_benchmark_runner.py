import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from benchmark_environment import campaign_lock
from benchmark_fixtures import make_csvs, make_spec
from benchmark_plan import schedule, validate_spec
from benchmark_runner import create_campaign, execute_campaign, main, plan_campaign
from fake_benchmark_backend import FakeBackend


class BenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.csv = make_csvs(self.root / "csv")
        FakeBackend.fail_task = FakeBackend.interrupt_task = None

    def create(self, spec=None):
        campaign = self.root / "campaign"
        create_campaign(spec or make_spec(), self.csv, campaign, self.root / "fake-jena")
        return campaign

    def test_dry_plan_does_not_hash_generate_or_launch_processes(self):
        spec = json.loads((Path(__file__).parents[1] / "configs/benchmark-sensitivity-r04.json").read_text())
        with patch("benchmark_runner.sha256") as hashing:
            plan = plan_campaign(spec, self.csv, self.root / "future")
            hashing.assert_not_called()
        self.assertEqual(180, plan["task_count"])
        self.assertEqual(150, plan["measured_task_count"])
        self.assertFalse((self.root / "future").exists())
        sample = [j for j in plan["schedule"] if j["config"]["technique"] == "sample"]
        self.assertEqual({.005, .01, .05, .1}, {j["config"]["sample_probability"] for j in sample})
        self.assertEqual(set(range(1001, 1006)), {j["config"]["seed"] for j in sample})

    def test_schedule_reproducibility_blocks_and_seed_dimensions(self):
        spec = make_spec()
        first = schedule(spec)
        self.assertEqual(first, schedule(spec))
        self.assertEqual(12, len(first))
        self.assertEqual(2, sum(j["kind"] == "warmup" for j in first))
        spec["protocol"]["blocking"] = "configuration"
        second = schedule(spec)
        self.assertNotEqual(first, second)
        for block in {j["block_id"] for j in second}:
            jobs = [j for j in second if j["block_id"] == block]
            self.assertEqual(["warmup", *(["measured"] * 5)], [j["kind"] for j in jobs])

    def test_validation_rejects_unsafe_or_noncomparable_designs(self):
        for mutation in (lambda s: s["protocol"].update(measured_repetitions=0),
                         lambda s: s["protocol"].update(query_warmups=-1),
                         lambda s: s["protocol"].update(cache_state="externally_controlled", cache_evidence=""),
                         lambda s: s["configurations"][1]["config"].update(bucket_seconds=7),
                         lambda s: s["configurations"][1].update(id="../escape")):
            spec = make_spec()
            mutation(spec)
            with self.assertRaises(ValueError):
                validate_spec(spec)

    def test_five_repetitions_have_distinct_stores_and_warmups_are_excluded(self):
        campaign = self.create()
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"], state)
        paths = [e["attempts"][-1]["path"] for e in state["tasks"].values()]
        self.assertEqual(12, len(set(paths)))
        summary = json.loads((campaign / state["reports"][-1] / "performance-statistics.json").read_text())
        generation = [r for r in summary["summaries"] if r["metric"] == "generation_seconds"]
        self.assertEqual(2, len(generation))
        for item in generation:
            self.assertEqual(5, item["n_defined"])
            self.assertEqual(3, item["mean"])
        self.assertEqual(2, len(summary["exclusions"]))
        resumed = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual(state["reports"], resumed["reports"])

    def test_failed_attempt_preserved_and_retry_gets_fresh_path(self):
        spec = make_spec()
        spec["protocol"].update(order="fixed", warmup_repetitions=0, measured_repetitions=2)
        campaign = self.create(spec)
        FakeBackend.fail_task = "aggregate-measured-001"
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("failed", state["status"])
        failed = state["tasks"][FakeBackend.fail_task]["attempts"][0]["path"]
        baseline = state["tasks"]["baseline-measured-001"]["attempts"][0]["path"]
        with self.assertRaises(ValueError):
            execute_campaign(campaign, backend_factory=FakeBackend)
        FakeBackend.fail_task = None
        state = execute_campaign(campaign, retry_failed=True, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"], state)
        self.assertTrue((campaign / failed / "partial.txt").exists())
        self.assertEqual(baseline, state["tasks"]["baseline-measured-001"]["attempts"][0]["path"])
        self.assertEqual(2, len(state["tasks"]["aggregate-measured-001"]["attempts"]))

    def test_interrupt_and_resume_without_overwriting(self):
        spec = make_spec()
        spec["protocol"].update(order="fixed", warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        FakeBackend.interrupt_task = "baseline-measured-001"
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("interrupted", state["status"])
        previous = state["tasks"][FakeBackend.interrupt_task]["attempts"][0]["path"]
        FakeBackend.interrupt_task = None
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"], state)
        self.assertTrue((campaign / previous / "partial.txt").exists())

    def test_live_orphan_pid_blocks_resume(self):
        spec = make_spec()
        spec["protocol"].update(order="fixed", warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        FakeBackend.interrupt_task = "baseline-measured-001"
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        previous = campaign / state["tasks"][FakeBackend.interrupt_task]["attempts"][0]["path"]
        (previous / "orphan.process.json").write_text(json.dumps({"pid": os.getpid()}))
        with self.assertRaisesRegex(RuntimeError, "still alive"):
            execute_campaign(campaign, backend_factory=FakeBackend)

    def test_lock_blocks_second_runner(self):
        with campaign_lock(self.root / "runner.lock"):
            with self.assertRaises(RuntimeError):
                with campaign_lock(self.root / "runner.lock"):
                    pass

    def test_completed_evidence_tampering_is_detected(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        attempt = campaign / next(iter(state["tasks"].values()))["attempts"][0]["path"]
        (attempt / "result.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "evidence changed"):
            execute_campaign(campaign, backend_factory=FakeBackend)

    def test_source_drift_and_storage_shortage_are_not_ignored(self):
        campaign = self.create()
        with patch("benchmark_runner.disk_budget", return_value={"sufficient": False}):
            with self.assertRaisesRegex(ValueError, "Storage reserve"):
                execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual([], list((campaign / "attempts").iterdir()))
        with (campaign / "sources/phase_timing.py").open("a") as f:
            f.write("\n# changed")
        with self.assertRaisesRegex(ValueError, "Frozen source"):
            execute_campaign(campaign, backend_factory=FakeBackend)

    def test_sampling_probabilities_are_not_pooled_in_quality_reports(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}},
                          {"id": "s005", "config": {"technique": "sample", "sample_probability": .005, "seed": 1001}},
                          {"id": "s01", "config": {"technique": "sample", "sample_probability": .01, "seed": 1001}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"], state)
        quality = json.loads((campaign / state["reports"][-1] / "quality-index.json").read_text())
        self.assertEqual({"deterministic", "sampling-p0d005", "sampling-p0d01"}, {r["group"] for r in quality})

    def test_cli_plan_and_refusal_to_overwrite(self):
        path = self.root / "spec.json"
        path.write_text(json.dumps(make_spec()))
        args = ["plan", "--spec", str(path), "--csv-root", str(self.csv), "--output", str(self.root / "plan.json")]
        self.assertEqual(0, main(args))
        self.assertEqual(2, main(args))

    def test_cli_run_resume_retry_and_summarize(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        source = self.root / "spec.json"
        source.write_text(json.dumps(spec))
        campaign = self.root / "cli-campaign"
        FakeBackend.fail_task = "baseline-measured-001"
        actual_execute = execute_campaign
        with patch("benchmark_runner.execute_campaign", lambda *a, **k: actual_execute(*a, backend_factory=FakeBackend, **k)):
            self.assertEqual(1, main(["run", "--spec", str(source), "--csv-root", str(self.csv), "--jena-home", str(self.root), "--output", str(campaign)]))
            FakeBackend.fail_task = None
            self.assertEqual(0, main(["resume", "--campaign", str(campaign), "--retry-failed"]))
        self.assertEqual(0, main(["summarize", "--campaign", str(campaign)]))
        context = json.loads((campaign / "campaign.json").read_text())
        self.assertEqual(str(source.resolve()), context["original_specification_path"])

    def test_recovery_after_report_failure_does_not_repeat_valid_loads(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        with patch("benchmark_runner.consolidate", side_effect=OSError("Injected report failure")):
            state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("failed", state["status"])
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"])
        self.assertEqual(1, len(next(iter(state["tasks"].values()))["attempts"]))

    def test_committed_completion_with_stale_root_state_is_recovered(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        state["status"] = "running"
        next(iter(state["tasks"].values()))["status"] = "running"
        (campaign / "state.json").write_text(json.dumps(state))
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", state["status"])
        self.assertEqual(1, len(next(iter(state["tasks"].values()))["attempts"]))

    def test_unsealed_success_is_preserved_as_interrupted_before_new_attempt(self):
        spec = make_spec([{"id": "baseline", "config": {"technique": "baseline"}}])
        spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1)
        campaign = self.create(spec)
        state = execute_campaign(campaign, backend_factory=FakeBackend)
        entry = next(iter(state["tasks"].values()))
        previous = campaign / entry["attempts"][-1]["path"]
        # Simulate the precise crash window before the completion seal/root commit.
        (previous / "completion.json").unlink()
        entry["status"] = "running"
        state["status"] = "running"
        (campaign / "state.json").write_text(json.dumps(state))
        resumed = execute_campaign(campaign, backend_factory=FakeBackend)
        self.assertEqual("success", resumed["status"])
        self.assertEqual(2, len(next(iter(resumed["tasks"].values()))["attempts"]))
        self.assertTrue(list(previous.glob("unsealed-state-*.json")))
        self.assertEqual("interrupted", json.loads((previous / "state.json").read_text())["status"])


if __name__ == "__main__":
    unittest.main()

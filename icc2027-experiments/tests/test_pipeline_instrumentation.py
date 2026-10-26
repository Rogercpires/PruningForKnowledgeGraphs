import json
import sys
import tempfile
import unittest
import copy
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from benchmark_fixtures import make_csvs
from icc2027_pipeline import discover_inputs, write_run
from benchmark_plan import configurations
from benchmark_backend import verify_generated_identity
from icc2027_pipeline import DEFAULT_TEMPORAL_RULES, sha256
from phase_timing import PhaseTiming


class InstrumentationTests(unittest.TestCase):
    def test_six_policy_rdf_bytes_match_stage3(self):
        expected = json.loads((Path(__file__).parent / "fixtures/stage3-r04-artifact-checksums.json").read_text())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = discover_inputs(make_csvs(root / "csv"))
            for name, before in expected.items():
                with self.subTest(policy=name):
                    m = write_run(before["config"], inputs, root / name, name, "normalized")
                    self.assertEqual(before["artifact_sha256"], [a["sha256"] for a in m["artifacts"]])
                    self.assertEqual(before["selected_flows"], m["metrics"]["selected_flows"])
                    self.assertEqual(before["label_counts"], m["metrics"]["label_counts"])
                    self.assertAlmostEqual(m["timing"]["partition_total_seconds"], sum(m["timing"]["exclusive_seconds"].values()), places=7)
                    self.assertIsNone(m["metrics"]["tdb2_load_seconds"])
                    if name == "sample":
                        self.assertEqual(1001, m["sampling"]["seed"])
                        self.assertEqual(m["metrics"]["selected_flows"] / 24, m["sampling"]["probability_effective"])
                    self.assertEqual(4 if name in {"topk", "temporal"} else 3, m["timing"]["passes_over_csv"])

    def test_invalid_parameters_fail_before_input_passes(self):
        for config in ({"technique": "labels", "labels": []}, {"technique": "sample", "sample_probability": 0, "seed": 1},
                       {"technique": "topk", "k": 0}, {"technique": "aggregate", "bucket_seconds": 0},
                       {"technique": "sample", "sample_probability": .5, "seed": 1, "labels": ["DDoS"]}):
            with patch("icc2027_pipeline.sha256") as hash_function:
                with self.assertRaises(ValueError):
                    write_run(config, [], Path("not-created"), "bad")
                hash_function.assert_not_called()

    def test_all_sensitivity_configurations_generate_tiny_r04_manifests(self):
        spec = json.loads((Path(__file__).parents[1] / "configs/benchmark-sensitivity-r04.json").read_text())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = discover_inputs(make_csvs(root / "csv"))
            for item in configurations(spec):
                with self.subTest(configuration=item["id"]):
                    m = write_run(item["config"], inputs, root / item["id"], item["id"], "normalized")
                    self.assertEqual("2026-10-22.2-r04", m["temporal_interpretation"]["rules_version"])
                    self.assertEqual("exclude", m["temporal_interpretation"]["ambiguous_policy"])
                    self.assertLessEqual(m["metrics"]["selected_flows"], 24)

    def test_exclusive_timer_clock_and_nested_phase_rejection(self):
        clock = iter([0.0, 2.0])
        timer = PhaseTiming(lambda: next(clock))
        with timer.measure("work"):
            with self.assertRaises(RuntimeError):
                with timer.measure("nested"):
                    pass
        self.assertEqual({"work": 2.0, "unattributed_seconds": 1.0}, timer.snapshot(3))

    def test_per_repetition_identity_guard_rejects_changed_inputs_and_rules(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = discover_inputs(make_csvs(root / "csv"))
            task = {"config": {"technique": "baseline"}}
            m = write_run(task["config"], inputs, root / "generation", "unique-attempt", "normalized")
            context = {"inputs": copy.deepcopy(m["input_csvs"]), "rules_sha256": sha256(DEFAULT_TEMPORAL_RULES),
                       "spec": {"temporal_mode": "normalized", "ambiguous_policy": "exclude"}}
            verify_generated_identity(m, context, task)
            changed = copy.deepcopy(m)
            changed["temporal_interpretation"]["rules_sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "temporal interpretation"):
                verify_generated_identity(changed, context, task)
            changed = copy.deepcopy(m)
            changed["input_csvs"][0]["sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "Input CSV identity"):
                verify_generated_identity(changed, context, task)
            with inputs[0].open("a") as sink:
                sink.write("changed after generation\n")
            with self.assertRaisesRegex(ValueError, "Input CSV identity"):
                verify_generated_identity(m, context, task)


if __name__ == "__main__":
    unittest.main()

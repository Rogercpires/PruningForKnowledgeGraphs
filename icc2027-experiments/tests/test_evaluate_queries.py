import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from evaluate_queries import (PACKAGE, answer, calculate, check_answers, compare_saved, export_reports, import_legacy,
                              load_bundle, load_plan, main, new_output, query_paths)
from evaluation_fixtures import make_inputs


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.registry, self.runs, self.legacy = make_inputs(self.root)

    def imported(self):
        plan = load_plan(self.registry, self.runs, None, require_stores=False)
        output = self.root / "out"
        output.mkdir()
        bundle = import_legacy(plan, self.legacy, output, PACKAGE / "queries")
        return output, bundle, {p["id"]: p["manifest_data"] for p in plan["policies"]}

    def test_replay_compatibility_and_no_jena_dependency(self):
        output, bundle, manifests = self.imported()
        report = export_reports(bundle, manifests, output)
        restored, saved_manifests = load_bundle(output)
        self.assertEqual(report, calculate(restored, saved_manifests))
        self.assertIsNone(report["policies"][0]["observed_stored_distinct_triples"])
        self.assertEqual(2, report["policies"][0]["attack_preservation"]["baseline_attack_count"])
        self.assertEqual(1, report["policies"][1]["attack_preservation"]["observed_attack_count"])
        self.assertIsNotNone(report["policies"][1]["protocol"]["horvitz_thompson"])
        self.assertEqual(0, main(["report", "--input", str(output), "--output", str(self.root / "replay")]))
        self.assertEqual((output / "metrics.json").read_bytes(), (self.root / "replay/metrics.json").read_bytes())
        self.assertTrue((output / "class-preservation.csv").is_file())
        self.assertTrue((output / "category-errors.csv").is_file())
        self.assertTrue((output / "sampling-statistics.csv").is_file())

    def test_corrupt_saved_raw_output_fails_checksum(self):
        output, _, _ = self.imported()
        (output / "raw/sample/top.stdout").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "checksum"):
            load_bundle(output)

    def test_exact_comparison_command_and_same_results(self):
        output, _, _ = self.imported()
        comparison = compare_saved(output, output)
        self.assertTrue(comparison["all_equal"])
        self.assertEqual(6, len(comparison["queries"]))
        self.assertEqual(0, main(["compare", "--reference", str(output), "--candidate", str(output),
                                 "--output", str(self.root / "comparison")]))

    def test_missing_saved_query_is_not_success(self):
        output, bundle, _ = self.imported()
        del bundle["policies"][0]["queries"]["hour"]
        (output / "bundle.json").write_text(json.dumps(bundle))
        with self.assertRaisesRegex(ValueError, "missing required"):
            load_bundle(output)

    def test_failure_is_not_an_empty_answer(self):
        _, bundle, manifests = self.imported()
        bundle["policies"][1]["queries"]["protocol"].update(status="timeout", result=None, error="timeout")
        report = calculate(bundle, manifests)["policies"][1]
        self.assertEqual("failed", report["status"])
        self.assertIsNone(report["protocol"]["observed_total"])
        self.assertIsNone(report["protocol"]["count_l1_error_pct"])
        self.assertIsNone(answer(bundle["policies"][1], "protocol"))

    def test_wrong_graph_detected_by_population(self):
        _, bundle, manifests = self.imported()
        sample = bundle["policies"][1]
        sample["queries"]["hour"]["result"] = {}
        check_answers(sample, manifests["sample"])
        self.assertEqual("population_mismatch", sample["queries"]["hour"]["status"])
        self.assertIsNone(sample["queries"]["hour"]["result"])

    def test_representation_selects_aggregate_queries(self):
        plan = load_plan(self.registry, self.runs, None, require_stores=False)
        manifest = copy.deepcopy(plan["policies"][0]["manifest_data"])
        manifest["config"]["technique"] = "aggregate"
        queries = query_paths(manifest, PACKAGE / "queries")
        self.assertIn("aggregate.rq", queries["hour"].name)
        self.assertEqual("stored-triples-default.rq", queries["stored"].name)

    def test_aggregation_counts_are_not_instances(self):
        _, bundle, manifests = self.imported()
        manifest = manifests["sample"]
        manifest["config"] = {"technique": "aggregate", "bucket_seconds": 60}
        manifest["metrics"]["aggregated_groups"] = 1
        report = calculate(bundle, manifests)["policies"][1]
        self.assertEqual(0.5, report["attack_preservation"]["attack_count_retention"])
        self.assertEqual(0, report["attack_instance_retention"])

    def test_input_population_mismatch_and_duplicate_ids_fail(self):
        path = self.runs / "sample/manifest.json"
        d = json.loads(path.read_text())
        d["input_csvs"][0]["sha256"] = "c" * 64
        path.write_text(json.dumps(d))
        with self.assertRaisesRegex(ValueError, "different input"):
            load_plan(self.registry, self.runs, None, require_stores=False)
        reg = json.loads(self.registry.read_text())
        reg["policies"][1]["id"] = "baseline"
        self.registry.write_text(json.dumps(reg))
        with self.assertRaises(ValueError):
            load_plan(self.registry, self.runs, None, require_stores=False)

    def test_missing_tdb_path_and_overwrite_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "TDB2 store not found"):
            load_plan(self.registry, self.runs, self.root / "absent", require_stores=True)
        output = self.root / "out"
        output.mkdir()
        (output / "keep.txt").write_text("keep")
        with self.assertRaises(FileExistsError):
            new_output(output, [self.runs])
        self.assertEqual("keep", (output / "keep.txt").read_text())
        with self.assertRaises(ValueError):
            new_output(self.runs / "nested", [self.runs])

    def test_cli_failure_nonzero_without_creating_database(self):
        self.assertEqual(2, main(["run", "--registry", str(self.registry), "--runs-root", str(self.runs),
                                 "--tdb-root", str(self.root / "missing"), "--jena-home", str(self.root),
                                 "--output", str(self.root / "output")]))
        self.assertFalse((self.root / "missing").exists())


if __name__ == "__main__":
    unittest.main()

"""Archived answer regression; portable, no live TDB2 or complete CICIDS CSVs."""
import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PACKAGE / "src"))
from evaluate_queries import calculate, compare_saved, export_reports, load_bundle


class ReferenceRegressionTests(unittest.TestCase):
    def setUp(self):
        self.bundle, self.manifests = load_bundle(PACKAGE / "reports/stage3-reference-r04")

    def test_reproduced_csvs_match_exploratory_reports(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            export_reports(self.bundle, self.manifests, output)
            for name in ("query-preservation-summary.csv", "query-preservation-extended-r04.csv"):
                with (output / name).open(encoding="utf-8", newline="") as source:
                    actual = list(csv.DictReader(source, delimiter=";"))
                with (PACKAGE / "tests/fixtures" / name).open(encoding="utf-8-sig", newline="") as source:
                    expected = list(csv.DictReader(source, delimiter=";"))
                self.assertEqual(len(actual), 13)
                self.assertEqual(len(actual), len(expected))
                for row, reference in zip(actual, expected):
                    self.assertEqual(set(row), set(reference))
                    for field in row:
                        if field == "policy" or row[field] in ("", "True", "False"):
                            self.assertEqual(row[field], reference[field], field)
                        else:
                            self.assertAlmostEqual(float(row[field]), float(reference[field]), places=10, msg=field)

    def test_aggregation_topk_and_five_sampling_seeds(self):
        report = calculate(self.bundle, self.manifests)
        policies = {p["id"]: p for p in report["policies"]}
        aggregate = policies["aggregation-60s"]
        self.assertTrue(aggregate["top_exact_results_equal"])
        for kind in ("hour", "protocol"):
            self.assertTrue(aggregate[kind]["exact_results_equal"])
            self.assertEqual(0, aggregate[kind]["tvd_pct"])
        topk = policies["top-k-20"]
        self.assertEqual(20, topk["ranking"]["overlap_count"])
        self.assertTrue(topk["ranking"]["same_ranking"])
        self.assertEqual(4, topk["hour"]["baseline_total"] - topk["hour"]["observed_total"])
        self.assertAlmostEqual(0.0007173, topk["hour"]["count_l1_error_pct"], places=6)
        stats = report["sampling_statistics"]
        self.assertAlmostEqual(0.3435, stats["protocol_tvd_pct"]["mean"], places=4)
        self.assertAlmostEqual(1.0552, stats["hour_tvd_pct"]["mean"], places=4)
        self.assertAlmostEqual(0.7458, stats["protocol_estimated_count_l1_error_pct"]["mean"], places=4)
        self.assertAlmostEqual(2.3020, stats["hour_estimated_count_l1_error_pct"]["mean"], places=4)
        self.assertEqual((11, 12), (stats["retained_attack_classes"]["min"], stats["retained_attack_classes"]["max"]))

    def test_offline_replay_is_stable_across_python_hash_seeds(self):
        with tempfile.TemporaryDirectory() as temp:
            paths = []
            for seed in ("1", "2"):
                output = Path(temp) / seed
                subprocess.run([sys.executable, "-B", str(PACKAGE / "src/evaluate_queries.py"), "report",
                                "--input", str(PACKAGE / "reports/stage3-reference-r04"), "--output", str(output)],
                               env={**os.environ, "PYTHONHASHSEED": seed}, check=True, capture_output=True, timeout=30)
                paths.append(output)
            for name in ("metrics.json", "query-preservation-summary.csv", "category-errors.csv"):
                self.assertEqual((paths[0] / name).read_bytes(), (paths[1] / name).read_bytes(), name)

    def test_39_actual_jena_answers_match_archived_exploratory_answers(self):
        comparison = compare_saved(PACKAGE / "reports/stage3-reference-r04", PACKAGE / "reports/stage3-jena-r04")
        self.assertTrue(comparison["all_equal"])
        self.assertEqual(39, len(comparison["queries"]))


if __name__ == "__main__":
    unittest.main()

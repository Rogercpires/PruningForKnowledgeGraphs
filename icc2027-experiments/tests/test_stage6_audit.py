"""Regressions for confirmed final-audit findings; synthetic data only."""
import copy
import csv
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from benchmark_fixtures import make_csvs, make_spec
from benchmark_plan import configurations, select_design, schedule
from benchmark_runner import execute_campaign, main
from fake_benchmark_backend import FakeBackend
from evaluate_queries import PACKAGE, calculate, export_reports, import_legacy, load_plan
from evaluation_fixtures import make_inputs
from evaluation_metrics import compare_rankings
from icc2027_pipeline import DEFAULT_TEMPORAL_RULES, load_temporal_rules, temporal_preflight, write_run


class FinalAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_canonical_labels_retain_cp1252_web_attacks(self):
        spec = json.loads((PACKAGE / "configs/benchmark-sensitivity-r04.json").read_text())
        canonical = json.loads((PACKAGE / "configs/labels-all-attacks.json").read_text(encoding="utf-8"))
        self.assertEqual(canonical["labels"], spec["sensitivity"]["labels"])
        config = next(c["config"] for c in configurations(spec) if c["id"] == "labels")
        csv_root = make_csvs(self.root / "csv")
        source = csv_root / "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv"
        header = ["Flow ID", "Source IP", "Destination IP", "Timestamp", "Label", "Protocol"]
        labels = ["DDoS", "Web Attack \u2013 Brute Force", "Web Attack \u2013 Sql Injection", "Web Attack \u2013 XSS"]
        with source.open("w", encoding="cp1252", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(header)
            for i, label in enumerate(labels):
                writer.writerow([str(i), "10.0.0.1", "10.0.0.2", "6/7/2017 10:00", label, "6"])
        fixed = write_run(config, [source], self.root / "fixed", "fixed", "normalized")
        broken_config = copy.deepcopy(config)
        broken_config["labels"] = [label.replace("\u2013", "\u00e2\u20ac\u201c") for label in config["labels"]]
        broken = write_run(broken_config, [source], self.root / "broken", "broken", "normalized")
        self.assertEqual(4, fixed["metrics"]["selected_flows"])
        self.assertEqual(set(labels), set(fixed["metrics"]["label_counts"]))
        self.assertEqual({"DDoS": 1}, broken["metrics"]["label_counts"])
        spec["sensitivity"]["labels"] = broken_config["labels"]
        with self.assertRaisesRegex(ValueError, "canonical"):
            configurations(spec)

    def test_preflight_excludes_ambiguous_and_counts_invalid_once(self):
        source = Path("Wednesday-workingHours.pcap_ISCX.csv")
        records = [(55504, {"Source IP": "a", "Destination IP": "b", "Label": "DDoS", "Timestamp": "5/7/2017 1:43"}),
                   (55505, {"Source IP": "a", "Destination IP": "b", "Label": "BENIGN", "Timestamp": "5/7/2017 9:00"}),
                   (55506, {"Source IP": "a", "Destination IP": "b", "Label": "DDoS", "Timestamp": "invalid"})]
        with patch("icc2027_pipeline.source_rows", return_value=iter(records)):
            excluded = temporal_preflight([source], "normalized", load_temporal_rules(DEFAULT_TEMPORAL_RULES), "exclude")
        self.assertEqual((2, 1, 1, 1, 0), tuple(excluded[k] for k in
                         ("counting_version", "invalid", "ambiguous_excluded", "eligible_flows", "labelled_attacks")))
        with patch("icc2027_pipeline.source_rows", return_value=iter(records)):
            retained = temporal_preflight([source], "normalized", load_temporal_rules(DEFAULT_TEMPORAL_RULES), "raw-local")
        self.assertEqual((2, 1, 0), tuple(retained[k] for k in ("eligible_flows", "labelled_attacks", "ambiguous_excluded")))

    def test_short_ranking_exposes_fixed_n_and_baseline_recall(self):
        result = compare_rankings({"b": 1, "a": 1}, {"a": 1, "b": 1}, n=20)
        self.assertEqual(.1, result["overlap"])
        self.assertEqual(20, result["overlap_denominator"])
        self.assertEqual("fixed_requested_n", result["overlap_definition"])
        self.assertEqual(1, result["baseline_recall"])
        self.assertEqual(2, result["baseline_recall_denominator"])
        self.assertIsNone(compare_rankings({}, {}, n=20)["baseline_recall"])

    def test_batch_keeps_baseline_budget_and_original_spec(self):
        original = make_spec()
        before = copy.deepcopy(original)
        batch = select_design(original, ["aggregate"], 2, 0)
        self.assertEqual(before, original)
        self.assertEqual({"baseline", "aggregate"}, {c["id"] for c in configurations(batch)})
        self.assertEqual(original["storage"], batch["storage"])
        self.assertEqual(4, len(schedule(batch)))
        with self.assertRaisesRegex(ValueError, "Unknown"):
            select_design(original, ["typo"])
        with self.assertRaises(ValueError):
            select_design(original, measured_repetitions=0)

    def test_plan_cli_overrides_are_validated_and_do_not_execute(self):
        spec = self.root / "spec.json"
        spec.write_text(json.dumps(make_spec()))
        csv_root = make_csvs(self.root / "csv")
        output = self.root / "plan.json"
        with patch("benchmark_runner.create_campaign") as create:
            self.assertEqual(0, main(["plan", "--spec", str(spec), "--csv-root", str(csv_root),
                                     "--output", str(output), "--config-id", "aggregate",
                                     "--measured-repetitions", "2", "--warmup-repetitions", "0"]))
            create.assert_not_called()
        self.assertEqual(4, json.loads(output.read_text())["task_count"])
        self.assertEqual(5, json.loads(spec.read_text())["protocol"]["measured_repetitions"])
        self.assertEqual(2, main(["plan", "--spec", str(spec), "--csv-root", str(csv_root),
                                 "--output", str(self.root / "bad.json"), "--config-id", "unknown"]))
        self.assertFalse((self.root / "bad.json").exists())

    def test_mixed_sampling_probabilities_are_not_pooled(self):
        registry, runs, legacy = make_inputs(self.root)
        plan = load_plan(registry, runs, None, require_stores=False)
        out = self.root / "out"
        out.mkdir()
        bundle = import_legacy(plan, legacy, out, PACKAGE / "queries")
        manifests = {p["id"]: p["manifest_data"] for p in plan["policies"]}
        uniform = calculate(bundle, manifests)
        self.assertEqual(uniform["sampling_statistics"], uniform["sampling_statistics_by_probability"]["0.5"])
        second = copy.deepcopy(bundle["policies"][1])
        second["id"] = "sample-other-p"
        bundle["policies"].append(second)
        manifests[second["id"]] = copy.deepcopy(manifests["sample"])
        manifests[second["id"]]["config"]["sample_probability"] = .1
        report = export_reports(bundle, manifests, out)
        self.assertEqual(2, report["schema_version"])
        self.assertIsNone(report["sampling_statistics"])
        self.assertEqual({"0.1", "0.5"}, set(report["sampling_statistics_by_probability"]))
        self.assertEqual(1, report["sampling_statistics_by_probability"]["0.1"]["protocol_tvd_pct"]["n_defined"])
        self.assertEqual(1, len((out / "sampling-statistics.csv").read_text().splitlines()))
        self.assertTrue((out / "sampling-statistics-by-probability.csv").is_file())

    def test_run_cli_persists_selected_design_and_original_spec(self):
        spec = self.root / "spec.json"
        spec.write_text(json.dumps(make_spec()))
        csv_root = make_csvs(self.root / "csv")
        output = self.root / "campaign"
        FakeBackend.fail_task = FakeBackend.interrupt_task = None
        with patch("benchmark_runner.execute_campaign",
                   side_effect=lambda path: execute_campaign(path, backend_factory=FakeBackend)):
            self.assertEqual(0, main(["run", "--spec", str(spec), "--csv-root", str(csv_root),
                                     "--output", str(output), "--jena-home", str(self.root / "jena"),
                                     "--config-id", "aggregate", "--measured-repetitions", "1",
                                     "--warmup-repetitions", "0"]))
        state = json.loads((output / "state.json").read_text())
        self.assertEqual("success", state["status"])
        self.assertEqual(2, len(state["tasks"]))
        self.assertEqual(5, json.loads(spec.read_text())["protocol"]["measured_repetitions"])

    def test_sampling_seed_reproducible_within_eligibility_mode(self):
        source = self.root / "Wednesday-workingHours.pcap_ISCX.csv"
        header = "Flow ID,Source IP,Destination IP,Timestamp,Label,Protocol\n"
        source.write_text(header + "ambiguous,a,b,5/7/2017 1:43,DDoS,6\n" +
                          "".join(f"flow{i},a,b,5/7/2017 9:00,BENIGN,6\n" for i in range(40)))
        rules = load_temporal_rules(DEFAULT_TEMPORAL_RULES)
        # Move a real R04 ambiguity to this tiny fixture's second physical line.
        exceptions = rules["file_rules"][source.name]["line_exceptions"]
        ambiguous = next(e for e in exceptions if e["action"] == "ambiguous")
        ambiguous["line"] = 2
        config = {"technique": "sample", "seed": 1001, "sample_probability": .5}
        results = []
        for identity, mode in (("a", "exclude"), ("b", "exclude"), ("c", "raw-local")):
            manifest = write_run(config, [source], self.root / identity, identity, "normalized", rules, mode)
            artifact = self.root / identity / manifest["artifacts"][0]["path"]
            with gzip.open(artifact, "rt", encoding="utf-8") as stream:
                results.append(stream.read())
        self.assertEqual(results[0], results[1])
        self.assertNotEqual(results[0], results[2])


if __name__ == "__main__":
    unittest.main()

import gzip
import importlib.util
import json
import copy
import sys
import tempfile
import unittest
from pathlib import Path

try:
    import rdflib
except ImportError:  # The project generator itself has no third-party dependency.
    rdflib = None

MODULE = Path(__file__).parents[1] / "src" / "icc2027_pipeline.py"
sys.path.insert(0, str(MODULE.parent))
SPEC = importlib.util.spec_from_file_location("pipeline", MODULE)
PIPELINE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIPELINE)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "csv"
        self.root.mkdir()
        self.write_fixture([
            "10.0.0.1,10.0.0.2,03/07/2017 10:00:00,DDoS,1,80,6,2",
            "10.0.0.1,10.0.0.3,03/07/2017 11:00:00,PortScan,2,443,6,3",
        ])

    def write_fixture(self, rows):
        text = "Source IP,Destination IP,Timestamp,Label,Source Port,Destination Port,Protocol,Flow Duration\n" + "\n".join(rows) + "\n"
        for name in PIPELINE.FILES:
            (self.root / name).write_text(text, encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_baseline_manifest_has_exact_triple_count(self):
        output = Path(self.temp.name) / "baseline"
        result = PIPELINE.write_run({"technique": "baseline"}, PIPELINE.discover_inputs(self.root), output, "b01")
        self.assertEqual(16, result["metrics"]["selected_flows"])
        lines = 0
        for artifact in output.glob("*.nt.gz"):
            with gzip.open(artifact, "rt", encoding="utf-8") as source:
                lines += sum(1 for _ in source)
        self.assertEqual(lines, result["metrics"]["emitted_triples"])
        self.assertEqual("b01", json.loads((output / "manifest.json").read_text())["run_id"])

    def test_sampling_is_repeatable_for_the_same_seed(self):
        inputs = PIPELINE.discover_inputs(self.root)
        first = PIPELINE.write_run({"technique": "sample", "sample_probability": 0.5, "seed": 7}, inputs, Path(self.temp.name) / "one", "s01")
        second = PIPELINE.write_run({"technique": "sample", "sample_probability": 0.5, "seed": 7}, inputs, Path(self.temp.name) / "two", "s02")
        self.assertEqual(first["metrics"]["selected_flows"], second["metrics"]["selected_flows"])
        self.assertEqual(first["metrics"]["emitted_triples"], second["metrics"]["emitted_triples"])

    def test_aggregate_label_counts_preserve_unique_classes(self):
        inputs = PIPELINE.discover_inputs(self.root)
        result = PIPELINE.write_run({"technique": "aggregate", "bucket_seconds": 3600}, inputs, Path(self.temp.name) / "aggregate", "a01")
        self.assertEqual({"DDoS": 8, "PortScan": 8}, result["metrics"]["aggregate_label_counts"])
        self.assertEqual(16, result["metrics"]["aggregate_label_count_sum"])
        self.assertEqual(16, result["metrics"]["selected_flows"])

    def test_aggregate_label_counts_preserve_mixed_classes_and_stable_iris(self):
        self.write_fixture([
            "10.0.0.1,10.0.0.2,03/07/2017 10:00:00,DDoS,1,80,6,2",
            "10.0.0.1,10.0.0.2,03/07/2017 10:00:10,PortScan,1,80,6,2",
            "10.0.0.1,10.0.0.2,03/07/2017 10:00:20,DDoS,1,80,6,2",
        ])
        inputs = PIPELINE.discover_inputs(self.root)
        first = PIPELINE.write_run({"technique": "aggregate", "bucket_seconds": 60}, inputs, Path(self.temp.name) / "one", "a01")
        second = PIPELINE.write_run({"technique": "aggregate", "bucket_seconds": 60}, inputs, Path(self.temp.name) / "two", "a02")
        self.assertEqual(8, first["metrics"]["aggregated_groups"])
        self.assertEqual({"DDoS": 16, "PortScan": 8}, first["metrics"]["aggregate_label_counts"])
        self.assertEqual(first["metrics"]["aggregate_label_counts"], second["metrics"]["aggregate_label_counts"])
        self.assertEqual(
            [item["sha256"] for item in first["artifacts"]],
            [item["sha256"] for item in second["artifacts"]],
        )
        with gzip.open(Path(self.temp.name) / "one" / first["artifacts"][0]["path"], "rt", encoding="utf-8") as source:
            rdf = source.read()
        self.assertIn("<http://example.org/ns#hasLabelCount>", rdf)
        self.assertIn('"DDoS"', rdf)
        self.assertIn('"PortScan"', rdf)

    def test_normalized_mode_integrates_all_policies_and_excludes_ambiguous_fixture(self):
        """Smoke test the shared temporal layer through all six pruning policies."""
        special = [
            "wed-dawn,10.0.0.1,10.0.0.2,5/7/2017 2:24,BENIGN,1,80,6,2",
            "wed-ambiguous,10.0.0.1,10.0.0.2,5/7/2017 1:43,BENIGN,1,80,6,2",
            "wed-noon,10.0.0.1,10.0.0.2,5/7/2017 12:00,BENIGN,1,80,6,2",
            "wed-morning-after-noon,10.0.0.1,10.0.0.2,5/7/2017 8:55,BENIGN,1,80,6,2",
        ]
        header = "Flow ID,Source IP,Destination IP,Timestamp,Label,Source Port,Destination Port,Protocol,Flow Duration\n"
        normal = "normal,10.0.0.1,10.0.0.2,3/7/2017 10:00,BENIGN,1,80,6,2\n"
        for name in PIPELINE.FILES:
            payload = header + ("\n".join(special) + "\n" if name.startswith("Wednesday") else normal)
            (self.root / name).write_text(payload, encoding="utf-8")
        rules = json.loads((MODULE.parent.parent / "configs" / "temporal-normalization-rules.json").read_text(encoding="utf-8"))
        rules["_path"] = str(MODULE.parent.parent / "configs" / "temporal-normalization-rules.json")
        # Fixture line numbers are deliberately small; the production R04 line
        # identities are covered in test_temporal_audit.
        rules = copy.deepcopy(rules)
        rules["file_rules"]["Wednesday-workingHours.pcap_ISCX.csv"]["line_exceptions"] = [
            {"line": 2, "timestamp_original": "5/7/2017 2:24", "id": "fixture-dawn", "action": "preserve", "classification": "preserved", "confidence": "provisional", "rationale": "fixture"},
            {"line": 3, "timestamp_original": "5/7/2017 1:43", "id": "fixture-ambiguous", "action": "ambiguous", "classification": "ambiguous", "confidence": "ambiguous", "rationale": "fixture"},
        ]
        inputs = PIPELINE.discover_inputs(self.root)
        raw = PIPELINE.write_run({"technique": "baseline"}, inputs, Path(self.temp.name) / "raw", "raw-r01", "raw", rules)
        expected = {
            "baseline": {"technique": "baseline"}, "temporal": {"technique": "temporal", "window_hours": 24},
            "labels": {"technique": "labels", "labels": ["benign"]}, "sample": {"technique": "sample", "sample_probability": 1, "seed": 7},
            "topk": {"technique": "topk", "k": 1}, "aggregate": {"technique": "aggregate", "bucket_seconds": 60},
        }
        manifests = {}
        for technique, config in expected.items():
            manifests[technique] = PIPELINE.write_run(config, inputs, Path(self.temp.name) / technique, f"{technique}-r01", "normalized", rules, "exclude")
            temporal = manifests[technique]["temporal_interpretation"]
            self.assertEqual("normalized", temporal["mode"])
            self.assertEqual("2026-10-22.2-r04", temporal["rules_version"])
            self.assertEqual(1, temporal["input_counts"]["ambiguous_excluded"])
        self.assertEqual(11, raw["metrics"]["selected_flows"])
        self.assertEqual(10, manifests["baseline"]["metrics"]["selected_flows"])
        with gzip.open(Path(self.temp.name) / "baseline" / manifests["baseline"]["artifacts"][2]["path"], "rt", encoding="utf-8") as source:
            rdf = source.read()
        self.assertIn('"2017-07-05T02:24:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', rdf)
        self.assertNotIn("timestampEpoch", rdf)
        self.assertNotIn("Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", rdf)
        for artifact in (Path(self.temp.name) / "aggregate").glob("*.nt.gz"):
            with gzip.open(artifact, "rt", encoding="utf-8") as source:
                ntriples = source.read()
                for line in ntriples.splitlines():
                    self.assertTrue(line.startswith("<") and line.rstrip().endswith("."))
            if rdflib is not None:
                graph = rdflib.Graph()
                graph.parse(data=ntriples, format="nt")


if __name__ == "__main__":
    unittest.main()



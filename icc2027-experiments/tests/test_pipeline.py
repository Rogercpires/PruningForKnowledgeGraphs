import gzip
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE = Path(__file__).parents[1] / "src" / "icc2027_pipeline.py"
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


if __name__ == "__main__":
    unittest.main()




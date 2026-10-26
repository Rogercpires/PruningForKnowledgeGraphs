"""Tiny end-to-end benchmark infrastructure check, not a CICIDS campaign."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(PACKAGE / "src"), str(PACKAGE / "tests")]
from benchmark_fixtures import make_csvs, make_spec
from benchmark_runner import create_campaign, execute_campaign, verify_completed
from evaluate_queries import load_bundle
from jena_executor import JenaExecutor
from sparql_results import parse_json


@unittest.skipUnless(os.environ.get("ICC2027_JENA_HOME"), "Set ICC2027_JENA_HOME for tiny real-Jena benchmark integration")
class BenchmarkJenaIntegrationTests(unittest.TestCase):
    def test_fresh_load_validation_warmups_reporting_and_noop_resume(self):
        with tempfile.TemporaryDirectory(prefix="stage4-tiny-jena-") as temp:
            root = Path(temp)
            csv = make_csvs(root / "csv")
            spec = make_spec()
            spec["protocol"].update(warmup_repetitions=0, measured_repetitions=1,
                                    query_warmups=1, query_measurements=1, subprocess_timeout_seconds=120)
            spec["storage"].update(reserve_free_bytes=16 * 1024 ** 2, estimated_bytes_per_repetition=512 * 1024 ** 2)
            campaign = root / "campaign"
            home = Path(os.environ["ICC2027_JENA_HOME"])
            create_campaign(spec, csv, campaign, home, jvm_args=["-Xmx1g"])
            state = execute_campaign(campaign)
            self.assertEqual("success", state["status"], state)
            stores = []
            for entry in state["tasks"].values():
                attempt = campaign / entry["attempts"][-1]["path"]
                verify_completed(attempt)
                stores.append(attempt / "tdb2")
                observations = json.loads((attempt / "query-observations.json").read_text())
                self.assertEqual(3, sum(o["kind"] == "warmup" for o in observations))
                self.assertEqual(3, sum(o["kind"] == "measured" for o in observations))
                result = json.loads((attempt / "result.json").read_text())
                self.assertEqual(attempt.name, json.loads(Path(result["manifest_file"]).read_text())["run_id"])
                self.assertGreater(result["metrics"]["tdb2_load_seconds"], 0)
                self.assertAlmostEqual(result["metrics"]["total_seconds"], sum(result["exclusive_process_stages_seconds"].values()), places=6)
            self.assertEqual(2, len(set(stores)))
            report = campaign / state["reports"][-1] / "quality-deterministic-rep001"
            bundle, _ = load_bundle(report)
            baseline, aggregate = bundle["policies"]
            for kind in ("top", "protocol", "hour"):
                self.assertEqual(baseline["queries"][kind]["result"], aggregate["queries"][kind]["result"])
            resumed = execute_campaign(campaign)
            self.assertEqual(len(state["reports"]), len(resumed["reports"]))
            invalid = root / "bad.rq"
            invalid.write_text("THIS IS NOT SPARQL")
            rec = JenaExecutor(home).query(stores[0], invalid, root / "invalid-query", 45, lambda data: parse_json(data, "top"))
            self.assertEqual("execution_error", rec["status"])
            self.assertIsNone(rec["result"])


if __name__ == "__main__":
    unittest.main()

"""Optional installed-Jena tests. Four flows in RAM, no TDB2 load or creation."""
import json
import os
import sys
import tempfile
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PACKAGE / "src"))
from icc2027_pipeline import aggregate_key, emit_aggregate, emit_flow
from jena_executor import JenaExecutor, run_recorded
from sparql_results import parse_json


@unittest.skipUnless(os.environ.get("ICC2027_JENA_HOME"), "Set ICC2027_JENA_HOME for installed Jena integration")
class InstalledJenaTests(unittest.TestCase):
    def test_queries_on_flow_and_aggregate_representations(self):
        executor = JenaExecutor(Path(os.environ["ICC2027_JENA_HOME"]))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            executor.probe(root / "probe", 45)
            data = [("09:59:59", "10.0.0.2", "6", "BENIGN"),
                    ("10:00:00", "10.0.0.2", "6", "Attack"),
                    ("10:00:30", "10.0.0.2", "6", "Attack"),
                    ("10:00:59", "10.0.0.3", "17", "Attack")]
            flow, aggregate = [], {}
            for line, (time, dest, protocol, label) in enumerate(data, 2):
                row = {"Source IP": "10.0.0.1", "Destination IP": dest, "Protocol": protocol,
                       "Label": label, "Destination Port": "80", "Source Port": "123", "Flow Duration": "2"}
                timestamp = datetime.fromisoformat("2017-07-05T" + time)
                flow.extend(emit_flow("fixture", line, row, timestamp))
                key = aggregate_key(row, timestamp, 60)
                state = aggregate.setdefault(key, {"count": 0, "first": timestamp, "last": timestamp, "label_counts": Counter()})
                state["count"] += 1
                state["last"] = timestamp
                state["label_counts"][label] += 1
            agg = [triple for key, state in aggregate.items() for triple in emit_aggregate("fixture", key, state)]
            answers = {}
            for representation, triples in (("flow", flow), ("aggregate", agg)):
                source = root / f"{representation}.nt"
                source.write_text("".join(triples), encoding="utf-8")
                for kind, stem in (("top", "01-top-destination-ip"), ("protocol", "02-flows-by-protocol"), ("hour", "03-attack-flows-per-hour")):
                    query = PACKAGE / "queries" / f"{stem}-{representation}.rq"
                    command = [*executor.command[:-1], "arq.sparql", "--data=" + str(source), "--query=" + str(query), "--results=JSON"]
                    result = run_recorded(command, root / f"{representation}-{kind}", 45, lambda raw, k=kind: parse_json(raw, k))
                    self.assertEqual("success", result["status"], result)
                    answers[representation, kind] = result["result"]
                command = [*executor.command[:-1], "arq.sparql", "--data=" + str(source), "--query=" + str(PACKAGE / "queries/flow-count.rq"), "--results=JSON"]
                rec = run_recorded(command, root / f"{representation}-entities", 45, json.loads)
                self.assertEqual("success", rec["status"])
                self.assertEqual("4" if representation == "flow" else "0", rec["result"]["results"]["bindings"][0]["flows"]["value"])
            for kind in ("top", "protocol", "hour"):
                self.assertEqual(answers["flow", kind], answers["aggregate", kind])
            self.assertEqual({"2017-07-05T10": 3}, answers["flow", "hour"])


if __name__ == "__main__":
    unittest.main()

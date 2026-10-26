"""Test double for lifecycle failures. Its counts/times are NOT Jena observations."""
import json
from pathlib import Path

from evaluate_queries import query_paths
from icc2027_pipeline import discover_inputs, load_temporal_rules, sha256, write_run
from jena_executor import write_json
from sparql_results import SCHEMAS, XSD
from tdb_storage import logical_storage


class FakeBackend:
    fail_task = None
    interrupt_task = None

    def __init__(self, root, context):
        self.root, self.context = root, context

    def prepare(self, folder):
        folder.mkdir()
        return {"fake": {"stdout_sha256": "synthetic", "stderr_sha256": "synthetic"}}

    def execute(self, task, attempt, phase, space_check):
        phase("synthetic_test_backend")
        if task["task_id"] == self.fail_task:
            (attempt / "partial.txt").write_text("preserve failed attempt")
            raise RuntimeError("Injected execution failure")
        if task["task_id"] == self.interrupt_task:
            (attempt / "partial.txt").write_text("preserve interrupted attempt")
            raise KeyboardInterrupt
        generated = attempt / "generation"
        manifest = write_run(task["config"], discover_inputs(Path(self.context["csv_root"])), generated,
                             attempt.name, "normalized", load_temporal_rules(self.root / "rules.json"))
        tdb = attempt / "tdb2/Data-0001"
        tdb.mkdir(parents=True)
        (tdb / "SPO.dat").write_bytes(b"test-double")
        answers = {"top": {"http://ip/fixture": manifest["metrics"]["selected_flows"]},
                   "protocol": {"6": manifest["metrics"]["selected_flows"]},
                   "hour": {"2017-07-05T10": sum(v for k, v in manifest["metrics"]["label_counts"].items() if k != "BENIGN")},
                   "stored": {"default_graph": manifest["metrics"]["emitted_triples"]}}
        records = {}
        logs = attempt / "logs"
        logs.mkdir()
        for kind, query in query_paths(manifest, self.root / "queries").items():
            key, counter = SCHEMAS[kind]
            rows = []
            for k, v in answers[kind].items():
                row = {counter: {"type": "literal", "datatype": XSD + "integer", "value": str(v)}}
                if key:
                    row[key] = {"type": "uri" if kind == "top" else "literal", "value": k}
                rows.append(row)
            stdout, stderr = logs / (kind + ".stdout"), logs / (kind + ".stderr")
            stdout.write_text(json.dumps({"head": {"vars": [key, counter] if key else [counter]}, "results": {"bindings": rows}}))
            stderr.write_bytes(b"")
            records[kind] = {"status": "success", "error": None, "result": answers[kind],
                             "stdout_file": str(stdout), "stderr_file": str(stderr), "stdout_sha256": sha256(stdout),
                             "stderr_sha256": sha256(stderr), "query_file": str(query), "query_sha256": sha256(query),
                             "command": ["SYNTHETIC_TEST_DOUBLE_NOT_JENA"], "duration_seconds": 0.0, "returncode": 0}
        return {"manifest_file": str(generated / "manifest.json"), "queries": records,
                "metrics": {"generation_seconds": float(task["repetition"]), "jena_internal_engine_seconds": None},
                "exclusive_process_stages_seconds": {}, "storage": logical_storage(attempt / "tdb2")}

"""Reports retain failures/warm-ups and stratify quality by Sampling probability."""
import json
import shutil
import uuid
from pathlib import Path

from evaluate_queries import export_reports, provenance, snapshot_plan, write_csv
from icc2027_pipeline import sha256
from jena_executor import write_json
from performance_statistics import summarize_performance


def consolidate(root, campaign, state):
    folder = root / "reports" / ("report-" + uuid.uuid4().hex)
    folder.mkdir(parents=True)
    write_json(folder / "report-provenance.json", provenance())
    jobs = {j["task_id"]: j for j in campaign["schedule"]}
    completed, raw, excluded = {}, [], []
    for task_id, entry in state["tasks"].items():
        job = jobs[task_id]
        for attempt in entry["attempts"]:
            path = root / attempt["path"]
            outcome = json.loads((path / "state.json").read_text()) if (path / "state.json").exists() else {"status": "interrupted", "error": "No final state"}
            result = json.loads((path / "result.json").read_text()) if outcome["status"] == "success" else None
            row = {"task_id": task_id, "configuration_id": job["configuration_id"], "seed": job["config"].get("seed"),
                   "kind": job["kind"], "repetition": job["repetition"], "block_id": job["block_id"],
                   "order_index": job["order_index"], "attempt": attempt["path"], "status": outcome["status"],
                   "error": outcome.get("error"), "metrics": result["metrics"] if result else None}
            row["elapsed_seconds"] = outcome.get("elapsed_seconds")
            raw.append(row)
            if result and entry["status"] == "success" and attempt == entry["attempts"][-1]:
                completed[task_id] = (job, path, result)
            if job["kind"] == "warmup" or outcome["status"] != "success":
                excluded.append({"task_id": task_id, "attempt": attempt["path"],
                                 "reason": "predeclared_warmup" if job["kind"] == "warmup" else outcome.get("error", outcome["status"])})
    summaries = []
    for config in campaign["spec"]["configurations"]:
        valid = [r for j, _, r in completed.values() if j["configuration_id"] == config["id"] and j["kind"] == "measured"]
        metrics = sorted({k for r in valid for k in r["metrics"] if k.endswith("_seconds")})
        for name in metrics:
            requested_ci = campaign["spec"].get("confidence_interval", {"method": "none"})
            complete = len(valid) == campaign["spec"]["protocol"]["measured_repetitions"]
            summary = summarize_performance([r["metrics"].get(name) for r in valid], requested_ci if complete else {"method": "none"})
            if not complete and requested_ci["method"] != "none":
                summary["ci_unavailable_reason"] = "incomplete_planned_repetitions"
            summaries.append({"configuration_id": config["id"], "seed": config["config"].get("seed"), "metric": name,
                              "complete_measured_repetitions": complete, "requested_ci": requested_ci, **summary})
    write_json(folder / "performance-statistics.json", {"protocol_sha256": campaign["protocol_sha256"],
                                                       "unit": "fresh generation/ingestion repetition, grouped by fixed policy parameters and seed",
                                                       "summaries": summaries, "exclusions": excluded})
    write_json(folder / "raw-attempts.json", raw)
    columns = ["task_id", "configuration_id", "seed", "kind", "repetition", "block_id", "order_index", "attempt", "status", "error", "elapsed_seconds"]
    metrics = sorted({k for row in raw if row["metrics"] for k in row["metrics"]})
    write_csv(folder / "raw-attempts.csv", columns + metrics,
              [{**{k: r[k] for k in columns}, **{m: r["metrics"].get(m) if r["metrics"] else None for m in metrics}} for r in raw])
    summary_columns = ["configuration_id", "seed", "metric", "n_total", "n_defined", "n_undefined", "mean", "median", "sample_stdev", "min", "max", "q1", "q3", "iqr", "coefficient_of_variation"]
    write_csv(folder / "performance-statistics.csv", summary_columns, [{k: s[k] for k in summary_columns} for s in summaries])
    quality = []
    configs = campaign["spec"]["configurations"]
    baseline = next(c["id"] for c in configs if c["config"]["technique"] == "baseline")
    groups = {"deterministic": [c["id"] for c in configs if c["config"]["technique"] != "sample"]}
    for c in configs:
        if c["config"]["technique"] == "sample":
            group = "sampling-p" + str(c["config"]["sample_probability"]).replace(".", "d")
            groups.setdefault(group, [baseline]).append(c["id"])
    for repetition in range(1, campaign["spec"]["protocol"]["measured_repetitions"] + 1):
        available = {j["configuration_id"]: (j, p, r) for j, p, r in completed.values() if j["kind"] == "measured" and j["repetition"] == repetition}
        for name, identities in groups.items():
            if not set(identities).issubset(available):
                quality.append({"repetition": repetition, "group": name, "status": "incomplete"})
                continue
            out = folder / f"quality-{name}-rep{repetition:03d}"
            out.mkdir()
            plan = {"baseline_id": baseline, "graph_scope": "default", "ontology_scope": "excluded", "policies": []}
            manifests = {}
            for identity in identities:
                job, attempt, result = available[identity]
                manifest_file = Path(result["manifest_file"])
                manifests[identity] = json.loads(manifest_file.read_text(encoding="utf-8"))
                plan["policies"].append({"id": identity, "label": identity, "manifest_path": manifest_file, "tdb_path": attempt / "tdb2"})
            bundle = snapshot_plan(plan, out)
            for policy in bundle["policies"]:
                result = available[policy["id"]][2]
                policy["storage"] = result["storage"]
                for kind, record in result["queries"].items():
                    rec = dict(record)
                    for stream in ("stdout", "stderr"):
                        relative = f"raw/{policy['id']}/{kind}.{stream}"
                        shutil.copyfile(record[stream + "_file"], out / relative)
                        rec[stream + "_file"] = relative
                    query = Path(record["query_file"])
                    destination = out / "queries" / query.name
                    if not destination.exists():
                        shutil.copyfile(query, destination)
                    rec.update(query_file="queries/" + query.name, query_sha256=sha256(destination), format="sparql-json", query_provenance="executed_snapshot")
                    policy["queries"][kind] = rec
            write_json(out / "bundle.json", bundle)
            export_reports(bundle, manifests, out)
            quality.append({"repetition": repetition, "group": name, "status": "success", "path": out.relative_to(root).as_posix()})
    write_json(folder / "quality-index.json", quality)
    return folder.relative_to(root).as_posix()

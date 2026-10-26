"""Reproducible quality evaluation of EXISTING TDB2 databases or saved answers.

This is not an ingestion tool or a latency benchmark. See EVALUATION.md.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from evaluation_metrics import (absolute_error, class_preservation, compare_rankings,
                                describe, horvitz_thompson, normalized_l1, number,
                                reduction_pct, relative_error, tvd)
from icc2027_pipeline import normalized_label, sha256
from jena_executor import JenaExecutor, write_json
from sparql_results import parse_json, parse_legacy_text
from tdb_storage import COMPONENTS, logical_storage

PACKAGE = Path(__file__).resolve().parents[1]
QUERY_NAMES = {"top": "01-top-destination-ip", "protocol": "02-flows-by-protocol",
               "hour": "03-attack-flows-per-hour"}
SUMMARY_COLUMNS = ["policy", "top20_overlap", "top20_same_ranking", "protocol_error_pct",
                   "hourly_attack_error_pct", "protocol_flow_total", "hourly_attack_total"]
EXTENDED_COLUMNS = [*SUMMARY_COLUMNS, "top20_mean_rank_change", "protocol_tvd_pct", "hour_tvd_pct",
                    "sampling_estimated_protocol_error_pct", "sampling_estimated_hour_error_pct"]


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def child(root: Path, name: str) -> Path:
    if not isinstance(name, str):
        raise ValueError("Relative input path must be a string")
    root = root.resolve()
    path = (root / name).resolve()
    if not path.is_relative_to(root) or path == root:
        raise ValueError(f"Input path escapes its root: {name}")
    return path


def attack_counts(manifest: dict) -> dict[str, int]:
    return {k: v for k, v in manifest["metrics"]["label_counts"].items() if normalized_label(k) != "benign"}


def validate_manifest(manifest: dict) -> None:
    metrics, config = manifest["metrics"], manifest["config"]
    if not isinstance(metrics, dict) or not isinstance(config, dict) or not isinstance(metrics.get("label_counts"), dict):
        raise ValueError("Invalid manifest metrics/configuration schema")
    for value in (metrics["selected_flows"], metrics["emitted_triples"], *metrics["label_counts"].values()):
        number(value)
        if int(value) != value:
            raise ValueError("Manifest counts must be integers")
    if sum(metrics["label_counts"].values()) != metrics["selected_flows"]:
        raise ValueError("Manifest selected flows disagree with label counts")
    technique = config["technique"]
    if technique not in {"baseline", "aggregate", "temporal", "labels", "topk", "sample"}:
        raise ValueError("Unknown policy technique")
    if technique == "aggregate":
        seconds = config["bucket_seconds"]
        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds <= 0 or 3600 % seconds:
            raise ValueError("Hourly aggregation comparison requires bucket_seconds to divide 3600")
        if metrics["aggregate_input_flows"] != metrics["selected_flows"] or metrics["aggregate_label_counts"] != metrics["label_counts"]:
            raise ValueError("Inconsistent aggregate representation counts")
        if metrics["aggregate_label_count_sum"] != metrics["selected_flows"]:
            raise ValueError("Inconsistent aggregate label sum")
        number(metrics["aggregated_groups"])
    if technique == "sample":
        horvitz_thompson({}, config["sample_probability"], technique=technique)


def population_signature(manifest: dict) -> tuple:
    inputs = tuple((p["path"].replace("\\", "/").rsplit("/", 1)[-1], p["sha256"], p["bytes"])
                   for p in manifest["input_csvs"])
    temporal = manifest["temporal_interpretation"]
    if not inputs or any(not re.fullmatch(r"[0-9a-fA-F]{64}", p[1]) for p in inputs):
        raise ValueError("Manifest must identify input CSV hashes")
    if temporal.get("mode") not in {"raw", "normalized"} or not temporal.get("rules_sha256"):
        raise ValueError("Manifest must identify the temporal interpretation")
    return inputs, tuple(temporal.get(k) for k in ("mode", "ambiguous_policy", "rules_version", "rules_sha256"))


def load_plan(registry: Path, runs_root: Path, tdb_root: Path | None, *, require_stores: bool) -> dict:
    plan = read_json(registry)
    if plan.get("schema_version") != 1 or plan.get("graph_scope") != "default":
        raise ValueError("Registry must declare schema_version=1 and graph_scope=default")
    if plan.get("ontology_scope") not in {"excluded", "included", "not_verified"}:
        raise ValueError("Registry must explicitly declare ontology_scope")
    policies = plan["policies"]
    if not policies or len(policies) != plan.get("expected_policy_count", len(policies)):
        raise ValueError("Unexpected policy count")
    ids, stores, signature = set(), set(), None
    for policy in policies:
        identity = policy["id"]
        if not isinstance(identity, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", identity) or identity in ids:
            raise ValueError("Policy ids must be unique safe names")
        ids.add(identity)
        if not isinstance(policy["label"], str) or not policy["label"]:
            raise ValueError("Policy label is required")
        path = child(runs_root, policy["manifest"])
        manifest = read_json(path)
        validate_manifest(manifest)
        if manifest["run_id"] != path.parent.name:
            raise ValueError("Manifest run_id does not match the registered run directory")
        current = population_signature(manifest)
        if signature is not None and current != signature:
            raise ValueError("Policies have different input hashes or temporal interpretations")
        signature = current
        tdb = child(tdb_root, policy["tdb"]) if tdb_root is not None else None
        if policy["tdb"] in stores:
            raise ValueError("A TDB2 store cannot silently represent two policies")
        stores.add(policy["tdb"])
        if require_stores and (tdb is None or not any((d / "SPO.dat").is_file() for d in tdb.glob("Data-*"))):
            raise ValueError(f"Existing TDB2 store not found: {tdb}")
        policy.update(manifest_path=path, manifest_data=manifest, tdb_path=tdb)
    baselines = [p for p in policies if p["manifest_data"]["config"]["technique"] == "baseline"]
    if len(baselines) != 1 or baselines[0]["id"] != plan["baseline_id"]:
        raise ValueError("Registry must identify exactly one Baseline")
    return plan


def query_paths(manifest: dict, queries_root: Path, stored: bool = True) -> dict[str, Path]:
    suffix = "aggregate" if manifest["config"]["technique"] == "aggregate" else "flow"
    queries = {kind: queries_root / f"{stem}-{suffix}.rq" for kind, stem in QUERY_NAMES.items()}
    if stored:
        queries["stored"] = queries_root / "stored-triples-default.rq"
    if any(not p.is_file() for p in queries.values()):
        raise ValueError("Versioned query file not found")
    return queries


def provenance() -> dict:
    def git(*args):
        try:
            return subprocess.check_output(["git", "-C", str(PACKAGE), *args], stderr=subprocess.DEVNULL,
                                           text=True, timeout=10).strip()
        except (OSError, subprocess.SubprocessError):
            return None
    status = git("status", "--porcelain=v1")
    return {"created_utc": datetime.now(timezone.utc).isoformat(), "python": sys.version,
            "platform": platform.platform(), "argv": sys.argv,
            "git_commit": git("rev-parse", "HEAD"), "git_dirty": bool(status) if status is not None else None,
            "module_sha256": {p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")}}


def new_output(output: Path, protected: list[Path]) -> None:
    output = output.resolve()
    if any(output.is_relative_to(p.resolve()) or p.resolve().is_relative_to(output) for p in protected):
        raise ValueError("Output must be separate from input/store directories")
    output.mkdir(parents=True, exist_ok=False)


def snapshot_plan(plan: dict, output: Path) -> dict:
    (output / "manifests").mkdir()
    (output / "raw").mkdir()
    (output / "queries").mkdir()
    bundle = {"schema_version": 1, "baseline_id": plan["baseline_id"], "graph_scope": plan["graph_scope"],
              "ontology_scope": plan["ontology_scope"], "provenance": provenance(), "policies": []}
    for policy in plan["policies"]:
        relative = f"manifests/{policy['id']}.json"
        shutil.copyfile(policy["manifest_path"], output / relative)
        (output / "raw" / policy["id"]).mkdir()
        bundle["policies"].append({"id": policy["id"], "label": policy["label"],
                                   "manifest_file": relative, "manifest_sha256": sha256(output / relative),
                                   "tdb_path": str(policy["tdb_path"]) if policy["tdb_path"] else None,
                                   "queries": {}, "storage": None})
    return bundle


def check_answers(policy: dict, manifest: dict) -> None:
    """Detect wrong graph/representation, including misleading successful empties."""
    selected = manifest["metrics"]["selected_flows"]
    for kind, record in policy["queries"].items():
        if record["status"] not in {"success", "imported"}:
            continue
        answer = record["result"]
        error = None
        if kind == "top" and ((selected > 0 and not answer) or any(v > selected for v in answer.values())):
            error = "Top destinations contradict selected flow population"
        if kind == "protocol" and sum(answer.values()) > selected:
            error = "Protocol counts exceed selected flows"
        if kind == "hour" and sum(answer.values()) != sum(attack_counts(manifest).values()):
            error = "Hourly attacks disagree with selected manifest label counts; check graph/representation"
        if error:
            record.update(status="population_mismatch", error=error, result=None)


def capture_storage(policy: dict, tdb: Path | None) -> None:
    if tdb is not None and tdb.is_dir():
        policy["storage"] = logical_storage(tdb)


def execute(plan: dict, output: Path, executor: JenaExecutor, queries_root: Path, timeout: float, stored: bool) -> dict:
    # Query files are validated before any process or output creation by caller.
    bundle = snapshot_plan(plan, output)
    bundle["cli_probe"] = executor.probe(output / "cli", min(timeout, 60))
    for original, policy in zip(plan["policies"], bundle["policies"]):
        for kind, query in query_paths(original["manifest_data"], queries_root, stored).items():
            snapshot = output / "queries" / query.name
            if not snapshot.exists():
                shutil.copyfile(query, snapshot)
            prefix = output / "raw" / policy["id"] / kind
            record = executor.query(original["tdb_path"], snapshot, prefix, timeout,
                                    lambda data, k=kind: parse_json(data, k))
            record.update(format="sparql-json", stdout_file=prefix.with_suffix(".stdout").relative_to(output).as_posix(),
                          stderr_file=prefix.with_suffix(".stderr").relative_to(output).as_posix(),
                          query_file=snapshot.relative_to(output).as_posix(), query_sha256=sha256(snapshot),
                          query_provenance="executed_snapshot")
            policy["queries"][kind] = record
            print(f"{policy['id']}/{kind}: {record['status']}", flush=True)
        check_answers(policy, original["manifest_data"])
        capture_storage(policy, original["tdb_path"])
    write_json(output / "bundle.json", bundle)
    return bundle


def import_legacy(plan: dict, legacy: Path, output: Path, queries_root: Path) -> dict:
    # Parse everything first; an incomplete table is never a zero-count answer.
    parsed = {}
    for p in plan["policies"]:
        for kind in QUERY_NAMES:
            path = legacy / f"{p['id']}-{kind}.txt"
            parsed[p["id"], kind] = parse_legacy_text(path.read_bytes(), kind)
    bundle = snapshot_plan(plan, output)
    bundle["historical_limit"] = "Original execution commands, query hashes, durations and exit codes were not recorded; import validates content only."
    for original, policy in zip(plan["policies"], bundle["policies"]):
        for kind, query in query_paths(original["manifest_data"], queries_root, stored=False).items():
            source = legacy / f"{policy['id']}-{kind}.txt"
            raw = output / "raw" / policy["id"] / f"{kind}.stdout"
            shutil.copyfile(source, raw)
            snapshot = output / "queries" / query.name
            if not snapshot.exists():
                shutil.copyfile(query, snapshot)
            policy["queries"][kind] = {
                "status": "imported", "format": "jena-text-legacy", "error": None,
                "result": parsed[policy["id"], kind], "stdout_file": raw.relative_to(output).as_posix(),
                "stdout_sha256": sha256(raw), "source_file": str(source.resolve()),
                "command": None, "duration_seconds": None, "returncode": None,
                "query_file": snapshot.relative_to(output).as_posix(), "query_sha256": sha256(snapshot),
                "query_provenance": "current_reference_not_proof_of_historical_execution"}
        check_answers(policy, original["manifest_data"])
        capture_storage(policy, original["tdb_path"])
    write_json(output / "bundle.json", bundle)
    return bundle


def load_bundle(folder: Path) -> tuple[dict, dict[str, dict]]:
    bundle = read_json(folder / "bundle.json")
    if bundle.get("schema_version") != 1:
        raise ValueError("Unsupported saved evaluation schema")
    policies = bundle["policies"]
    if not isinstance(policies, list) or not policies or len({p["id"] for p in policies}) != len(policies):
        raise ValueError("Saved evaluation requires nonempty, unique policy ids")
    if sum(p["id"] == bundle["baseline_id"] for p in policies) != 1:
        raise ValueError("Saved evaluation must identify one Baseline")
    manifests = {}
    signature = None
    for policy in bundle["policies"]:
        manifest = child(folder, policy["manifest_file"])
        if sha256(manifest) != policy["manifest_sha256"]:
            raise ValueError("Saved manifest checksum mismatch")
        manifests[policy["id"]] = read_json(manifest)
        validate_manifest(manifests[policy["id"]])
        current = population_signature(manifests[policy["id"]])
        if signature is not None and current != signature:
            raise ValueError("Saved policies refer to different populations")
        signature = current
        if not set(QUERY_NAMES).issubset(policy["queries"]) or not set(policy["queries"]).issubset({*QUERY_NAMES, "stored"}):
            raise ValueError("Saved evaluation is missing required queries or has unknown queries")
        for kind, record in policy["queries"].items():
            if record["status"] not in {"success", "imported", "timeout", "execution_error", "invalid_result", "population_mismatch"}:
                raise ValueError("Unknown saved result status")
            if record["format"] not in {"sparql-json", "jena-text-legacy"}:
                raise ValueError("Unsupported saved result format")
            raw, query = child(folder, record["stdout_file"]), child(folder, record["query_file"])
            if sha256(raw) != record["stdout_sha256"] or sha256(query) != record["query_sha256"]:
                raise ValueError("Saved Jena result or query checksum mismatch")
            if record.get("stderr_file") and sha256(child(folder, record["stderr_file"])) != record["stderr_sha256"]:
                raise ValueError("Saved Jena stderr checksum mismatch")
            if record["status"] in {"success", "imported"}:
                parser = parse_json if record["format"] == "sparql-json" else parse_legacy_text
                if parser(raw.read_bytes(), kind) != record["result"]:
                    raise ValueError("Saved normalized result differs from raw output")
            elif record["result"] is not None:
                raise ValueError("A failed query cannot contain a successful normalized result")
        check_answers(policy, manifests[policy["id"]])
    return bundle, manifests


def pct(value: float | None) -> float | None:
    return value * 100 if value is not None else None


def answer(policy: dict, kind: str) -> dict | None:
    rec = policy["queries"].get(kind)
    return rec["result"] if rec and rec["status"] in {"success", "imported"} else None


def distribution_comparison(baseline: dict | None, candidate: dict | None, probability: float | None) -> dict:
    if baseline is None or candidate is None:
        return {"status": "unavailable", "baseline_total": None, "observed_total": None,
                "count_l1_error_pct": None, "tvd_pct": None, "estimated_count_l1_error_pct": None,
                "exact_results_equal": None}
    keys = sorted(baseline.keys() | candidate.keys())
    result = {"status": "available", "baseline_total": sum(baseline.values()),
              "observed_total": sum(candidate.values()), "exact_results_equal": baseline == candidate,
              "count_l1_error_pct": pct(normalized_l1(baseline, candidate)), "tvd_pct": pct(tvd(baseline, candidate)),
              "estimated_count_l1_error_pct": None,
              "per_category": {k: {"baseline": baseline.get(k, 0), "observed": candidate.get(k, 0),
                                    "absolute_error": absolute_error(baseline.get(k, 0), candidate.get(k, 0)),
                                    "relative_error": relative_error(baseline.get(k, 0), candidate.get(k, 0))} for k in keys}}
    if probability is not None:
        ht = horvitz_thompson({k: candidate.get(k, 0) for k in keys}, probability, technique="sample")
        result["horvitz_thompson"] = ht
        result["estimated_total"] = sum(ht["estimated"].values())
        result["estimated_count_l1_error_pct"] = pct(normalized_l1(baseline, ht["estimated"]))
    return result


def calculate(bundle: dict, manifests: dict[str, dict]) -> dict:
    baseline = next(p for p in bundle["policies"] if p["id"] == bundle["baseline_id"])
    base_manifest = manifests[baseline["id"]]
    base_metrics = base_manifest["metrics"]
    reports = []
    for policy in bundle["policies"]:
        manifest = manifests[policy["id"]]
        metrics, config = manifest["metrics"], manifest["config"]
        aggregate = config["technique"] == "aggregate"
        probability = config.get("sample_probability") if config["technique"] == "sample" else None
        btop, ctop = answer(baseline, "top"), answer(policy, "top")
        ranking = compare_rankings(btop, ctop) if btop is not None and ctop is not None else None
        bstored, cstored = answer(baseline, "stored"), answer(policy, "stored")
        classes = class_preservation(attack_counts(base_manifest), attack_counts(manifest))
        observed_entities = 0 if aggregate else classes["observed_attack_count"]
        reports.append({
            "id": policy["id"], "policy": policy["label"], "run_id": manifest["run_id"], "technique": config["technique"],
            "status": "success" if all(r["status"] in {"success", "imported"} for r in policy["queries"].values()) else "failed",
            "query_status": {k: {"status": r["status"], "error": r["error"]} for k, r in policy["queries"].items()},
            "baseline_flows": base_metrics["selected_flows"], "observed_represented_flows": metrics["selected_flows"],
            "flow_reduction_pct": reduction_pct(base_metrics["selected_flows"], metrics["selected_flows"]),
            "stored_flow_entities": metrics["aggregated_groups"] if aggregate else metrics["selected_flows"],
            "entity_representation": "AggregatedFlow groups" if aggregate else "NetworkFlow instances",
            "baseline_emitted_rdf_statements": base_metrics["emitted_triples"], "observed_emitted_rdf_statements": metrics["emitted_triples"],
            "emitted_rdf_reduction_pct": reduction_pct(base_metrics["emitted_triples"], metrics["emitted_triples"]),
            "baseline_stored_distinct_triples": bstored["default_graph"] if bstored is not None else None,
            "observed_stored_distinct_triples": cstored["default_graph"] if cstored is not None else None,
            "stored_distinct_triples_reduction_pct": reduction_pct(bstored["default_graph"], cstored["default_graph"]) if bstored is not None and cstored is not None else None,
            "attack_preservation": classes, "observed_attack_instances": observed_entities,
            "attack_instance_retention": (observed_entities / classes["baseline_attack_count"] if classes["baseline_attack_count"] else None),
            "attack_instance_scope": "NetworkFlow entities; aggregation represents counts without individual identities",
            "ranking": ranking, "top_exact_results_equal": btop == ctop if btop is not None and ctop is not None else None,
            "tie_expanded_overlap": None, "tie_expanded_overlap_reason": "LIMIT 20 cannot reveal all ties beyond the cutoff",
            "protocol": distribution_comparison(answer(baseline, "protocol"), answer(policy, "protocol"), probability),
            "hour": distribution_comparison(answer(baseline, "hour"), answer(policy, "hour"), probability),
        })
    sample = [r for r in reports if r["technique"] == "sample"]
    def statistics_for(rows):
        result = {f"{kind}_{metric}": describe([r[kind][metric] for r in rows])
                  for kind in ("protocol", "hour") for metric in ("tvd_pct", "estimated_count_l1_error_pct")}
        result["retained_attack_classes"] = describe([r["attack_preservation"]["retained_class_count"] for r in rows])
        return result
    probabilities = sorted({manifests[r["id"]]["config"]["sample_probability"] for r in sample})
    stratified = {str(p): statistics_for([r for r in sample if manifests[r["id"]]["config"]["sample_probability"] == p])
                  for p in probabilities}
    summary = statistics_for(sample) if len(probabilities) <= 1 else None
    return {"schema_version": 2, "baseline_id": baseline["id"], "graph_scope": bundle["graph_scope"],
            "ontology_scope": bundle["ontology_scope"], "policies": reports, "sampling_statistics": summary,
            "sampling_statistics_by_probability": stratified,
            "sampling_statistics_unavailable_reason": "mixed_probabilities_use_stratified_statistics" if summary is None else None}


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("x", encoding="utf-8", newline="") as sink:
        writer = csv.DictWriter(sink, fieldnames=columns, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def rounded(value, digits):
    return round(value, digits) if value is not None else None


def compare_saved(reference: Path, candidate: Path) -> dict:
    """Exact per-category comparison of the three required saved SELECT answers."""
    a, _ = load_bundle(reference)
    b, _ = load_bundle(candidate)
    left, right = ({p["id"]: p for p in bundle["policies"]} for bundle in (a, b))
    if set(left) != set(right) or a["baseline_id"] != b["baseline_id"]:
        raise ValueError("Evaluations must refer to the same policy registry")
    rows = []
    for identity, p in left.items():
        q = right[identity]
        if p["manifest_sha256"] != q["manifest_sha256"]:
            raise ValueError("Cannot compare different source manifests as a regression")
        for kind in QUERY_NAMES:
            expected, actual = answer(p, kind), answer(q, kind)
            equal = expected == actual if expected is not None and actual is not None else None
            differences = ({k: {"reference": expected.get(k), "candidate": actual.get(k)}
                            for k in sorted(expected.keys() | actual.keys())
                            if k not in expected or k not in actual or expected[k] != actual[k]}
                           if equal is not None else None)
            rows.append({"policy_id": identity, "query": kind, "equal": equal, "differences": differences,
                         "reference_status": p["queries"][kind]["status"], "candidate_status": q["queries"][kind]["status"]})
    return {"schema_version": 1, "all_equal": all(r["equal"] is True for r in rows),
            "reference_bundle_sha256": sha256(reference / "bundle.json"),
            "candidate_bundle_sha256": sha256(candidate / "bundle.json"), "queries": rows}


def export_reports(bundle: dict, manifests: dict[str, dict], output: Path) -> dict:
    report = calculate(bundle, manifests)
    write_json(output / "metrics.json", report)
    summary_rows, extended_rows, metric_rows, storage_rows = [], [], [], []
    class_rows, category_rows = [], []
    for r, policy in zip(report["policies"], bundle["policies"]):
        ranking = r["ranking"]
        row = {"policy": r["policy"], "top20_overlap": ranking["overlap_count"] if ranking else None,
               "top20_same_ranking": ranking["same_ranking"] if ranking else None,
               "protocol_error_pct": rounded(r["protocol"]["count_l1_error_pct"], 6),
               "hourly_attack_error_pct": rounded(r["hour"]["count_l1_error_pct"], 6),
               "protocol_flow_total": r["protocol"]["observed_total"], "hourly_attack_total": r["hour"]["observed_total"]}
        summary_rows.append(row)
        extended_rows.append({**row, "top20_mean_rank_change": rounded(ranking["common_mean_position_shift"], 4) if ranking else None,
                              "protocol_tvd_pct": rounded(r["protocol"]["tvd_pct"], 4), "hour_tvd_pct": rounded(r["hour"]["tvd_pct"], 4),
                              "sampling_estimated_protocol_error_pct": rounded(r["protocol"]["estimated_count_l1_error_pct"], 4),
                              "sampling_estimated_hour_error_pct": rounded(r["hour"]["estimated_count_l1_error_pct"], 4)})
        metric_rows.append({k: r[k] for k in ("policy", "status", "baseline_flows", "observed_represented_flows", "stored_flow_entities", "entity_representation", "flow_reduction_pct", "baseline_emitted_rdf_statements", "observed_emitted_rdf_statements", "emitted_rdf_reduction_pct", "baseline_stored_distinct_triples", "observed_stored_distinct_triples", "stored_distinct_triples_reduction_pct", "observed_attack_instances", "attack_instance_retention")})
        manifest = manifests[r["id"]]
        classes = r["attack_preservation"]
        probability = manifest["config"].get("sample_probability") if r["technique"] == "sample" else None
        ht = horvitz_thompson({k: v["observed"] for k, v in classes["per_class"].items()}, probability, technique="sample") if probability is not None else None
        for name, values in classes["per_class"].items():
            class_rows.append({"policy": r["policy"], "class": name, "baseline_attack_count": values["baseline"],
                               "observed_represented_attack_count": values["observed"], "retention": values["retention"],
                               "inclusion_probability": probability, "estimated_attack_count": ht["estimated"][name] if ht else None,
                               "estimated_standard_error": ht["standard_error"][name] if ht else None})
        for kind in ("protocol", "hour"):
            comparison = r[kind]
            ht = comparison.get("horvitz_thompson")
            for name, values in comparison.get("per_category", {}).items():
                category_rows.append({"policy": r["policy"], "query": kind, "category": name,
                                      **values, "inclusion_probability": probability,
                                      "estimated_count": ht["estimated"][name] if ht else None,
                                      "estimated_standard_error": ht["standard_error"][name] if ht else None})
        storage = policy["storage"]
        if storage:
            storage_rows.append({"policy": policy["label"], "TotalBytes": storage["total_bytes"],
                                 "TotalMiB": storage["total_mib"], **storage["components_bytes"], "OtherBytes": storage["other_bytes"]})
    write_csv(output / "query-preservation-summary.csv", SUMMARY_COLUMNS, summary_rows)
    write_csv(output / "query-preservation-extended-r04.csv", EXTENDED_COLUMNS, extended_rows)
    write_csv(output / "population-metrics.csv", list(metric_rows[0]), metric_rows)
    write_csv(output / "class-preservation.csv", ["policy", "class", "baseline_attack_count", "observed_represented_attack_count", "retention", "inclusion_probability", "estimated_attack_count", "estimated_standard_error"], class_rows)
    write_csv(output / "category-errors.csv", ["policy", "query", "category", "baseline", "observed", "absolute_error", "relative_error", "inclusion_probability", "estimated_count", "estimated_standard_error"], category_rows)
    write_csv(output / "sampling-statistics.csv", ["metric", "n_total", "n_defined", "n_undefined", "mean", "median", "sample_stdev", "min", "max"],
              [{"metric": name, **values} for name, values in (report["sampling_statistics"] or {}).items()])
    write_csv(output / "sampling-statistics-by-probability.csv", ["probability", "metric", "n_total", "n_defined", "n_undefined", "mean", "median", "sample_stdev", "min", "max"],
              [{"probability": p, "metric": name, **values} for p, group in report["sampling_statistics_by_probability"].items() for name, values in group.items()])
    write_csv(output / "storage-logical.csv", ["policy", "TotalBytes", "TotalMiB", *COMPONENTS, "OtherBytes"], storage_rows)
    write_json(output / "storage-logical.json", {p["id"]: p["storage"] for p in bundle["policies"]})
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    for name in ("run", "import-legacy"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--registry", type=Path, required=True)
        cmd.add_argument("--runs-root", type=Path, required=True)
        cmd.add_argument("--tdb-root", type=Path, required=name == "run")
        cmd.add_argument("--queries-root", type=Path, default=PACKAGE / "queries")
        cmd.add_argument("--output", type=Path, required=True)
        if name == "run":
            cmd.add_argument("--jena-home", type=Path, required=True)
            cmd.add_argument("--java", default="java")
            cmd.add_argument("--jvm-arg", action="append", default=[])
            cmd.add_argument("--timeout", type=float, default=300)
            cmd.add_argument("--skip-stored-count", action="store_true")
        else:
            cmd.add_argument("--legacy-dir", type=Path, required=True)
    cmd = sub.add_parser("report")
    cmd.add_argument("--input", type=Path, required=True)
    cmd.add_argument("--output", type=Path, required=True)
    cmd = sub.add_parser("compare")
    cmd.add_argument("--reference", type=Path, required=True)
    cmd.add_argument("--candidate", type=Path, required=True)
    cmd.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.operation == "compare":
            comparison = compare_saved(args.reference, args.candidate)
            new_output(args.output, [args.reference, args.candidate])
            write_json(args.output / "comparison.json", comparison)
            write_csv(args.output / "comparison.csv", ["policy_id", "query", "equal", "reference_status", "candidate_status"],
                      [{k: r[k] for k in ("policy_id", "query", "equal", "reference_status", "candidate_status")} for r in comparison["queries"]])
            return 0 if comparison["all_equal"] else 1
        if args.operation == "report":
            bundle, manifests = load_bundle(args.input)
            new_output(args.output, [args.input])
            write_json(args.output / "replay-source.json", {"bundle": str(args.input.resolve()), "sha256": sha256(args.input / "bundle.json"), "provenance": provenance()})
        else:
            plan = load_plan(args.registry, args.runs_root, args.tdb_root, require_stores=args.operation == "run")
            stored = args.operation == "run" and not args.skip_stored_count
            for policy in plan["policies"]:
                query_paths(policy["manifest_data"], args.queries_root, stored)
            executor = None
            if args.operation == "run":
                if not math.isfinite(args.timeout) or args.timeout <= 0:
                    raise ValueError("Timeout must be finite and positive")
                executor = JenaExecutor(args.jena_home, args.java, tuple(args.jvm_arg))
            protected = [args.runs_root, args.queries_root, args.registry]
            if args.tdb_root:
                protected.append(args.tdb_root)
            if args.operation == "import-legacy":
                protected.append(args.legacy_dir)
            new_output(args.output, protected)
            if executor:
                bundle = execute(plan, args.output, executor, args.queries_root, args.timeout, stored)
            else:
                bundle = import_legacy(plan, args.legacy_dir, args.output, args.queries_root)
            manifests = {p["id"]: p["manifest_data"] for p in plan["policies"]}
        report = export_reports(bundle, manifests, args.output)
        return 0 if all(r["status"] == "success" for r in report["policies"]) else 1
    except (ValueError, KeyError, TypeError, OSError, StopIteration) as exc:
        print(f"Evaluation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Sequential real-Jena stages for one fresh repetition. No existing store reuse."""
import json
import random
import sys
from pathlib import Path
from time import perf_counter

from evaluate_queries import check_answers, query_paths, validate_manifest
from icc2027_pipeline import sha256
from jena_executor import JenaExecutor, run_recorded, write_json
from sparql_results import parse_json
from tdb_storage import logical_storage


def verify_generated_identity(manifest, context, task):
    """Bracket generation with frozen input identities; never load drifted data."""
    if manifest["config"] != task["config"]:
        raise ValueError("Generated policy differs from the frozen task")
    temporal = manifest["temporal_interpretation"]
    expected_ambiguity = context["spec"]["ambiguous_policy"] if context["spec"]["temporal_mode"] == "normalized" else "not-applicable"
    if (temporal["mode"] != context["spec"]["temporal_mode"] or temporal["ambiguous_policy"] != expected_ambiguity or
            temporal["rules_sha256"] != context["rules_sha256"]):
        raise ValueError("Generated temporal interpretation differs from frozen rules")
    if len(manifest["input_csvs"]) != len(context["inputs"]):
        raise ValueError("Generated input inventory differs from frozen inputs")
    for observed, expected in zip(manifest["input_csvs"], context["inputs"]):
        path = Path(expected["path"])
        if (Path(observed["path"]).resolve() != path.resolve() or observed["sha256"] != expected["sha256"] or
                observed["bytes"] != expected["bytes"] or path.stat().st_size != expected["bytes"] or sha256(path) != expected["sha256"]):
            raise ValueError("Input CSV identity changed before/during generation; refusing TDB2 load")


class JenaBenchmarkBackend:
    def __init__(self, campaign, context):
        self.root, self.context = campaign, context
        self.spec = context["spec"]
        self.protocol = self.spec["protocol"]
        self.executor = JenaExecutor(Path(context["jena_home"]), context["java"], tuple(context["jvm_args"]))
        self.timeout = self.protocol["subprocess_timeout_seconds"]

    def prepare(self, folder):
        evidence = self.executor.probe(folder, min(self.timeout, 60))
        for name, main in (("loader", "tdb2.tdbloader"), ("riot", "riotcmd.riot")):
            record = run_recorded([*self.executor.command[:-1], main, "--help"], folder / name, min(self.timeout, 60))
            self.require(record)
            evidence[name] = record
        help_text = (folder / "loader.stdout").read_text(errors="replace") + (folder / "loader.stderr").read_text(errors="replace")
        if self.protocol["loader"] not in help_text:
            raise ValueError("Installed loader did not advertise configured loader strategy")
        return evidence

    @staticmethod
    def require(record):
        if record["status"] != "success":
            raise RuntimeError(f"{record['status']}: {record['error']}")

    def execute(self, task, attempt, phase, space_check):
        logs = attempt / "logs"
        logs.mkdir()
        phases = {}
        def stage(name, operation):
            phase(name)
            started = perf_counter()
            try:
                return operation()
            finally:
                phases[name] = perf_counter() - started
        generated = attempt / "generation"
        config_path = attempt / "configuration.json"
        rules = self.root / "rules.json"
        def generate():
            record = run_recorded([
                self.context["python_executable"], "-B", str(self.root / "sources/icc2027_pipeline.py"),
                "--config", str(config_path), "--csv-root", self.context["csv_root"],
                "--output-dir", str(generated), "--run-id", attempt.name,
                "--temporal-mode", self.spec["temporal_mode"], "--temporal-rules", str(rules),
                "--ambiguous-policy", self.spec["ambiguous_policy"]], logs / "generation", self.timeout)
            self.require(record)
            return record
        generation_record = stage("generation_process_seconds", generate)
        manifest_path = generated / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        validate_manifest(manifest)
        if manifest["run_id"] != attempt.name:
            raise ValueError("Generator run ID does not identify this exclusive attempt")
        stage("input_identity_validation_seconds", lambda: verify_generated_identity(manifest, self.context, task))
        artifacts = [generated / a["path"] for a in manifest["artifacts"]]
        def validate_rdf():
            for a, path in zip(manifest["artifacts"], artifacts):
                if path.parent.resolve() != generated.resolve() or path.stat().st_size != a["bytes"] or sha256(path) != a["sha256"]:
                    raise ValueError("Generated artifact checksum/length mismatch")
            record = run_recorded([*self.executor.command[:-1], "riotcmd.riot", "--validate", "--syntax=NTRIPLES",
                                   *map(str, artifacts)], logs / "rdf-validation", self.timeout)
            self.require(record)
        stage("rdf_validation_seconds", validate_rdf)
        tdb = attempt / "tdb2"
        if tdb.exists():
            raise ValueError("Refusing to load a previously existing store")
        space_check()
        tdb.mkdir()
        def load():
            record = run_recorded([*self.executor.command[:-1], "tdb2.tdbloader", "--loader=" + self.protocol["loader"],
                                   "--loc=" + str(tdb), *map(str, artifacts)], logs / "tdb2-load", self.timeout)
            self.require(record)
        stage("tdb2_load_seconds", load)
        query_files = query_paths(manifest, self.root / "queries")
        representation = "aggregate" if task["config"]["technique"] == "aggregate" else "flow"
        def query(kind, path, prefix):
            record = self.executor.query(tdb, path, prefix, self.timeout, lambda raw: parse_json(raw, kind))
            record.update(stdout_file=str(prefix.with_suffix(".stdout")), stderr_file=str(prefix.with_suffix(".stderr")),
                          query_file=str(path), query_sha256=sha256(path))
            self.require(record)
            return record
        stored = None
        def validate_load():
            nonlocal stored
            stored = query("stored", query_files["stored"], logs / "stored")
            entities = query("entities", self.root / "queries" / f"validate-entities-{representation}.rq", logs / "entities")
            expected = manifest["metrics"].get("aggregated_groups", manifest["metrics"]["selected_flows"])
            if entities["result"]["default_graph"] != expected or (expected > 0 and stored["result"]["default_graph"] == 0):
                raise ValueError("Loaded entities contradict the generator manifest")
        stage("tdb2_validation_seconds", validate_load)
        answers, observations, first = {}, [], {"stored": stored}
        order_rng = random.Random(self.protocol["order_seed"] + task["order_index"])
        def trials(kind, repetitions):
            for repetition in range(1, repetitions + 1):
                workload = ["top", "protocol", "hour"]
                if self.protocol["order"] == "randomized":
                    order_rng.shuffle(workload)
                for position, query_kind in enumerate(workload):
                    prefix = logs / f"{query_kind}-{kind}-{repetition:03d}"
                    record = query(query_kind, query_files[query_kind], prefix)
                    observation = {"query": query_kind, "kind": kind, "trial": repetition, "position": position,
                                   "duration_seconds": record["duration_seconds"], "status": record["status"],
                                   "command": record["command"], "started_utc": record["started_utc"],
                                   "stdout_sha256": record["stdout_sha256"]}
                    observations.append(observation)
                    with (attempt / "query-observations.jsonl").open("a", encoding="utf-8") as log:
                        log.write(json.dumps(observation) + "\n")
                    if query_kind in answers and answers[query_kind] != record["result"]:
                        raise ValueError("Query answers changed between warm-up/measured trials")
                    answers[query_kind] = record["result"]
                    if kind == "measured":
                        first.setdefault(query_kind, record)
        stage("sparql_warmup_seconds", lambda: trials("warmup", self.protocol["query_warmups"]))
        stage("sparql_query_seconds", lambda: trials("measured", self.protocol["query_measurements"]))
        policy = {"queries": first}
        check_answers(policy, manifest)
        for record in first.values():
            self.require(record)
        storage = stage("storage_inventory_seconds", lambda: logical_storage(tdb))
        metrics = {k: v for k, v in manifest["metrics"].items() if k.endswith("_seconds")}
        metrics.update({"generator_" + k: v for k, v in manifest["timing"]["exclusive_seconds"].items()})
        metrics["generator_total_seconds"] = metrics.pop("total_seconds")
        metrics.update(phases)
        metrics["generation_subprocess_wall_seconds"] = generation_record["duration_seconds"]
        for query_kind in ("top", "protocol", "hour"):
            values = [r["duration_seconds"] for r in observations if r["kind"] == "measured" and r["query"] == query_kind]
            metrics[f"query_mean_{query_kind}_seconds"] = sum(values) / len(values)
        write_json(attempt / "query-observations.json", observations)
        return {"manifest_file": str(manifest_path), "queries": first, "metrics": metrics,
                "exclusive_process_stages_seconds": phases, "storage": storage,
                "query_measurement_unit": "within-store trials; not independent ingestion repetitions",
                "query_execution_model": "one new JVM per trial; no claim of warmed JVM/JIT",
                "jena_internal_engine_seconds": None}

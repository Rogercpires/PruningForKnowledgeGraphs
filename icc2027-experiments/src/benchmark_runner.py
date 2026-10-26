"""Plan, execute, resume and summarize isolated benchmark repetitions.

No campaign is run merely by importing the module or creating a dry plan.
"""
import argparse
import copy
import json
import os
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from benchmark_backend import JenaBenchmarkBackend
from benchmark_environment import campaign_lock, disk_budget, hardware, pid_alive
from benchmark_plan import configurations, schedule, validate_spec
from benchmark_reports import consolidate
from evaluate_queries import child, provenance, read_json
from icc2027_pipeline import DEFAULT_TEMPORAL_RULES, discover_inputs, sha256
from jena_executor import write_json

PACKAGE = Path(__file__).resolve().parents[1]


def utc():
    return datetime.now(timezone.utc).isoformat()


def replace_state(path, data):
    """Explicit atomic update of owned execution state; data artifacts stay exclusive."""
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    write_json(temp, data)
    os.replace(temp, path)


def event(root, event_name, **details):
    with (root / "events.jsonl").open("a", encoding="utf-8") as log:
        log.write(json.dumps({"utc": utc(), "event": event_name, **details}, ensure_ascii=False) + "\n")
        log.flush()
        os.fsync(log.fileno())


def plan_campaign(spec, csv_root, output):
    validate_spec(spec)
    jobs = schedule(spec)
    inputs = discover_inputs(csv_root)
    budget = disk_budget(output, **{ "reserve": spec["storage"]["reserve_free_bytes"],
                                    "estimated": spec["storage"]["estimated_bytes_per_repetition"], "repetitions": len(jobs)})
    return {"schema_version": 1, "dry_run": True, "created_utc": utc(), "spec": spec,
            "schedule": jobs, "task_count": len(jobs), "measured_task_count": sum(j["kind"] == "measured" for j in jobs),
            "input_paths": [{"path": str(p.resolve()), "bytes": p.stat().st_size} for p in inputs],
            "output": str(output.resolve()), "storage_budget": budget,
            "note": "No input hashing, generation, Jena or cache manipulation performed"}


def ensure_output_safe(path, csv_root):
    output, inputs = path.resolve(), csv_root.resolve()
    if output.is_relative_to(inputs) or inputs.is_relative_to(output):
        raise ValueError("Campaign output must be separate from input CSVs")
    repo = PACKAGE.parent.resolve()
    if output.is_relative_to(repo) and not output.is_relative_to(PACKAGE / "benchmark-runs"):
        raise ValueError("Large campaign outputs inside this repository must use ignored benchmark-runs/")


def create_campaign(spec, csv_root, output, jena_home, java="java", jvm_args=(), rules=DEFAULT_TEMPORAL_RULES, source_spec=None):
    setup_started = perf_counter()
    csv_root, output, jena_home, rules = csv_root.resolve(), output.resolve(), jena_home.resolve(), rules.resolve()
    ensure_output_safe(output, csv_root)
    plan = plan_campaign(spec, csv_root, output)
    if not plan["storage_budget"]["sufficient"]:
        raise ValueError("Insufficient free storage for the declared campaign budget")
    source = provenance()
    if spec["protocol"].get("require_clean_git", True) and source["git_dirty"] is not False:
        raise ValueError("A clean, identified Git commit is required by this protocol")
    resolved = copy.deepcopy(spec)
    resolved["configurations"] = configurations(spec)
    rules_data = read_json(rules)
    if rules_data.get("rules_version") != "2026-10-22.2-r04":
        raise ValueError("This protocol requires the R04 rule version")
    output.mkdir(parents=True, exist_ok=False)
    (output / "sources").mkdir()
    (output / "queries").mkdir()
    (output / "attempts").mkdir()
    (output / "cli").mkdir()
    for p in Path(__file__).parent.glob("*.py"):
        shutil.copyfile(p, output / "sources" / p.name)
    for p in (PACKAGE / "queries").glob("*.rq"):
        shutil.copyfile(p, output / "queries" / p.name)
    shutil.copyfile(rules, output / "rules.json")
    write_json(output / "specification.json", resolved)
    if source_spec is not None:
        shutil.copyfile(source_spec, output / "original-specification.json")
    else:
        write_json(output / "original-specification.json", spec)
    inputs = [{"path": str(p.resolve()), "bytes": p.stat().st_size, "sha256": sha256(p)} for p in discover_inputs(csv_root)]
    context = {"campaign_id": uuid.uuid4().hex, "root": str(output.resolve()), "spec": resolved, "schedule": plan["schedule"],
               "created_utc": utc(), "csv_root": str(csv_root.resolve()), "inputs": inputs,
               "jena_home": str(jena_home.resolve()), "java": java, "jvm_args": list(jvm_args),
               "python_executable": sys.executable, "hardware": hardware(), "source": source,
               "rules_sha256": sha256(output / "rules.json"), "protocol_sha256": sha256(output / "specification.json"),
               "original_specification_sha256": sha256(output / "original-specification.json"),
               "original_specification_path": str(Path(source_spec).resolve()) if source_spec else None,
               "source_files_sha256": {p.name: sha256(p) for p in (output / "sources").glob("*.py")},
               "queries_sha256": {p.name: sha256(p) for p in (output / "queries").glob("*.rq")},
               "toolchain_jars_sha256": {p.name: sha256(p) for p in (jena_home / "lib").glob("*.jar")},
               "publication_ready": source["git_dirty"] is False, "initial_storage_budget": plan["storage_budget"]}
    context["campaign_setup_seconds"] = perf_counter() - setup_started
    write_json(output / "campaign.json", context)
    write_json(output / "campaign-seal.json", {"campaign_sha256": sha256(output / "campaign.json")})
    write_json(output / "state.json", {"status": "pending", "tasks": {j["task_id"]: {"status": "pending", "attempts": []} for j in plan["schedule"]}, "reports": [], "sessions": []})
    event(output, "campaign_created", campaign_id=context["campaign_id"])
    return context


def verify_context(root, context):
    if sha256(root / "campaign.json") != read_json(root / "campaign-seal.json")["campaign_sha256"]:
        raise ValueError("Frozen campaign manifest changed")
    if str(root.resolve()) != context["root"]:
        raise ValueError("Campaign relocation is not supported for resume")
    if (sha256(root / "rules.json") != context["rules_sha256"] or sha256(root / "specification.json") != context["protocol_sha256"] or
            sha256(root / "original-specification.json") != context["original_specification_sha256"]):
        raise ValueError("Frozen rules/specification changed")
    for group, directory in (("source_files_sha256", root / "sources"), ("queries_sha256", root / "queries")):
        for name, expected in context[group].items():
            if sha256(directory / name) != expected:
                raise ValueError("Frozen source/query changed")
            if group == "source_files_sha256" and sha256(Path(__file__).parent / name) != expected:
                raise ValueError("Runner source changed; use the same checkpoint to resume")
    for item in context["inputs"]:
        p = Path(item["path"])
        if p.stat().st_size != item["bytes"] or sha256(p) != item["sha256"]:
            raise ValueError("Input CSVs changed since campaign creation")
    current = hardware()
    for key in ("cpu", "logical_cpu_count", "ram_total_bytes", "operating_system", "python_version", "hostname", "inherited_jvm_options_sha256"):
        if current[key] != context["hardware"][key]:
            raise ValueError(f"Environment drift for {key}; start a separate campaign")
    jars = {p.name: sha256(p) for p in (Path(context["jena_home"]) / "lib").glob("*.jar")}
    if jars != context["toolchain_jars_sha256"]:
        raise ValueError("Jena jars changed")


def verify_completed(path):
    completion = read_json(path / "completion.json")
    for name, expected in completion["small_files_sha256"].items():
        if sha256(child(path, name)) != expected:
            raise ValueError("Completed repetition evidence changed")
    result = read_json(path / "result.json")
    manifest = read_json(Path(result["manifest_file"]))
    for artifact in manifest["artifacts"]:
        p = child(path / "generation", artifact["path"])
        if p.stat().st_size != artifact["bytes"] or sha256(p) != artifact["sha256"]:
            raise ValueError("Completed RDF artifact changed")
    for item in result["storage"]["files"]:
        if Path(item["path"]).name == "tdb.lock":
            continue
        if child(path / "tdb2", item["path"]).stat().st_size != item["bytes"]:
            raise ValueError("Completed database file lengths changed")


def check_orphans(path):
    for evidence in path.rglob("*.process.json"):
        meta = evidence.with_name(evidence.name.replace(".process.json", ".meta.json"))
        if not meta.exists() and pid_alive(read_json(evidence)["pid"]):
            raise RuntimeError("A prior child PID is still alive; verify/stop it before resuming (PID reuse is possible)")


def execute_campaign(root, retry_failed=False, backend_factory=JenaBenchmarkBackend):
    verification_started = perf_counter()
    root = root.resolve()
    context = read_json(root / "campaign.json")
    with campaign_lock(root / "runner.lock"):
        state = read_json(root / "state.json")
        verify_context(root, context)
        recovered = False
        for entry in state["tasks"].values():
            if entry["status"] == "success":
                verify_completed(root / entry["attempts"][-1]["path"])
            elif entry["attempts"]:
                previous = root / entry["attempts"][-1]["path"]
                check_orphans(previous)
                if (previous / "completion.json").exists():
                    verify_completed(previous)
                    entry["status"] = "success"
                    recovered = True
                    event(root, "recovered_sealed_completion", attempt=str(previous))
                elif (previous / "state.json").exists():
                    old = read_json(previous / "state.json")
                    if old["status"] == "success":
                        write_json(previous / ("unsealed-state-" + uuid.uuid4().hex + ".json"), old)
                        old.update(status="interrupted", error="Completion seal was not committed before interruption")
                        replace_state(previous / "state.json", old)
                        entry["status"] = "interrupted"
                        recovered = True
                        event(root, "unsealed_completion_preserved", attempt=str(previous))
        if recovered:
            replace_state(root / "state.json", state)
        if any(t["status"] == "failed" for t in state["tasks"].values()) and not retry_failed:
            raise ValueError("Failed attempts were preserved; --retry-failed is required for a new attempt")
        if all(t["status"] == "success" for t in state["tasks"].values()):
            if state["status"] != "success":
                state["reports"].append(consolidate(root, context, state))
                state["status"] = "success"
                state.pop("error", None)
                replace_state(root / "state.json", state)
            return state
        def space(remaining=1):
            budget = disk_budget(root, context["spec"]["storage"]["reserve_free_bytes"],
                                 context["spec"]["storage"]["estimated_bytes_per_repetition"], remaining)
            event(root, "storage_check", **budget)
            if not budget["sufficient"]:
                raise ValueError("Storage reserve would be violated")
        space(sum(t["status"] != "success" for t in state["tasks"].values()))
        session = {"id": uuid.uuid4().hex, "started_utc": utc(), "status": "running",
                   "preflight_verification_seconds": perf_counter() - verification_started}
        state["sessions"].append(session)
        state["status"] = "running"
        state.pop("error", None)
        replace_state(root / "state.json", state)
        started = perf_counter()
        try:
            backend = backend_factory(root, context)
            cli = backend.prepare(root / "cli" / ("session-" + session["id"]))
            fingerprint = {k: (v.get("stdout_sha256"), v.get("stderr_sha256")) for k, v in cli.items()}
            if state.get("cli_fingerprint") is not None and state["cli_fingerprint"] != {k: list(v) for k, v in fingerprint.items()}:
                raise ValueError("Jena/Java CLI identity changed between sessions")
            state["cli_fingerprint"] = {k: list(v) for k, v in fingerprint.items()}
            for job in context["schedule"]:
                entry = state["tasks"][job["task_id"]]
                if entry["status"] == "success":
                    continue
                space()
                path = root / "attempts" / (job["task_id"] + "-" + uuid.uuid4().hex)
                path.mkdir()
                relative = path.relative_to(root).as_posix()
                entry["attempts"].append({"path": relative})
                entry["status"] = "running"
                outcome = {"status": "running", "started_utc": utc(), "task_id": job["task_id"],
                           "execution_id": path.name,
                           "session_id": session["id"], "phase": "setup", "error": None}
                write_json(path / "configuration.json", job["config"])
                write_json(path / "state.json", outcome)
                replace_state(root / "state.json", state)
                event(root, "attempt_started", task_id=job["task_id"], attempt=relative)
                attempt_started = perf_counter()
                def phase(name):
                    outcome["phase"] = name
                    replace_state(path / "state.json", outcome)
                    event(root, "phase_started", task_id=job["task_id"], attempt=relative, phase=name)
                try:
                    result = backend.execute(job, path, phase, space)
                    total = perf_counter() - attempt_started
                    accounted = sum(result["exclusive_process_stages_seconds"].values())
                    if accounted > total + 1e-6:
                        raise ValueError("Overlapping external phase accounting")
                    result["metrics"]["total_seconds"] = total
                    result["exclusive_process_stages_seconds"]["unattributed_seconds"] = max(0, total - accounted)
                    write_json(path / "result.json", result)
                    outcome.update(status="success", finished_utc=utc(), phase="complete", elapsed_seconds=total)
                    replace_state(path / "state.json", outcome)
                    small = {p.relative_to(path).as_posix(): sha256(p) for p in path.rglob("*")
                             if p.is_file() and not p.is_relative_to(path / "tdb2") and
                             (not p.is_relative_to(path / "generation") or p.name == "manifest.json")}
                    write_json(path / "completion.json", {"small_files_sha256": small})
                    entry["status"] = "success"
                    event(root, "attempt_succeeded", task_id=job["task_id"], attempt=relative)
                    replace_state(root / "state.json", state)
                    print(job["task_id"] + ": success", flush=True)
                except (Exception, KeyboardInterrupt) as exc:
                    interrupted = isinstance(exc, KeyboardInterrupt)
                    outcome.update(status="interrupted" if interrupted else "failed", error=str(exc) or type(exc).__name__,
                                   finished_utc=utc(), elapsed_seconds=perf_counter() - attempt_started)
                    replace_state(path / "state.json", outcome)
                    entry["status"] = outcome["status"]
                    replace_state(root / "state.json", state)
                    event(root, "attempt_ended", task_id=job["task_id"], attempt=relative, status=outcome["status"], error=outcome["error"])
                    raise
            state["status"] = "success"
            session["status"] = "success"
        except (Exception, KeyboardInterrupt) as exc:
            session["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
            state.update(status=session["status"], error=str(exc) or type(exc).__name__)
        finally:
            reporting_started = perf_counter()
            try:
                state["reports"].append(consolidate(root, context, state))
            except Exception as exc:
                state.update(status="failed", error="Report consolidation failed: " + str(exc))
                session["status"] = "failed"
            session.update(finished_utc=utc(), active_seconds=perf_counter() - started,
                           reporting_seconds=perf_counter() - reporting_started)
            replace_state(root / "state.json", state)
        return state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--spec", type=Path, required=True)
        cmd.add_argument("--csv-root", type=Path, required=True)
        cmd.add_argument("--output", type=Path, required=True)
        if name == "run":
            cmd.add_argument("--jena-home", type=Path, required=True)
            cmd.add_argument("--java", default="java")
            cmd.add_argument("--jvm-arg", action="append", default=[])
            cmd.add_argument("--rules", type=Path, default=DEFAULT_TEMPORAL_RULES)
    for name in ("resume", "summarize"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--campaign", type=Path, required=True)
        if name == "resume":
            cmd.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            write_json(args.output, plan_campaign(read_json(args.spec), args.csv_root, args.output.parent))
            return 0
        if args.command == "run":
            create_campaign(read_json(args.spec), args.csv_root, args.output, args.jena_home, args.java, args.jvm_arg, args.rules, args.spec)
            state = execute_campaign(args.output)
        elif args.command == "resume":
            state = execute_campaign(args.campaign, args.retry_failed)
        else:
            args.campaign = args.campaign.resolve()
            with campaign_lock(args.campaign / "runner.lock"):
                context, state = read_json(args.campaign / "campaign.json"), read_json(args.campaign / "state.json")
                verify_context(args.campaign, context)
                for entry in state["tasks"].values():
                    if entry["status"] == "success":
                        verify_completed(args.campaign / entry["attempts"][-1]["path"])
                state["reports"].append(consolidate(args.campaign, context, state))
                replace_state(args.campaign / "state.json", state)
        print(json.dumps({"status": state["status"], "reports": state["reports"]}))
        return 0 if state["status"] == "success" else (130 if state["status"] == "interrupted" else 1)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        print(f"Benchmark failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Validated sensitivity design and reproducible schedule. No execution here."""
import copy
import json
import math
import random
from pathlib import Path

from icc2027_pipeline import normalized_label, validate_config
from performance_statistics import validate_ci


def integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def configurations(spec):
    if "configurations" in spec:
        result = copy.deepcopy(spec["configurations"])
    else:
        grid = spec["sensitivity"]
        result = [{"id": "baseline", "config": {"technique": "baseline"}}]
        if grid.get("labels"):
            canonical = json.loads((Path(__file__).resolve().parents[1] / "configs/labels-all-attacks.json").read_text(encoding="utf-8"))["labels"]
            known = {normalized_label(v) for v in canonical} | {"benign"}
            if any(normalized_label(v) not in known for v in grid["labels"]):
                raise ValueError("Unknown CICIDS2017 sensitivity labels; use canonical labels-all-attacks.json")
            result.append({"id": "labels", "config": {"technique": "labels", "labels": grid["labels"]}})
        for probability in grid["sample_probabilities"]:
            for seed in grid["sample_seeds"]:
                integer(seed, "sample seed")
                result.append({"id": f"sample-p{str(probability).replace('.', 'd')}-s{seed}",
                               "config": {"technique": "sample", "sample_probability": probability, "seed": seed}})
        for k in grid["topk_values"]:
            result.append({"id": f"topk-{k}", "config": {"technique": "topk", "k": k}})
        for hours in grid["temporal_hours"]:
            result.append({"id": f"temporal-{hours}h", "config": {"technique": "temporal", "window_hours": hours}})
        for seconds in grid["aggregation_buckets_seconds"]:
            result.append({"id": f"aggregate-{seconds}s", "config": {"technique": "aggregate", "bucket_seconds": seconds}})
    ids = set()
    import re
    for item in result:
        identity = item["id"]
        if not isinstance(identity, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", identity) or identity in ids:
            raise ValueError("Configuration IDs must be unique safe names")
        ids.add(identity)
        validate_config(item["config"])
        if item["config"]["technique"] == "aggregate" and 3600 % item["config"]["bucket_seconds"]:
            raise ValueError("Hourly workload requires aggregation buckets dividing 3600")
    if sum(p["config"]["technique"] == "baseline" for p in result) != 1:
        raise ValueError("Exactly one Baseline is required for paired quality reports")
    return result


def validate_spec(spec):
    if spec.get("schema_version") != 1:
        raise ValueError("Unsupported benchmark specification")
    configs = configurations(spec)
    p = spec["protocol"]
    for name, minimum in (("warmup_repetitions", 0), ("measured_repetitions", 1), ("query_warmups", 0), ("query_measurements", 1)):
        integer(p[name], name, minimum)
    integer(p["order_seed"], "order_seed")
    if p["order"] not in {"fixed", "randomized"} or p["blocking"] not in {"repetition", "configuration"}:
        raise ValueError("Unknown order or block design")
    if p.get("loader") not in {"basic", "sequential"} or not isinstance(p.get("require_clean_git", True), bool):
        raise ValueError("Require a transactional serial loader and boolean Git cleanliness policy")
    if p["cache_state"] not in {"unknown", "os_cache_uncontrolled", "externally_controlled"}:
        raise ValueError("Invalid cache declaration")
    if not isinstance(p.get("cache_evidence"), str) or (p["cache_state"] == "externally_controlled" and not p["cache_evidence"].strip()):
        raise ValueError("Cache evidence must be supplied; process restart is not cache clearing")
    for name in ("subprocess_timeout_seconds",):
        v = p[name]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
            raise ValueError("Invalid timeout")
    for name in ("reserve_free_bytes", "estimated_bytes_per_repetition"):
        integer(spec["storage"][name], name, 1)
    if spec["temporal_mode"] not in {"raw", "normalized"} or spec["ambiguous_policy"] not in {"exclude", "raw-local"}:
        raise ValueError("Invalid temporal interpretation")
    validate_ci(spec.get("confidence_interval", {"method": "none"}))
    return configs


def schedule(spec):
    configs = validate_spec(spec)
    p, rng, jobs = spec["protocol"], random.Random(spec["protocol"]["order_seed"]), []
    def order(items):
        items = list(items)
        if p["order"] == "randomized":
            rng.shuffle(items)
        return items
    def add(config, kind, repetition, block):
        jobs.append({"task_id": f"{config['id']}-{kind}-{repetition:03d}", "configuration_id": config["id"],
                     "kind": kind, "repetition": repetition, "block_id": block, "order_index": len(jobs),
                     "config": copy.deepcopy(config["config"])})
    if p["blocking"] == "repetition":
        for kind, count in (("warmup", p["warmup_repetitions"]), ("measured", p["measured_repetitions"])):
            for repetition in range(1, count + 1):
                for config in order(configs):
                    add(config, kind, repetition, f"{kind}-{repetition:03d}")
    else:
        for config in order(configs):
            for kind, count in (("warmup", p["warmup_repetitions"]), ("measured", p["measured_repetitions"])):
                for repetition in range(1, count + 1):
                    add(config, kind, repetition, config["id"])
    return jobs


def select_design(spec, configuration_ids=None, measured_repetitions=None, warmup_repetitions=None):
    """Explicit batch selection; preserve estimates and always retain its Baseline."""
    result = copy.deepcopy(spec)
    configs = configurations(result)
    if configuration_ids:
        selected = set(configuration_ids)
        if selected - {c["id"] for c in configs}:
            raise ValueError("Unknown --config-id; inspect the dry-run schedule")
        selected.update(c["id"] for c in configs if c["config"]["technique"] == "baseline")
        result["configurations"] = [c for c in configs if c["id"] in selected]
    if measured_repetitions is not None:
        result["protocol"]["measured_repetitions"] = measured_repetitions
    if warmup_repetitions is not None:
        result["protocol"]["warmup_repetitions"] = warmup_repetitions
    validate_spec(result)
    return result

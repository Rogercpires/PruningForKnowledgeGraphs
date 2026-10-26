#!/usr/bin/env python3
"""Deterministic CIC-IDS-2017 CSV-to-RDF experiment generator.

The program intentionally has no third-party dependency. It writes one immutable
run directory with compressed N-Triples and a machine-readable manifest.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import platform
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Dict, Iterable, Iterator, Optional, Tuple

from temporal_audit import decision_for, parse_local_timestamp, source_rows

FILES = (
    "Monday-WorkingHours.pcap_ISCX.csv",
    "Tuesday-WorkingHours.pcap_ISCX.csv",
    "Wednesday-workingHours.pcap_ISCX.csv",
    "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
    "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
    "Friday-WorkingHours-Morning.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
)
REQUIRED = {"Source IP", "Destination IP", "Timestamp", "Label"}
EX = "http://example.org/ns#"
FLOW = "http://kg/cicids/flow/"
IP = "http://kg/cicids/ip/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD = "http://www.w3.org/2001/XMLSchema#"
DEFAULT_TEMPORAL_RULES = Path(__file__).resolve().parents[1] / "configs" / "temporal-normalization-rules.json"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalized_label(value: str) -> str:
    return (value or "").strip().casefold()


def literal(value: object, datatype: Optional[str] = None) -> str:
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
    suffix = f"^^<{datatype}>" if datatype else ""
    return f'"{escaped}"{suffix}'


def triple(subject: str, predicate: str, obj: str) -> str:
    return f"<{subject}> <{predicate}> {obj} .\n"


def uri_token(value: object) -> str:
    # CIC-IDS IP values are URI-safe; encode unexpected spaces deterministically.
    return str(value).strip().replace(" ", "%20")


def discover_inputs(root: Path) -> list[Path]:
    inputs = [root / name for name in FILES]
    missing = [str(path) for path in inputs if not path.is_file()]
    if missing:
        raise ValueError("Missing canonical CIC-IDS-2017 CSV files: " + ", ".join(missing))
    return inputs


def load_temporal_rules(path: Path) -> dict:
    rules = json.loads(path.read_text(encoding="utf-8"))
    rules["_path"] = str(path)
    return rules


def temporal_value(
    path: Path, line_no: int, row: Dict[str, str], temporal_mode: str,
    rules: dict, ambiguous_policy: str,
) -> tuple[Optional[datetime], str, str, str]:
    """Return local civil time and the applicable decision, never an asserted UTC instant."""
    timestamp = parse_local_timestamp(row.get("Timestamp", ""))
    if timestamp is None:
        return None, "invalid-timestamp", "invalid", "invalid"
    if temporal_mode == "raw":
        return timestamp, "raw-civil-time", "preserved", "raw"
    decision = decision_for(
        path.name, timestamp, rules, line_no=line_no,
        timestamp_original=row.get("Timestamp", ""),
    )
    if decision.classification == "ambiguous":
        if ambiguous_policy == "exclude":
            return None, decision.rule_id, decision.classification, decision.confidence
        if ambiguous_policy == "raw-local":
            return timestamp, decision.rule_id, decision.classification, decision.confidence
        raise ValueError(f"Unsupported ambiguous timestamp policy: {ambiguous_policy}")
    return decision.proposed, decision.rule_id, decision.classification, decision.confidence


def rows(
    path: Path, temporal_mode: str = "raw", rules: Optional[dict] = None,
    ambiguous_policy: str = "exclude",
) -> Iterator[Tuple[int, Dict[str, str], datetime]]:
    if temporal_mode not in {"raw", "normalized"}:
        raise ValueError("temporal_mode must be raw or normalized")
    rules = rules or {}
    for line_no, row in source_rows(path):
        if not row["Source IP"] or not row["Destination IP"] or not row["Label"]:
            continue
        timestamp, _, _, _ = temporal_value(path, line_no, row, temporal_mode, rules, ambiguous_policy)
        if timestamp is not None:
            yield line_no, row, timestamp


def temporal_preflight(inputs: Iterable[Path], temporal_mode: str, rules: dict, ambiguous_policy: str) -> dict:
    counts, attack_labels = Counter(), Counter()
    for path in inputs:
        for line_no, row in source_rows(path):
            timestamp = parse_local_timestamp(row.get("Timestamp", ""))
            if not row.get("Source IP") or not row.get("Destination IP") or not row.get("Label"):
                counts["invalid_structural_rows"] += 1
                continue
            _, _, classification, _ = temporal_value(path, line_no, row, temporal_mode, rules, ambiguous_policy)
            counts[classification] += 1
            if classification == "ambiguous" and ambiguous_policy == "exclude":
                counts["ambiguous_excluded"] += 1
            if timestamp is not None:
                counts["eligible_flows"] += 1
                if row["Label"].casefold() != "benign":
                    attack_labels[row["Label"]] += 1
            else:
                counts["invalid"] += 1
    return {
        "corrected": counts["corrected"], "preserved": counts["preserved"],
        "ambiguous": counts["ambiguous"], "ambiguous_excluded": counts["ambiguous_excluded"],
        "invalid": counts["invalid"] + counts["invalid_structural_rows"],
        "eligible_flows": counts["eligible_flows"],
        "labelled_attacks": sum(attack_labels.values()), "attack_classes": len(attack_labels),
    }


def global_max_timestamp(
    inputs: Iterable[Path], temporal_mode: str, rules: dict, ambiguous_policy: str,
) -> datetime:
    maximum: Optional[datetime] = None
    for input_path in inputs:
        for _, _, timestamp in rows(input_path, temporal_mode, rules, ambiguous_policy):
            maximum = timestamp if maximum is None or timestamp > maximum else maximum
    if maximum is None:
        raise ValueError("No valid timestamp found in input CSV files")
    return maximum


def topk_edges(inputs: Iterable[Path], k: int, temporal_mode: str, rules: dict, ambiguous_policy: str) -> set[tuple[str, str]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for input_path in inputs:
        for _, row, _ in rows(input_path, temporal_mode, rules, ambiguous_policy):
            counts[row["Source IP"]][row["Destination IP"]] += 1
    keep: set[tuple[str, str]] = set()
    for source, destinations in counts.items():
        # Deterministic resolution of ties is material to reproducibility.
        ranked = sorted(destinations.items(), key=lambda item: (-item[1], item[0]))[:k]
        keep.update((source, destination) for destination, _ in ranked)
    return keep


def emit_flow(file_key: str, line_no: int, row: Dict[str, str], timestamp: datetime) -> list[str]:
    flow = f"{FLOW}{file_key}/line/{line_no}"
    source = f"{IP}{uri_token(row['Source IP'])}"
    destination = f"{IP}{uri_token(row['Destination IP'])}"
    # CICIDS2017 does not establish a UTC offset.  Keep the auditable local
    # civil-time convention in RDF instead of fabricating an absolute instant.
    iso = timestamp.isoformat(timespec="seconds")
    output = [
        triple(flow, RDF_TYPE, f"<{EX}NetworkFlow>"),
        triple(source, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(destination, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(flow, f"{EX}hasSourceAddress", f"<{source}>"),
        triple(flow, f"{EX}hasDestinationAddress", f"<{destination}>"),
        triple(flow, f"{EX}timestamp", literal(iso, f"{XSD}dateTime")),
        triple(flow, f"{EX}label", literal(row["Label"])),
    ]
    for column, predicate, datatype in (("Source Port", "srcPort", "integer"), ("Destination Port", "dstPort", "integer"), ("Protocol", "protocol", None), ("Flow Duration", "duration", "double")):
        value = row.get(column, "")
        if value:
            try:
                value = int(float(value)) if datatype == "integer" else float(value) if datatype == "double" else value
                output.append(triple(flow, f"{EX}{predicate}", literal(value, f"{XSD}{datatype}" if datatype else None)))
            except ValueError:
                pass
    return output


def floor_local_bucket(timestamp: datetime, bucket_seconds: int) -> datetime:
    day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((timestamp - day_start).total_seconds())
    return day_start + timedelta(seconds=(elapsed // bucket_seconds) * bucket_seconds)


def aggregate_key(row: Dict[str, str], timestamp: datetime, bucket_seconds: int) -> tuple[str, str, str, str, str]:
    bucket = floor_local_bucket(timestamp, bucket_seconds).isoformat(timespec="seconds")
    return (bucket, row["Source IP"], row["Destination IP"], row.get("Protocol", ""), row.get("Destination Port", ""))


def emit_aggregate(file_key: str, key: tuple[str, str, str, str, str], state: dict) -> list[str]:
    bucket, source_ip, destination_ip, protocol, destination_port = key
    stable_id = hashlib.sha256("\x1f".join(map(str, key)).encode("utf-8")).hexdigest()[:24]
    aggregate = f"{FLOW}{file_key}/aggregate/{stable_id}"
    source, destination = f"{IP}{uri_token(source_ip)}", f"{IP}{uri_token(destination_ip)}"
    output = [
        triple(aggregate, RDF_TYPE, f"<{EX}AggregatedFlow>"),
        triple(source, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(destination, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(aggregate, f"{EX}hasSourceAddress", f"<{source}>"),
        triple(aggregate, f"{EX}hasDestinationAddress", f"<{destination}>"),
        triple(aggregate, f"{EX}bucketStart", literal(bucket, f"{XSD}dateTime")),
        triple(aggregate, f"{EX}count", literal(state["count"], f"{XSD}integer")),
        triple(aggregate, f"{EX}firstSeen", literal(state["first"].isoformat(timespec="seconds"), f"{XSD}dateTime")),
        triple(aggregate, f"{EX}lastSeen", literal(state["last"].isoformat(timespec="seconds"), f"{XSD}dateTime")),
    ]
    if protocol:
        output.append(triple(aggregate, f"{EX}protocol", literal(protocol)))
    if destination_port:
        try:
            output.append(triple(aggregate, f"{EX}dstPort", literal(int(float(destination_port)), f"{XSD}integer")))
        except ValueError:
            pass
    for label, count in sorted(state["label_counts"].items()):
        label_count_id = hashlib.sha256(
            ("\x1f".join(map(str, key)) + "\x1f" + label).encode("utf-8")
        ).hexdigest()[:24]
        label_count = f"{aggregate}/label-count/{label_count_id}"
        output.extend((
            triple(aggregate, f"{EX}hasLabelCount", f"<{label_count}>"),
            triple(label_count, RDF_TYPE, f"<{EX}LabelCount>"),
            triple(label_count, f"{EX}label", literal(label)),
            triple(label_count, f"{EX}count", literal(count, f"{XSD}integer")),
        ))
    return output


def write_run(
    config: dict, inputs: list[Path], output_dir: Path, run_id: str,
    temporal_mode: str = "raw", temporal_rules: Optional[dict] = None,
    ambiguous_policy: str = "exclude",
) -> dict:
    technique = config.get("technique")
    if technique not in {"baseline", "temporal", "labels", "sample", "topk", "aggregate"}:
        raise ValueError("Unsupported technique")
    if temporal_mode not in {"raw", "normalized"}:
        raise ValueError("temporal_mode must be raw or normalized")
    if ambiguous_policy not in {"exclude", "raw-local"}:
        raise ValueError("ambiguous_policy must be exclude or raw-local")
    temporal_rules = temporal_rules or load_temporal_rules(DEFAULT_TEMPORAL_RULES)
    required_config = {
        "temporal": ("window_hours",), "labels": ("labels",),
        "sample": ("sample_probability", "seed"), "topk": ("k",),
        "aggregate": ("bucket_seconds",), "baseline": (),
    }
    missing = [key for key in required_config[technique] if key not in config]
    if missing:
        raise ValueError(f"Configuration for {technique} is missing: {', '.join(missing)}")
    randomizer = random.Random(config.get("seed"))
    cutoff = None
    keep_edges = None
    labels = {normalized_label(value) for value in config.get("labels", [])}
    total_started = perf_counter()
    verification_started = perf_counter()
    input_details = [{"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size} for path in inputs]
    input_verification_seconds = perf_counter() - verification_started
    preflight_started = perf_counter()
    temporal_summary = temporal_preflight(inputs, temporal_mode, temporal_rules, ambiguous_policy)
    temporal_preflight_seconds = perf_counter() - preflight_started
    if technique == "temporal":
        reference = global_max_timestamp(inputs, temporal_mode, temporal_rules, ambiguous_policy)
        cutoff = reference - timedelta(hours=float(config["window_hours"]))
    if technique == "topk":
        keep_edges = topk_edges(inputs, int(config["k"]), temporal_mode, temporal_rules, ambiguous_policy)
    if technique == "sample" and not 0 < float(config.get("sample_probability", 0)) <= 1:
        raise ValueError("sample_probability must be in (0, 1]")
    if technique == "topk" and int(config["k"]) < 1:
        raise ValueError("k must be positive")
    if technique == "aggregate" and int(config["bucket_seconds"]) < 1:
        raise ValueError("bucket_seconds must be positive")
    output_dir.mkdir(parents=True, exist_ok=False)

    started_monotonic = perf_counter()
    artifacts, label_counts = [], Counter()
    aggregate_label_counts = Counter()
    total_flows = total_triples = total_aggregate_groups = 0
    for input_path in inputs:
        file_key = input_path.name.removesuffix(".pcap_ISCX.csv")
        artifact = output_dir / f"{file_key}.nt.gz"
        flows = triples = 0
        aggregate: dict[tuple[str, str, str, str, str], dict] = {}
        # Fixed gzip metadata makes identical RDF output produce the same SHA-256.
        with artifact.open("wb") as binary, gzip.GzipFile(
            filename="", mode="wb", fileobj=binary, mtime=0
        ) as compressed, io.TextIOWrapper(compressed, encoding="utf-8", newline="\n") as sink:
            for line_no, row, timestamp in rows(input_path, temporal_mode, temporal_rules, ambiguous_policy):
                selected = True
                if cutoff is not None:
                    selected = timestamp >= cutoff
                elif labels:
                    selected = normalized_label(row["Label"]) in labels
                elif keep_edges is not None:
                    selected = (row["Source IP"], row["Destination IP"]) in keep_edges
                elif technique == "sample":
                    selected = randomizer.random() < float(config["sample_probability"])
                if not selected:
                    continue
                label_counts[row["Label"].strip()] += 1
                flows += 1
                if technique == "aggregate":
                    key = aggregate_key(row, timestamp, int(config["bucket_seconds"]))
                    state = aggregate.setdefault(key, {
                        "count": 0,
                        "first": timestamp,
                        "last": timestamp,
                        "label_counts": Counter(),
                    })
                    state["count"] += 1
                    state["first"] = min(state["first"], timestamp)
                    state["last"] = max(state["last"], timestamp)
                    state["label_counts"][row["Label"].strip()] += 1
                else:
                    lines = emit_flow(file_key, line_no, row, timestamp)
                    sink.writelines(lines)
                    triples += len(lines)
            if technique == "aggregate":
                for key in sorted(aggregate):
                    if sum(aggregate[key]["label_counts"].values()) != aggregate[key]["count"]:
                        raise RuntimeError(
                            "Aggregate label counts differ from its input flow count"
                        )
                    lines = emit_aggregate(file_key, key, aggregate[key])
                    sink.writelines(lines)
                    triples += len(lines)
                    aggregate_label_counts.update(aggregate[key]["label_counts"])
                total_aggregate_groups += len(aggregate)
        artifacts.append({"path": artifact.name, "sha256": sha256(artifact), "bytes": artifact.stat().st_size,
                          "selected_flows": flows, "emitted_triples": triples,
                          **({"aggregated_groups": len(aggregate), "aggregate_input_flows": flows}
                             if technique == "aggregate" else {})})
        total_flows += flows
        total_triples += triples
    if technique == "aggregate" and aggregate_label_counts != label_counts:
        raise RuntimeError(
            "Aggregate label counts differ from the selected input flow label counts"
        )
    manifest = {
        "schema_version": 2, "run_id": run_id,
        "generator": {"name": Path(__file__).name, "python": sys.version, "platform": platform.platform()},
        "config": config, "input_csvs": input_details, "artifacts": artifacts,
        "temporal_interpretation": {
            "mode": temporal_mode,
            "ambiguous_policy": ambiguous_policy if temporal_mode == "normalized" else "not-applicable",
            "rules_version": temporal_rules.get("rules_version"),
            "rules_sha256": sha256(Path(temporal_rules["_path"])),
            "timezone_convention": temporal_rules.get("timezone_convention"),
            "input_counts": temporal_summary,
            **({
                "reference_local": reference.isoformat(timespec="seconds"),
                "cutoff_local": cutoff.isoformat(timespec="seconds"),
            } if technique == "temporal" else {}),
        },
        "metrics": {
            "selected_flows": total_flows,
            "emitted_triples": total_triples,
            "label_counts": dict(sorted(label_counts.items())),
            **({
                "aggregated_groups": total_aggregate_groups,
                "aggregate_input_flows": total_flows,
                "aggregate_label_counts": dict(sorted(aggregate_label_counts.items())),
                "aggregate_label_count_sum": sum(aggregate_label_counts.values()),
            } if technique == "aggregate" else {}),
            "input_verification_seconds": round(input_verification_seconds, 6),
            "temporal_preflight_seconds": round(temporal_preflight_seconds, 6),
            "generation_seconds": round(perf_counter() - started_monotonic, 6),
            "total_seconds": round(perf_counter() - total_started, 6),
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--csv-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--temporal-mode", choices=("raw", "normalized"), default="raw")
    parser.add_argument("--temporal-rules", type=Path, default=DEFAULT_TEMPORAL_RULES)
    parser.add_argument("--ambiguous-policy", choices=("exclude", "raw-local"), default="exclude")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        manifest = write_run(
            config, discover_inputs(args.csv_root), args.output_dir, args.run_id,
            args.temporal_mode, load_temporal_rules(args.temporal_rules), args.ambiguous_policy,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"run_id": manifest["run_id"], "metrics": manifest["metrics"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




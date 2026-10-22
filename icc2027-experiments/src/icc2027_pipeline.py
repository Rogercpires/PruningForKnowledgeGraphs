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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from time import perf_counter
from typing import Dict, Iterable, Iterator, Optional, Tuple

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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalized_label(value: str) -> str:
    return (value or "").strip().casefold()


def parse_timestamp(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


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


def rows(path: Path) -> Iterator[Tuple[int, Dict[str, str], datetime]]:
    csv_encoding = (
        "cp1252"
        if path.name == "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv"
        else "utf-8-sig"
    )
    with path.open("r", encoding=csv_encoding, newline="", errors="strict") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"Empty or headerless CSV: {path}")
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        missing = REQUIRED.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"{path.name} lacks required columns: {sorted(missing)}")
        for line_no, row in enumerate(reader, start=2):
            row = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
            if not row["Source IP"] or not row["Destination IP"] or not row["Label"]:
                continue
            timestamp = parse_timestamp(row["Timestamp"])

            if timestamp is None:
                try:
                    timestamp = datetime.strptime(
                        row["Timestamp"], "%d/%m/%Y %H:%M"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

            yield line_no, row, timestamp


def global_max_timestamp(inputs: Iterable[Path]) -> datetime:
    maximum: Optional[datetime] = None
    for input_path in inputs:
        for _, _, timestamp in rows(input_path):
            maximum = timestamp if maximum is None or timestamp > maximum else maximum
    if maximum is None:
        raise ValueError("No valid timestamp found in input CSV files")
    return maximum


def topk_edges(inputs: Iterable[Path], k: int) -> set[tuple[str, str]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for input_path in inputs:
        for _, row, _ in rows(input_path):
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
    iso = timestamp.isoformat().replace("+00:00", "Z")
    output = [
        triple(flow, RDF_TYPE, f"<{EX}NetworkFlow>"),
        triple(source, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(destination, RDF_TYPE, f"<{EX}IPAddress>"),
        triple(flow, f"{EX}hasSourceAddress", f"<{source}>"),
        triple(flow, f"{EX}hasDestinationAddress", f"<{destination}>"),
        triple(flow, f"{EX}timestamp", literal(iso, f"{XSD}dateTime")),
        triple(flow, f"{EX}timestampEpoch", literal(int(timestamp.timestamp()), f"{XSD}integer")),
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


def aggregate_key(row: Dict[str, str], timestamp: datetime, bucket_seconds: int) -> tuple[int, str, str, str, str]:
    epoch = int(timestamp.timestamp())
    return ((epoch // bucket_seconds) * bucket_seconds, row["Source IP"], row["Destination IP"], row.get("Protocol", ""), row.get("Destination Port", ""))


def emit_aggregate(file_key: str, key: tuple[int, str, str, str, str], state: dict) -> list[str]:
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
        triple(aggregate, f"{EX}bucketStart", literal(bucket, f"{XSD}integer")),
        triple(aggregate, f"{EX}count", literal(state["count"], f"{XSD}integer")),
        triple(aggregate, f"{EX}firstSeen", literal(state["first"], f"{XSD}integer")),
        triple(aggregate, f"{EX}lastSeen", literal(state["last"], f"{XSD}integer")),
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


def write_run(config: dict, inputs: list[Path], output_dir: Path, run_id: str) -> dict:
    technique = config.get("technique")
    if technique not in {"baseline", "temporal", "labels", "sample", "topk", "aggregate"}:
        raise ValueError("Unsupported technique")
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
    if technique == "temporal":
        cutoff = global_max_timestamp(inputs) - timedelta(hours=float(config["window_hours"]))
    if technique == "topk":
        keep_edges = topk_edges(inputs, int(config["k"]))
    if technique == "sample" and not 0 < float(config.get("sample_probability", 0)) <= 1:
        raise ValueError("sample_probability must be in (0, 1]")
    if technique == "topk" and int(config["k"]) < 1:
        raise ValueError("k must be positive")
    if technique == "aggregate" and int(config["bucket_seconds"]) < 1:
        raise ValueError("bucket_seconds must be positive")
    output_dir.mkdir(parents=True, exist_ok=False)

    started = datetime.now(timezone.utc)
    started_monotonic = perf_counter()
    input_details = [{"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size} for path in inputs]
    artifacts, label_counts = [], Counter()
    aggregate_label_counts = Counter()
    total_flows = total_triples = total_aggregate_groups = 0
    for input_path in inputs:
        file_key = input_path.name.removesuffix(".pcap_ISCX.csv")
        artifact = output_dir / f"{file_key}.nt.gz"
        flows = triples = 0
        aggregate: dict[tuple[int, str, str, str, str], dict] = {}
        # Fixed gzip metadata makes identical RDF output produce the same SHA-256.
        with artifact.open("wb") as binary, gzip.GzipFile(
            filename="", mode="wb", fileobj=binary, mtime=0
        ) as compressed, io.TextIOWrapper(compressed, encoding="utf-8", newline="\n") as sink:
            for line_no, row, timestamp in rows(input_path):
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
                    epoch = int(timestamp.timestamp())
                    state = aggregate.setdefault(key, {
                        "count": 0,
                        "first": epoch,
                        "last": epoch,
                        "label_counts": Counter(),
                    })
                    state["count"] += 1
                    state["first"] = min(state["first"], epoch)
                    state["last"] = max(state["last"], epoch)
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
    finished = datetime.now(timezone.utc)
    manifest = {
        "schema_version": 1, "run_id": run_id, "created_at_utc": finished.isoformat(),
        "generator": {"name": Path(__file__).name, "python": sys.version, "platform": platform.platform()},
        "config": config, "input_csvs": input_details, "artifacts": artifacts,
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
            "generation_seconds": round(perf_counter() - started_monotonic, 6),
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
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        manifest = write_run(config, discover_inputs(args.csv_root), args.output_dir, args.run_id)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"run_id": manifest["run_id"], "metrics": manifest["metrics"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




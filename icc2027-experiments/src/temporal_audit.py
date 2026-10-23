#!/usr/bin/env python3
"""Audit CIC-IDS-2017 timestamps without modifying experiment inputs or RDF.

This module intentionally uses naive ``datetime`` values.  CICIDS2017 CSV
timestamps do not establish a UTC offset; treating them as UTC would fabricate
an absolute instant and silently change temporal buckets.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Mapping, Optional


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
REQUIRED_COLUMNS = {"Source IP", "Destination IP", "Timestamp", "Label"}
EXPECTED_INVARIANTS = {
    "eligible_flows": 2_830_743,
    "labelled_attacks": 557_646,
    "attack_classes": 14,
}
REPORT_COLUMNS = (
    "file", "line", "flow_id", "timestamp_original", "timestamp_proposed",
    "bucket_start_local", "rule_id", "classification", "confidence", "reason",
    "eligible_for_pipeline",
)
NEIGHBOR_COLUMNS = (
    "target_line", "relative_line", "line", "timestamp_original", "flow_id",
    "source_ip", "source_port", "destination_ip", "destination_port", "protocol",
    "label",
)


@dataclass(frozen=True)
class AuditDecision:
    proposed: Optional[datetime]
    rule_id: str
    classification: str
    confidence: str
    reason: str


def discover_inputs(csv_root: Path) -> list[Path]:
    paths = [csv_root / name for name in FILES]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise ValueError("Missing canonical CIC-IDS-2017 CSV files: " + ", ".join(missing))
    return paths


def csv_encoding(path: Path) -> str:
    return "cp1252" if path.name == "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv" else "utf-8-sig"


def source_rows(path: Path) -> Iterator[tuple[int, dict[str, str]]]:
    with path.open("r", encoding=csv_encoding(path), newline="", errors="strict") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"Empty or headerless CSV: {path}")
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
        missing = REQUIRED_COLUMNS.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"{path.name} lacks required columns: {sorted(missing)}")
        for line_no, row in enumerate(reader, start=2):
            yield line_no, {(key or "").strip(): (value or "").strip() for key, value in row.items()}


def parse_local_timestamp(value: str) -> Optional[datetime]:
    """Parse the documented D/M/Y convention as local civil time, never UTC."""
    value = (value or "").strip()
    for fmt in (
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
        "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %I:%M %p",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def local_iso(value: Optional[datetime]) -> str:
    return "" if value is None else value.isoformat(timespec="seconds")


def local_bucket(value: Optional[datetime], seconds: int) -> str:
    if value is None:
        return ""
    if seconds < 1:
        raise ValueError("bucket seconds must be positive")
    day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
    offset = int((value - day_start).total_seconds())
    return local_iso(day_start + timedelta(seconds=(offset // seconds) * seconds))


def decision_for(
    file_name: str,
    timestamp: Optional[datetime],
    rules: Mapping[str, object],
    *,
    line_no: Optional[int] = None,
    timestamp_original: str = "",
) -> AuditDecision:
    if timestamp is None:
        return AuditDecision(None, "invalid-timestamp", "invalid", "invalid", "Timestamp does not match a supported CICIDS2017 local format.")
    file_rules = rules.get("file_rules", {})
    rule = file_rules.get(file_name) if isinstance(file_rules, dict) else None
    if isinstance(rule, dict):
        for exception in rule.get("line_exceptions", []):
            if not isinstance(exception, dict):
                continue
            if exception.get("line") != line_no or exception.get("timestamp_original") != timestamp_original:
                continue
            action = exception.get("action")
            if action == "preserve":
                return AuditDecision(
                    timestamp, str(exception["id"]), str(exception["classification"]),
                    str(exception["confidence"]), str(exception["rationale"]),
                )
            if action == "ambiguous":
                return AuditDecision(
                    None, str(exception["id"]), str(exception["classification"]),
                    str(exception["confidence"]), str(exception["rationale"]),
                )
            raise ValueError(f"Unsupported temporal exception action: {action}")
    if isinstance(rule, dict) and timestamp.hour in set(rule.get("raw_clock_hours", [])):
        proposed = timestamp + timedelta(hours=int(rule.get("add_hours", 0)))
        if proposed.hour in set(rule.get("permitted_proposed_hours", [])):
            return AuditDecision(
                proposed, str(rule["id"]), "corrected", str(rule.get("confidence", "high")),
                str(rule.get("rationale", "Configured file-level temporal-session rule.")),
            )
        return AuditDecision(None, str(rule["id"]), "ambiguous", "ambiguous", "Configured rule did not produce a permitted local working-hour value.")
    default = rules.get("default_rule", {})
    if not isinstance(default, dict):
        default = {}
    return AuditDecision(timestamp, str(default.get("id", "preserve-uncovered-file")), "preserved", str(default.get("confidence", "preserved")), str(default.get("rationale", "No authorized correction rule applies.")))


def eligible(row: Mapping[str, str], timestamp: Optional[datetime]) -> bool:
    return bool(timestamp and row.get("Source IP") and row.get("Destination IP") and row.get("Label"))


def report_row(path: Path, line_no: int, row: Mapping[str, str], decision: AuditDecision, bucket_seconds: int) -> dict[str, str]:
    return {
        "file": path.name,
        "line": str(line_no),
        "flow_id": row.get("Flow ID", ""),
        "timestamp_original": row.get("Timestamp", ""),
        "timestamp_proposed": local_iso(decision.proposed),
        "bucket_start_local": local_bucket(decision.proposed, bucket_seconds),
        "rule_id": decision.rule_id,
        "classification": decision.classification,
        "confidence": decision.confidence,
        "reason": decision.reason,
        "eligible_for_pipeline": str(eligible(row, parse_local_timestamp(row.get("Timestamp", "")))).lower(),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def collect_wednesday_neighborhoods(path: Path, target_lines: set[int], output: Path) -> None:
    with output.open("w", encoding="utf-8", newline="") as sink:
        writer = csv.DictWriter(sink, fieldnames=NEIGHBOR_COLUMNS)
        writer.writeheader()
        if not target_lines:
            return
        # The Wednesday CSV has hundreds of thousands of rows.  A second
        # streaming pass keeps this investigation bounded instead of holding a
        # complete duplicate of the file in memory.
        for line_no, row in source_rows(path):
            for target in sorted(target_lines):
                if not target - 2 <= line_no <= target + 2:
                    continue
                writer.writerow({
                    "target_line": target, "relative_line": line_no - target, "line": line_no,
                    "timestamp_original": row.get("Timestamp", ""), "flow_id": row.get("Flow ID", ""),
                    "source_ip": row.get("Source IP", ""), "source_port": row.get("Source Port", ""),
                    "destination_ip": row.get("Destination IP", ""), "destination_port": row.get("Destination Port", ""),
                    "protocol": row.get("Protocol", ""), "label": row.get("Label", ""),
                })


def audit(inputs: list[Path], rules: Mapping[str, object], output_dir: Path, bucket_seconds: int = 60) -> dict[str, object]:
    """Create immutable audit-only evidence.  The source files are read only."""
    if output_dir.exists():
        raise FileExistsError(f"Audit output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=False)
    report_path = output_dir / "temporal-audit.csv.gz"
    differences_path = output_dir / "temporal-differences.csv"
    per_file: dict[str, dict[str, object]] = {}
    total = Counter()
    attack_labels: Counter[str] = Counter()
    wednesday_targets: set[int] = set()

    with report_path.open("wb") as binary, gzip.GzipFile(filename="", mode="wb", fileobj=binary, mtime=0) as compressed, differences_path.open("w", encoding="utf-8", newline="") as difference_sink:
        report_sink = compressed
        import io
        with io.TextIOWrapper(report_sink, encoding="utf-8", newline="") as report_text:
            writer = csv.DictWriter(report_text, fieldnames=REPORT_COLUMNS)
            diff_writer = csv.DictWriter(difference_sink, fieldnames=REPORT_COLUMNS)
            writer.writeheader()
            diff_writer.writeheader()
            for path in inputs:
                counts = Counter()
                first_noon_line: Optional[int] = None
                noon_seen = False
                labels = Counter()
                for line_no, row in source_rows(path):
                    timestamp = parse_local_timestamp(row.get("Timestamp", ""))
                    if timestamp and timestamp.hour == 12 and first_noon_line is None:
                        first_noon_line = line_no
                        noon_seen = True
                    decision = decision_for(
                        path.name, timestamp, rules, line_no=line_no,
                        timestamp_original=row.get("Timestamp", ""),
                    )
                    item = report_row(path, line_no, row, decision, bucket_seconds)
                    writer.writerow(item)
                    if decision.classification != "preserved":
                        diff_writer.writerow(item)
                    counts["source_rows"] += 1
                    counts[decision.classification] += 1
                    if eligible(row, timestamp):
                        counts["eligible_flows"] += 1
                        label = row["Label"].strip()
                        labels[label] += 1
                        if label.casefold() != "benign":
                            attack_labels[label] += 1
                    if timestamp and 1 <= timestamp.hour <= 5:
                        counts["early_clock_before_first_noon" if not noon_seen else "early_clock_after_first_noon"] += 1
                        if path.name == "Wednesday-workingHours.pcap_ISCX.csv" and not noon_seen:
                            wednesday_targets.add(line_no)
                    if timestamp and noon_seen and 8 <= timestamp.hour <= 11:
                        counts["morning_08_11_after_first_noon"] += 1
                per_file[path.name] = {
                    "source_rows": counts["source_rows"], "eligible_flows": counts["eligible_flows"],
                    "corrected": counts["corrected"], "preserved": counts["preserved"],
                    "ambiguous": counts["ambiguous"], "invalid": counts["invalid"],
                    "first_12h_line": first_noon_line,
                    "early_clock_01_05_before_first_12h": counts["early_clock_before_first_noon"],
                    "early_clock_01_05_after_first_12h": counts["early_clock_after_first_noon"],
                    "clock_08_11_after_first_12h": counts["morning_08_11_after_first_noon"],
                    "label_counts": dict(sorted(labels.items())),
                }
                for key in ("source_rows", "eligible_flows", "corrected", "preserved", "ambiguous", "invalid"):
                    total[key] += counts[key]
    wednesday_path = next(path for path in inputs if path.name == "Wednesday-workingHours.pcap_ISCX.csv")
    collect_wednesday_neighborhoods(wednesday_path, wednesday_targets, output_dir / "wednesday-pre-noon-neighborhoods.csv")
    observed = {
        "eligible_flows": total["eligible_flows"], "labelled_attacks": sum(attack_labels.values()),
        "attack_classes": len(attack_labels),
    }
    invariants = {
        key: {"expected": value, "observed": observed[key], "matches": value == observed[key]}
        for key, value in EXPECTED_INVARIANTS.items()
    }
    summary = {
        "schema_version": 1,
        "mode": "audit-only",
        "rules_version": rules.get("rules_version"),
        "rules_sha256": sha256_file(Path(rules["_path"])),
        "timezone_convention": rules.get("timezone_convention"),
        "bucket_seconds": bucket_seconds,
        "reports": {
            "full_audit_csv_gzip": report_path.name,
            "differences_csv": differences_path.name,
            "wednesday_neighborhoods_csv": "wednesday-pre-noon-neighborhoods.csv",
        },
        "files": per_file,
        "totals": dict(sorted(total.items())),
        "dataset_invariants": invariants,
        "wednesday_pre_first_12h_investigation": {
            "target_count": len(wednesday_targets),
            "target_lines": sorted(wednesday_targets),
            "scope": "Rows with raw 01h-05h timestamps before the first raw 12h observation; selection is evidence-only and is not a normalization rule.",
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-root", type=Path, required=True)
    parser.add_argument("--rules", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bucket-seconds", type=int, default=60)
    parser.add_argument("--mode", choices=("audit-only",), default="audit-only")
    args = parser.parse_args()
    try:
        if args.bucket_seconds < 1:
            raise ValueError("bucket seconds must be positive")
        rules = json.loads(args.rules.read_text(encoding="utf-8"))
        rules["_path"] = str(args.rules)
        summary = audit(discover_inputs(args.csv_root), rules, args.output_dir, args.bucket_seconds)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"mode": summary["mode"], "totals": summary["totals"], "invariants": summary["dataset_invariants"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


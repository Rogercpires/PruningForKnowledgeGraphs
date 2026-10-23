from __future__ import annotations

import csv
import gzip
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("temporal_audit", ROOT / "src" / "temporal_audit.py")
temporal_audit = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["temporal_audit"] = temporal_audit
SPEC.loader.exec_module(temporal_audit)


HEADER = [
    "Flow ID", "Source IP", "Source Port", "Destination IP", "Destination Port",
    "Protocol", "Timestamp", "Label",
]


def row(flow_id: str, timestamp: str, label: str = "BENIGN") -> list[str]:
    return [flow_id, "10.0.0.1", "1234", "10.0.0.2", "80", "6", timestamp, label]


class TemporalAuditTests(unittest.TestCase):
    def rules(self) -> dict:
        rules = json.loads((ROOT / "configs" / "temporal-normalization-rules.json").read_text(encoding="utf-8"))
        rules["_path"] = str(ROOT / "configs" / "temporal-normalization-rules.json")
        return rules

    def make_inputs(self, root: Path) -> list[Path]:
        rows_by_file = {name: [row("normal", "3/7/2017 09:00")] for name in temporal_audit.FILES}
        rows_by_file["Monday-WorkingHours.pcap_ISCX.csv"] = [
            row("mon-early", "03/07/2017 02:24:30"), row("mon-noon", "03/07/2017 12:00:00"),
            row("mon-morning-after-noon", "03/07/2017 08:55:58"),
        ]
        rows_by_file["Wednesday-workingHours.pcap_ISCX.csv"] = [
            row("wed-neighbor-before", "5/7/2017 8:42"), row("wed-early", "5/7/2017 1:43"),
            row("wed-neighbor-after", "5/7/2017 9:00"), row("wed-noon", "5/7/2017 12:00"),
            row("wed-morning-after-noon", "5/7/2017 10:00"),
        ]
        for name, rows in rows_by_file.items():
            with (root / name).open("w", encoding="cp1252" if "Thursday-WorkingHours-Morning" in name else "utf-8-sig", newline="") as sink:
                writer = csv.writer(sink)
                writer.writerow(HEADER)
                writer.writerows(rows)
        return temporal_audit.discover_inputs(root)

    def test_parser_keeps_local_civil_time_with_and_without_seconds(self) -> None:
        with_seconds = temporal_audit.parse_local_timestamp("03/07/2017 08:55:58")
        without_seconds = temporal_audit.parse_local_timestamp("5/7/2017 8:42")
        self.assertEqual(with_seconds, datetime(2017, 7, 3, 8, 55, 58))
        self.assertEqual(without_seconds, datetime(2017, 7, 5, 8, 42))
        self.assertIsNone(with_seconds.tzinfo)
        self.assertEqual(temporal_audit.local_bucket(with_seconds, 60), "2017-07-03T08:55:00")

    def test_configured_early_hours_are_corrected_without_using_label(self) -> None:
        first = temporal_audit.decision_for("Monday-WorkingHours.pcap_ISCX.csv", datetime(2017, 7, 3, 2, 24), self.rules())
        second = temporal_audit.decision_for("Wednesday-workingHours.pcap_ISCX.csv", datetime(2017, 7, 5, 1, 43), self.rules())
        self.assertEqual((first.classification, first.confidence, first.proposed.hour), ("corrected", "high", 14))
        self.assertEqual((second.classification, second.confidence, second.proposed.hour), ("corrected", "high", 13))
        afternoon = temporal_audit.decision_for("Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv", datetime(2017, 7, 7, 2, 24), self.rules())
        self.assertEqual((afternoon.classification, afternoon.confidence, afternoon.proposed.hour), ("corrected", "high", 14))

    def test_non_chronological_morning_record_is_preserved(self) -> None:
        decision = temporal_audit.decision_for("Monday-WorkingHours.pcap_ISCX.csv", datetime(2017, 7, 3, 8, 55, 58), self.rules())
        self.assertEqual(decision.classification, "preserved")
        self.assertEqual(decision.proposed, datetime(2017, 7, 3, 8, 55, 58))

    def test_wednesday_line_exceptions_do_not_use_label_or_row_order(self) -> None:
        dawn = temporal_audit.decision_for(
            "Wednesday-workingHours.pcap_ISCX.csv", datetime(2017, 7, 5, 2, 24), self.rules(),
            line_no=6560, timestamp_original="5/7/2017 2:24",
        )
        benign = temporal_audit.decision_for(
            "Wednesday-workingHours.pcap_ISCX.csv", datetime(2017, 7, 5, 1, 43), self.rules(),
            line_no=55504, timestamp_original="5/7/2017 1:43",
        )
        normal = temporal_audit.decision_for(
            "Wednesday-workingHours.pcap_ISCX.csv", datetime(2017, 7, 5, 2, 24), self.rules(),
            line_no=55505, timestamp_original="5/7/2017 2:24",
        )
        self.assertEqual((dawn.classification, dawn.confidence, dawn.proposed.hour), ("preserved", "provisional", 2))
        self.assertEqual((benign.classification, benign.confidence, benign.proposed), ("ambiguous", "ambiguous", None))
        self.assertEqual((normal.classification, normal.proposed.hour), ("corrected", 14))

    def test_audit_reports_wednesday_neighbors_and_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = self.make_inputs(root)
            first = temporal_audit.audit(inputs, self.rules(), root / "audit-one")
            second = temporal_audit.audit(inputs, self.rules(), root / "audit-two")
            self.assertEqual(first, second)
            self.assertEqual(first["mode"], "audit-only")
            self.assertEqual(first["files"]["Monday-WorkingHours.pcap_ISCX.csv"]["clock_08_11_after_first_12h"], 1)
            self.assertEqual(first["wednesday_pre_first_12h_investigation"]["target_count"], 1)
            self.assertEqual(
                gzip.decompress((root / "audit-one" / "temporal-audit.csv.gz").read_bytes()),
                gzip.decompress((root / "audit-two" / "temporal-audit.csv.gz").read_bytes()),
            )
            neighborhood = (root / "audit-one" / "wednesday-pre-noon-neighborhoods.csv").read_text(encoding="utf-8")
            self.assertIn("wed-early", neighborhood)
            self.assertIn("wed-neighbor-before", neighborhood)
            self.assertIn("wed-neighbor-after", neighborhood)


if __name__ == "__main__":
    unittest.main()

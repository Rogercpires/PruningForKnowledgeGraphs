import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from sparql_results import parse_json, parse_legacy_text, XSD


def select(kind="protocol", key="6", value="2"):
    grouping, counter = {"protocol": ("protocol", "flows"), "top": ("destination", "flows"), "hour": ("hour", "attacks")}[kind]
    return {"head": {"vars": [grouping, counter]}, "results": {"bindings": [{
        grouping: {"type": "uri" if kind == "top" else "literal", "value": key},
        counter: {"type": "literal", "datatype": XSD + "integer", "value": value}}]}}


class SelectTests(unittest.TestCase):
    def parse(self, obj, kind="protocol"):
        return parse_json(json.dumps(obj).encode(), kind)

    def test_valid_structured_counts_and_empty(self):
        self.assertEqual({"6": 2}, self.parse(select()))
        obj = select()
        obj["results"]["bindings"] = []
        self.assertEqual({}, self.parse(obj))

    def test_wrong_variables_missing_bindings_and_invalid_json(self):
        for obj in ({}, [], {"head": []}, {"head": {"vars": ["protocol", "flows"]}, "results": {}}):
            with self.assertRaises(ValueError):
                self.parse(obj)
        with self.assertRaises(ValueError):
            parse_json(b"Jena failed", "protocol")

    def test_invalid_numeric_literals(self):
        for v in ("-1", "NaN", "Infinity", "1.5", "1e2", "", "foo"):
            with self.assertRaises(ValueError):
                self.parse(select(value=v))

    def test_malformed_unbound_duplicate_and_string_count(self):
        obj = select()
        obj["results"]["bindings"].append(obj["results"]["bindings"][0])
        with self.assertRaises(ValueError):
            self.parse(obj)
        obj = select()
        del obj["results"]["bindings"][0]["flows"]
        with self.assertRaises(ValueError):
            self.parse(obj)
        obj = select()
        obj["results"]["bindings"][0]["flows"] = {"type": "literal", "value": "2"}
        with self.assertRaises(ValueError):
            self.parse(obj)

    def test_top_order_and_type_are_checked(self):
        self.assertEqual({"http://ip/a": 2}, self.parse(select("top", "http://ip/a"), "top"))
        obj = select("top", "http://ip/b")
        obj["results"]["bindings"] += select("top", "http://ip/a")["results"]["bindings"]
        with self.assertRaises(ValueError):
            self.parse(obj, "top")

    def test_invalid_hours_are_rejected(self):
        self.assertEqual({"2017-07-05T02": 2}, self.parse(select("hour", "2017-07-05T02"), "hour"))
        for key in ("2017-07-05T24", "2017-02-31T09", "2017-07-05T02Z"):
            with self.assertRaises(ValueError):
                self.parse(select("hour", key), "hour")

    def test_stored_count_zero_is_a_valid_count(self):
        obj = {"head": {"vars": ["stored_triples"]}, "results": {"bindings": [{"stored_triples": {"type": "literal", "datatype": XSD + "integer", "value": "0"}}]}}
        self.assertEqual({"default_graph": 0}, self.parse(obj, "stored"))
        obj["results"]["bindings"] = []
        with self.assertRaises(ValueError):
            self.parse(obj, "stored")

    def test_narrow_legacy_table_import_and_truncation(self):
        raw = b'----------------\n| protocol | flows |\n================\n| "6" | 2 |\n----------------\n'
        self.assertEqual({"6": 2}, parse_legacy_text(raw, "protocol"))
        for damaged in (raw[:-18], b"error\n" + raw, raw.replace(b'"6"', b'unbound')):
            with self.assertRaises(ValueError):
                parse_legacy_text(damaged, "protocol")


if __name__ == "__main__":
    unittest.main()

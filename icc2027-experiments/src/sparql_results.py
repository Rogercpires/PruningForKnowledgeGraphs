"""Strict SELECT result validation and a narrow, explicit historical text importer."""
from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

SCHEMAS = {"top": ("destination", "flows"), "protocol": ("protocol", "flows"),
           "hour": ("hour", "attacks"), "stored": (None, "stored_triples")}
XSD = "http://www.w3.org/2001/XMLSchema#"
INTEGER_TYPES = {XSD + s for s in ("integer", "int", "long", "short", "byte", "nonNegativeInteger", "positiveInteger", "unsignedLong", "unsignedInt", "decimal")}


def integer(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"\+?\d+(?:\.0+)?", value):
        raise ValueError(f"Not a nonnegative integer count: {value!r}")
    try:
        n = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Invalid numeric count") from exc
    return int(n)


def validate_rows(rows: list[tuple[str, int]], kind: str) -> dict[str, int]:
    result = {}
    for key, value in rows:
        if not key or key in result:
            raise ValueError(f"Empty or duplicate result key: {key!r}")
        if kind == "hour":
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}", key):
                raise ValueError("Invalid local civil hour")
            datetime.strptime(key, "%Y-%m-%dT%H")
        result[key] = value
    if kind == "top":
        if len(result) > 20 or list(result) != sorted(result, key=lambda k: (-result[k], k)):
            raise ValueError("Top-20 must use descending counts and lexical tie-breaking")
    return result


def parse_json(data: bytes, kind: str) -> dict[str, int]:
    key, value = SCHEMAS[kind]
    obj = json.loads(data.decode("utf-8-sig"))
    expected = [key, value] if key else [value]
    if not isinstance(obj, dict) or not isinstance(obj.get("head"), dict) or obj["head"].get("vars") != expected:
        raise ValueError(f"Unexpected SELECT variables; expected {expected}")
    body = obj.get("results")
    bindings = body.get("bindings") if isinstance(body, dict) else None
    if not isinstance(bindings, list):
        raise ValueError("Missing SELECT result bindings")
    rows = []
    for binding in bindings:
        if not isinstance(binding, dict) or set(binding) != set(expected):
            raise ValueError("Unbound or extra result columns")
        count = binding[value]
        if not isinstance(count, dict) or count.get("type") not in {"literal", "typed-literal"} or count.get("datatype") not in INTEGER_TYPES:
            raise ValueError("Counts must be typed integer-valued literals")
        v = integer(count.get("value"))
        if key:
            term = binding[key]
            if not isinstance(term, dict) or term.get("type") != ("uri" if kind == "top" else "literal"):
                raise ValueError("Unexpected grouping term type")
            k = term.get("value")
            if not isinstance(k, str):
                raise ValueError("Invalid grouping term")
        else:
            k = "default_graph"
        rows.append((k, v))
    if kind == "stored" and len(rows) != 1:
        raise ValueError("COUNT query must have exactly one result")
    return validate_rows(rows, kind)


def parse_legacy_text(data: bytes, kind: str) -> dict[str, int]:
    """Only the archived Jena two-column table shape, never arbitrary logs."""
    key, value = SCHEMAS[kind]
    lines = data.decode("utf-8-sig").strip().splitlines()
    if key is None or len(lines) < 4:
        raise ValueError("Unsupported historical table")
    if not re.fullmatch(r"-+", lines[0]) or not re.fullmatch(r"=+", lines[2]) or lines[-1] != lines[0]:
        raise ValueError("Incomplete Jena text table")
    def cells(line):
        if not line.startswith("|") or not line.endswith("|"):
            raise ValueError("Invalid table row")
        return [v.strip() for v in line[1:-1].split("|")]
    if cells(lines[1]) != [key, value]:
        raise ValueError("Historical table columns differ from query")
    rows = []
    for line in lines[3:-1]:
        c = cells(line)
        if len(c) != 2:
            raise ValueError("Invalid historical column count")
        term, n = c
        if kind == "top":
            if not term.startswith("<") or not term.endswith(">"):
                raise ValueError("Expected destination IRI")
            term = term[1:-1]
        else:
            term = json.loads(term)
            if not isinstance(term, str):
                raise ValueError("Expected string literal")
        rows.append((term, integer(n)))
    return validate_rows(rows, kind)

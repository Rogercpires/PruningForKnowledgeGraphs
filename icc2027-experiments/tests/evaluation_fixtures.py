"""Tiny synthetic manifests and answer tables; no network traffic dataset."""
import json
from pathlib import Path


def make_inputs(root: Path) -> tuple[Path, Path, Path]:
    runs, legacy = root / "inputs", root / "legacy"
    runs.mkdir()
    legacy.mkdir()
    policies = []
    for identity, label, config, labels in (
        ("baseline", "Baseline", {"technique": "baseline"}, {"BENIGN": 1, "Attack": 2}),
        ("sample", "Sampling", {"technique": "sample", "sample_probability": 0.5, "seed": 7}, {"Attack": 1}),
    ):
        run = runs / identity
        run.mkdir()
        manifest = {"run_id": identity, "config": config,
                    "input_csvs": [{"path": "fixture.csv", "sha256": "a" * 64, "bytes": 100}],
                    "temporal_interpretation": {"mode": "normalized", "ambiguous_policy": "exclude",
                                                "rules_version": "fixture", "rules_sha256": "b" * 64},
                    "metrics": {"selected_flows": sum(labels.values()), "emitted_triples": 10 * sum(labels.values()), "label_counts": labels}}
        (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        policies.append({"id": identity, "label": label, "manifest": f"{identity}/manifest.json", "tdb": identity})
        rows = {"top": [("http://ip/a", 2), ("http://ip/b", 1)] if identity == "baseline" else [("http://ip/b", 1)],
                "protocol": [("6", 2), ("17", 1)] if identity == "baseline" else [("6", 1)],
                "hour": [("2017-07-05T09", 2 if identity == "baseline" else 1)]}
        for kind, data in rows.items():
            k, v = {"top": ("destination", "flows"), "protocol": ("protocol", "flows"), "hour": ("hour", "attacks")}[kind]
            lines = ["----------------", f"| {k} | {v} |", "================"]
            lines += [f'| {"<" + a + ">" if kind == "top" else json.dumps(a)} | {b} |' for a, b in data]
            lines.append("----------------")
            (legacy / f"{identity}-{kind}.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    registry = root / "registry.json"
    registry.write_text(json.dumps({"schema_version": 1, "baseline_id": "baseline", "graph_scope": "default",
                                    "ontology_scope": "not_verified", "policies": policies}), encoding="utf-8")
    return registry, runs, legacy

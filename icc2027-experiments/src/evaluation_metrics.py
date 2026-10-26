"""Pure quality metrics. Ratios are fractions; only *_pct values are percentages.

See EVALUATION.md for populations, denominators and undefined cases. This module
does not inspect CSVs, regenerate RDF, or infer inclusion probabilities.
"""
from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence


def number(value: float) -> float:
    """Require a finite, nonnegative real count (booleans are not counts)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Counts must be real numbers")
    if not math.isfinite(value) or value < 0:
        raise ValueError("Counts must be finite and nonnegative")
    return value


def counts(values: Mapping[str, float]) -> dict[str, float]:
    if not isinstance(values, Mapping) or any(not isinstance(k, str) or not k for k in values):
        raise ValueError("Count keys must be nonempty strings")
    return {k: number(v) for k, v in values.items()}


def ratio(candidate: float, baseline: float) -> float | None:
    number(candidate)
    number(baseline)
    return candidate / baseline if baseline else None


def reduction_pct(baseline: float, candidate: float) -> float | None:
    retained = ratio(candidate, baseline)
    return 100 * (1 - retained) if retained is not None else None


def absolute_error(baseline: float, candidate: float) -> float:
    return abs(number(candidate) - number(baseline))


def relative_error(baseline: float, candidate: float) -> float | None:
    return ratio(absolute_error(baseline, candidate), baseline)


def normalized_l1(baseline: Mapping[str, float], candidate: Mapping[str, float]) -> float | None:
    """Sum absolute COUNT errors on union support / sum baseline counts."""
    a, b = counts(baseline), counts(candidate)
    return ratio(math.fsum(abs(a.get(k, 0) - b.get(k, 0)) for k in sorted(a.keys() | b.keys())), math.fsum(a.values()))


def tvd(baseline: Mapping[str, float], candidate: Mapping[str, float]) -> float | None:
    """Half L1 between separately normalized distributions; empty => None."""
    a, b = counts(baseline), counts(candidate)
    sa, sb = math.fsum(a.values()), math.fsum(b.values())
    if not sa or not sb:
        return None
    return 0.5 * math.fsum(abs(a.get(k, 0) / sa - b.get(k, 0) / sb) for k in sorted(a.keys() | b.keys()))


def class_preservation(baseline: Mapping[str, float], candidate: Mapping[str, float]) -> dict:
    """Inputs are ATTACK counts, excluding BENIGN. Entity identity is not implied."""
    a, b = counts(baseline), counts(candidate)
    classes = {k for k, v in a.items() if v > 0}
    retained = {k for k in classes if b.get(k, 0) > 0}
    return {
        "baseline_attack_count": sum(a.values()), "observed_attack_count": sum(b.values()),
        "attack_count_retention": ratio(sum(b.values()), sum(a.values())),
        "baseline_class_count": len(classes), "retained_class_count": len(retained),
        "attack_class_coverage": ratio(len(retained), len(classes)),
        "unexpected_classes": sorted(k for k, v in b.items() if v and k not in classes),
        "per_class": {k: {"baseline": a.get(k, 0), "observed": b.get(k, 0),
                          "retention": ratio(b.get(k, 0), a.get(k, 0))}
                      for k in sorted(a.keys() | b.keys())},
    }


def ranked(values: Mapping[str, float]) -> list[str]:
    return sorted(counts(values), key=lambda k: (-values[k], k))


def top_with_ties(values: Mapping[str, float], n: int) -> list[str]:
    """Requires the FULL ranking, not a LIMIT n query result."""
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")
    order = ranked(values)
    if len(order) <= n:
        return order
    threshold = values[order[n - 1]]
    return [k for k in order if values[k] >= threshold]


def compare_rankings(baseline: Mapping[str, float], candidate: Mapping[str, float], n: int = 20) -> dict:
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")
    a, b = ranked(baseline)[:n], ranked(candidate)[:n]
    pa, pb = {k: i + 1 for i, k in enumerate(a)}, {k: i + 1 for i, k in enumerate(b)}
    common, union = pa.keys() & pb.keys(), pa.keys() | pb.keys()
    return {
        "n": n, "baseline_size": len(a), "candidate_size": len(b),
        "overlap_count": len(common), "overlap": len(common) / n if a else None,
        "overlap_denominator": n, "overlap_definition": "fixed_requested_n",
        "baseline_recall": len(common) / len(a) if a else None,
        "baseline_recall_denominator": len(a),
        "same_ranking": a == b if a else None,
        "common_mean_position_shift": statistics.mean(abs(pa[k] - pb[k]) for k in common) if common else None,
        "union_mean_position_shift": statistics.mean(abs(pa.get(k, n + 1) - pb.get(k, n + 1)) for k in union) if a else None,
        "baseline_order": a, "candidate_order": b,
    }


def horvitz_thompson(observed: Mapping[str, float], probability: float, *, technique: str) -> dict:
    """Bernoulli count estimator only. Zero sampled counts do not imply certainty."""
    if technique != "sample":
        raise ValueError("Horvitz-Thompson is supported only for Bernoulli Sampling")
    p = number(probability)
    if not 0 < p <= 1:
        raise ValueError("Inclusion probability must be in (0, 1]")
    observed = counts(observed)
    if any(int(v) != v for v in observed.values()):
        raise ValueError("Observed Bernoulli counts must be integers")
    estimated = {k: v / p for k, v in observed.items()}
    variance = {k: (v * (1 - p) / p ** 2 if v or p == 1 else None) for k, v in observed.items()}
    return {"probability": p, "observed": observed, "estimated": estimated,
            "variance": variance, "standard_error": {k: math.sqrt(v) if v is not None else None for k, v in variance.items()}}


def describe(values: Sequence[float | None]) -> dict:
    """Describe defined observations; disclose missing values, no imputation."""
    present = [number(v) for v in values if v is not None]
    return {"n_total": len(values), "n_defined": len(present), "n_undefined": len(values) - len(present),
            "mean": statistics.mean(present) if present else None,
            "median": statistics.median(present) if present else None,
            "sample_stdev": statistics.stdev(present) if len(present) >= 2 else None,
            "min": min(present) if present else None, "max": max(present) if present else None}

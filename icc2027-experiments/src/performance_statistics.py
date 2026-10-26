"""Performance summaries without outlier removal or pooling of seed populations."""
import math
import statistics
from functools import lru_cache

from evaluation_metrics import describe


def _integral(f, a, b, tolerance=1e-11):
    def refine(a, b, fa, fm, fb, whole, tol, depth):
        m = (a + b) / 2
        l, r = f((a + m) / 2), f((m + b) / 2)
        left = (m - a) * (fa + 4 * l + fm) / 6
        right = (b - m) * (fm + 4 * r + fb) / 6
        delta = left + right - whole
        if abs(delta) <= 15 * tol:
            return left + right + delta / 15
        if depth == 0:
            raise ValueError("Student-t integration did not converge")
        return (refine(a, m, fa, l, fm, left, tol / 2, depth - 1) +
                refine(m, b, fm, r, fb, right, tol / 2, depth - 1))
    fa, fm, fb = f(a), f((a + b) / 2), f(b)
    return refine(a, b, fa, fm, fb, (b - a) * (fa + 4 * fm + fb) / 6, tolerance, 24)


@lru_cache(maxsize=256)
def student_t_critical(df: int, level: float = 0.95) -> float:
    """Invert the normalized t density numerically; central two-sided level."""
    if isinstance(df, bool) or not isinstance(df, int) or df < 1 or not 0.5 <= level <= 0.999:
        raise ValueError("Require df>=1 and confidence level in [0.5, 0.999]")
    normalizer = math.exp(math.lgamma((df + 1) / 2) - math.lgamma(df / 2)) / math.sqrt(df * math.pi)
    def pdf(x):
        return normalizer * math.exp(-(df + 1) / 2 * math.log1p(x * x / df))
    target, lower, upper = level / 2, 0.0, 1.0
    while _integral(pdf, 0, upper) < target:
        upper *= 2
    for _ in range(60):
        middle = (lower + upper) / 2
        if _integral(pdf, 0, middle) < target:
            lower = middle
        else:
            upper = middle
    return (lower + upper) / 2


def validate_ci(spec: dict):
    if not isinstance(spec, dict) or spec.get("method") not in {"none", "student_t"}:
        raise ValueError("Unknown confidence interval method")
    if spec["method"] == "student_t":
        if spec.get("assumptions") != "independent_normal_repetitions" or not str(spec.get("justification", "")).strip():
            raise ValueError("Student-t CI requires explicit assumptions and a justification")
        level = spec.get("level", 0.95)
        if isinstance(level, bool) or not isinstance(level, (int, float)) or not 0.5 <= level <= 0.999:
            raise ValueError("Unsupported confidence level")


def summarize_performance(values, ci=None):
    ci = ci or {"method": "none"}
    validate_ci(ci)
    result = describe(values)
    present = [v for v in values if v is not None]
    quartiles = statistics.quantiles(present, n=4, method="inclusive") if len(present) >= 2 else (present * 3 if present else [None] * 3)
    result.update(q1=quartiles[0], q3=quartiles[2],
                  iqr=quartiles[2] - quartiles[0] if present else None,
                  quantile_method="inclusive (linear interpolation, endpoints included)",
                  coefficient_of_variation=(result["sample_stdev"] / result["mean"] if result["sample_stdev"] is not None and result["mean"] else None),
                  values=list(values), confidence_interval=None, ci_spec=ci,
                  ci_unavailable_reason="not_requested" if ci["method"] == "none" else "fewer_than_two_defined_repetitions")
    if ci["method"] == "student_t" and len(present) >= 2:
        critical = student_t_critical(len(present) - 1, ci.get("level", 0.95))
        halfwidth = critical * result["sample_stdev"] / math.sqrt(len(present))
        result.update(confidence_interval=[result["mean"] - halfwidth, result["mean"] + halfwidth], ci_unavailable_reason=None)
    return result

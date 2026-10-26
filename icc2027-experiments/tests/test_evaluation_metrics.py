"""Hand-computable examples; no external datasets or Jena required."""
import math
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from evaluation_metrics import (absolute_error, class_preservation, compare_rankings, describe,
                                horvitz_thompson, normalized_l1, ranked, reduction_pct,
                                relative_error, top_with_ties, tvd)


class MetricTests(unittest.TestCase):
    def test_reduction_denominators_and_growth(self):
        self.assertEqual(75, reduction_pct(4, 1))
        self.assertEqual(-50, reduction_pct(2, 3))
        self.assertIsNone(reduction_pct(0, 0))
        self.assertEqual(100, reduction_pct(2, 0))

    def test_errors_small_counts(self):
        self.assertEqual(1, absolute_error(0, 1))
        self.assertIsNone(relative_error(0, 1))
        self.assertIsNone(relative_error(0, 0))
        self.assertEqual(0.5, relative_error(2, 1))

    def test_l1_counts_uses_union_and_baseline_population(self):
        self.assertEqual(1, normalized_l1({"a": 2}, {"a": 1, "b": 1}))
        self.assertEqual(1, normalized_l1({"a": 2}, {}))
        self.assertEqual(0.5, normalized_l1({"a": 2}, {"a": 1}))
        self.assertIsNone(normalized_l1({}, {"a": 1}))

    def test_tvd_equal_disjoint_and_scaled(self):
        self.assertEqual(0, tvd({"a": 2, "b": 2}, {"a": 1, "b": 1}))
        self.assertEqual(1, tvd({"a": 1}, {"b": 1}))
        self.assertEqual(0.5, tvd({"a": 1, "b": 1}, {"a": 1}))
        self.assertEqual(0, tvd({"a": 100}, {"a": 1}))

    def test_l1_on_proportions_is_twice_tvd(self):
        a, b = {"a": 0.75, "b": 0.25}, {"a": 0.25, "c": 0.75}
        self.assertEqual(normalized_l1(a, b), 2 * tvd(a, b))

    def test_empty_distributions_are_not_zero_error(self):
        for a, b in (({}, {}), ({"a": 0}, {"a": 1}), ({"a": 1}, {})):
            self.assertIsNone(tvd(a, b))

    def test_absent_classes_distinguish_instances_from_classes(self):
        result = class_preservation({"common": 99, "rare": 1}, {"common": 1})
        self.assertEqual(0.01, result["attack_count_retention"])
        self.assertEqual(0.5, result["attack_class_coverage"])
        self.assertEqual(0, result["per_class"]["rare"]["retention"])
        self.assertEqual([], result["unexpected_classes"])

    def test_unexpected_class_and_empty_class_denominators(self):
        result = class_preservation({}, {"new": 1})
        self.assertEqual(["new"], result["unexpected_classes"])
        self.assertIsNone(result["attack_class_coverage"])
        self.assertIsNone(result["per_class"]["new"]["retention"])

    def test_rank_ties_are_lexical(self):
        values = {"ip2": 3, "ip10": 3, "ip1": 4}
        self.assertEqual(["ip1", "ip10", "ip2"], ranked(values))
        self.assertEqual(["ip1", "ip10", "ip2"], top_with_ties(values, 2))

    def test_rank_no_overlap_penalizes_missing_on_union(self):
        result = compare_rankings({"a": 2, "b": 1}, {"c": 2, "d": 1}, 2)
        self.assertEqual(0, result["overlap_count"])
        self.assertIsNone(result["common_mean_position_shift"])
        self.assertEqual(1.5, result["union_mean_position_shift"])

    def test_rank_partial_overlap_and_short_candidate(self):
        result = compare_rankings({"a": 3, "b": 2, "c": 1}, {"c": 3, "a": 2}, 3)
        self.assertEqual(2 / 3, result["overlap"])
        self.assertEqual(1.5, result["common_mean_position_shift"])
        self.assertEqual(5 / 3, result["union_mean_position_shift"])
        self.assertFalse(result["same_ranking"])

    def test_rank_empty_baseline_is_undefined(self):
        self.assertIsNone(compare_rankings({}, {}, 20)["overlap"])
        self.assertEqual(0, compare_rankings({"a": 1}, {}, 20)["overlap"])
        with self.assertRaises(ValueError):
            compare_rankings({}, {}, 0)

    def test_ht_estimates_and_small_counts(self):
        result = horvitz_thompson({"a": 1, "missing": 0}, 0.1, technique="sample")
        self.assertEqual(10, result["estimated"]["a"])
        self.assertAlmostEqual(90, result["variance"]["a"])
        self.assertAlmostEqual(math.sqrt(90), result["standard_error"]["a"])
        self.assertIsNone(result["variance"]["missing"])
        self.assertEqual(0, result["estimated"]["missing"])
        self.assertEqual(0, horvitz_thompson({"a": 0}, 1, technique="sample")["variance"]["a"])

    def test_ht_rejects_other_policies_and_bad_probabilities(self):
        for technique in ("temporal", "labels", "topk", "aggregate", "baseline"):
            with self.assertRaises(ValueError):
                horvitz_thompson({"a": 1}, 0.01, technique=technique)
        for p in (0, -1, 1.1, float("nan"), True):
            with self.assertRaises(ValueError):
                horvitz_thompson({}, p, technique="sample")

    def test_ht_design_simulation_without_real_csvs(self):
        rng = random.Random(731)
        within = 0
        for _ in range(1000):
            n = sum(rng.random() < 0.5 for _ in range(1000))
            estimate = horvitz_thompson({"n": n}, 0.5, technique="sample")
            within += abs(estimate["estimated"]["n"] - 1000) <= 3 * estimate["standard_error"]["n"]
        self.assertGreaterEqual(within, 990)

    def test_statistics_known_sample(self):
        result = describe([1, 2, 3, None])
        self.assertEqual((2, 2, 1, 1, 3), tuple(result[k] for k in ("mean", "median", "sample_stdev", "min", "max")))
        self.assertEqual(1, result["n_undefined"])
        self.assertIsNone(describe([1])["sample_stdev"])
        self.assertIsNone(describe([])["mean"])

    def test_bad_counts_fail_explicitly(self):
        for value in (-1, float("nan"), float("inf"), True, "1"):
            with self.assertRaises(ValueError):
                normalized_l1({"a": value}, {})


if __name__ == "__main__":
    unittest.main()

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from performance_statistics import student_t_critical, summarize_performance


class PerformanceStatisticsTests(unittest.TestCase):
    def test_inclusive_quartiles_and_cv(self):
        r = summarize_performance([1, 2, 3, 4, 100])
        self.assertEqual((2, 4, 2), (r["q1"], r["q3"], r["iqr"]))
        self.assertEqual(100, r["max"])
        self.assertEqual([1, 2, 3, 4, 100], r["values"])
        self.assertEqual(r["sample_stdev"] / r["mean"], r["coefficient_of_variation"])
        self.assertIsNone(r["confidence_interval"])

    def test_empty_singleton_zero_and_missing(self):
        self.assertIsNone(summarize_performance([])["q1"])
        r = summarize_performance([2])
        self.assertEqual(0, r["iqr"])
        self.assertIsNone(r["sample_stdev"])
        self.assertIsNone(summarize_performance([0, 0])["coefficient_of_variation"])
        self.assertEqual(1, summarize_performance([1, None])["n_undefined"])

    def test_student_t_against_reference_critical_values(self):
        self.assertAlmostEqual(12.706204736, student_t_critical(1), places=7)
        self.assertAlmostEqual(2.776445105, student_t_critical(4), places=7)
        self.assertAlmostEqual(2.262157163, student_t_critical(9), places=7)

    def test_ci_opt_in_requires_justification_and_keeps_negative_limit(self):
        spec = {"method": "student_t", "assumptions": "independent_normal_repetitions", "justification": "Synthetic normal-population example", "level": .95}
        r = summarize_performance([1, 2, 3, 4, 5], spec)
        halfwidth = 2.776445105 * math.sqrt(2.5 / 5)
        self.assertAlmostEqual(3 - halfwidth, r["confidence_interval"][0], places=7)
        self.assertLess(summarize_performance([0, 1], spec)["confidence_interval"][0], 0)
        self.assertIsNone(summarize_performance([1], spec)["confidence_interval"])
        with self.assertRaises(ValueError):
            summarize_performance([1, 2], {"method": "student_t"})


if __name__ == "__main__":
    unittest.main()

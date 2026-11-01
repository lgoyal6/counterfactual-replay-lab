import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from counterfactual_replay_lab.core import RefundAgent


def load_script(name):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = load_script("generate_replay_cases")
benchmark = load_script("benchmark_replay")


class BenchmarkCaseTests(unittest.TestCase):
    def read_cases(self, cases):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cases.json"
            path.write_text(json.dumps(cases))
            return benchmark.load_cases(path)

    def test_large_cohort_is_unique_and_frozen_without_consulting_agent(self):
        with patch.object(RefundAgent, "apply_policy", side_effect=AssertionError("oracle leaked")):
            cases = generator.generate_cases(10000, 20261101)
        self.assertEqual(cases, generator.generate_cases(10000, 20261101))
        self.assertEqual(len(self.read_cases(cases)), 10000)
        self.assertEqual(sum(c["expected_decision"] == "approve_refund" for c in cases), 5000)
        self.assertEqual(sum(c["expected_decision"] == "deny_refund" for c in cases), 3500)
        self.assertEqual(sum(c["expected_decision"] == "review_refund" for c in cases), 1500)
        self.assertNotEqual(cases, generator.generate_cases(10000, 20261102))

    def test_duplicate_id_or_request_is_rejected_before_scoring(self):
        cases = generator.generate_cases(20, 7)
        for field in ("scenario_id", "request"):
            duplicate = [dict(case) for case in cases]
            duplicate[1][field] = duplicate[0][field]
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "distinct"):
                self.read_cases(duplicate)

    def test_empty_or_invalid_cohort_is_rejected(self):
        for cases in ([], {}, [{"scenario_id": "missing-fields"}]):
            with self.subTest(cases=cases), self.assertRaises((ValueError, TypeError)):
                self.read_cases(cases)
        with self.assertRaises(ValueError):
            generator.generate_cases(21, 7)

    def test_incorrect_policy_lowers_measured_accuracy(self):
        cases = generator.generate_cases(20, 7)
        correct = benchmark.summarize(benchmark.study(cases, repetitions=1, warmup=0))
        self.assertEqual((correct["baseline_correct"], correct["suffix_correct"]), (10, 20))
        # A broken policy must be visible in the results, rather than rewriting labels.
        controls = [case for case in cases if case["eligibility"] != "eligible"]
        with patch.object(RefundAgent, "apply_policy", return_value="review_refund"):
            result = benchmark.summarize(benchmark.study(controls, repetitions=1, warmup=0))
        self.assertEqual(result["suffix_correct"], 3)

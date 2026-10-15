"""Reject scalar trace edits that disagree with the captured execution spans.

core.py hashes the span sequence and replay state for retained trace identity.
Derived decision and scoring fields must agree before comparison can recommend KEEP.
"""
from dataclasses import replace
import unittest

from counterfactual_replay_lab.core import RefundAgent, Trace, validate_trace


class TraceIntegrityTests(unittest.TestCase):
    def test_decision_and_score_cannot_disagree_with_captured_spans(self):
        trace = RefundAgent().run("Order 1042 is eligible for a refund", "v3")
        self.assertEqual(trace.decision, "deny_refund")
        tampered = replace(trace, decision="approve_refund", verdict="PASS", score=1.0)
        with self.assertRaisesRegex(ValueError, "decision|suffix|spans"):
            validate_trace(tampered)

    def test_original_trace_remains_valid(self):
        trace = RefundAgent().run("Order 1042 is eligible for a refund", "v2")
        self.assertEqual(validate_trace(trace), trace.replay_state)

    def test_execution_and_measurement_edits_require_a_new_checksum(self):
        trace = RefundAgent().run("Order 1042 is eligible for a refund", "v2")
        for fields in (
            {"executed_tools": ("apply_policy", "local_refund")},
            {"ledger_after": (("forged", "effect"),)},
            {"latency_ms": 999999},
            {"cost_usd": 12.34},
        ):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                Trace.from_dict(replace(trace, **fields).to_dict())

    def test_recomputed_checksum_does_not_bypass_fixture_consistency(self):
        trace = RefundAgent().run("Order 1042 is eligible for a refund", "v2")
        for fields in (
            {"executed_tools": ("external_refund",)},
            {"ledger_after": ()},
            {"latency_ms": -1},
            {"latency_ms": True},
            {"cost_usd": 12.34},
        ):
            modified = replace(trace, **fields)
            modified = replace(modified, content_checksum=modified.checksum)
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                Trace.from_dict(modified.to_dict())

    def test_nonfinite_timing_is_refused(self):
        trace = RefundAgent().run("Order 1042 is eligible for a refund", "v2")
        for timing in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(timing=timing), self.assertRaises(ValueError):
                Trace.from_dict(replace(trace, latency_ms=timing).to_dict())

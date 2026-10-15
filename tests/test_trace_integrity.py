"""Reject scalar trace edits that disagree with the captured execution spans.

core.py hashes the span sequence and replay state for retained trace identity.
Derived decision and scoring fields must agree before comparison can recommend KEEP.
"""
from dataclasses import replace
import unittest

from counterfactual_replay_lab.core import RefundAgent, validate_trace


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

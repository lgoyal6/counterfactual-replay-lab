import unittest
from counterfactual_replay_lab.core import RefundAgent, ReplayEngine, compare, run_mvp


class ReplayTests(unittest.TestCase):
    def test_mvp_preserves_request_prefix_and_improves_verdict(self):
        baseline, counterfactual, experiment = run_mvp()
        self.assertEqual(baseline.verdict, "FAIL")
        self.assertEqual(counterfactual.verdict, "PASS")
        self.assertTrue(experiment.same_request)
        self.assertTrue(experiment.same_prefix)
        self.assertEqual(experiment.first_divergence, "apply_policy")
        self.assertEqual(experiment.recommendation, "KEEP")


    def test_replay_rejects_unsupported_variable(self):
        baseline = RefundAgent().run("Order 1042 is eligible for a refund", "v1")
        with self.assertRaisesRegex(ValueError, "Only policy_version"):
            ReplayEngine(RefundAgent().run).replay(
                baseline, changed_variable="model", new_value="model-b"
            )


    def test_comparison_rejects_changed_request(self):
        agent = RefundAgent()
        baseline = agent.run("Order 1042 is eligible for a refund", "v1")
        changed = agent.run("Order 999 is eligible for a refund", "v2")
        with self.assertRaisesRegex(ValueError, "different requests"):
            compare(baseline, changed, changed_variable="policy_version", old_value="v1", new_value="v2")


if __name__ == "__main__":
    unittest.main()

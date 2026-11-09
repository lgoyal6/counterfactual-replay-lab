"""First divergence is deterministic, causal and includes added/removed nodes."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from counterfactual_replay_lab.engine import capture, replay
from counterfactual_replay_lab.explain import compare_runs
from counterfactual_replay_lab.store import RunStore
from counterfactual_replay_lab.workflows import REFUND_ENV, refund_graph

SRC = str(Path(__file__).resolve().parents[1] / "src")


class Divergence(unittest.TestCase):
    def setUp(self):
        self.store = RunStore()
        self.snap, self.base = capture(self.store, refund_graph(), REFUND_ENV, "req-1042")

    def explain(self, graph, snapshot=None):
        r = replay(self.store, self.base.run_id, graph, snapshot or self.snap)
        return compare_runs(self.store, self.base.run_id, r.run_id)

    def test_policy_change_diverges_at_route_and_swaps_effects(self):
        out = self.explain(refund_graph(policy={"auto_approve_max_cents": 10000}))
        first = out["first_divergence"]
        self.assertEqual(first["node"], "route")
        self.assertIn("params changed: auto_approve_max_cents 20000 -> 10000", first["reasons"][0])
        kinds = {n["node"]: n["kind"] for n in out["nodes"]}
        self.assertEqual(kinds["issue_refund"], "deactivated")
        self.assertEqual(kinds["open_review"], "activated")
        self.assertEqual(kinds["eligibility"], "unchanged")
        self.assertEqual([e["intent"] for e in out["effect_diff"]["added"]],
                         ['{"kind":"review_ticket","queue":"refunds","reason":"manual","target":"1042"}'])
        self.assertEqual(len(out["effect_diff"]["removed"]), 1)
        reasons = {n["node"]: n["reasons"] for n in out["nodes"]}
        self.assertTrue(any("control edge from route" in r for r in reasons["open_review"]))

    def test_observation_edit_explains_captured_response(self):
        snap2 = self.snap.with_observation("order_lookup", {"found": True, "amount_cents": 12900,
                                                            "days_since_delivery": 40, "category": "apparel",
                                                            "delivered": True})
        out = self.explain(refund_graph(), snap2)
        self.assertEqual(out["first_divergence"]["node"], "order_lookup")
        self.assertIn("captured observation edited", out["first_divergence"]["reasons"])
        kinds = {n["node"]: n["kind"] for n in out["nodes"]}
        self.assertEqual(kinds["send_denial"], "activated")

    def test_added_node_is_reported_not_ignored(self):
        out = self.explain(refund_graph(fraud_hold=10))
        added = [n for n in out["nodes"] if n["kind"] == "added"]
        self.assertEqual([n["node"] for n in added], ["fraud_hold"])
        self.assertEqual(out["first_divergence"]["node"], "fraud_hold")
        self.assertIn("node added in replay graph version", added[0]["reasons"])

    def test_no_change_has_no_divergence(self):
        out = self.explain(refund_graph(policy={"risk_rounds": 20000}))
        self.assertIsNone(out["first_divergence"])

    def test_explanation_is_byte_identical_across_processes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "e.db")
            store = RunStore(db)
            snap, base = capture(store, refund_graph(), REFUND_ENV, "req-1042")
            r = replay(store, base.run_id, refund_graph(risk_version="v2", policy={"max_risk": 20}), snap)
            store.close()
            code = ("import json,sys;from counterfactual_replay_lab.store import RunStore;"
                    "from counterfactual_replay_lab.explain import compare_runs;"
                    f"print(json.dumps(compare_runs(RunStore({db!r}), {base.run_id!r}, {r.run_id!r}), sort_keys=True))")
            outs = {subprocess.check_output([sys.executable, "-c", code], text=True,
                                            env=dict(os.environ, PYTHONPATH=SRC, PYTHONHASHSEED=seed))
                    for seed in ("0", "7", "99")}
            self.assertEqual(len(outs), 1)
            self.assertEqual(json.loads(outs.pop())["first_divergence"]["node"], "risk_check")


if __name__ == "__main__":
    unittest.main()

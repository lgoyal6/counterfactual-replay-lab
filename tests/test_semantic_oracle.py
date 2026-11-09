"""Semantic oracle: independent expectations, full-rerun equivalence and mutants.

Three independent references: the original fixed-prefix RefundAgent (three
eligibility states), hand-written refund business rules (refund_expectations)
and the recursive full-rerun oracle. Every mutant must be caught.
"""
import itertools
import json
import unittest

from counterfactual_replay_lab.core import RefundAgent, ReplayEngine as PrefixReplay
from counterfactual_replay_lab.dag import Graph, Guard, NodeSpec
from counterfactual_replay_lab.engine import MODES, Engine, capture, replay
from counterfactual_replay_lab.mutants import MUTANTS
from counterfactual_replay_lab.oracle import check_equivalence, full_rerun
from counterfactual_replay_lab.store import RunStore
from counterfactual_replay_lab.workflows import DEFAULT_POLICY, REFUND_ENV, compat_graph, refund_graph

from refund_expectations import expected_outcome, intents_to_effects


class FixedPrefixCompatibility(unittest.TestCase):
    """Milestone 1: the graph engine reproduces the existing three-state replay."""

    EXPECTED = {  # independent table: (eligibility, policy) -> (decision, refund effects)
        ("eligible", "v1"): ("deny_refund", 0), ("eligible", "v2"): ("approve_refund", 1),
        ("eligible", "v3"): ("deny_refund", 0), ("ineligible", "v1"): ("deny_refund", 0),
        ("ineligible", "v2"): ("deny_refund", 0), ("ineligible", "v3"): ("deny_refund", 0),
        ("unknown", "v1"): ("review_refund", 0), ("unknown", "v2"): ("review_refund", 0),
        ("unknown", "v3"): ("review_refund", 0),
    }

    def test_three_states_match_prefix_replay_and_independent_table(self):
        agent = RefundAgent()
        for eligibility, policy in itertools.product(("eligible", "ineligible", "unknown"), ("v2", "v3")):
            with self.subTest(eligibility=eligibility, policy=policy):
                request = (f"Order 7 is {eligibility} for a refund" if eligibility != "unknown"
                           else "Ambiguous order request")
                baseline = agent.run(request, "v1")
                legacy = PrefixReplay(agent.run).replay(baseline, changed_variable="policy_version", new_value=policy)
                store = RunStore()
                env = {"eligibility": {request: eligibility}}
                snap, base = capture(store, compat_graph(request, "v1"), env, request)
                result = replay(store, base.run_id, compat_graph(request, policy), snap)
                records = store.records(result.run_id)
                decision = json.loads(store.blob(records["apply_policy"][2]))
                self.assertEqual(decision, legacy.decision)
                self.assertEqual((decision, len(store.ledger(result.run_id))), self.EXPECTED[(eligibility, policy)])
                self.assertEqual(len(store.ledger(result.run_id)), len(legacy.ledger_after))
                # captured lookup is reused, never re-run, exactly like the fixed prefix
                self.assertEqual(records["lookup_order"][3], "reused")
                base_decision = json.loads(store.blob(store.records(base.run_id)["apply_policy"][2]))
                self.assertEqual(base_decision, baseline.decision)


def scenario_grid():
    policies = [
        {}, {"auto_approve_max_cents": 10000}, {"window_days": 10}, {"approve_eligible": False},
        {"excluded_categories": []}, {"max_risk": 20}, {"restocking_fee_pct": 0, "cap_cents": 9000},
    ]
    for order_id, customer_id, policy, risk, hold in itertools.product(
            sorted(REFUND_ENV["orders"]) + ["9999"], ["c-77", "c-12", "c-missing"], policies,
            ("v1", "v2"), (None, 20)):
        yield order_id, customer_id, policy, risk, hold


class RefundScenarioOracle(unittest.TestCase):
    """Each scenario: capture the default policy, then replay a changed policy/version/graph."""

    def test_incremental_and_full_agree_with_independent_rules(self):
        checked = 0
        captured = {}
        store = RunStore()
        for order_id, customer_id, policy, risk, hold in scenario_grid():
            key = (order_id, customer_id)
            if key not in captured:
                captured[key] = capture(store, refund_graph(order_id, customer_id), REFUND_ENV,
                                        f"req-{order_id}-{customer_id}")
            snap, base = captured[key]
            graph = refund_graph(order_id, customer_id, policy, risk, hold)
            oracle = full_rerun(json.loads(graph.to_json()), json.loads(snap.to_json()))
            expected = expected_outcome(REFUND_ENV["orders"].get(order_id), REFUND_ENV["customers"].get(customer_id),
                                        dict(DEFAULT_POLICY, **policy), risk, hold, order_id)
            for mode in ("incremental", "full"):
                with self.subTest(order=order_id, customer=customer_id, policy=policy, risk=risk, hold=hold, mode=mode):
                    r = replay(store, base.run_id, graph, snap, mode)
                    self.assertEqual(check_equivalence(store, r.run_id, oracle), [])
                    decision = json.loads(store.blob(store.records(r.run_id)["decision"][2]))
                    self.assertEqual(decision["decision"], expected["decision"])
                    self.assertEqual(decision["amount_cents"], expected["amount_cents"])
                    self.assertEqual(intents_to_effects(store.ledger(r.run_id).values()), expected["effects"])
                    checked += 1
        self.assertEqual(checked, 6 * 3 * 7 * 2 * 2 * 2)

    def test_hand_computed_literal_cases(self):
        # Written by hand from REFUND_ENV, independent of both expectation code and engine.
        cases = [
            (("1042", "c-77", {}, "v1", None), ("approve_refund", {("refund", "1042", 11610)})),
            (("1042", "c-77", {"auto_approve_max_cents": 10000}, "v1", None),
             ("manual_review", {("review_ticket", "1042", "manual")})),
            (("2001", "c-77", {}, "v1", None), ("manual_review", {("review_ticket", "2001", "manual")})),
            (("3003", "c-77", {}, "v1", None), ("deny_refund", {("denial_notice", "3003", "ineligible")})),
            (("5005", "c-77", {}, "v1", None), ("review_refund", {("review_ticket", "5005", "review")})),
            (("1042", "c-12", {}, "v1", None), ("manual_review", {("review_ticket", "1042", "manual")})),
            (("1042", "c-77", {}, "v1", 10), ("hold_refund", {("refund", "1042", 11610)})),
        ]
        store = RunStore()
        for (order_id, customer_id, policy, risk, hold), (decision, effects) in cases:
            with self.subTest(order=order_id, customer=customer_id, policy=policy, hold=hold):
                snap, base = capture(store, refund_graph(order_id, customer_id), REFUND_ENV, f"req-{order_id}")
                r = replay(store, base.run_id, refund_graph(order_id, customer_id, policy, risk, hold), snap)
                self.assertEqual(json.loads(store.blob(store.records(r.run_id)["decision"][2]))["decision"], decision)
                self.assertEqual(intents_to_effects(store.ledger(r.run_id).values()), effects)


class MutantsAreCaught(unittest.TestCase):
    """Negative controls: each broken engine must disagree with the oracle somewhere."""

    def control_graph(self, invert, salt=1, version="v1", obs_key="s"):
        def w(id_, inputs, guards=(), s=1, v="v1"):
            return NodeSpec(id_, "syn.work", v, {"salt": s, "rounds": 5, "out_bytes": 8}, tuple(inputs),
                            tuple(guards), output_schema="hex")
        return Graph([
            NodeSpec("s", "syn.obs", "v1", {"key": obs_key}, effect="observation", output_schema="hex"),
            w("a", ["s"], s=salt, v=version),
            NodeSpec("g", "syn.gate", "v1", {"modulus": 2, "threshold": 1, "invert": invert}, ("s",),
                     output_schema="bool"),
            w("then", ["s"], [Guard("g", (True,))], s=3), w("else", ["s"], [Guard("g", (False,))], s=4),
            NodeSpec("e_then", "syn.effect", "v1", {"target": "t1"}, ("then",), (Guard("g", (True,)),),
                     "effect_intent", "intent"),
            NodeSpec("e_a", "syn.effect", "v1", {"target": "t2"}, ("a",), (), "effect_intent", "intent"),
            NodeSpec("m", "syn.merge", "v1", {"salt": 0, "rounds": 5, "out_bytes": 8}, ("a", "then", "else"),
                     output_schema="hex")])

    CHANGES = {
        "param": lambda self: (self.control_graph(False, salt=2), None),
        "op_version": lambda self: (self.control_graph(False, version="v2"), None),
        "gate_flip": lambda self: (self.control_graph(True), None),
        "observation": lambda self: (self.control_graph(False), "observation"),
        "unrelated": lambda self: (self.control_graph(False).replace_nodes(
            NodeSpec("z", "syn.source", "v1", {"salt": 1}, output_schema="hex")), None),
    }

    def outcomes(self, engine_cls):
        caught = {}
        for name, make in self.CHANGES.items():
            store = RunStore()
            env = {"syn": {"s": "12" * 8}}
            snap, base = capture(store, self.control_graph(False), env, "req-1", engine_cls=engine_cls)
            graph, edit = make(self)
            if edit == "observation":
                snap2 = snap.with_observation("s", "34" * 8)
            else:
                snap2 = snap
            engine = engine_cls(store)
            result = engine.execute(engine.plan_replay(base.run_id, graph, snap2))
            oracle = full_rerun(json.loads(graph.to_json()), json.loads(snap2.to_json()))
            caught[name] = bool(check_equivalence(store, result.run_id, oracle))
        return caught

    def test_correct_engine_passes_every_change(self):
        self.assertEqual(self.outcomes(Engine), {k: False for k in self.CHANGES})

    def test_every_mutant_is_detected_by_some_change(self):
        expected_detectors = {
            "stale_key_ignores_spec": {"param", "op_version"},
            "node_id_only_key": {"param", "op_version", "observation"},
            "key_ignores_guards": {"gate_flip"},
            "key_ignores_observation": {"observation"},
            "omit_control_edges": {"gate_flip"},
            "skip_reused_effects": {"param", "op_version", "gate_flip", "unrelated"},
        }
        for name, detectors in expected_detectors.items():
            with self.subTest(mutant=name):
                caught = self.outcomes(MUTANTS[name])
                self.assertTrue(detectors <= {k for k, v in caught.items() if v}, caught)
                self.assertFalse(caught["unrelated"] and name != "skip_reused_effects")

    def test_independent_rules_catch_a_bug_shared_by_engine_and_oracle(self):
        """If an operation is wrong, oracle equivalence passes but the rule table fails."""
        from counterfactual_replay_lab import ops
        original = ops.REGISTRY[("refund.amount", "v1")]
        broken = ops.OpDef(original.name, original.version, original.kind, original.schema,
                           lambda p, i, c: original.fn(dict(p, restocking_fee_pct=0), i, c))
        ops.REGISTRY[("refund.amount", "v1")] = broken
        try:
            store = RunStore()
            snap, base = capture(store, refund_graph(), REFUND_ENV, "req-1042")
            g = refund_graph(policy={"max_risk": 60})
            r = replay(store, base.run_id, g, snap)
            oracle = full_rerun(json.loads(g.to_json()), json.loads(snap.to_json()))
            self.assertEqual(check_equivalence(store, r.run_id, oracle), [])  # both arms share the bug
            expected = expected_outcome(REFUND_ENV["orders"]["1042"], REFUND_ENV["customers"]["c-77"],
                                        dict(DEFAULT_POLICY, max_risk=60))
            self.assertNotEqual(intents_to_effects(store.ledger(r.run_id).values()), expected["effects"])
        finally:
            ops.REGISTRY[("refund.amount", "v1")] = original


if __name__ == "__main__":
    unittest.main()

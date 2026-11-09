"""DAG contracts: deterministic order, cycle rejection and binding cache keys."""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from counterfactual_replay_lab.dag import Graph, GraphError, Guard, NodeSpec
from counterfactual_replay_lab.engine import Engine
from counterfactual_replay_lab.store import RunStore
from counterfactual_replay_lab.workflows import refund_graph

SRC = str(Path(__file__).resolve().parents[1] / "src")


def n(id_, inputs=(), guards=(), op="syn.work", version="v1", **params):
    return NodeSpec(id_, op, version, params or {"salt": 1}, tuple(inputs), tuple(guards), output_schema="hex")


class OrderingAndValidation(unittest.TestCase):
    def test_order_is_topological_and_tie_broken_by_id(self):
        g = Graph([n("d", ["b", "c"]), n("c", ["a"]), n("b", ["a"]), n("a")])
        self.assertEqual(g.order, ("a", "b", "c", "d"))
        g2 = Graph([n("a"), n("b", ["a"]), n("c", ["a"]), n("d", ["b", "c"])])
        self.assertEqual(g.order, g2.order)
        self.assertEqual(g.graph_hash, g2.graph_hash)

    def test_order_and_hash_identical_across_processes_and_hash_seeds(self):
        code = ("from counterfactual_replay_lab.workflows import refund_graph;g=refund_graph();"
                "print(g.graph_hash, ','.join(g.order))")
        outputs = set()
        for seed in ("0", "1", "12345"):
            env = dict(os.environ, PYTHONPATH=SRC, PYTHONHASHSEED=seed)
            outputs.add(subprocess.check_output([sys.executable, "-c", code], env=env, text=True))
        self.assertEqual(len(outputs), 1)

    def test_cycles_are_rejected_before_execution(self):
        with self.assertRaisesRegex(GraphError, "Cycle detected: a -> a"):
            Graph([n("a", ["a"])])
        with self.assertRaisesRegex(GraphError, "Cycle detected"):
            Graph([n("a", ["c"]), n("b", ["a"]), n("c", ["b"]), n("root")])
        # control edges participate in cycle detection too
        with self.assertRaisesRegex(GraphError, "Cycle detected"):
            Graph([n("a", ["b"]), n("b", [], [Guard("a", (True,))])])

    def test_missing_duplicate_invalid_and_oversized_are_rejected(self):
        for bad, pattern in [
            ([n("a", ["ghost"])], "missing node"),
            ([n("a"), n("a")], "Duplicate"),
            ([n("bad id")], "Invalid node id"),
            ([NodeSpec("a", "syn.work", "v1", {"x": float("nan")})], "canonical"),
            ([NodeSpec("a", "syn.work", "v1", {"x": "y" * 5000})], "exceed"),
            ([NodeSpec("a", "syn.work", "v1", effect="external_write")], "effect kind"),
            ([n("a"), n("b", ["a", "a"])], "repeats"),
            ([n("a"), n("b", [], [Guard("a", ())])], "admits nothing"),
            ([], "no nodes"),
        ]:
            with self.subTest(pattern=pattern), self.assertRaisesRegex(GraphError, pattern):
                Graph(bad)

    def test_graph_is_immutable_and_mutation_is_detected(self):
        g = refund_graph()
        with self.assertRaises(AttributeError):
            g.order = ()
        g.nodes["eligibility"].params["window_days"] = 999  # sneaky in-place edit
        with self.assertRaisesRegex(GraphError, "changed after it was frozen"):
            g.verify_frozen()

    def test_json_roundtrip_preserves_identity(self):
        g = refund_graph(fraud_hold=40)
        self.assertEqual(Graph.from_json(g.to_json()).graph_hash, g.graph_hash)
        with self.assertRaises(GraphError):
            Graph.from_json('{"format":1,"nodes":[{"id":"a","op":"x","op_version":"v1","oops":1}]}')

    def test_descendants_follow_data_and_control_edges_only_forward(self):
        g = Graph([n("a"), n("b", ["a"]), n("gate", ["a"], op="syn.gate"),
                   n("c", [], [Guard("gate", (True,))]), n("d", ["c"]), n("z")])
        self.assertEqual(g.descendants({"gate"}), {"gate", "c", "d"})
        self.assertEqual(g.descendants({"a"}), {"a", "b", "gate", "c", "d"})
        self.assertEqual(g.descendants({"z"}), {"z"})


class KeyBinding(unittest.TestCase):
    """Every field able to change an output changes the cache key."""

    def setUp(self):
        self.engine = Engine(RunStore())
        self.g = Graph([n("a"), n("b", ["a"]), n("gate", ["a"], op="syn.gate"),
                        n("c", ["b"], [Guard("gate", (True,))])], "env-1")

    def key(self, g, name="c", inputs=("h1",), guards=("h2",), obs=None, request="r1"):
        return self.engine.node_key(g, g.nodes[name], request, list(inputs), list(guards), obs)

    def test_each_identity_field_changes_the_key(self):
        base = self.key(self.g)
        c = self.g.nodes["c"]
        variants = {
            "op_version": self.g.replace_nodes(NodeSpec("c", c.op, "v2", c.params, c.inputs, c.guards,
                                                        c.effect, c.output_schema)),
            "params": self.g.replace_nodes(NodeSpec("c", c.op, c.op_version, {"salt": 2}, c.inputs, c.guards,
                                                    c.effect, c.output_schema)),
            "guard_values": self.g.replace_nodes(NodeSpec("c", c.op, c.op_version, c.params, c.inputs,
                                                          (Guard("gate", (False,)),), c.effect, c.output_schema)),
            "schema": self.g.replace_nodes(NodeSpec("c", c.op, c.op_version, c.params, c.inputs, c.guards,
                                                    c.effect, "any")),
            "env": self.g.replace_nodes(env_version="env-2"),
        }
        keys = {name: self.key(g) for name, g in variants.items()}
        keys["input_value"] = self.key(self.g, inputs=("h1x",))
        keys["guard_state"] = self.key(self.g, guards=("inactive",))
        keys["observation"] = self.key(self.g, obs="o1")
        keys["request"] = self.key(self.g, request="r2")
        for name, value in keys.items():
            with self.subTest(field=name):
                self.assertNotEqual(value, base)
        self.assertEqual(len(set(keys.values())), len(keys))

    def test_input_order_is_part_of_identity(self):
        g1 = Graph([n("a"), n("b"), n("c", ["a", "b"])])
        g2 = Graph([n("a"), n("b"), n("c", ["b", "a"])])
        self.assertNotEqual(g1.spec_hashes["c"], g2.spec_hashes["c"])


if __name__ == "__main__":
    unittest.main()

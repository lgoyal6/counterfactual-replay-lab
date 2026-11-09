"""Affected-subgraph replay reuses exactly the safe nodes and recomputes the rest."""
import json
import unittest

from counterfactual_replay_lab.dag import Graph, Guard, NodeSpec
from counterfactual_replay_lab.engine import (MODES, CacheCorruption, Engine, PlanError, capture, replay)
from counterfactual_replay_lab.oracle import check_equivalence, full_rerun
from counterfactual_replay_lab.store import RunStore, StoreError

ENV = {"syn": {"s": "aa" * 8, "s2": "bb" * 8, "o2": "cc" * 8, "ob": "dd" * 8}}


def w(id_, inputs=(), guards=(), salt=1, version="v1", rounds=10):
    return NodeSpec(id_, "syn.work", version, {"salt": salt, "rounds": rounds, "out_bytes": 8},
                    tuple(inputs), tuple(guards), output_schema="hex")


def src(id_, key):
    return NodeSpec(id_, "syn.obs", "v1", {"key": key}, effect="observation", output_schema="hex")


def gate(id_, inp, invert=False, threshold=1, modulus=2):
    return NodeSpec(id_, "syn.gate", "v1", {"modulus": modulus, "threshold": threshold, "invert": invert},
                    (inp,), output_schema="bool")


def merge(id_, inputs):
    return NodeSpec(id_, "syn.merge", "v1", {"salt": 0, "rounds": 10, "out_bytes": 8}, tuple(inputs),
                    output_schema="hex")


def effect(id_, inp, guards=(), target="t"):
    return NodeSpec(id_, "syn.effect", "v1", {"target": target}, (inp,), tuple(guards), "effect_intent", "intent")


class ReplayCase(unittest.TestCase):
    def setUp(self):
        self.store = RunStore()

    def run_case(self, graph, new_graph=None, new_snapshot=None, modes=MODES[1:]):
        snap, base = capture(self.store, graph, ENV, "req-1")
        self.assertIn(base.status, ("completed", "completed_with_failures"))
        new_graph = new_graph or graph
        new_snapshot = new_snapshot(snap) if callable(new_snapshot) else (new_snapshot or snap)
        oracle = full_rerun(json.loads(new_graph.to_json()), json.loads(new_snapshot.to_json()))
        results = {}
        for mode in modes:
            result = replay(self.store, base.run_id, new_graph, new_snapshot, mode)
            self.assertEqual(check_equivalence(self.store, result.run_id, oracle), [], mode)
            results[mode] = result
        self.base = base
        return results

    def sources(self, run_id, kind):
        return {k for k, v in self.store.records(run_id).items() if v[3] == kind and v[0] != "blocked"}

    def executed(self, result):
        return {k for k, v in self.store.records(result.run_id).items() if v[3] == "executed"}

    def reused(self, result):
        return {k for k, v in self.store.records(result.run_id).items() if v[3] in ("reused", "cutoff")}


class TopologyFixtures(ReplayCase):
    def test_diamond_param_change(self):
        g = Graph([src("s", "s"), w("a", ["s"]), w("b", ["s"], salt=2), w("j", ["a", "b"]), w("t", ["j"])])
        g2 = g.replace_nodes(w("a", ["s"], salt=99))
        r = self.run_case(g, g2)["incremental"]
        self.assertEqual(self.executed(r), {"a", "j", "t"})
        self.assertEqual(self.reused(r), {"s", "b"})
        self.assertEqual(r.counters["executed"], 3)

    def test_fanout_fanin_single_branch(self):
        mids = [w(f"m{i}", ["s"], salt=i) for i in range(5)]
        g = Graph([src("s", "s"), *mids, merge("f", [m.id for m in mids])])
        g2 = g.replace_nodes(w("m3", ["s"], salt=77))
        r = self.run_case(g, g2)["incremental"]
        self.assertEqual(self.executed(r), {"m3", "f"})
        self.assertEqual(self.reused(r), {"s", "m0", "m1", "m2", "m4"})

    def test_disconnected_component_untouched(self):
        g = Graph([src("s1", "s"), w("x1", ["s1"]), w("y1", ["x1"]),
                   src("s2", "s2"), w("x2", ["s2"]), w("y2", ["x2"])])
        r = self.run_case(g, g.replace_nodes(w("x2", ["s2"], salt=5)))["incremental"]
        self.assertEqual(self.executed(r), {"x2", "y2"})
        self.assertEqual(self.reused(r), {"s1", "x1", "y1", "s2"})

    def control_graph(self, invert):
        return Graph([src("s", "s"), w("w1", ["s"]), w("w2", ["w1"]), gate("g", "w2", invert),
                      w("then", ["w2"], [Guard("g", (True,))], salt=3),
                      w("else", ["w2"], [Guard("g", (False,))], salt=4),
                      merge("m", ["then", "else"]), effect("eff", "then", [Guard("g", (True,))]),
                      w("tail", ["m"])])

    def test_late_control_change_activates_and_deactivates_branches(self):
        g = self.control_graph(False)
        snap, base = capture(self.store, g, ENV, "req-1")
        before = {k: v[0] for k, v in self.store.records(base.run_id).items()}
        g2 = self.control_graph(True)
        oracle = full_rerun(json.loads(g2.to_json()), json.loads(snap.to_json()))
        r = replay(self.store, base.run_id, g2, snap)
        self.assertEqual(check_equivalence(self.store, r.run_id, oracle), [])
        after = {k: v[0] for k, v in self.store.records(r.run_id).items()}
        self.assertNotEqual(before["then"], after["then"])
        self.assertNotEqual(before["else"], after["else"])
        self.assertEqual({before["then"], after["then"]}, {"ok", "inactive"})
        # data ancestors of the branch nodes are unchanged; their control edge is not
        self.assertEqual(self.reused(r), {"s", "w1", "w2"})
        self.assertIn("then", self.executed(r) | {k for k, v in self.store.records(r.run_id).items()
                                                   if v[0] == "inactive"})
        self.assertNotIn("then", {k for k, v in self.store.records(r.run_id).items() if v[3] == "cutoff"})
        self.assertEqual(len(self.store.ledger(base.run_id)) + len(self.store.ledger(r.run_id)), 1)

    def test_early_cutoff_when_recomputed_output_is_identical(self):
        g = self.control_graph(False)
        # threshold 1 -> 1 with modulus 2 is a no-op spec change: gate reruns, output identical
        g2 = g.replace_nodes(NodeSpec("g", "syn.gate", "v1", {"modulus": 2, "threshold": 1, "invert": False,
                                                              "note": "reviewed"}, ("w2",), output_schema="bool"))
        results = self.run_case(g, g2)
        inc, nocut = results["incremental"], results["incremental_nocutoff"]
        self.assertEqual(self.executed(inc), {"g"})
        self.assertGreater(inc.counters["cutoff"], 0)
        self.assertGreater(nocut.counters["executed"], inc.counters["executed"])

    def test_graph_addition_and_removal(self):
        g = Graph([src("s", "s"), w("a", ["s"]), w("j", ["a"]), w("t", ["j"]), w("u", ["s"], salt=8)])
        added = g.replace_nodes(w("x", ["a"], salt=50), w("j", ["x"]))
        r = self.run_case(g, added)["incremental"]
        self.assertEqual(self.executed(r), {"x", "j", "t"})
        self.assertEqual(self.reused(r), {"s", "a", "u"})
        removed = g.replace_nodes(w("j", ["s"]), remove=["a"])
        r2 = self.run_case(g, removed)["incremental"]
        self.assertEqual(self.executed(r2), {"j", "t"})
        self.assertNotIn("a", self.store.records(r2.run_id))

    def test_operation_version_change(self):
        g = Graph([src("s", "s"), w("a", ["s"]), w("b", ["a"]), w("c", ["s"], salt=3)])
        r = self.run_case(g, g.replace_nodes(w("a", ["s"], version="v2")))["incremental"]
        self.assertEqual(self.executed(r), {"a", "b"})

    def test_observation_edit_recomputes_downstream(self):
        g = Graph([src("s", "s"), w("a", ["s"]), src("o", "s2"), w("b", ["o"])])
        r = self.run_case(g, new_snapshot=lambda snap: snap.with_observation("s", "ee" * 8))["incremental"]
        self.assertEqual(self.executed(r), {"s", "a"})
        self.assertEqual(r.counters["observation_reads"], 1)
        self.assertEqual(self.reused(r), {"o", "b"})


class FailClosed(ReplayCase):
    def test_upstream_change_into_observation_fails_closed(self):
        g = Graph([src("s", "s"), w("a", ["s"]), NodeSpec("o2", "syn.obs", "v1", {"key": "o2"}, ("a",),
                   effect="observation", output_schema="hex"), w("t", ["o2"])])
        results = self.run_case(g, g.replace_nodes(w("a", ["s"], salt=5)))
        rec = self.store.records(results["incremental"].run_id)
        self.assertEqual(rec["o2"][0], "failed")
        self.assertTrue(rec["o2"][4].startswith("MissingObservation"))
        self.assertEqual(rec["t"][0], "blocked")
        self.assertEqual(results["incremental"].status, "completed_with_failures")

    def test_activated_branch_with_uncaptured_observation_fails_closed(self):
        def graph(invert):
            return Graph([src("s", "s"), gate("g", "s", invert),
                          NodeSpec("ob", "syn.obs", "v1", {"key": "ob"}, ("s",), (Guard("g", (True,)),),
                                   "observation", "hex"),
                          NodeSpec("ob2", "syn.obs", "v1", {"key": "ob"}, ("s",), (Guard("g", (False,)),),
                                   "observation", "hex"),
                          merge("m", ["ob", "ob2"])])
        results = self.run_case(graph(False), graph(True))
        rec = self.store.records(results["incremental"].run_id)
        failed = {k for k, v in rec.items() if v[0] == "failed"}
        self.assertEqual(len(failed), 1)
        self.assertTrue(all(rec[k][4].startswith("MissingObservation") for k in failed))

    def test_dynamic_dependency_discovery_is_rejected(self):
        g = Graph([src("s", "s"), NodeSpec("d", "syn.discover", "v1", {"wants": "x"}, ("s",), output_schema="hex")])
        snap, base = capture(self.store, g, ENV, "req-1")
        rec = self.store.records(base.run_id)
        self.assertEqual(rec["d"][0], "failed")
        self.assertIn("DynamicMutationRejected", rec["d"][4])

    def test_corrupted_cache_entry_fails_closed(self):
        g = Graph([src("s", "s"), w("a", ["s"]), w("b", ["a"]), w("c", ["b"])])
        snap, base = capture(self.store, g, ENV, "req-1")
        result_hash = self.store.records(base.run_id)["b"][2]
        self.store.conn.execute("UPDATE blobs SET body=? WHERE hash=?", ('"00"', result_hash))
        with self.assertRaises(CacheCorruption):
            replay(self.store, base.run_id, g.replace_nodes(w("c", ["b"], salt=9)), snap)

    def test_tampered_snapshot_or_graph_body_fails_closed(self):
        g = Graph([src("s", "s"), w("a", ["s"])])
        snap, base = capture(self.store, g, ENV, "req-1")
        run = self.store.run(base.run_id)
        self.store.conn.execute("UPDATE snapshots SET body=replace(body, 'aaaa', 'abab') WHERE snapshot_hash=?",
                                (run["snapshot_hash"],))
        with self.assertRaises(CacheCorruption):
            replay(self.store, base.run_id, g, snap)

    def test_refusals_before_execution(self):
        g = Graph([src("s", "s"), w("a", ["s"])])
        snap, base = capture(self.store, g, ENV, "req-1")
        from counterfactual_replay_lab.snapshot import Snapshot
        other = Snapshot("req-OTHER", snap.observations, snap.ledger)
        with self.assertRaisesRegex(PlanError, "request identity"):
            replay(self.store, base.run_id, g, other)
        with self.assertRaises(PlanError):
            replay(self.store, base.run_id, g, snap, "capture")
        with self.assertRaises(StoreError):
            replay(self.store, "r99999-missing", g, snap)

    def test_terminal_runs_are_immutable_evidence(self):
        g = Graph([src("s", "s"), w("a", ["s"]), effect("e", "a")])
        snap, base = capture(self.store, g, ENV, "req-1")
        for sql in ("UPDATE nodes SET result_hash='x' WHERE run_id=?", "DELETE FROM nodes WHERE run_id=?",
                    "UPDATE runs SET status='running' WHERE run_id=?", "DELETE FROM effects WHERE run_id=?",
                    "INSERT INTO effects VALUES (?, 'k', 'v', NULL, 'run')"):
            with self.subTest(sql=sql), self.assertRaisesRegex(Exception, "immutable|append-only|evidence"):
                self.store.conn.execute(sql, (base.run_id,))

    def test_incompatible_store_schema_is_refused(self):
        import tempfile, os, sqlite3
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "s.db")
            RunStore(path).close()
            conn = sqlite3.connect(path)
            conn.execute("UPDATE meta SET v='replay-store-v0' WHERE k='schema'")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(StoreError, "Incompatible"):
                RunStore(path)

    def test_idempotency_conflict_with_captured_ledger_fails_node(self):
        from counterfactual_replay_lab.engine import effect_key
        from counterfactual_replay_lab.snapshot import Snapshot
        g = Graph([src("s", "s"), w("a", ["s"]), effect("e", "a", target="t9")])
        snap, base = capture(self.store, g, ENV, "req-1")
        intent = json.loads(list(self.store.ledger(base.run_id).values())[0])
        conflicting = Snapshot(snap.request_id, snap.observations,
                               ((effect_key("req-1", "e", intent), '{"kind":"syn_effect","target":"t9","value":"x"}'),))
        oracle = full_rerun(json.loads(g.to_json()), json.loads(conflicting.to_json()))
        r = replay(self.store, base.run_id, g, conflicting)
        self.assertEqual(check_equivalence(self.store, r.run_id, oracle), [])
        self.assertTrue(self.store.records(r.run_id)["e"][4].startswith("IdempotencyConflict"))
        self.assertEqual(r.counters["effect_conflicts"], 1)


if __name__ == "__main__":
    unittest.main()

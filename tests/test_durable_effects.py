"""Crash, cancellation and restart keep the isolated ledger exact.

Crash tests run the replay in a child process that calls os._exit at a hook,
then reopen the SQLite store in this process and resume the run.
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from counterfactual_replay_lab.dag import Graph, Guard, NodeSpec
from counterfactual_replay_lab.engine import Engine, PlanError, capture
from counterfactual_replay_lab.oracle import check_equivalence, full_rerun
from counterfactual_replay_lab.store import RunStore
from counterfactual_replay_lab.workflows import REFUND_ENV, refund_graph

SRC = str(Path(__file__).resolve().parents[1] / "src")


def w(id_, inputs=(), guards=(), salt=1, rounds=10, op="syn.work", seconds=None):
    params = {"salt": salt, "rounds": rounds, "out_bytes": 8} if seconds is None else {"seconds": seconds}
    return NodeSpec(id_, op, "v1", params, tuple(inputs), tuple(guards), output_schema="hex")


def effect(id_, inp, target):
    return NodeSpec(id_, "syn.effect", "v1", {"target": target}, (inp,), (), "effect_intent", "intent")


def effect_graph(salt=1):
    return Graph([NodeSpec("s", "syn.obs", "v1", {"key": "s"}, effect="observation", output_schema="hex"),
                  w("a", ["s"], salt=salt), effect("e1", "a", "t1"), w("b", ["a"]), effect("e2", "b", "t2"),
                  w("c", ["s"], salt=3), effect("e3", "c", "t3"), w("d", ["b", "c"])])


ENV = {"syn": {"s": "ab" * 8}}


class CrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "replay.db")
        store = RunStore(self.db)
        self.snapshot, self.base = capture(store, effect_graph(), ENV, "req-1")
        store.close()

    def tearDown(self):
        self.tmp.cleanup()

    def crash(self, graph, event, node, extra_env=None, engine="engine"):
        gpath, spath = os.path.join(self.tmp.name, "g.json"), os.path.join(self.tmp.name, "s.json")
        Path(gpath).write_text(graph.to_json())
        Path(spath).write_text(self.snapshot.to_json())
        env = dict(os.environ, PYTHONPATH=SRC, COMMIT_EVERY="2", **(extra_env or {}))
        proc = subprocess.run([sys.executable, "-m", "counterfactual_replay_lab.crashchild", self.db,
                               self.base.run_id, gpath, spath, event, node, "incremental", engine],
                              env=env, capture_output=True, text=True, timeout=60)
        return proc

    def reopen_and_resume(self, graph, engine_cls=Engine):
        store = RunStore(self.db)
        pending = [r for r in store.runs() if r["status"] not in ("completed", "completed_with_failures")]
        self.assertEqual(len(pending), 1)
        run_id = pending[0]["run_id"]
        result = engine_cls(store).resume(run_id)
        oracle = full_rerun(json.loads(graph.to_json()), json.loads(self.snapshot.to_json()))
        return store, run_id, result, check_equivalence(store, run_id, oracle)

    def test_crash_points_never_duplicate_or_lose_effects(self):
        changed = effect_graph(salt=2)   # a, e1, b, e2, d affected; c, e3 reused
        for event, node in [("before_result_write", "a"), ("before_result_write", "e1"),
                            ("after_effect_commit", "e1"), ("after_effect_commit", "e2"),
                            ("before_result_write", "e3"), ("after_effect_commit", "e3"),
                            ("after_flush", "*"), ("before_run_complete", "*")]:
            with self.subTest(event=event, node=node):
                proc = self.crash(changed, event, node)
                self.assertEqual(proc.returncode, 77, proc.stderr)
                store, run_id, result, problems = self.reopen_and_resume(changed)
                try:
                    self.assertEqual(problems, [])
                    self.assertEqual(result.status, "completed")
                    ledger = store.ledger_rows(run_id)
                    self.assertEqual(len([r for r in ledger if r[3] == "run"]), 3)
                    self.assertEqual(len({r[1] for r in ledger}), 3)
                    if event == "after_effect_commit":
                        self.assertGreaterEqual(result.counters["adopted_after_restart"], 1)
                finally:
                    store.close()

    def test_non_atomic_mutant_duplicates_after_the_same_crash(self):
        changed = effect_graph(salt=2)
        proc = self.crash(changed, "after_effect_commit", "e1", engine="non_atomic_effects")
        self.assertEqual(proc.returncode, 77, proc.stderr)
        from counterfactual_replay_lab.mutants import NonAtomicEffects
        store, run_id, result, problems = self.reopen_and_resume(changed, NonAtomicEffects)
        try:
            self.assertTrue(any("ledger" in p for p in problems), problems)
        finally:
            store.close()

    def test_crash_while_stopped_by_cancellation_then_resume(self):
        changed = effect_graph(salt=2)
        proc = self.crash(changed, "after_effect_commit", "e1", {"CRASH_AFTER_CANCEL": "1"})
        self.assertEqual(proc.returncode, 78, proc.stderr)
        self.assertIn("stopped", proc.stdout)
        store, run_id, result, problems = self.reopen_and_resume(changed)
        try:
            self.assertEqual(problems, [])
            self.assertEqual(len(store.ledger(run_id)), 3)
        finally:
            store.close()


class Cancellation(unittest.TestCase):
    def setUp(self):
        self.store = RunStore()

    def test_stop_between_nodes_and_resume(self):
        g = effect_graph()
        snap, base = capture(self.store, g, ENV, "req-1")
        cancel = threading.Event()
        calls = []

        def hook(event, node):
            if event == "after_effect_commit":
                calls.append(node)
                cancel.set()
        engine = Engine(self.store, hooks=hook, cancel=cancel)
        result = engine.execute(engine.plan_replay(base.run_id, effect_graph(2), snap, "full"))
        self.assertEqual(result.status, "stopped")
        self.assertEqual(self.store.run(result.run_id)["status"], "stopped")
        self.assertEqual(len(self.store.ledger(result.run_id)), 1)
        done = Engine(self.store).resume(result.run_id)
        self.assertEqual(done.status, "completed")
        oracle = full_rerun(json.loads(effect_graph(2).to_json()), json.loads(snap.to_json()))
        self.assertEqual(check_equivalence(self.store, result.run_id, oracle), [])
        with self.assertRaises(PlanError):
            Engine(self.store).resume(result.run_id)

    def slow_graph(self, op, seconds):
        return Graph([NodeSpec("s", "syn.obs", "v1", {"key": "s"}, effect="observation", output_schema="hex"),
                      w("slow", ["s"], op=op, seconds=seconds), w("after", ["slow"]), effect("e", "after", "t")])

    def test_cooperative_long_tool_acknowledges_cancellation(self):
        snap, base = capture(self.store, self.slow_graph("syn.slow", 0.01), ENV, "req-1")
        g2 = self.slow_graph("syn.slow", 5.0)
        cancel = threading.Event()
        engine = Engine(self.store, cancel=cancel)
        plan = engine.plan_replay(base.run_id, g2, snap)
        threading.Timer(0.2, cancel.set).start()
        started = time.monotonic()
        result = engine.execute(plan)
        latency = time.monotonic() - started
        self.assertEqual(result.status, "cancelled_in_node")
        self.assertEqual(result.detail["node"], "slow")
        self.assertLess(latency, 1.0)
        self.assertNotIn("slow", self.store.records(result.run_id))
        self.assertEqual(self.store.ledger(result.run_id), {})

    def test_non_cooperative_tool_is_reported_uncertain_not_stopped(self):
        snap, base = capture(self.store, self.slow_graph("syn.stubborn", 0.01), ENV, "req-1")
        g2 = self.slow_graph("syn.stubborn", 1.5)
        cancel = threading.Event()
        engine = Engine(self.store, cancel=cancel, tool_timeout=5.0)
        plan = engine.plan_replay(base.run_id, g2, snap)
        threading.Timer(0.2, cancel.set).start()
        result = engine.execute(plan)
        self.assertEqual(result.status, "uncertain")
        self.assertEqual(result.detail["node"], "slow")
        self.assertNotIn("slow", self.store.records(result.run_id))
        resumed = Engine(self.store, tool_timeout=5.0).resume(result.run_id)
        self.assertEqual(resumed.status, "completed")
        self.assertEqual(len(self.store.ledger(result.run_id)), 1)

    def test_bounded_timeout_fails_node_without_marking_stopped(self):
        snap, base = capture(self.store, self.slow_graph("syn.stubborn", 0.01), ENV, "req-1")
        engine = Engine(self.store, tool_timeout=0.2)
        result = engine.execute(engine.plan_replay(base.run_id, self.slow_graph("syn.stubborn", 2.0), snap))
        rec = self.store.records(result.run_id)
        self.assertEqual(result.status, "completed_with_failures")
        self.assertTrue(rec["slow"][4].startswith("Timeout"))
        self.assertEqual(rec["after"][0], "blocked")
        self.assertEqual(self.store.ledger(result.run_id), {})

    def test_db_cancel_flag_from_another_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "r.db")
            store = RunStore(db)
            snap, base = capture(store, effect_graph(), ENV, "req-1")
            other = RunStore(db)

            def hook(event, node):
                if event == "after_effect_commit" and node == "e1":
                    run_id = [r for r in other.runs() if r["status"] == "running"][0]["run_id"]
                    other.request_cancel(run_id)
            engine = Engine(store, hooks=hook, poll_db_cancel=True)
            result = engine.execute(engine.plan_replay(base.run_id, effect_graph(2), snap, "full"))
            self.assertEqual(result.status, "stopped")
            other.close()
            store.close()


class RefundReplayDurability(unittest.TestCase):
    def test_refund_effect_is_never_duplicated_across_reused_and_recomputed_runs(self):
        store = RunStore()
        snap, base = capture(store, refund_graph(), REFUND_ENV, "req-1042")
        # risk params change only: the approval branch is reused, its refund intent re-committed once
        r = Engine(store).execute(Engine(store).plan_replay(base.run_id, refund_graph(policy={"max_risk": 55}), snap))
        ledger = store.ledger_rows(r.run_id)
        self.assertEqual(len(ledger), 1)
        self.assertEqual(json.loads(ledger[0][1])["kind"], "refund")
        self.assertEqual(store.records(r.run_id)["issue_refund"][3], "cutoff")


if __name__ == "__main__":
    unittest.main()

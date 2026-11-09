"""Deliberately broken engines used as negative controls.

Each mutant removes one correctness ingredient. The semantic oracle and the
independent scenario expectations must catch every one of them; a mutant that
survives a workload means that workload cannot detect that class of bug.
"""
from __future__ import annotations

import os

from .dag import Graph
from .engine import Engine, _Execution, effect_key
from .replay_state import digest


class StaleKeyIgnoresSpec(Engine):
    """Cache key omits op, op version and params: version/policy edits hit stale entries."""
    def node_key(self, graph, node, request_id, in_hashes, guard_hashes, obs_hash):
        return digest({"id": node.id, "inputs": in_hashes, "guards": guard_hashes, "obs": obs_hash})


class NodeIdOnlyKey(Engine):
    """Cache key is the node id: every affected node is 'reused'."""
    def node_key(self, graph, node, request_id, in_hashes, guard_hashes, obs_hash):
        return digest({"id": node.id})


class KeyIgnoresGuards(Engine):
    """Cache key omits control-edge state: a flipped branch reuses old activation."""
    def node_key(self, graph, node, request_id, in_hashes, guard_hashes, obs_hash):
        return digest({"spec": graph.spec_hashes[node.id], "inputs": in_hashes, "obs": obs_hash})


class KeyIgnoresObservation(Engine):
    """Cache key omits the captured observation hash: edited tool responses are ignored."""
    def node_key(self, graph, node, request_id, in_hashes, guard_hashes, obs_hash):
        return digest({"spec": graph.spec_hashes[node.id], "inputs": in_hashes, "guards": guard_hashes})


class OmitControlEdges(Engine):
    """Invalidation follows data edges only; control-dependent nodes keep stale results."""
    def affected(self, graph: Graph, seeds):
        data_succ: dict[str, list[str]] = {n: [] for n in graph.nodes}
        for n, node in graph.nodes.items():
            for p in node.inputs:
                data_succ[p].append(n)
        out, stack = set(), list(seeds)
        while stack:
            n = stack.pop()
            if n in out:
                continue
            out.add(n)
            stack.extend(data_succ[n])
        return out

    def plan_replay(self, base_run, graph, snapshot, mode="incremental"):
        # Without early cutoff the stale result is reused verbatim.
        return super().plan_replay(base_run, graph, snapshot, "incremental_nocutoff" if mode == "incremental" else mode)


class _SkipReusedEffectsExec(_Execution):
    def commit_reused_effect(self, name):
        status, key, result_hash, _source, _error = self.base[name]
        self.record(name, status, key, result_hash, "reused")  # trusts cached 'effect succeeded'


class SkipReusedEffects(Engine):
    """Treats a reused effect intent as already applied; the isolated ledger misses it."""
    def _run(self, run_id, plan, started, resumed):
        return _SkipReusedEffectsExec(self, run_id, plan, started, resumed).run()


class _NonAtomicEffectsExec(_Execution):
    def commit_effect(self, name, key, result_hash, body, source):
        import json
        intent = json.loads(body)
        idem = digest({"k": effect_key(self.request_id, name, intent), "nonce": os.urandom(8).hex()})
        self.flush()
        self.store.begin()
        self.store.conn.execute("INSERT INTO effects VALUES (?, ?, ?, ?, 'run')", (self.run_id, idem, body, name))
        self.store.commit()
        self.counters["effects_committed"] += 1
        self.e.hooks("after_effect_commit", name)
        self.store.begin()
        self.store.conn.execute("INSERT OR IGNORE INTO blobs VALUES (?, ?)", (result_hash, body))
        self.store.conn.execute("INSERT INTO nodes VALUES (?, ?, 'ok', ?, ?, ?, NULL)",
                                (self.run_id, name, key, result_hash, source))
        self.store.commit()
        self.state[name] = ("ok", key, result_hash, source, None)


class NonAtomicEffects(Engine):
    """Commits the effect and the node record separately with a non-idempotent key."""
    def _run(self, run_id, plan, started, resumed):
        return _NonAtomicEffectsExec(self, run_id, plan, started, resumed).run()


MUTANTS = {
    "stale_key_ignores_spec": StaleKeyIgnoresSpec,
    "node_id_only_key": NodeIdOnlyKey,
    "key_ignores_guards": KeyIgnoresGuards,
    "key_ignores_observation": KeyIgnoresObservation,
    "omit_control_edges": OmitControlEdges,
    "skip_reused_effects": SkipReusedEffects,
    "non_atomic_effects": NonAtomicEffects,
}

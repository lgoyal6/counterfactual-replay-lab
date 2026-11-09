"""Explain how a replay run differs from its original, in causal order.

compare_runs() walks the union of both graph versions in a deterministic
topological order and classifies every node: unchanged, changed output,
activated, deactivated, added, removed, newly failed or recovered. The first
semantic difference is reported with its causes: the node's own spec fields,
its captured observation, its changed data inputs or its changed control edge.
"""
from __future__ import annotations

import heapq
from typing import Any

from .dag import Graph
from .snapshot import Snapshot
from .store import RunStore


def _load(store: RunStore, run_id: str) -> tuple[dict, Graph, Snapshot, dict]:
    run = store.run(run_id)
    graph = Graph.from_json(store.graph_body(run["graph_hash"]))
    snapshot = Snapshot.from_json(store.snapshot_body(run["snapshot_hash"]))
    return run, graph, snapshot, store.records(run_id)


def causal_order(a: Graph, b: Graph) -> list[str]:
    """Kahn over the union of both versions' edges, ties by id; cycles fall back to b then a."""
    names = set(a.nodes) | set(b.nodes)
    preds: dict[str, set[str]] = {n: set() for n in names}
    for g in (a, b):
        for n, node in g.nodes.items():
            preds[n].update(node.predecessors)
    succ: dict[str, list[str]] = {n: [] for n in names}
    indeg = {n: len(p) for n, p in preds.items()}
    for n, ps in preds.items():
        for p in ps:
            succ[p].append(n)
    ready = [n for n, d in indeg.items() if d == 0]
    heapq.heapify(ready)
    out = []
    while ready:
        n = heapq.heappop(ready)
        out.append(n)
        for s in succ[n]:
            indeg[s] -= 1
            if indeg[s] == 0:
                heapq.heappush(ready, s)
    if len(out) != len(names):
        return list(b.order) + [n for n in a.order if n not in b.nodes]
    return out


def _spec_changes(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    reasons = []
    for field in ("op", "op_version", "effect", "output_schema"):
        if old[field] != new[field]:
            reasons.append(f"{field} {old[field]} -> {new[field]}")
    keys = sorted(set(old["params"]) | set(new["params"]))
    changed = [k for k in keys if old["params"].get(k) != new["params"].get(k)]
    if changed:
        reasons.append("params changed: " + ", ".join(
            f"{k} {old['params'].get(k)!r} -> {new['params'].get(k)!r}" for k in changed))
    if old["inputs"] != new["inputs"]:
        reasons.append(f"data inputs {old['inputs']} -> {new['inputs']}")
    if old["guards"] != new["guards"]:
        reasons.append("control guards changed")
    return reasons


def compare_runs(store: RunStore, original: str, replay: str, value_preview: bool = True) -> dict[str, Any]:
    run_a, ga, sa, ra = _load(store, original)
    run_b, gb, sb, rb = _load(store, replay)
    changed_obs = sa.diff(sb)
    env_changed = ga.env_version != gb.env_version
    nodes = []
    first = None
    semantic_change: dict[str, bool] = {}

    def preview(result_hash):
        if not value_preview or result_hash is None:
            return None
        body = store.blob(result_hash)
        return body if len(body) <= 160 else body[:157] + "..."

    for name in causal_order(ga, gb):
        a, b = ra.get(name), rb.get(name)
        sa_, sb_ = (a[0] if a else "absent"), (b[0] if b else "absent")
        if a is None:
            kind = "added"
        elif b is None:
            kind = "removed"
        elif sa_ != sb_:
            kind = {("inactive", "ok"): "activated", ("ok", "inactive"): "deactivated"}.get(
                (sa_, sb_), "newly_failed" if sb_ in ("failed", "blocked") else "recovered"
                if sa_ in ("failed", "blocked") else "status_changed")
        elif sa_ == "ok" and a[2] != b[2]:
            kind = "changed_output"
        else:
            kind = "unchanged"
        if kind in ("added", "removed"):
            semantic_change[name] = (sb_ if kind == "added" else sa_) != "inactive"
        else:
            semantic_change[name] = kind != "unchanged"
        reasons = []
        if kind != "unchanged":
            if env_changed:
                reasons.append(f"environment {ga.env_version} -> {gb.env_version}")
            if a is not None and b is not None and name in ga.nodes and name in gb.nodes:
                reasons += _spec_changes(ga.nodes[name].to_dict(), gb.nodes[name].to_dict())
            if name in changed_obs:
                reasons.append("captured observation edited")
            if name in gb.nodes:
                node = gb.nodes[name]
                for p in node.inputs:
                    if semantic_change.get(p):
                        reasons.append(f"data input {p} changed")
                for g in node.guards:
                    if semantic_change.get(g.node):
                        reasons.append(f"control edge from {g.node} (allowed {list(g.allowed)}) changed")
            if kind == "added":
                reasons.append("node added in replay graph version")
            if kind == "removed":
                reasons.append("node removed from replay graph version")
            if sb_ == "failed":
                reasons.append(f"replay failure: {b[4]}")
        entry = {"node": name, "kind": kind, "original": sa_, "replay": sb_,
                 "source": b[3] if b else None, "reasons": reasons}
        if value_preview and kind in ("changed_output", "activated", "deactivated", "added", "removed"):
            entry["original_value"] = preview(a[2]) if a else None
            entry["replay_value"] = preview(b[2]) if b else None
        nodes.append(entry)
        if first is None and semantic_change[name]:
            first = entry
    la, lb = store.ledger(original), store.ledger(replay)
    effect_diff = {"added": [{"key": k[:16], "intent": lb[k]} for k in sorted(set(lb) - set(la))],
                   "removed": [{"key": k[:16], "intent": la[k]} for k in sorted(set(la) - set(lb))],
                   "changed": [{"key": k[:16], "original": la[k], "replay": lb[k]}
                               for k in sorted(set(la) & set(lb)) if la[k] != lb[k]]}
    counts: dict[str, int] = {}
    for entry in nodes:
        counts[entry["kind"]] = counts.get(entry["kind"], 0) + 1
    return {"original_run": original, "replay_run": replay, "mode": run_b["mode"],
            "first_divergence": first, "counts": counts, "effect_diff": effect_diff, "nodes": nodes}

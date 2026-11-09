"""Comparative incremental-vs-full replay study with matched baselines.

Every workload unit is captured once into a file-backed SQLite store. For each
repetition and arm the captured store file is copied fresh, so every arm starts
from the identical cache state, and arms run in a seeded random order. Every
arm's run is checked against the independent full-rerun oracle; a mismatch is
recorded as a correctness failure, never dropped.

Arms (all persist node records and commit effects to an isolated ledger):
  full                  re-execute every node (no reuse)
  prefix                generalized fixed-prefix replay: reuse nodes before the
                        first changed node in topological order, rerun the rest
  memo_value            joblib-style memo: hash full input values of every node
  incremental_nocutoff  dependency-aware invalidation without early cutoff (ablation)
  incremental           dependency-aware invalidation with early cutoff (proposed)
"""
from __future__ import annotations

import gc
import json
import math
import os
import platform
import random
import shutil
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from .dag import Graph, NodeSpec
from .engine import Engine, capture
from .oracle import check_equivalence, full_rerun
from .store import RunStore
from .workflows import (FAMILIES, REFUND_ENV, apply_change, refund_graph, synthetic_graph)

ARMS = ("full", "prefix", "memo_value", "incremental_nocutoff", "incremental")


def build_units(spec: dict[str, Any]) -> list[dict[str, Any]]:
    units = []
    for family in spec["families"]:
        for size in (spec["sizes"] if family != "refund" else [12]):
            for seed in (spec.get("refund_seeds", spec["seeds"]) if family == "refund" else spec["seeds"]):
                for regime_name, regime in spec["regimes"].items():
                    for change in spec["changes"]:
                        locs = [0.5] if change == "env" or family == "refund" else spec["localities"]
                        for loc in locs:
                            units.append({"unit_id": f"{family}-{size}-{seed}-{regime_name}-{change}-{loc}",
                                          "family": family, "size": size, "seed": seed, "regime": regime_name,
                                          "rounds": regime["rounds"], "out_bytes": regime["out_bytes"],
                                          "change": change, "locality": loc})
    return units


def _inject_fault(graph: Graph, locality: float) -> tuple[Graph, str]:
    candidates = [n for n in graph.order if graph.nodes[n].op == "syn.work"]
    name = candidates[min(len(candidates) - 1, int(locality * len(candidates)))]
    node = graph.nodes[name]
    return graph.replace_nodes(NodeSpec(name, "syn.fail", "v1", {"fail": True, "code": "E_INJECTED"}, node.inputs,
                                        node.guards, "pure", "hex")), name


REFUND_CASES = [("1042", "c-77"), ("2001", "c-77"), ("3003", "c-12"), ("1042", "c-12"), ("5005", "c-77"),
                ("4004", "c-77")]


def _refund_change(unit, base_graph, snapshot, rng):
    order, customer = REFUND_CASES[unit["seed"] % len(REFUND_CASES)]
    policy = {"risk_rounds": unit["rounds"]}
    change = unit["change"]
    if change == "param":
        return refund_graph(order, customer, dict(policy, auto_approve_max_cents=10000)), snapshot, {"type": change}
    if change == "op_version":
        return refund_graph(order, customer, policy, "v2"), snapshot, {"type": change}
    if change == "gate_flip":
        return refund_graph(order, customer, dict(policy, approve_eligible=False)), snapshot, {"type": change}
    if change == "add_node":
        return refund_graph(order, customer, policy, fraud_hold=10), snapshot, {"type": change}
    if change == "observation":
        entries = snapshot.entries_for("customer_status")
        value = json.loads(entries[0][1])
        value["prior_refunds"] = value.get("prior_refunds", 0) + 4
        return base_graph, snapshot.with_observation("customer_status", value), {"type": change}
    if change == "env":
        return base_graph.replace_nodes(env_version="refund-fixture-v2"), snapshot, {"type": change}
    raise ValueError(f"refund family does not support {change}")


def prepare_unit(unit: dict[str, Any], workdir: Path) -> dict[str, Any] | None:
    """Capture the base run into workdir/base.db; return replay inputs or None if not applicable."""
    rng = random.Random(unit["unit_id"])
    db = workdir / "base.db"
    if db.exists():
        db.unlink()
    if unit["family"] == "refund":
        order, customer = REFUND_CASES[unit["seed"] % len(REFUND_CASES)]
        graph, env = refund_graph(order, customer, {"risk_rounds": unit["rounds"]}), REFUND_ENV
    else:
        graph, env = synthetic_graph(unit["family"], unit["size"], unit["seed"], unit["rounds"], unit["out_bytes"])
    change = unit["change"]
    base_graph, new_graph, fault = graph, None, None
    if change == "recover_fault":
        if unit["family"] == "refund":
            return None
        base_graph, fault = _inject_fault(graph, unit["locality"])
        new_graph = graph
    store = RunStore(str(db))
    try:
        snapshot, base = capture(store, base_graph, env, f"req-{unit['unit_id']}", populate_memo=True)
        try:
            if change == "recover_fault":
                new_snapshot, desc = snapshot, {"type": change, "node": fault}
            elif change == "inject_fault":
                if unit["family"] == "refund":
                    return None
                new_graph, name = _inject_fault(graph, unit["locality"])
                new_snapshot, desc = snapshot, {"type": change, "node": name}
            elif unit["family"] == "refund":
                new_graph, new_snapshot, desc = _refund_change(unit, base_graph, snapshot, rng)
            else:
                new_graph, new_snapshot, desc = apply_change(base_graph, snapshot, change, unit["locality"], rng)
        except ValueError as error:
            return {"not_applicable": str(error)}
    finally:
        store.close()
    return {"base_run": base.run_id, "graph": new_graph, "snapshot": new_snapshot, "change": desc,
            "base_status": base.status, "nodes": len(new_graph.nodes)}


def _timed_replay(src_db: Path, dst_db: Path, prepared: dict[str, Any], mode: str) -> dict[str, Any]:
    shutil.copyfile(src_db, dst_db)
    store = RunStore(str(dst_db))
    try:
        before = store.size_bytes()
        engine = Engine(store)
        gc.collect()
        started = time.perf_counter_ns()
        plan = engine.plan_replay(prepared["base_run"], prepared["graph"], prepared["snapshot"], mode)
        result = engine.execute(plan)
        wall = time.perf_counter_ns() - started
        after = store.size_bytes()
        oracle = prepared["oracle"]
        problems = check_equivalence(store, result.run_id, oracle)
        dup = store.conn.execute("SELECT COUNT(*) - COUNT(DISTINCT node_id || '|' || value) FROM effects "
                                 "WHERE run_id=? AND origin='run'", (result.run_id,)).fetchone()[0]
        return {"mode": mode, "wall_ns": wall, "status": result.status, "counters": result.counters,
                "timings_ns": result.timings_ns, "scheduled": len(plan.schedule), "reused_planned": len(plan.reuse),
                "seeds": len(plan.seeds), "equivalent": not problems, "problems": problems[:5],
                "storage_delta_bytes": {k: after[k] - before[k] for k in after}, "duplicate_effects": dup}
    finally:
        store.close()
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(dst_db) + suffix)
            if p.exists():
                p.unlink()


def run_unit(unit: dict[str, Any], workdir: Path, repetitions: int, arms=ARMS) -> dict[str, Any]:
    workdir.mkdir(parents=True, exist_ok=True)
    prepared = prepare_unit(unit, workdir)
    if prepared is None or "not_applicable" in prepared:
        return {"unit": unit, "applicable": False, "reason": (prepared or {}).get("not_applicable", "unsupported")}
    graph_doc, snap_doc = json.loads(prepared["graph"].to_json()), json.loads(prepared["snapshot"].to_json())
    t = time.perf_counter_ns()
    prepared["oracle"] = full_rerun(graph_doc, snap_doc)
    oracle_ns = time.perf_counter_ns() - t
    rng = random.Random("order:" + unit["unit_id"])
    samples = []
    for rep in range(repetitions):
        order = list(arms)
        rng.shuffle(order)
        for mode in order:
            row = _timed_replay(workdir / "base.db", workdir / f"arm-{mode}.db", prepared, mode)
            row["rep"], row["position"] = rep, order.index(mode)
            samples.append(row)
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(workdir / "base.db") + suffix)
        if p.exists():
            p.unlink()
    oracle_failed = sum(s in ("failed", "blocked") for s in prepared["oracle"].status.values())
    return {"unit": unit, "applicable": True, "nodes": prepared["nodes"], "change": prepared["change"],
            "base_status": prepared["base_status"], "oracle_ns": oracle_ns, "oracle_failed_or_blocked": oracle_failed,
            "samples": samples}


# ------------------------------------------------------------------ summary

def _gmean(xs):
    return math.exp(statistics.mean(math.log(x) for x in xs)) if xs else None


def _boot(values, fn, seed=20261104, n=2000):
    if len(values) < 2:
        return [None, None]
    rng = random.Random(seed)
    stats = sorted(fn(rng.choices(values, k=len(values))) for _ in range(n))
    return [stats[int(0.025 * n)], stats[int(0.975 * n) - 1]]


def unit_medians(row: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for mode in {s["mode"] for s in row["samples"]}:
        ss = [s for s in row["samples"] if s["mode"] == mode]
        out[mode] = {"wall_ns": statistics.median(s["wall_ns"] for s in ss),
                     "executed": ss[0]["counters"]["executed"],
                     "observation_reads": ss[0]["counters"]["observation_reads"],
                     "scheduled": ss[0]["scheduled"], "equivalent_all_reps": all(s["equivalent"] for s in ss),
                     "duplicate_effects": max(s["duplicate_effects"] for s in ss),
                     "hashed_bytes": ss[0]["counters"]["hashed_bytes"],
                     "loaded_bytes": ss[0]["counters"]["loaded_bytes"],
                     "storage_delta_bytes": sum(ss[0]["storage_delta_bytes"].values()),
                     "cutoff": ss[0]["counters"]["cutoff"]}
    return out


def summarize(rows: list[dict[str, Any]], protocol: dict[str, Any]) -> dict[str, Any]:
    applicable = [r for r in rows if r["applicable"]]
    per_unit = []
    for r in applicable:
        med = unit_medians(r)
        full = med["full"]
        entry = {"unit_id": r["unit"]["unit_id"], "family": r["unit"]["family"], "size": r["nodes"],
                 "regime": r["unit"]["regime"], "change": r["unit"]["change"], "locality": r["unit"]["locality"],
                 "affected_fraction": med["incremental"]["scheduled"] / r["nodes"],
                 "oracle_failed_or_blocked": r["oracle_failed_or_blocked"]}
        for mode, m in med.items():
            entry[f"{mode}_wall_ms"] = m["wall_ns"] / 1e6
            entry[f"{mode}_executed"] = m["executed"]
            entry[f"{mode}_ratio_vs_full"] = m["wall_ns"] / full["wall_ns"]
            entry[f"{mode}_equivalent"] = m["equivalent_all_reps"]
            entry[f"{mode}_dup_effects"] = m["duplicate_effects"]
            entry[f"{mode}_hashed_bytes"] = m["hashed_bytes"]
            entry[f"{mode}_loaded_bytes"] = m["loaded_bytes"]
            entry[f"{mode}_storage_bytes"] = m["storage_delta_bytes"]
        entry["incremental_cutoff"] = med["incremental"]["cutoff"]
        per_unit.append(entry)
    crit = protocol["win_criterion"]
    localized = [u for u in per_unit if u["affected_fraction"] <= crit["max_affected_fraction"]
                 and u["regime"] in crit["regimes"] and u["family"] in crit["families"]]

    def ratio_summary(units, mode, base="full"):
        vals = [u[f"{mode}_wall_ms"] / u[f"{base}_wall_ms"] for u in units]
        logs = [math.log(v) for v in vals]
        ci = _boot(logs, statistics.mean)
        return {"n_units": len(vals), "geomean_ratio": _gmean(vals),
                "ci95_geomean_ratio": [math.exp(c) if c is not None else None for c in ci],
                "median_ratio": statistics.median(vals) if vals else None,
                "units_where_slower": sum(v > 1 for v in vals)}

    def exec_summary(units, mode):
        a = sum(u[f"{mode}_executed"] for u in units)
        b = sum(u["full_executed"] for u in units)
        return {"executed": a, "full_executed": b, "ratio": a / b if b else None}

    equivalence = {mode: {"units": len(per_unit), "equivalent": sum(u[f"{mode}_equivalent"] for u in per_unit),
                          "duplicate_effect_units": sum(u[f"{mode}_dup_effects"] > 0 for u in per_unit)}
                   for mode in ARMS if per_unit and f"{mode}_equivalent" in per_unit[0]}
    by_regime, by_change, by_family = {}, {}, {}
    for key, bucket in (("regime", by_regime), ("change", by_change), ("family", by_family)):
        for u in per_unit:
            bucket.setdefault(u[key], []).append(u)
    bins = [(0, 0.05), (0.05, 0.25), (0.25, 0.5), (0.5, 0.9), (0.9, 1.01)]
    breakeven = []
    for lo, hi in bins:
        for regime in sorted(by_regime):
            us = [u for u in per_unit if lo <= u["affected_fraction"] < hi and u["regime"] == regime]
            if us:
                breakeven.append({"affected_fraction": [lo, min(hi, 1.0)], "regime": regime,
                                  **ratio_summary(us, "incremental")})
    primary = ratio_summary(localized, "incremental")
    primary_exec = exec_summary(localized, "incremental")
    all_equiv = all(v["equivalent"] == v["units"] and v["duplicate_effect_units"] == 0 for v in equivalence.values())
    wall_win = (primary["geomean_ratio"] is not None and primary["geomean_ratio"] <= crit["max_wall_ratio"]
                and primary["ci95_geomean_ratio"][1] is not None and primary["ci95_geomean_ratio"][1] < 1.0)
    exec_win = primary_exec["ratio"] is not None and primary_exec["ratio"] <= crit["max_exec_ratio"]
    return {
        "n_units_total": len(rows), "n_units_applicable": len(applicable),
        "not_applicable": sorted({r["unit"]["unit_id"] for r in rows if not r["applicable"]}),
        "repetitions": protocol["repetitions"], "equivalence": equivalence,
        "primary": {"definition": crit, "localized_units": len(localized), "incremental_vs_full_wall": primary,
                    "incremental_vs_full_tool_executions": primary_exec,
                    "wall_criterion_met": wall_win, "exec_criterion_met": exec_win,
                    "semantics_preserved": all_equiv,
                    "win": bool(all_equiv and (wall_win or exec_win))},
        "secondary": {
            "localized_incremental_vs_prefix": ratio_summary(localized, "incremental", "prefix"),
            "localized_incremental_vs_memo_value": ratio_summary(localized, "incremental", "memo_value"),
            "localized_incremental_vs_nocutoff": ratio_summary(localized, "incremental", "incremental_nocutoff"),
            "all_units": {mode: ratio_summary(per_unit, mode) for mode in ARMS if mode != "full"},
            "by_regime": {k: ratio_summary(v, "incremental") for k, v in sorted(by_regime.items())},
            "by_change": {k: ratio_summary(v, "incremental") for k, v in sorted(by_change.items())},
            "by_family": {k: ratio_summary(v, "incremental") for k, v in sorted(by_family.items())},
            "break_even_by_affected_fraction": breakeven,
            "tool_executions_all_units": {mode: exec_summary(per_unit, mode) for mode in ARMS},
            "hashed_bytes_all_units": {mode: sum(u[f"{mode}_hashed_bytes"] for u in per_unit) for mode in ARMS},
            "loaded_bytes_all_units": {mode: sum(u[f"{mode}_loaded_bytes"] for u in per_unit) for mode in ARMS},
            "storage_delta_bytes_all_units": {mode: sum(u[f"{mode}_storage_bytes"] for u in per_unit) for mode in ARMS},
        },
        "per_unit": per_unit,
    }


def environment_info() -> dict[str, Any]:
    import sqlite3
    return {"python": sys.version, "platform": platform.platform(), "machine": platform.machine(),
            "cpu_count": os.cpu_count(), "load_average_start": os.getloadavg(), "sqlite": sqlite3.sqlite_version,
            "isolation": "no CPU pinning; other local processes may be active"}


def run_study(protocol: dict[str, Any], out_dir: Path, phase: str, limit: int | None = None) -> dict[str, Any]:
    spec = protocol["phases"][phase]
    units = build_units(spec)
    if limit:
        units = units[:limit]
    out_dir.mkdir(parents=True, exist_ok=False)
    workdir = out_dir / "scratch"
    raw = out_dir / "raw_units.jsonl"
    # explicit warmup on a dedicated unit, never reported
    warm = dict(units[0], unit_id="warmup")
    run_unit(warm, workdir, 1)
    started = time.time()
    rows = []
    with raw.open("w") as fh:
        for i, unit in enumerate(units):
            row = run_unit(unit, workdir, protocol["repetitions"])
            rows.append(row)
            fh.write(json.dumps(row) + "\n")
            fh.flush()
            if (i + 1) % 20 == 0:
                print(f"{phase}: {i+1}/{len(units)} units, {time.time()-started:.0f}s", file=sys.stderr, flush=True)
    shutil.rmtree(workdir, ignore_errors=True)
    summary = summarize(rows, protocol)
    summary["environment"] = environment_info()
    summary["elapsed_s"] = time.time() - started
    summary["phase"] = phase
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


# ------------------------------------------------- recovery and cancellation

def recovery_study(out_dir: Path, seeds: list[int], positions: list[float], size: int = 256,
                   rounds: int = 2000) -> dict[str, Any]:
    """Crash a child replay at a schedule position, reopen, resume and verify.

    Compares resume wall time with a fresh incremental replay of the same plan,
    and checks the isolated ledger against the oracle after every recovery.
    """
    import subprocess
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    src = str(Path(__file__).resolve().parents[1])
    for seed in seeds:
        for position in positions:
            work = out_dir / f"rec-{seed}-{position}"
            shutil.rmtree(work, ignore_errors=True)
            work.mkdir()
            graph, env = synthetic_graph("layered", size, seed, rounds, 32, effect_rate=0.1)
            db = work / "r.db"
            store = RunStore(str(db))
            snapshot, base = capture(store, graph, env, f"req-rec-{seed}")
            new_graph, new_snapshot, desc = apply_change(graph, snapshot, "param", 0.05, random.Random(seed))
            plan = Engine(store).plan_replay(base.run_id, new_graph, new_snapshot)
            work_nodes = [n for n in plan.schedule if n not in plan.commit_only]
            target = work_nodes[min(len(work_nodes) - 1, int(position * len(work_nodes)))]
            effect_nodes = [n for n in plan.schedule if new_graph.nodes[n].effect == "effect_intent"]
            store.close()
            gpath, spath = work / "g.json", work / "s.json"
            gpath.write_text(new_graph.to_json())
            spath.write_text(new_snapshot.to_json())
            event = "before_result_write"
            proc = subprocess.run([sys.executable, "-m", "counterfactual_replay_lab.crashchild", str(db), base.run_id,
                                   str(gpath), str(spath), event, target, "incremental"],
                                  env=dict(os.environ, PYTHONPATH=src, COMMIT_EVERY="16"),
                                  capture_output=True, text=True, timeout=300)
            store = RunStore(str(db))
            pending = [r for r in store.runs() if r["status"] == "running"]
            t = time.perf_counter_ns()
            resumed = Engine(store).resume(pending[0]["run_id"]) if pending else None
            resume_ns = time.perf_counter_ns() - t
            oracle = full_rerun(json.loads(new_graph.to_json()), json.loads(new_snapshot.to_json()))
            problems = check_equivalence(store, pending[0]["run_id"], oracle) if pending else ["no crashed run"]
            t = time.perf_counter_ns()
            fresh = Engine(store).execute(Engine(store).plan_replay(base.run_id, new_graph, new_snapshot))
            fresh_ns = time.perf_counter_ns() - t
            rows.append({"seed": seed, "position": position, "crash_node": target, "child_exit": proc.returncode,
                         "scheduled": len(plan.schedule), "effect_nodes_scheduled": len(effect_nodes),
                         "adopted_after_restart": resumed.counters["adopted_after_restart"] if resumed else None,
                         "executed_on_resume": resumed.counters["executed"] if resumed else None,
                         "executed_fresh": fresh.counters["executed"], "resume_ms": resume_ns / 1e6,
                         "fresh_replay_ms": fresh_ns / 1e6, "equivalent": not problems, "problems": problems[:3],
                         "ledger_rows": len(store.ledger(pending[0]["run_id"])) if pending else None,
                         "oracle_ledger_rows": len(oracle.ledger)})
            store.close()
            shutil.rmtree(work, ignore_errors=True)
    return {"rows": rows, "n": len(rows), "all_equivalent": all(r["equivalent"] for r in rows),
            "all_crashed": all(r["child_exit"] == 77 for r in rows),
            "median_resume_ms": statistics.median(r["resume_ms"] for r in rows),
            "median_fresh_replay_ms": statistics.median(r["fresh_replay_ms"] for r in rows)}


def cancellation_study(n: int, seed: int = 7) -> dict[str, Any]:
    """Latency from cancel request to a stopped run, cooperative tool vs between nodes (sleep-based tool)."""
    import threading
    from .dag import Graph as G, NodeSpec as N
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        delay = rng.uniform(0.05, 0.3)
        def graph(sec):
            return G([N("s", "syn.obs", "v1", {"key": "src0000"}, effect="observation", output_schema="hex"),
                      N("slow", "syn.slow", "v1", {"seconds": sec}, ("s",), output_schema="hex"),
                      N("after", "syn.work", "v1", {"salt": 1, "rounds": 10}, ("slow",), output_schema="hex"),
                      N("e", "syn.effect", "v1", {"target": "t"}, ("after",), (), "effect_intent", "intent")])
        store = RunStore()
        snap, base = capture(store, graph(0.01), {"syn": {"src0000": "ab" * 8}}, f"req-c{i}")
        cancel = threading.Event()
        engine = Engine(store, cancel=cancel)
        plan = engine.plan_replay(base.run_id, graph(2.0), snap)
        marks = {}
        def fire():
            marks["t"] = time.perf_counter_ns()
            cancel.set()
        timer = threading.Timer(delay, fire)
        timer.start()
        result = engine.execute(plan)
        done = time.perf_counter_ns()
        rows.append({"delay_s": delay, "status": result.status, "latency_ms": (done - marks["t"]) / 1e6,
                     "ledger_rows": len(store.ledger(result.run_id))})
        store.close()
    lat = sorted(r["latency_ms"] for r in rows)
    return {"n": n, "statuses": sorted({r["status"] for r in rows}), "median_ms": statistics.median(lat),
            "max_ms": lat[-1], "effects_after_cancel": sum(r["ledger_rows"] for r in rows), "rows": rows,
            "note": "syn.slow is a sleep-based cooperative tool polling every 5 ms; this measures the engine's "
                    "acknowledgement path, not a real tool"}

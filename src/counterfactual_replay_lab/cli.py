"""Command line for dependency replay: capture, replay, compare, resume, study, report, demo.

Run with PYTHONPATH=src python -m counterfactual_replay_lab.cli <command> --help.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from .dag import Graph, GraphError
from .engine import MODES, Engine, PlanError, capture
from .explain import compare_runs
from .oracle import check_equivalence, full_rerun
from .snapshot import Snapshot, SnapshotError
from .store import RunStore, StoreError
from .workflows import REFUND_ENV, refund_graph


def _graph(args) -> Graph:
    if args.graph:
        return Graph.from_json(Path(args.graph).read_text())
    policy = json.loads(args.policy) if args.policy else None
    return refund_graph(args.order, args.customer, policy, args.risk_version, args.fraud_hold)


def _add_graph_args(p):
    p.add_argument("--graph", help="graph JSON file (default: built-in refund DAG)")
    p.add_argument("--order", default="1042")
    p.add_argument("--customer", default="c-77")
    p.add_argument("--policy", help="JSON object of refund policy overrides")
    p.add_argument("--risk-version", default="v1", choices=["v1", "v2"])
    p.add_argument("--fraud-hold", type=int, help="add the fraud_hold node with this threshold")


def cmd_capture(args):
    env = json.loads(Path(args.environment).read_text()) if args.environment else REFUND_ENV
    store = RunStore(args.db)
    snapshot, result = capture(store, _graph(args), env, args.request_id)
    out = {"run_id": result.run_id, "status": result.status, "snapshot_hash": snapshot.snapshot_hash,
           "counters": result.counters}
    if args.snapshot_out:
        Path(args.snapshot_out).write_text(snapshot.to_json())
    print(json.dumps(out, indent=2))


def cmd_replay(args):
    store = RunStore(args.db)
    base = store.run(args.base)
    snapshot = Snapshot.from_json(Path(args.snapshot).read_text()) if args.snapshot else \
        Snapshot.from_json(store.snapshot_body(base["snapshot_hash"]))
    for edit in args.edit_observation or []:
        node, _, value = edit.partition("=")
        snapshot = snapshot.with_observation(node, json.loads(value))
    graph = _graph(args)
    engine = Engine(store)
    result = engine.execute(engine.plan_replay(args.base, graph, snapshot, args.mode))
    out = {"run_id": result.run_id, "status": result.status, "counters": result.counters,
           "timings_ms": {k: v / 1e6 for k, v in result.timings_ns.items()}}
    if args.verify:
        oracle = full_rerun(json.loads(graph.to_json()), json.loads(snapshot.to_json()))
        out["oracle_mismatches"] = check_equivalence(store, result.run_id, oracle)
    print(json.dumps(out, indent=2))
    return 0 if not out.get("oracle_mismatches") else 3


def cmd_compare(args):
    print(json.dumps(compare_runs(RunStore(args.db), args.original, args.replay), indent=2))


def cmd_resume(args):
    result = Engine(RunStore(args.db)).resume(args.run)
    print(json.dumps({"run_id": result.run_id, "status": result.status, "counters": result.counters}, indent=2))


def cmd_runs(args):
    for run in RunStore(args.db).runs():
        print(run["run_id"], run["mode"], run["status"], run["base_run"] or "-")


def cmd_study(args):
    from .study import run_study
    protocol_bytes = Path(args.protocol).read_bytes()
    protocol = json.loads(protocol_bytes)
    if args.expect_sha256 and hashlib.sha256(protocol_bytes).hexdigest() != args.expect_sha256:
        raise SystemExit("protocol.json hash differs from the pre-registered hash")
    summary = run_study(protocol, Path(args.out), args.phase, args.limit)
    summary.pop("per_unit", None)
    print(json.dumps(summary["primary"], indent=2))


def cmd_recovery(args):
    from .study import cancellation_study, recovery_study
    out = Path(args.out)
    rec = recovery_study(out / "recovery_scratch", args.seeds, args.positions)
    can = cancellation_study(args.cancellations)
    out.mkdir(parents=True, exist_ok=True)
    (out / "recovery.json").write_text(json.dumps({"recovery": rec, "cancellation": can}, indent=2) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k != "rows"}, indent=2))
    print(json.dumps({k: v for k, v in can.items() if k != "rows"}, indent=2))


def cmd_report(args):
    from .report import render
    text = render(Path(args.study), Path(args.recovery) if args.recovery else None)
    Path(args.out).write_text(text)
    print(f"Wrote {args.out}")


def cmd_demo(args):
    from .demo import serve
    serve(args.host, args.port, Path(args.data_dir) if args.data_dir else None)


def main(argv=None):
    p = argparse.ArgumentParser(prog="replaylab", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("capture", help="run a graph with live local fixture tools and freeze a snapshot")
    _add_graph_args(c)
    c.add_argument("--db", required=True)
    c.add_argument("--environment", help="fixture environment JSON (default: refund fixture)")
    c.add_argument("--request-id", default="req-1042")
    c.add_argument("--snapshot-out")
    c.set_defaults(fn=cmd_capture)
    r = sub.add_parser("replay", help="replay a changed graph/snapshot against a captured run")
    _add_graph_args(r)
    r.add_argument("--db", required=True)
    r.add_argument("--base", required=True, help="base run id")
    r.add_argument("--snapshot", help="snapshot JSON (default: the base run's)")
    r.add_argument("--edit-observation", action="append", metavar="NODE=JSON")
    r.add_argument("--mode", default="incremental", choices=[m for m in MODES if m != "capture"])
    r.add_argument("--verify", action="store_true", help="check against the full-rerun oracle")
    r.set_defaults(fn=cmd_replay)
    k = sub.add_parser("compare", help="first divergence and effect diff between two runs")
    k.add_argument("--db", required=True)
    k.add_argument("original")
    k.add_argument("replay")
    k.set_defaults(fn=cmd_compare)
    s = sub.add_parser("resume", help="resume a crashed, stopped or uncertain run")
    s.add_argument("--db", required=True)
    s.add_argument("run")
    s.set_defaults(fn=cmd_resume)
    l = sub.add_parser("runs", help="list runs in a store")
    l.add_argument("--db", required=True)
    l.set_defaults(fn=cmd_runs)
    st = sub.add_parser("study", help="run a pre-registered comparative study phase")
    st.add_argument("--protocol", required=True)
    st.add_argument("--expect-sha256")
    st.add_argument("--phase", required=True, choices=["exploration", "final"])
    st.add_argument("--out", required=True)
    st.add_argument("--limit", type=int)
    st.set_defaults(fn=cmd_study)
    rc = sub.add_parser("recovery", help="crash/resume and cancellation latency study")
    rc.add_argument("--out", required=True)
    rc.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    rc.add_argument("--positions", type=float, nargs="+", default=[0.25, 0.5, 0.75])
    rc.add_argument("--cancellations", type=int, default=20)
    rc.set_defaults(fn=cmd_recovery)
    rp = sub.add_parser("report", help="render a markdown report from study output")
    rp.add_argument("--study", required=True)
    rp.add_argument("--recovery")
    rp.add_argument("--out", required=True)
    rp.set_defaults(fn=cmd_report)
    d = sub.add_parser("demo", help="serve the local interactive dependency-replay demo")
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8766)
    d.add_argument("--data-dir", help="where saved demo runs live (default: a new temporary directory)")
    d.set_defaults(fn=cmd_demo)
    args = p.parse_args(argv)
    try:
        return args.fn(args) or 0
    except (GraphError, SnapshotError, StoreError, PlanError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

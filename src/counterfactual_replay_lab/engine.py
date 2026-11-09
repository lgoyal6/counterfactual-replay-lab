"""Dependency-aware counterfactual replay with durable isolated effects.

API: capture(store, graph, environment) -> (snapshot, run_id);
plan_replay(store, base_run, graph, snapshot) -> Plan; execute(store, plan) -> RunResult;
resume(store, run_id) -> RunResult. explain.py compares two runs.

Correctness model (see docs/dependency-replay.md):
* A node's cache key binds its spec hash (op, op version, params, ordered data
  inputs, guards, effect kind, schema), the environment version, the request
  identity, each data/guard predecessor's result hash or inactivity, and for an
  observation node the hash of its captured snapshot entry.
* plan_replay seeds invalidation from spec differences, added nodes, changed
  snapshot entries, ledger/env changes and prior failures, then closes the seed
  set over data and control successors with the indexed adjacency of dag.Graph.
  Only unaffected nodes are reused without recomputing their key.
* Affected nodes run in canonical topological order. In `incremental` mode an
  affected node whose recomputed key equals its base record is reused (early
  cutoff) without executing its operation.
* Effect-intent nodes always commit to the current run's isolated ledger, even
  when the intent itself is reused, in the same SQLite transaction as the node
  record. Idempotency keys conflict exactly as EffectLedger.apply does.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
import threading
import time
from typing import Any, Callable

from .dag import Graph, GraphError, NodeSpec
from .ops import Cancelled, OpContext, ToolError, check_schema, lookup
from .replay_state import canonical, digest
from .snapshot import Snapshot, SnapshotError, observation_key
from .store import TERMINAL, RunStore, StoreError

MODES = ("capture", "full", "prefix", "memo_value", "incremental_nocutoff", "incremental")
RESUMABLE = ("running", "stopped", "cancelled_in_node", "uncertain")
Hook = Callable[[str, str | None], None]


class PlanError(ValueError):
    """Replay was refused before execution; nothing was written."""


class CacheCorruption(StoreError):
    """A stored result no longer matches its hash; replay fails closed."""


def _h(text: str) -> str:
    return sha256(text.encode()).hexdigest()


def effect_key(request_id: str, node_id: str, intent: dict[str, Any]) -> str:
    return digest({"request": request_id, "node": node_id, "operation": intent["kind"],
                   "target": intent["target"]})


@dataclass
class Plan:
    mode: str
    graph: Graph
    snapshot: Snapshot | None
    base_run: str | None
    seeds: dict[str, str]
    schedule: list[str]
    reuse: list[str]
    commit_only: list[str]
    plan_ns: int = 0
    environment: dict[str, Any] | None = None
    ledger: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "base_run": self.base_run, "seeds": self.seeds,
                "schedule": self.schedule, "commit_only": self.commit_only,
                "reuse_count": len(self.reuse)}


@dataclass
class RunResult:
    run_id: str
    status: str
    counters: dict[str, int]
    timings_ns: dict[str, int]
    detail: dict[str, Any] = field(default_factory=dict)
    snapshot: Snapshot | None = None


class Engine:
    """Planner and executor. Negative-control mutants override single methods."""

    tool_timeout: float | None = None
    commit_every: int = 64

    def __init__(self, store: RunStore, *, hooks: Hook | None = None, cancel: threading.Event | None = None,
                 tool_timeout: float | None = None, poll_db_cancel: bool = False, commit_every: int = 64,
                 populate_memo: bool = False):
        self.store = store
        self.hooks = hooks or (lambda event, node: None)
        self.cancel = cancel or threading.Event()
        self.tool_timeout = tool_timeout
        self.poll_db_cancel = poll_db_cancel
        self.commit_every = commit_every
        self.populate_memo = populate_memo

    # ================================================================ keys
    def node_key(self, graph: Graph, node: NodeSpec, request_id: str, in_hashes: list[str],
                 guard_hashes: list[str], obs_hash: str | None) -> str:
        return digest({"spec": graph.spec_hashes[node.id], "env": graph.env_version, "request": request_id,
                       "inputs": in_hashes, "guards": guard_hashes, "obs": obs_hash})

    def value_key(self, graph: Graph, node: NodeSpec, request_id: str, in_values: list[Any],
                  guard_values: list[Any], obs_hash: str | None) -> str:
        """joblib-style memo key over full canonical input values (baseline arm)."""
        return digest({"spec": graph.spec_hashes[node.id], "env": graph.env_version, "request": request_id,
                       "inputs": in_values, "guards": guard_values, "obs": obs_hash})

    # ============================================================= planning
    def seeds(self, graph: Graph, snapshot: Snapshot, base_graph: Graph, base_snapshot: Snapshot,
              base_run: str) -> dict[str, str]:
        seeds: dict[str, str] = {}
        if graph.env_version != base_graph.env_version:
            return {n: "env_changed" for n in graph.order}
        for name, spec_hash in graph.spec_hashes.items():
            old = base_graph.spec_hashes.get(name)
            if old is None:
                seeds[name] = "added"
            elif old != spec_hash:
                seeds[name] = "spec_changed"
        for name in snapshot.diff(base_snapshot):
            if name in graph.nodes:
                seeds.setdefault(name, "observation_changed")
        if snapshot.ledger != base_snapshot.ledger:
            for name, node in graph.nodes.items():
                if node.effect == "effect_intent":
                    seeds.setdefault(name, "ledger_changed")
        for (name,) in self.store.conn.execute(
                "SELECT node_id FROM nodes WHERE run_id=? AND status IN ('failed','blocked')", (base_run,)):
            if name in graph.nodes:
                seeds.setdefault(name, "prior_failure")
        return seeds

    def affected(self, graph: Graph, seeds: dict[str, str]) -> set[str]:
        return graph.descendants(seeds)

    def _load_run_inputs(self, run_id: str) -> tuple[dict, Graph, Snapshot]:
        run = self.store.run(run_id)
        graph = Graph.from_json(self.store.graph_body(run["graph_hash"]))
        if graph.graph_hash != run["graph_hash"]:
            raise CacheCorruption("Stored graph body does not match its identity")
        if run["snapshot_hash"] is None:
            raise StoreError("Run has no frozen snapshot")
        snapshot = Snapshot.from_json(self.store.snapshot_body(run["snapshot_hash"]))
        if snapshot.snapshot_hash != run["snapshot_hash"]:
            raise CacheCorruption("Stored snapshot body does not match its identity")
        return run, graph, snapshot

    def plan_replay(self, base_run: str, graph: Graph, snapshot: Snapshot, mode: str = "incremental") -> Plan:
        started = time.perf_counter_ns()
        if mode not in MODES or mode == "capture":
            raise PlanError(f"Unsupported replay mode {mode!r}")
        graph.verify_frozen()
        base, base_graph, base_snapshot = self._load_run_inputs(base_run)
        if base["status"] not in TERMINAL:
            raise PlanError(f"Base run {base_run} is {base['status']}; only terminal runs can be reused")
        if snapshot.request_id != base_snapshot.request_id:
            raise PlanError("Counterfactual replay must keep the captured request identity")
        count = self.store.conn.execute("SELECT COUNT(*) FROM nodes WHERE run_id=?", (base_run,)).fetchone()[0]
        if count != len(base_graph.nodes):
            raise CacheCorruption("Base run is missing node records")
        if mode in ("full", "memo_value"):
            seeds = {n: "full_rerun" for n in graph.order}
            schedule = list(graph.order)
        else:
            seeds = self.seeds(graph, snapshot, base_graph, base_snapshot, base_run)
            if mode == "prefix":
                first = min((graph.index[s] for s in seeds), default=len(graph.order))
                schedule = list(graph.order[first:])
            else:
                schedule = sorted(self.affected(graph, seeds), key=graph.index.__getitem__)
        scheduled = set(schedule)
        reuse, commit_only = [], []
        for name in graph.order:
            if name not in scheduled:
                (commit_only if graph.nodes[name].effect == "effect_intent" else reuse).append(name)
        schedule = sorted(scheduled | set(commit_only), key=graph.index.__getitem__)
        return Plan(mode, graph, snapshot, base_run, seeds, schedule, reuse, commit_only,
                    time.perf_counter_ns() - started)

    def capture_plan(self, graph: Graph, environment: dict[str, Any], request_id: str,
                     ledger: tuple[tuple[str, str], ...] = ()) -> Plan:
        graph.verify_frozen()
        return Plan("capture", graph, None, None, {n: "capture" for n in graph.order}, list(graph.order),
                    [], [], 0, environment, tuple(sorted(ledger)))

    # ============================================================ execution
    def start(self, plan: Plan) -> str:
        """Create the run header, pre-run ledger and reused records atomically."""
        store = self.store
        seq = store.next_seq()
        snapshot_hash = plan.snapshot.snapshot_hash if plan.snapshot else None
        run_id = f"r{seq:05d}-" + digest({"graph": plan.graph.graph_hash, "snapshot": snapshot_hash,
                                         "base": plan.base_run, "mode": plan.mode, "seq": seq})[:10]
        ledger = plan.snapshot.ledger if plan.snapshot else plan.ledger
        store.begin()
        try:
            store.conn.execute("INSERT OR IGNORE INTO graphs VALUES (?, ?)", (plan.graph.graph_hash, plan.graph.to_json()))
            if plan.snapshot:
                store.conn.execute("INSERT OR IGNORE INTO snapshots VALUES (?, ?)", (snapshot_hash, plan.snapshot.to_json()))
            body = plan.to_dict()
            if plan.mode == "capture":
                body["request_id"] = plan.environment.get("request_id") if plan.environment else None
                body["ledger"] = [list(r) for r in ledger]
            store.conn.execute("INSERT INTO runs(run_id, seq, graph_hash, snapshot_hash, base_run, mode, status, plan) "
                               "VALUES (?, ?, ?, ?, ?, ?, 'running', ?)",
                               (run_id, seq, plan.graph.graph_hash, snapshot_hash, plan.base_run, plan.mode,
                                canonical(body)))
            store.conn.executemany("INSERT INTO effects VALUES (?, ?, ?, NULL, 'snapshot')",
                                   [(run_id, k, v) for k, v in ledger])
            if plan.reuse:
                store.conn.execute(
                    "INSERT INTO nodes(run_id, node_id, status, key, result_hash, source, error) "
                    "SELECT ?, node_id, status, key, result_hash, 'reused', NULL FROM nodes "
                    "WHERE run_id=? AND node_id IN (SELECT value FROM json_each(?))",
                    (run_id, plan.base_run, json.dumps(plan.reuse)))
            store.commit()
        except BaseException:
            store.rollback()
            raise
        return run_id

    def execute(self, plan: Plan) -> RunResult:
        started = time.perf_counter_ns()
        run_id = self.start(plan)
        return self._run(run_id, plan, started, resumed=False)

    def resume(self, run_id: str) -> RunResult:
        started = time.perf_counter_ns()
        run, graph, snapshot = self._load_run_inputs(run_id)
        if run["status"] not in RESUMABLE:
            raise PlanError(f"Run {run_id} is {run['status']}; it cannot be resumed")
        if run["mode"] == "capture":
            raise PlanError("Capture runs are not resumable; capture again")
        body = run["plan"]
        plan = Plan(run["mode"], graph, snapshot, run["base_run"], body["seeds"], body["schedule"], [],
                    body["commit_only"])
        self.store.conn.execute("UPDATE runs SET cancel_requested=0, status='running' WHERE run_id=?", (run_id,))
        self.cancel.clear()
        return self._run(run_id, plan, started, resumed=True)

    # ------------------------------------------------------------------
    def _run(self, run_id: str, plan: Plan, started: int, resumed: bool) -> RunResult:
        return _Execution(self, run_id, plan, started, resumed).run()


class _Execution:
    """One pass over a run's schedule. State lives in SQLite between passes."""

    def __init__(self, engine: Engine, run_id: str, plan: Plan, started: int, resumed: bool):
        self.e, self.store, self.run_id, self.plan = engine, engine.store, run_id, plan
        self.graph = plan.graph
        self.started, self.resumed = started, resumed
        self.capture = plan.mode == "capture"
        self.request_id = (plan.snapshot.request_id if plan.snapshot else plan.environment["request_id"])
        self.snap_index = plan.snapshot.index() if plan.snapshot else {}
        self.captured: list[tuple[str, str, str]] = []
        self.state: dict[str, tuple] = {}      # node -> (status, key, result_hash, source, error)
        self.values: dict[str, Any] = {}
        self.pending_nodes: list[tuple] = []
        self.pending_blobs: dict[str, str] = {}
        self.pending_memo: list[tuple] = []
        self._snapshot = None
        self.counters = {k: 0 for k in ("reused", "reused_effect_intents", "cutoff", "memo_hit", "executed",
                                        "observation_reads", "inactive", "failed", "blocked",
                                        "effects_committed", "effects_adopted", "effect_conflicts",
                                        "adopted_after_restart", "hashed_bytes", "loaded_bytes")}
        self.counters["reused"] = len(plan.reuse)
        self.timings = {"plan_ns": plan.plan_ns}
        self.commit_only = set(plan.commit_only)
        self.cutoff = plan.mode == "incremental"
        self.memo = plan.mode == "memo_value"

    # ---------------------------------------------------------- helpers
    def status(self, name: str) -> str:
        return self.state[name][0]

    def value(self, name: str) -> Any:
        if name not in self.values:
            result_hash = self.state[name][2]
            body = self.pending_blobs.get(result_hash) or self.store.blob(result_hash)
            self.counters["loaded_bytes"] += len(body)
            if _h(body) != result_hash:
                raise CacheCorruption(f"Stored result for {name} does not match its hash")
            self.values[name] = json.loads(body)
        return self.values[name]

    def record(self, name: str, status: str, key: str | None, result_hash: str | None, source: str,
               error: str | None = None, body: str | None = None) -> None:
        self.state[name] = (status, key, result_hash, source, error)
        self.pending_nodes.append((self.run_id, name, status, key, result_hash, source, error))
        if body is not None:
            self.pending_blobs[result_hash] = body
        if status != "ok":
            self.counters[status] += 1
        if len(self.pending_nodes) >= self.e.commit_every:
            self.flush()

    def _write_pending(self) -> None:
        conn = self.store.conn
        if self.pending_blobs:
            conn.executemany("INSERT OR IGNORE INTO blobs VALUES (?, ?)", list(self.pending_blobs.items()))
        if self.pending_nodes:
            conn.executemany("INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?)", self.pending_nodes)
        if self.pending_memo:
            conn.executemany("INSERT OR IGNORE INTO memo VALUES (?, ?, ?)", self.pending_memo)

    def _clear_pending(self) -> None:
        self.pending_nodes, self.pending_blobs, self.pending_memo = [], {}, []

    def flush(self) -> None:
        if not (self.pending_nodes or self.pending_blobs or self.pending_memo):
            return
        self.store.begin()
        try:
            self._write_pending()
            self.store.commit()
        except BaseException:
            self.store.rollback()
            raise
        self._clear_pending()
        self.e.hooks("after_flush", None)

    def cancel_requested(self) -> bool:
        if self.e.cancel.is_set():
            return True
        if self.e.poll_db_cancel and self.store.cancel_requested(self.run_id):
            self.e.cancel.set()
            return True
        return False

    def stop(self, status: str, detail: dict[str, Any]) -> RunResult:
        self.flush()
        self.store.conn.execute("UPDATE runs SET status=?, detail=? WHERE run_id=?",
                                (status, canonical(detail), self.run_id))
        return self.result(status, detail)

    def result(self, status: str, detail: dict[str, Any]) -> RunResult:
        self.timings["total_ns"] = time.perf_counter_ns() - self.started + self.plan.plan_ns
        return RunResult(self.run_id, status, dict(self.counters), dict(self.timings), detail,
                         self._snapshot if self.capture else self.plan.snapshot)

    # ------------------------------------------------------------- effects
    def commit_effect(self, name: str, key: str, result_hash: str, body: str, source: str) -> None:
        intent = self.values.get(name)
        if intent is None:
            intent = json.loads(body)
        idem = effect_key(self.request_id, name, intent)
        self.e.hooks("before_result_write", name)
        store = self.store
        store.begin()
        try:
            self._write_pending()
            row = store.conn.execute("SELECT value FROM effects WHERE run_id=? AND idem_key=?",
                                     (self.run_id, idem)).fetchone()
            if row is not None and row[0] != body:
                store.conn.execute("INSERT INTO nodes VALUES (?, ?, 'failed', ?, NULL, ?, ?)",
                                   (self.run_id, name, key, source,
                                    "IdempotencyConflict: key reused with different effect"))
                state = ("failed", key, None, source, "IdempotencyConflict: key reused with different effect")
                self.counters["effect_conflicts"] += 1
                self.counters["failed"] += 1
            else:
                if row is None:
                    store.conn.execute("INSERT INTO effects VALUES (?, ?, ?, ?, 'run')",
                                       (self.run_id, idem, body, name))
                    self.counters["effects_committed"] += 1
                else:
                    self.counters["effects_adopted"] += 1
                store.conn.execute("INSERT OR IGNORE INTO blobs VALUES (?, ?)", (result_hash, body))
                store.conn.execute("INSERT INTO nodes VALUES (?, ?, 'ok', ?, ?, ?, NULL)",
                                   (self.run_id, name, key, result_hash, source))
                state = ("ok", key, result_hash, source, None)
            store.commit()
        except BaseException:
            store.rollback()
            raise
        self._clear_pending()
        self.state[name] = state
        self.e.hooks("after_effect_commit", name)

    # --------------------------------------------------------- operations
    def call_op(self, node: NodeSpec, values: list[Any]) -> Any:
        op = lookup(node.op, node.op_version)
        if op.kind != node.effect:
            raise ToolError(f"{node.op}@{node.op_version} is {op.kind}, node declares {node.effect}")
        if op.schema != node.output_schema and node.output_schema != "any":
            raise ToolError(f"{node.op}@{node.op_version} returns {op.schema}, node declares {node.output_schema}")
        timeout = self.e.tool_timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        ctx = OpContext(self.e.cancel, deadline, self.plan.environment if self.capture else None)
        if op.cooperative or timeout is None:
            result = op.fn(node.params, values, ctx)
        else:
            box: dict[str, Any] = {}

            def target():
                try:
                    box["value"] = op.fn(node.params, values, ctx)
                except BaseException as error:  # surfaced below
                    box["error"] = error
            worker = threading.Thread(target=target, daemon=True, name=f"tool-{node.id}")
            worker.start()
            while worker.is_alive() and time.monotonic() < deadline:
                worker.join(0.01)
                if self.cancel_requested():
                    worker.join(min(0.05, max(0.0, deadline - time.monotonic())))
                    if worker.is_alive():
                        raise _Uncertain(node.id)
            if worker.is_alive():
                raise _Timeout(f"{node.id} exceeded its {timeout}s bound; result discarded")
            if "error" in box:
                raise box["error"]
            result = box["value"]
        check_schema(op.schema, result)
        return result

    # --------------------------------------------------------------- main
    def prefetch(self) -> None:
        schedule = set(self.plan.schedule)
        frontier = {p for n in self.plan.schedule for p in self.graph.nodes[n].predecessors} - schedule
        mine = self.store.records(self.run_id, frontier | (schedule if self.resumed else set()))
        missing = frontier - set(mine)
        if missing:
            raise CacheCorruption(f"Reused predecessor records missing: {sorted(missing)[:5]}")
        self.state.update(mine)
        if self.resumed:
            self.counters["adopted_after_restart"] = len(set(mine) & schedule)
        self.base = {}
        if self.plan.base_run and (self.cutoff or self.commit_only):
            wanted = schedule if self.cutoff else self.commit_only
            self.base = self.store.records(self.plan.base_run, wanted)

    def run(self) -> RunResult:
        t = time.perf_counter_ns()
        self.prefetch()
        self.timings["prefetch_ns"] = time.perf_counter_ns() - t
        t = time.perf_counter_ns()
        graph = self.graph
        for name in self.plan.schedule:
            if name in self.state:
                continue
            if self.cancel_requested():
                return self.stop("stopped", {"stopped_before": name, "reason": "cancellation between nodes"})
            node = graph.nodes[name]
            try:
                if name in self.commit_only:
                    self.commit_reused_effect(name)
                else:
                    self.evaluate(node)
            except _Uncertain as error:
                return self.stop("uncertain", {"node": error.args[0],
                                               "reason": "cancel requested; read-only tool still in flight"})
            except Cancelled as error:
                if self.e.cancel.is_set():
                    return self.stop("cancelled_in_node", {"node": name, "reason": str(error)})
                self.record(name, "failed", None, None, "executed", f"Timeout: {error}")
        self.timings["execute_ns"] = time.perf_counter_ns() - t
        t = time.perf_counter_ns()
        self.flush()
        graph.verify_frozen()
        count = self.store.conn.execute("SELECT COUNT(*) FROM nodes WHERE run_id=?", (self.run_id,)).fetchone()[0]
        if count != len(graph.nodes):
            raise StoreError(f"Run {self.run_id} has {count} records for {len(graph.nodes)} nodes")
        failures = self.store.conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE run_id=? AND status IN ('failed','blocked')", (self.run_id,)).fetchone()[0]
        status = "completed_with_failures" if failures else "completed"
        detail = {"counters": self.counters}
        self.e.hooks("before_run_complete", None)
        self._snapshot = None
        if self.capture:
            self._snapshot = Snapshot(self.request_id, tuple(sorted(self.captured)), self.plan.ledger)
            self.store.conn.execute("INSERT OR IGNORE INTO snapshots VALUES (?, ?)",
                                    (self._snapshot.snapshot_hash, self._snapshot.to_json()))
            self.store.conn.execute("UPDATE runs SET snapshot_hash=? WHERE run_id=?",
                                    (self._snapshot.snapshot_hash, self.run_id))
        self.store.conn.execute("UPDATE runs SET status=?, detail=? WHERE run_id=?",
                                (status, canonical(detail), self.run_id))
        self.timings["finalize_ns"] = time.perf_counter_ns() - t
        return self.result(status, detail)

    def commit_reused_effect(self, name: str) -> None:
        status, key, result_hash, _source, _error = self.base[name]
        if status != "ok":
            self.record(name, status, key, result_hash, "reused")
            return
        body = self.store.blob(result_hash)
        if _h(body) != result_hash:
            raise CacheCorruption(f"Stored intent for {name} does not match its hash")
        self.counters["reused_effect_intents"] += 1
        self.commit_effect(name, key, result_hash, body, "reused")

    def evaluate(self, node: NodeSpec) -> None:
        name = node.id
        in_hashes, guard_hashes = [], []
        for pred in node.inputs:
            status = self.status(pred)
            if status in ("failed", "blocked"):
                return self.record(name, "blocked", None, None, "executed", f"Blocked by {pred}")
            in_hashes.append(self.state[pred][2] if status == "ok" else "inactive")
        for guard in node.guards:
            status = self.status(guard.node)
            if status in ("failed", "blocked"):
                return self.record(name, "blocked", None, None, "executed", f"Blocked by guard {guard.node}")
            guard_hashes.append(self.state[guard.node][2] if status == "ok" else "inactive")
        obs_hash, obs_body = None, None
        if node.effect == "observation" and not self.capture:
            obs_key = observation_key(name, node.op, node.op_version, node.params, in_hashes)
            entry = self.snap_index.get(obs_key)
            if entry is None:
                return self.record(name, "failed", None, None, "executed",
                                   "MissingObservation: no captured response for this input; live calls are disabled")
            obs_body = entry[1]
            obs_hash = _h(obs_body)
        key = self.e.node_key(self.graph, node, self.request_id, in_hashes, guard_hashes, obs_hash)
        if self.cutoff:
            base = self.base.get(name)
            if base is not None and base[1] == key and base[0] in ("ok", "inactive"):
                self.counters["cutoff"] += 1
                if base[0] == "ok" and node.effect == "effect_intent":
                    return self.commit_effect(name, key, base[2], self.store.blob(base[2]), "cutoff")
                return self.record(name, base[0], key, base[2], "cutoff")
        active = True
        for guard, guard_hash in zip(node.guards, guard_hashes):
            if guard_hash == "inactive" or not guard.admits(self.value(guard.node)):
                active = False
                break
        vkey = None
        if self.memo:
            vkey = self._value_key(node, in_hashes, guard_hashes, obs_hash)
            hit = self.store.conn.execute("SELECT status, result_hash FROM memo WHERE vkey=?", (vkey,)).fetchone()
            if hit is not None:
                self.counters["memo_hit"] += 1
                if hit[0] == "ok" and node.effect == "effect_intent":
                    return self.commit_effect(name, key, hit[1], self.store.blob(hit[1]), "memo")
                return self.record(name, hit[0], key, hit[1], "memo")
        if not active:
            self._memo(node, in_hashes, guard_hashes, obs_hash, "inactive", None, vkey)
            return self.record(name, "inactive", key, None, "executed")
        if node.effect == "observation" and not self.capture:
            body = obs_body
            self.counters["observation_reads"] += 1
        else:
            values = [self.value(p) if h != "inactive" else None for p, h in zip(node.inputs, in_hashes)]
            try:
                self.counters["executed"] += 1
                result = self.e_call(node, values)
            except (Cancelled, _Uncertain):
                raise
            except _Timeout as error:
                return self.record(name, "failed", key, None, "executed", f"Timeout: {error}")
            except Exception as error:  # deterministic operation failure
                return self.record(name, "failed", key, None, "executed", f"{type(error).__name__}: {error}")
            body = canonical(result)
            if node.effect == "observation":
                self.captured.append((observation_key(name, node.op, node.op_version, node.params, in_hashes),
                                      name, body))
                key = self.e.node_key(self.graph, node, self.request_id, in_hashes, guard_hashes, _h(body))
        result_hash = _h(body)
        self.counters["hashed_bytes"] += len(body)
        self.values[name] = json.loads(body)
        self._memo(node, in_hashes, guard_hashes, _h(body) if node.effect == "observation" else obs_hash,
                   "ok", result_hash, vkey)
        if node.effect == "effect_intent":
            return self.commit_effect(name, key, result_hash, body, "executed")
        self.e.hooks("before_result_write", name)
        self.record(name, "ok", key, result_hash, "executed", body=body)

    def e_call(self, node: NodeSpec, values: list[Any]) -> Any:
        return self.call_op(node, values)

    def _value_key(self, node, in_hashes, guard_hashes, obs_hash) -> str:
        in_values = [self.value(p) if h != "inactive" else None for p, h in zip(node.inputs, in_hashes)]
        guard_values = [self.value(g.node) if h != "inactive" else None for g, h in zip(node.guards, guard_hashes)]
        self.counters["hashed_bytes"] += sum(len(canonical(v)) for v in in_values + guard_values)
        return self.e.value_key(self.graph, node, self.request_id, in_values, guard_values, obs_hash)

    def _memo(self, node, in_hashes, guard_hashes, obs_hash, status, result_hash, vkey=None) -> None:
        if not (self.memo or (self.capture and self.e.populate_memo)):
            return
        if vkey is None:
            vkey = self._value_key(node, in_hashes, guard_hashes, obs_hash)
        self.pending_memo.append((vkey, status, result_hash))


class _Uncertain(Exception):
    pass


class _Timeout(Exception):
    pass


# ================================================================ facade
def capture(store: RunStore, graph: Graph, environment: dict[str, Any], request_id: str,
            ledger: tuple[tuple[str, str], ...] = (), engine_cls: type | None = None,
            **engine_kwargs) -> tuple[Snapshot, RunResult]:
    """Run every node with live local tools and freeze their observations."""
    engine = (engine_cls or Engine)(store, **engine_kwargs)
    env = dict(environment, request_id=request_id)
    result = engine.execute(engine.capture_plan(graph, env, request_id, ledger))
    if result.snapshot is None:
        raise SnapshotError("Capture did not complete; no snapshot was frozen")
    return result.snapshot, result


def replay(store: RunStore, base_run: str, graph: Graph, snapshot: Snapshot, mode: str = "incremental",
           engine_cls: type | None = None, **engine_kwargs) -> RunResult:
    engine = (engine_cls or Engine)(store, **engine_kwargs)
    return engine.execute(engine.plan_replay(base_run, graph, snapshot, mode))

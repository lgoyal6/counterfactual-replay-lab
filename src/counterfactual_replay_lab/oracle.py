"""Independent full-rerun oracle for dependency replay.

This module deliberately shares no planner, key, store or ledger code with
engine.py. It reads the serialized graph and snapshot, evaluates every node by
memoized recursion over declared dependencies, and keeps its own in-memory
ledger. It shares only the operation implementations and the documented
serialization/observation-key contract, so planner or cache bugs cannot make
both arms agree.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import sys
from typing import Any

from .ops import OpContext, ToolError, check_schema, lookup


def _enc(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass
class OracleResult:
    status: dict[str, str]          # ok / inactive / failed / blocked
    result_hash: dict[str, str]     # ok nodes only
    errors: dict[str, str]          # failed nodes: error class
    ledger: dict[str, str]          # idem_key -> canonical intent

    def outputs(self, node_id: str) -> str | None:
        return self.result_hash.get(node_id)


def full_rerun(graph_doc: dict[str, Any], snapshot_doc: dict[str, Any]) -> OracleResult:
    nodes = {n["id"]: n for n in graph_doc["nodes"]}
    request_id = snapshot_doc["request_id"]
    captured = {k: out for k, _node, out in snapshot_doc["observations"]}
    ledger = {k: v for k, v in snapshot_doc.get("ledger", [])}
    status: dict[str, str] = {}
    hashes: dict[str, str] = {}
    values: dict[str, Any] = {}
    errors: dict[str, str] = {}
    visiting: set[str] = set()
    limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(limit, 4 * len(nodes) + 100))

    def evaluate(name: str) -> None:
        if name in status:
            return
        if name in visiting:
            raise ValueError("cycle")
        visiting.add(name)
        node = nodes[name]
        for dep in list(node["inputs"]) + [g["node"] for g in node["guards"]]:
            evaluate(dep)
        visiting.discard(name)
        deps_bad = [d for d in list(node["inputs"]) + [g["node"] for g in node["guards"]]
                    if status[d] in ("failed", "blocked")]
        if deps_bad:
            status[name] = "blocked"
            return
        for guard in node["guards"]:
            if status[guard["node"]] != "ok" or _enc(values[guard["node"]]) not in {_enc(a) for a in guard["allowed"]}:
                status[name] = "inactive"
                return
        args = [values[d] if status[d] == "ok" else None for d in node["inputs"]]
        try:
            if node["effect"] == "observation":
                input_hashes = [hashes[d] if status[d] == "ok" else "inactive" for d in node["inputs"]]
                key = _sha(_enc({"node": name, "tool": node["op"], "version": node["op_version"],
                                 "params": node["params"], "inputs": input_hashes}))
                if key not in captured:
                    raise LookupError("MissingObservation")
                text = captured[key]
            else:
                op = lookup(node["op"], node["op_version"])
                if op.kind != node["effect"]:
                    raise ToolError("operation kind differs from declared effect")
                if node["output_schema"] not in ("any", op.schema):
                    raise ToolError("operation schema differs from declared schema")
                result = op.fn(node["params"], args, OpContext())
                check_schema(op.schema, result)
                text = _enc(result)
        except Exception as error:  # noqa: BLE001 - oracle records the class
            status[name] = "failed"
            errors[name] = "MissingObservation" if str(error) == "MissingObservation" else type(error).__name__
            return
        value = json.loads(text)
        if node["effect"] == "effect_intent":
            idem = _sha(_enc({"request": request_id, "node": name, "operation": value["kind"],
                              "target": value["target"]}))
            if idem in ledger and ledger[idem] != text:
                status[name] = "failed"
                errors[name] = "IdempotencyConflict"
                return
            ledger[idem] = text
        status[name] = "ok"
        hashes[name] = _sha(text)
        values[name] = value

    try:
        for name in sorted(nodes):
            evaluate(name)
    finally:
        sys.setrecursionlimit(limit)
    return OracleResult(status, hashes, errors, ledger)


def check_equivalence(store, run_id: str, oracle: OracleResult) -> list[str]:
    """Return human-readable mismatches between a stored run and the oracle."""
    records = store.records(run_id)
    problems = []
    for name in sorted(set(records) | set(oracle.status)):
        if name not in records:
            problems.append(f"{name}: missing from run")
            continue
        if name not in oracle.status:
            problems.append(f"{name}: not in oracle graph")
            continue
        status, _key, result_hash, _source, error = records[name]
        if status != oracle.status[name]:
            problems.append(f"{name}: status {status} != oracle {oracle.status[name]}")
        elif status == "ok" and result_hash != oracle.result_hash[name]:
            problems.append(f"{name}: result differs from oracle")
        elif status == "failed":
            cls = (error or "").split(":", 1)[0]
            if cls != oracle.errors[name]:
                problems.append(f"{name}: error {cls} != oracle {oracle.errors[name]}")
    ledger = store.ledger(run_id)
    if ledger != oracle.ledger:
        missing = sorted(set(oracle.ledger) - set(ledger))
        extra = sorted(set(ledger) - set(oracle.ledger))
        changed = sorted(k for k in set(ledger) & set(oracle.ledger) if ledger[k] != oracle.ledger[k])
        problems.append(f"ledger: missing={len(missing)} extra={len(extra)} changed={len(changed)}")
    dupes = store.conn.execute("SELECT COUNT(*) - COUNT(DISTINCT value || node_id) FROM effects "
                               "WHERE run_id=? AND origin='run'", (run_id,)).fetchone()[0]
    if dupes:
        problems.append(f"ledger: {dupes} duplicate committed effects")
    return problems

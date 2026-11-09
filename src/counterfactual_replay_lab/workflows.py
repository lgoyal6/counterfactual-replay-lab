"""Graph builders: the explicit refund DAG, the fixed-prefix compatibility graph
and seeded synthetic families for the incremental study.

Every builder returns a validated dag.Graph. Synthetic families declare every
data and control edge explicitly; nothing is discovered while executing.
"""
from __future__ import annotations

import random
from hashlib import sha256
from typing import Any

from .dag import Graph, Guard, NodeSpec

DEFAULT_POLICY = {"window_days": 30, "excluded_categories": ["gift_card"], "restocking_fee_pct": 10,
                  "cap_cents": 50000, "approve_eligible": True, "auto_approve_max_cents": 20000,
                  "max_risk": 50, "risk_rounds": 20000}


def refund_graph(order_id: str = "1042", customer_id: str = "c-77", policy: dict[str, Any] | None = None,
                 risk_version: str = "v1", fraud_hold: int | None = None) -> Graph:
    """Order lookup, customer status, eligibility, amount, risk, approval branch, effect intents.

    `fraud_hold` adds a node and edge (graph version change) that can turn an
    approval into a hold; None leaves it out.
    """
    p = dict(DEFAULT_POLICY, **(policy or {}))
    nodes = [
        NodeSpec("request", "refund.request", "v1", {"order_id": order_id, "customer_id": customer_id},
                 output_schema="object"),
        NodeSpec("order_lookup", "orders.lookup", "v1", {}, ("request",), effect="observation",
                 output_schema="object"),
        NodeSpec("customer_status", "crm.customer", "v1", {}, ("request",), effect="observation",
                 output_schema="object"),
        NodeSpec("eligibility", "refund.eligibility", "v1",
                 {"window_days": p["window_days"], "excluded_categories": p["excluded_categories"]},
                 ("order_lookup",), output_schema="string"),
        NodeSpec("amount", "refund.amount", "v1",
                 {"restocking_fee_pct": p["restocking_fee_pct"], "cap_cents": p["cap_cents"]},
                 ("order_lookup", "eligibility"), output_schema="number"),
        NodeSpec("risk_check", "risk.score", risk_version, {"rounds": p["risk_rounds"]},
                 ("customer_status", "amount"), output_schema="object"),
        NodeSpec("route", "refund.route", "v1",
                 {"approve_eligible": p["approve_eligible"], "auto_approve_max_cents": p["auto_approve_max_cents"],
                  "max_risk": p["max_risk"]}, ("eligibility", "amount", "risk_check"), output_schema="string"),
        NodeSpec("issue_refund", "refund.issue_intent", "v1", {}, ("request", "amount"),
                 (Guard("route", ("auto",)),), "effect_intent", "intent"),
        NodeSpec("open_review", "review.ticket_intent", "v1", {"queue": "refunds"}, ("request", "route"),
                 (Guard("route", ("manual", "review")),), "effect_intent", "intent"),
        NodeSpec("send_denial", "refund.denial_intent", "v1", {}, ("request", "eligibility"),
                 (Guard("route", ("deny",)),), "effect_intent", "intent"),
    ]
    decision_inputs = ("route", "amount", "issue_refund", "open_review", "send_denial")
    if fraud_hold is not None:
        nodes.append(NodeSpec("fraud_hold", "fraud.hold", "v1", {"hold_at": fraud_hold}, ("risk_check",),
                              (Guard("route", ("auto",)),), output_schema="bool"))
        decision_inputs += ("fraud_hold",)
    nodes.append(NodeSpec("decision", "refund.decision", "v1", {}, decision_inputs, output_schema="decision"))
    return Graph(nodes, "refund-fixture-v1")


REFUND_ENV = {
    "orders": {
        "1042": {"amount_cents": 12900, "days_since_delivery": 12, "category": "apparel", "delivered": True},
        "2001": {"amount_cents": 45000, "days_since_delivery": 5, "category": "electronics", "delivered": True},
        "3003": {"amount_cents": 5000, "days_since_delivery": 45, "category": "apparel", "delivered": True},
        "4004": {"amount_cents": 8000, "days_since_delivery": 3, "category": "gift_card", "delivered": True},
        "5005": {"amount_cents": 7000, "days_since_delivery": None, "category": "home", "delivered": None},
    },
    "customers": {
        "c-77": {"tier": "gold", "prior_refunds": 1, "account_age_days": 900},
        "c-12": {"tier": "new", "prior_refunds": 3, "account_age_days": 10},
    },
}


# ------------------------------------------------ fixed-prefix compatibility

def compat_graph(request: str, policy: str) -> Graph:
    """Graph form of core.RefundAgent: lookup -> apply_policy -> local refund effect."""
    return Graph([
        NodeSpec("lookup_order", "compat.lookup", "v1", {"request": request}, effect="observation",
                 output_schema="string"),
        NodeSpec("apply_policy", "compat.apply_policy", "v1", {"policy": policy}, ("lookup_order",),
                 output_schema="string"),
        NodeSpec("local_refund", "compat.refund_intent", "v1", {"request": request}, ("apply_policy",),
                 (Guard("apply_policy", ("approve_refund",)),), "effect_intent", "intent"),
    ], "compat-fixture-v1")


# -------------------------------------------------------- synthetic families

FAMILIES = ("layered", "diamond_chain", "fanout_fanin", "branchy", "disconnected", "chain")


def _work(i: int, inputs: tuple[str, ...], rounds: int, out_bytes: int, guards=()) -> NodeSpec:
    return NodeSpec(f"n{i:04d}", "syn.work", "v1", {"salt": i, "rounds": rounds, "out_bytes": out_bytes},
                    inputs, tuple(guards), output_schema="hex")


def synthetic_graph(family: str, size: int, seed: int, rounds: int = 2000, out_bytes: int = 32,
                    effect_rate: float = 0.05) -> tuple[Graph, dict[str, Any]]:
    """Seeded topology. Returns (graph, environment) with observation sources."""
    if family not in FAMILIES:
        raise ValueError(f"Unknown family {family}")
    if size < 8 or size > 4000:
        raise ValueError("size must be in [8, 4000]")
    rng = random.Random(f"{family}:{size}:{seed}")
    nodes: list[NodeSpec] = []
    env: dict[str, str] = {}

    def source(i: int) -> NodeSpec:
        key = f"src{i:04d}"
        env[key] = sha256(f"{seed}:{key}".encode()).hexdigest()[: 2 * out_bytes]
        return NodeSpec(f"n{i:04d}", "syn.obs", "v1", {"key": key}, effect="observation", output_schema="hex")

    if family == "chain":
        nodes.append(source(0))
        for i in range(1, size):
            nodes.append(_work(i, (f"n{i-1:04d}",), rounds, out_bytes))
    elif family == "layered":
        width = max(2, int(size ** 0.5))
        layers = [[i for i in range(min(width, size))]]
        nodes.extend(source(i) for i in layers[0])
        i = len(layers[0])
        while i < size:
            layer = []
            for _ in range(min(width, size - i)):
                prev = layers[-1]
                fan_in = tuple(sorted({f"n{x:04d}" for x in rng.sample(prev, min(len(prev), rng.randint(1, 3)))}))
                nodes.append(_work(i, fan_in, rounds, out_bytes))
                layer.append(i)
                i += 1
            layers.append(layer)
    elif family == "diamond_chain":
        nodes.append(source(0))
        i, tip = 1, 0
        while i + 3 <= size:
            left, right, join = i, i + 1, i + 2
            nodes.append(_work(left, (f"n{tip:04d}",), rounds, out_bytes))
            nodes.append(_work(right, (f"n{tip:04d}",), rounds, out_bytes))
            nodes.append(_work(join, (f"n{left:04d}", f"n{right:04d}"), rounds, out_bytes))
            tip, i = join, i + 3
        while i < size:
            nodes.append(_work(i, (f"n{tip:04d}",), rounds, out_bytes))
            tip, i = i, i + 1
    elif family == "fanout_fanin":
        groups = max(1, size // 40)
        i = 0
        roots = []
        for _ in range(groups):
            nodes.append(source(i))
            roots.append(i)
            i += 1
        per = (size - groups - 1) // groups
        mids = []
        for g in range(groups):
            for _ in range(per):
                nodes.append(_work(i, (f"n{roots[g]:04d}",), rounds, out_bytes))
                mids.append(i)
                i += 1
        while i < size - 1:
            nodes.append(_work(i, (f"n{rng.choice(roots):04d}",), rounds, out_bytes))
            mids.append(i)
            i += 1
        nodes.append(NodeSpec(f"n{i:04d}", "syn.merge", "v1", {"salt": i, "rounds": rounds, "out_bytes": out_bytes},
                              tuple(f"n{m:04d}" for m in mids), output_schema="hex"))
    elif family == "disconnected":
        comps = max(2, size // 25)
        i = 0
        for c in range(comps):
            nodes.append(source(i))
            prev = [i]
            i += 1
            for _ in range(size // comps - 1):
                if i >= size:
                    break
                fan = tuple(sorted({f"n{x:04d}" for x in rng.sample(prev, min(len(prev), rng.randint(1, 2)))}))
                nodes.append(_work(i, fan, rounds, out_bytes))
                prev.append(i)
                i += 1
        while i < size:
            nodes.append(source(i))
            i += 1
    elif family == "branchy":
        nodes.append(source(0))
        i, tip = 1, 0
        while i + 5 <= size:
            gate, then_, else_, merge = i, i + 1, i + 2, i + 3
            nodes.append(NodeSpec(f"n{gate:04d}", "syn.gate", "v1",
                                  {"modulus": 2, "threshold": 1, "invert": rng.random() < 0.5},
                                  (f"n{tip:04d}",), output_schema="bool"))
            nodes.append(_work(then_, (f"n{tip:04d}",), rounds, out_bytes, (Guard(f"n{gate:04d}", (True,)),)))
            nodes.append(_work(else_, (f"n{tip:04d}",), rounds, out_bytes, (Guard(f"n{gate:04d}", (False,)),)))
            nodes.append(NodeSpec(f"n{merge:04d}", "syn.merge", "v1",
                                  {"salt": merge, "rounds": rounds, "out_bytes": out_bytes},
                                  (f"n{then_:04d}", f"n{else_:04d}"), output_schema="hex"))
            nodes.append(_work(i + 4, (f"n{merge:04d}",), rounds, out_bytes))
            tip, i = i + 4, i + 5
        while i < size:
            nodes.append(_work(i, (f"n{tip:04d}",), rounds, out_bytes))
            tip, i = i, i + 1
    # effect intents hang off a seeded subset of non-source nodes
    work_ids = [n.id for n in nodes if n.op in ("syn.work", "syn.merge")]
    k = max(1, int(len(work_ids) * effect_rate))
    for j, target in enumerate(sorted(rng.sample(work_ids, min(k, len(work_ids))))):
        guards = next((n.guards for n in nodes if n.id == target), ())
        nodes.append(NodeSpec(f"e{j:04d}", "syn.effect", "v1", {"target": f"t{j}"}, (target,), guards,
                              "effect_intent", "intent"))
    return Graph(nodes, "syn-env-v1"), {"syn": env}


CHANGE_TYPES = ("param", "op_version", "observation", "gate_flip", "add_node", "remove_node", "env")


def apply_change(graph: Graph, snapshot, change: str, locality: float, rng: random.Random):
    """Return (new_graph, new_snapshot, description) for one seeded change.

    locality in [0, 1] picks the changed node by canonical topological position:
    0 is a source, 1 is near a sink.
    """
    order = graph.order
    def pick(pred) -> str:
        candidates = [n for n in order if pred(graph.nodes[n])]
        if not candidates:
            raise ValueError(f"No node supports change {change}")
        return candidates[min(len(candidates) - 1, int(locality * len(candidates)))]
    if change == "env":
        return graph.replace_nodes(env_version=graph.env_version + "+1"), snapshot, {"type": "env"}
    if change == "observation":
        name = pick(lambda n: n.effect == "observation")
        (key, out), = snapshot.entries_for(name)
        import json
        new = json.loads(out)[::-1]
        return graph, snapshot.with_observation(name, new), {"type": change, "node": name}
    if change == "param":
        name = pick(lambda n: n.op in ("syn.work", "syn.merge"))
        node = graph.nodes[name]
        changed = NodeSpec(node.id, node.op, node.op_version, dict(node.params, salt=node.params["salt"] + 100000),
                           node.inputs, node.guards, node.effect, node.output_schema)
        return graph.replace_nodes(changed), snapshot, {"type": change, "node": name}
    if change == "op_version":
        name = pick(lambda n: n.op == "syn.work" and n.op_version == "v1")
        node = graph.nodes[name]
        changed = NodeSpec(node.id, node.op, "v2", node.params, node.inputs, node.guards, node.effect,
                           node.output_schema)
        return graph.replace_nodes(changed), snapshot, {"type": change, "node": name}
    if change == "gate_flip":
        name = pick(lambda n: n.op == "syn.gate")
        node = graph.nodes[name]
        changed = NodeSpec(node.id, node.op, node.op_version, dict(node.params, invert=not node.params["invert"]),
                           node.inputs, node.guards, node.effect, node.output_schema)
        return graph.replace_nodes(changed), snapshot, {"type": change, "node": name}
    if change == "add_node":
        name = pick(lambda n: n.op == "syn.work" and graph.successors[n.id])
        node = graph.nodes[name]
        new_id = "x" + name[1:]
        added = NodeSpec(new_id, "syn.work", "v1", {"salt": 7, "rounds": node.params["rounds"],
                                                    "out_bytes": node.params["out_bytes"]},
                         (name,), node.guards, output_schema="hex")
        succ = graph.successors[name][0]
        s = graph.nodes[succ]
        rewired = NodeSpec(s.id, s.op, s.op_version, s.params, tuple(new_id if x == name else x for x in s.inputs),
                           s.guards, s.effect, s.output_schema)
        return graph.replace_nodes(added, rewired), snapshot, {"type": change, "node": new_id, "rewired": succ}
    if change == "remove_node":
        def removable(n):
            return (n.op == "syn.work" and len(n.inputs) == 1 and not n.guards
                    and all(not any(g.node == n.id for g in graph.nodes[s].guards) for s in graph.successors[n.id]))
        name = pick(removable)
        node = graph.nodes[name]
        parent = node.inputs[0]
        rewired = []
        for succ in graph.successors[name]:
            s = graph.nodes[succ]
            ins = []
            for x in s.inputs:
                y = parent if x == name else x
                if y not in ins:
                    ins.append(y)
            rewired.append(NodeSpec(s.id, s.op, s.op_version, s.params, tuple(ins), s.guards, s.effect,
                                    s.output_schema))
        return graph.replace_nodes(*rewired, remove=[name]), snapshot, {"type": change, "node": name}
    raise ValueError(f"Unknown change {change}")

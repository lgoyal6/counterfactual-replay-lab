"""Versioned deterministic operations used by dependency replay.

Each (name, version) pair is a separate implementation, so a version bump is a
real code change that engine.py must not satisfy from an older cache entry.
Observation operations run live only during capture; replay reads the frozen
snapshot. Effect-intent operations only describe a local effect; the engine
commits it to the run's isolated ledger.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import threading
import time
from typing import Any, Callable

from .core import RefundAgent
from .replay_state import canonical

SCHEMAS = ("any", "string", "number", "bool", "object", "hex", "intent", "decision")


class ToolError(RuntimeError):
    """A deterministic operation failed; dependents are blocked."""


class Cancelled(RuntimeError):
    """A cooperative operation acknowledged cancellation."""


class DynamicMutationRejected(RuntimeError):
    """An operation tried to add dependencies while the graph was frozen."""


class OpContext:
    """Cooperative cancellation and deadline checks for long-running operations."""

    def __init__(self, cancel: threading.Event | None = None, deadline: float | None = None,
                 environment: dict[str, Any] | None = None):
        self.cancel = cancel
        self.deadline = deadline
        self.environment = environment or {}

    def check(self) -> None:
        if self.cancel is not None and self.cancel.is_set():
            raise Cancelled("Operation acknowledged cancellation")
        if self.deadline is not None and time.monotonic() > self.deadline:
            raise Cancelled("Operation exceeded its time bound")

    def add_dependency(self, *_args: Any) -> None:
        raise DynamicMutationRejected("Dynamic dependency discovery is unsupported; declare and version the edge")


@dataclass(frozen=True)
class OpDef:
    name: str
    version: str
    kind: str
    schema: str
    fn: Callable[[dict[str, Any], list[Any], OpContext], Any]
    cooperative: bool = True


REGISTRY: dict[tuple[str, str], OpDef] = {}


def register(name: str, version: str, kind: str = "pure", schema: str = "any", cooperative: bool = True):
    def wrap(fn):
        if (name, version) in REGISTRY:
            raise ValueError(f"Duplicate operation {name}@{version}")
        if schema not in SCHEMAS:
            raise ValueError(f"Unknown schema {schema}")
        REGISTRY[(name, version)] = OpDef(name, version, kind, schema, fn, cooperative)
        return fn
    return wrap


def lookup(name: str, version: str) -> OpDef:
    try:
        return REGISTRY[(name, version)]
    except KeyError:
        raise ToolError(f"Unknown operation {name}@{version}") from None


def check_schema(schema: str, value: Any) -> None:
    ok = {
        "any": lambda v: True,
        "string": lambda v: isinstance(v, str),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "bool": lambda v: isinstance(v, bool),
        "object": lambda v: isinstance(v, dict),
        "hex": lambda v: isinstance(v, str) and all(c in "0123456789abcdef" for c in v),
        "intent": lambda v: isinstance(v, dict) and isinstance(v.get("kind"), str) and isinstance(v.get("target"), str),
        "decision": lambda v: isinstance(v, dict) and isinstance(v.get("decision"), str),
    }[schema](value)
    if not ok:
        raise ToolError(f"Output violates schema {schema}")
    canonical(value)  # rejects NaN/infinity and non-JSON values


def _burn(seed: bytes, rounds: int, ctx: OpContext) -> bytes:
    """Compute-bound deterministic work standing in for a local model or tool."""
    if not isinstance(rounds, int) or rounds < 0 or rounds > 2_000_000:
        raise ToolError("rounds must be an integer in [0, 2000000]")
    h = sha256(seed).digest()
    for i in range(rounds):
        if i % 4096 == 4095:
            ctx.check()
        h = sha256(h).digest()
    return h


def _expand(h: bytes, out_bytes: int) -> str:
    if not isinstance(out_bytes, int) or out_bytes < 1 or out_bytes > 1_000_000:
        raise ToolError("out_bytes must be in [1, 1000000]")
    text = h.hex()
    return (text * (out_bytes * 2 // len(text) + 1))[: out_bytes * 2]


# ---------------------------------------------------------------- refund DAG

@register("refund.request", "v1", schema="object")
def _request(params, inputs, ctx):
    return {"order_id": str(params["order_id"]), "customer_id": str(params["customer_id"])}


@register("orders.lookup", "v1", kind="observation", schema="object")
def _orders_lookup(params, inputs, ctx):
    order = ctx.environment.get("orders", {}).get(inputs[0]["order_id"])
    return {"found": False} if order is None else {"found": True, **order}


@register("crm.customer", "v1", kind="observation", schema="object")
def _crm_customer(params, inputs, ctx):
    customer = ctx.environment.get("customers", {}).get(inputs[0]["customer_id"])
    return {"found": False} if customer is None else {"found": True, **customer}


@register("refund.eligibility", "v1", schema="string")
def _eligibility(params, inputs, ctx):
    order = inputs[0]
    if not order.get("found") or order.get("delivered") is None:
        return "unknown"
    if not order["delivered"]:
        return "ineligible"
    if order["category"] in params.get("excluded_categories", []):
        return "ineligible"
    return "eligible" if order["days_since_delivery"] <= params["window_days"] else "ineligible"


@register("refund.amount", "v1", schema="number")
def _amount(params, inputs, ctx):
    order, eligibility = inputs
    if eligibility != "eligible":
        return 0
    cents = order["amount_cents"] * (100 - params["restocking_fee_pct"]) // 100
    return min(cents, params["cap_cents"])


def _risk(weights, params, inputs, ctx):
    customer, amount = inputs
    _burn(canonical([customer, amount]).encode(), params.get("rounds", 0), ctx)
    if not customer.get("found"):
        return {"score": 100, "band": "high"}
    score = (weights[0] * customer["prior_refunds"] + (weights[1] if amount > 20000 else 0)
             + (weights[2] if customer["account_age_days"] < 30 else 0)
             + (weights[3] if customer["tier"] == "new" else 0))
    score = min(score, 100)
    return {"score": score, "band": "high" if score >= 60 else "medium" if score >= 30 else "low"}


@register("risk.score", "v1", schema="object")
def _risk_v1(params, inputs, ctx):
    return _risk((15, 20, 25, 10), params, inputs, ctx)


@register("risk.score", "v2", schema="object")
def _risk_v2(params, inputs, ctx):
    # v2 weighs recent accounts more and large amounts less.
    return _risk((12, 10, 45, 10), params, inputs, ctx)


@register("refund.route", "v1", schema="string")
def _route(params, inputs, ctx):
    eligibility, amount, risk = inputs
    if eligibility == "unknown":
        return "review"
    if eligibility == "ineligible" or not params.get("approve_eligible", True):
        return "deny"
    if risk["score"] > params["max_risk"]:
        return "manual"
    return "auto" if amount <= params["auto_approve_max_cents"] else "manual"


@register("refund.issue_intent", "v1", kind="effect_intent", schema="intent")
def _issue(params, inputs, ctx):
    request, amount = inputs
    return {"kind": "refund", "target": request["order_id"], "amount_cents": amount}


@register("review.ticket_intent", "v1", kind="effect_intent", schema="intent")
def _ticket(params, inputs, ctx):
    request, route = inputs
    return {"kind": "review_ticket", "target": request["order_id"], "queue": params.get("queue", "refunds"),
            "reason": route}


@register("refund.denial_intent", "v1", kind="effect_intent", schema="intent")
def _denial(params, inputs, ctx):
    request, eligibility = inputs
    return {"kind": "denial_notice", "target": request["order_id"], "reason": eligibility}


@register("fraud.hold", "v1", schema="bool")
def _fraud_hold(params, inputs, ctx):
    risk = inputs[0]
    return risk["score"] >= params["hold_at"]


@register("refund.decision", "v1", schema="decision")
def _decision(params, inputs, ctx):
    route, amount, refund, ticket, denial = inputs[:5]
    hold = inputs[5] if len(inputs) > 5 else None
    mapping = {"auto": "approve_refund", "manual": "manual_review", "deny": "deny_refund",
               "review": "review_refund"}
    present = {"auto": refund, "manual": ticket, "review": ticket, "deny": denial}
    if present[route] is None:
        raise ToolError(f"Route {route} has no matching effect intent")
    decision = mapping[route]
    if hold is True and decision == "approve_refund":
        decision = "hold_refund"
    return {"decision": decision, "amount_cents": amount if route == "auto" else 0}


# ------------------------------------------- fixed-prefix compatibility graph

@register("compat.lookup", "v1", kind="observation", schema="string")
def _compat_lookup(params, inputs, ctx):
    return ctx.environment["eligibility"][params["request"]]


@register("compat.apply_policy", "v1", schema="string")
def _compat_policy(params, inputs, ctx):
    # Bind the original fixed-prefix policy implementation, not a copy.
    return RefundAgent.apply_policy(params["policy"], inputs[0])


@register("compat.refund_intent", "v1", kind="effect_intent", schema="intent")
def _compat_refund(params, inputs, ctx):
    return {"kind": "refund", "target": params["request"], "value": inputs[0]}


# ------------------------------------------------------- synthetic families

@register("syn.source", "v1", schema="hex")
def _syn_source(params, inputs, ctx):
    return _expand(_burn(canonical(params["salt"]).encode(), params.get("rounds", 0), ctx),
                   params.get("out_bytes", 16))


@register("syn.obs", "v1", kind="observation", schema="hex")
def _syn_obs(params, inputs, ctx):
    value = ctx.environment.get("syn", {}).get(params["key"])
    if value is None:
        raise ToolError(f"Synthetic source {params['key']} unavailable")
    return value


@register("syn.work", "v1", schema="hex")
def _syn_work(params, inputs, ctx):
    seed = canonical([params.get("salt", 0), inputs]).encode()
    return _expand(_burn(seed, params.get("rounds", 0), ctx), params.get("out_bytes", 16))


@register("syn.work", "v2", schema="hex")
def _syn_work_v2(params, inputs, ctx):
    seed = canonical(["v2", params.get("salt", 0), inputs]).encode()
    return _expand(_burn(seed, params.get("rounds", 0), ctx), params.get("out_bytes", 16))


@register("syn.gate", "v1", schema="bool")
def _syn_gate(params, inputs, ctx):
    bucket = int(sha256(canonical(inputs).encode()).hexdigest()[:8], 16) % params.get("modulus", 2)
    return (bucket < params.get("threshold", 1)) != bool(params.get("invert", False))


@register("syn.merge", "v1", schema="hex")
def _syn_merge(params, inputs, ctx):
    present = [x for x in inputs if x is not None]
    seed = canonical([params.get("salt", 0), len(present), present]).encode()
    return _expand(_burn(seed, params.get("rounds", 0), ctx), params.get("out_bytes", 16))


@register("syn.effect", "v1", kind="effect_intent", schema="intent")
def _syn_effect(params, inputs, ctx):
    return {"kind": "syn_effect", "target": str(params["target"]),
            "value": sha256(canonical(inputs).encode()).hexdigest()[:16]}


@register("syn.fail", "v1", schema="hex")
def _syn_fail(params, inputs, ctx):
    if params.get("fail", True):
        raise ToolError(f"Injected deterministic failure {params.get('code', 'E_FAULT')}")
    return _expand(sha256(canonical(inputs).encode()).digest(), 16)


@register("syn.slow", "v1", schema="hex")
def _syn_slow(params, inputs, ctx):
    """Sleep-based cooperative tool used only for cancellation tests (disclosed)."""
    end = time.monotonic() + params.get("seconds", 1.0)
    while time.monotonic() < end:
        ctx.check()
        time.sleep(0.005)
    return _expand(sha256(canonical(inputs).encode()).digest(), 16)


@register("syn.stubborn", "v1", schema="hex", cooperative=False)
def _syn_stubborn(params, inputs, ctx):
    """Non-cooperative sleep used to test uncertain in-flight cancellation."""
    time.sleep(params.get("seconds", 1.0))
    return _expand(sha256(canonical(inputs).encode()).digest(), 16)


@register("syn.discover", "v1", schema="hex")
def _syn_discover(params, inputs, ctx):
    ctx.add_dependency(params.get("wants", "unknown"))
    return "00"

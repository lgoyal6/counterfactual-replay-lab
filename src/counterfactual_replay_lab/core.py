from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
from typing import Any, Callable, Literal


@dataclass(frozen=True)
class Span:
    name: str
    kind: Literal["agent", "tool", "decision", "evaluator"]
    input: Any
    output: Any
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Trace:
    trace_id: str
    request: str
    policy_version: str
    spans: tuple[Span, ...]
    decision: str
    verdict: Literal["PASS", "FAIL"]
    score: float
    cost_usd: float
    latency_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "request": self.request,
            "policy_version": self.policy_version,
            "spans": [asdict(span) for span in self.spans],
            "decision": self.decision,
            "verdict": self.verdict,
            "score": self.score,
            "cost_usd": self.cost_usd,
            "latency_ms": self.latency_ms,
        }


@dataclass(frozen=True)
class Experiment:
    baseline_trace_id: str
    counterfactual_trace_id: str
    changed_variable: str
    old_value: str
    new_value: str
    same_request: bool
    same_prefix: bool
    first_divergence: str | None
    score_delta: float
    cost_delta_usd: float
    latency_delta_ms: int
    recommendation: Literal["KEEP", "REJECT", "INVESTIGATE"]
    refusal_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RefundAgent:
    """Deterministic fixture agent used to prove replay semantics before live wiring."""

    def run(self, request: str, policy_version: str) -> Trace:
        eligible = "eligible" in request.lower()
        policy_result = "approve" if policy_version == "v2" and eligible else "deny"
        decision = "approve_refund" if policy_result == "approve" else "deny_refund"
        verdict: Literal["PASS", "FAIL"] = "PASS" if decision == "approve_refund" else "FAIL"
        score = 1.0 if verdict == "PASS" else 0.0
        spans = (
            Span("refund_agent", "agent", request, "planned refund check"),
            Span("lookup_order", "tool", request, "eligible" if eligible else "ineligible"),
            Span("apply_policy", "tool", policy_version, policy_result),
            Span("decide", "decision", policy_result, decision),
            Span("policy_evaluator", "evaluator", decision, verdict),
        )
        identity = json.dumps(
            {"request": request, "policy": policy_version, "spans": [asdict(s) for s in spans]},
            sort_keys=True,
        )
        trace_id = sha256(identity.encode()).hexdigest()[:12]
        return Trace(
            trace_id=trace_id,
            request=request,
            policy_version=policy_version,
            spans=spans,
            decision=decision,
            verdict=verdict,
            score=score,
            cost_usd=0.002,
            latency_ms=240,
        )


class ReplayEngine:
    def __init__(self, runner: Callable[[str, str], Trace]):
        self.runner = runner

    def replay(self, baseline: Trace, *, changed_variable: str, new_value: str) -> Trace:
        if changed_variable != "policy_version":
            raise ValueError("Only policy_version is supported by the MVP")
        if not new_value or new_value == baseline.policy_version:
            raise ValueError("Counterfactual must change exactly one variable")
        result = self.runner(baseline.request, new_value)
        if result.request != baseline.request:
            raise RuntimeError("Replay changed the original request")
        if result.spans[:2] != baseline.spans[:2]:
            raise RuntimeError("Replay prefix changed before the intervention")
        return result


def compare(baseline: Trace, counterfactual: Trace, *, changed_variable: str, old_value: str, new_value: str) -> Experiment:
    if baseline.request != counterfactual.request:
        raise ValueError("Cannot compare traces with different requests")
    prefix_length = 0
    for left, right in zip(baseline.spans, counterfactual.spans):
        if left == right:
            prefix_length += 1
        else:
            break
    first_divergence = counterfactual.spans[prefix_length].name if prefix_length < len(counterfactual.spans) else None
    improved = baseline.verdict == "FAIL" and counterfactual.verdict == "PASS"
    recommendation: Literal["KEEP", "REJECT", "INVESTIGATE"]
    if improved:
        recommendation = "KEEP"
    elif baseline.verdict == counterfactual.verdict and baseline.score == counterfactual.score:
        recommendation = "REJECT"
    else:
        recommendation = "INVESTIGATE"
    return Experiment(
        baseline_trace_id=baseline.trace_id,
        counterfactual_trace_id=counterfactual.trace_id,
        changed_variable=changed_variable,
        old_value=old_value,
        new_value=new_value,
        same_request=True,
        same_prefix=prefix_length >= 2,
        first_divergence=first_divergence,
        score_delta=counterfactual.score - baseline.score,
        cost_delta_usd=counterfactual.cost_usd - baseline.cost_usd,
        latency_delta_ms=counterfactual.latency_ms - baseline.latency_ms,
        recommendation=recommendation,
    )


def run_mvp() -> tuple[Trace, Trace, Experiment]:
    agent = RefundAgent()
    baseline = agent.run("Order 1042 is eligible for a refund", "v1")
    counterfactual = ReplayEngine(agent.run).replay(
        baseline, changed_variable="policy_version", new_value="v2"
    )
    experiment = compare(
        baseline,
        counterfactual,
        changed_variable="policy_version",
        old_value="v1",
        new_value="v2",
    )
    return baseline, counterfactual, experiment

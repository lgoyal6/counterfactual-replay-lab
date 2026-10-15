"""Capture local refund decisions and compare one-policy replay interventions.

replay_state.py owns recorded dependencies and the isolated effect ledger.
Derived trace fields must agree with captured spans before comparison or import.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import re
from time import perf_counter_ns
from typing import Any, Callable, Literal

from .replay_state import EffectLedger, Observation, ReplayState, digest


@dataclass(frozen=True)
class Span:
    name: str
    kind: Literal["agent", "tool", "decision", "evaluator"]
    input: Any
    output: Any
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Scenario:
    """Truth is supplied independently of the agent's decision and evaluator."""
    scenario_id: str
    request: str
    eligibility: str
    expected_decision: str

    def __post_init__(self):
        if self.eligibility not in {"eligible", "ineligible", "unknown"}:
            raise ValueError("Invalid eligibility observation")
        if self.expected_decision not in {"approve_refund", "deny_refund", "review_refund"}:
            raise ValueError("Invalid oracle decision")


def _fixture(request: str) -> Scenario | None:
    # Convenience grammar for the original demo. Benchmarks supply explicit truth.
    match = re.fullmatch(r"Order (\d+) is (eligible|ineligible) for a refund", request, re.IGNORECASE)
    if not match:
        return None
    label = match[2].lower()
    return Scenario(match[1], request, label, "approve_refund" if label == "eligible" else "deny_refund")


@dataclass(frozen=True)
class Trace:
    trace_id: str
    request: str
    policy_version: str
    spans: tuple[Span, ...]
    decision: str
    verdict: Literal["PASS", "FAIL", "UNKNOWN"]
    score: float
    cost_usd: float | None
    latency_ms: float
    replay_state: ReplayState | None = None
    state_fingerprint: str | None = None
    executed_tools: tuple[str, ...] = ()
    ledger_after: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Trace":
        data = dict(value)
        data["spans"] = tuple(Span(**s) for s in data["spans"])
        state = data.get("replay_state")
        if state is None:
            raise ValueError("Captured state missing")
        state = dict(state)
        state["observations"] = tuple(Observation(**o) for o in state["observations"])
        state["ledger_snapshot"] = tuple(tuple(row) for row in state["ledger_snapshot"])
        data["replay_state"] = ReplayState(**state)
        data["executed_tools"] = tuple(data["executed_tools"])
        data["ledger_after"] = tuple(tuple(row) for row in data["ledger_after"])
        trace = cls(**data)
        validate_trace(trace)
        return trace


def validate_trace(trace: Trace) -> ReplayState:
    state = trace.replay_state
    if state is None or trace.state_fingerprint is None:
        raise ValueError("Captured state missing")
    state.validate(trace.request, [asdict(s) for s in trace.spans[:2]], trace.state_fingerprint)
    observed = json.loads(state.observations[0].output)
    expected_prefix = (
        Span("refund_agent", "agent", trace.request, "planned refund check"),
        Span("lookup_order", "tool", trace.request, observed, {"version": "fixture-lookup-v1", "sequence": 0}),
    )
    if trace.spans[:2] != expected_prefix:
        raise ValueError("Replay prefix disagrees with recorded observation")
    if trace.trace_id != digest({"request": trace.request, "policy": trace.policy_version,
                                 "spans": [asdict(s) for s in trace.spans], "state": state.fingerprint})[:12]:
        raise ValueError("Trace content changed")
    expected_verdict = "UNKNOWN" if state.expected_decision is None else (
        "PASS" if trace.decision == state.expected_decision else "FAIL")
    if trace.verdict != expected_verdict or trace.score != (1.0 if expected_verdict == "PASS" else 0.0):
        raise ValueError("Evaluator disagrees with scenario oracle")
    policy_decision = RefundAgent.apply_policy(trace.policy_version, observed)
    policy_result = policy_decision.removesuffix("_refund")
    expected_suffix = (
        Span("apply_policy", "tool", trace.policy_version, policy_result),
        Span("decide", "decision", policy_result, policy_decision),
        Span("policy_evaluator", "evaluator", policy_decision, expected_verdict),
    )
    if trace.decision != policy_decision or trace.spans[2:] != expected_suffix:
        raise ValueError("Trace decision or suffix disagrees with captured spans")
    return state


class RefundAgent:
    """Local fixture with captured read-only lookup and isolated refund effects."""

    def run(self, request: str, policy_version: str, *, scenario: Scenario | None = None,
            ledger: EffectLedger | None = None, observations: tuple[Observation, ...] | None = None,
            cancelled: Callable[[], bool] = lambda: False, crash_after_effect: bool = False) -> Trace:
        started = perf_counter_ns()
        scenario = scenario or _fixture(request)
        if scenario and scenario.request != request:
            raise ValueError("Scenario request differs")
        if cancelled():
            raise RuntimeError("Cancelled before lookup")
        observations = observations if observations is not None else (
            Observation.lookup(request, scenario.eligibility if scenario else "unknown"),)
        eligibility = self.lookup_order(request, observations)
        prefix = (
            Span("refund_agent", "agent", request, "planned refund check"),
            Span("lookup_order", "tool", request, eligibility, {"version": "fixture-lookup-v1", "sequence": 0}),
        )
        own_ledger = ledger is None
        ledger = ledger if ledger is not None else EffectLedger()
        state = ReplayState(digest(request), observations, ledger.snapshot(),
                            digest([asdict(s) for s in prefix]),
                            scenario.expected_decision if scenario else None,
                            scenario.scenario_id if scenario else "unlabeled")
        try:
            return self._suffix(request, policy_version, prefix, state, ledger, started,
                                ("lookup_order",), cancelled, crash_after_effect)
        finally:
            if own_ledger:
                ledger.close()

    @staticmethod
    def lookup_order(request: str, observations: tuple[Observation, ...]) -> str:
        if len(observations) != 1:
            raise ValueError("Recorded dependency state missing")
        observations[0].validate(request)
        return json.loads(observations[0].output)

    @staticmethod
    def apply_policy(policy: str, eligibility: str) -> str:
        if policy not in {"v1", "v2", "v3"}:
            raise ValueError("Unknown policy version")
        return "review_refund" if eligibility == "unknown" else (
            "approve_refund" if policy == "v2" and eligibility == "eligible" else "deny_refund")

    def _suffix(self, request, policy, prefix, state, ledger, started, executed,
                cancelled, crash_after_effect=False) -> Trace:
        if cancelled():
            raise RuntimeError("Cancelled before effect")
        eligibility = json.loads(state.observations[0].output)
        decision = self.apply_policy(policy, eligibility)
        policy_result = decision.removesuffix("_refund")
        if decision == "approve_refund":
            ledger.apply(digest({"request": request, "operation": "refund"}), decision,
                         crash_after_effect=crash_after_effect)
        verdict = "UNKNOWN" if state.expected_decision is None else (
            "PASS" if decision == state.expected_decision else "FAIL")
        spans = prefix + (
            Span("apply_policy", "tool", policy, policy_result),
            Span("decide", "decision", policy_result, decision),
            Span("policy_evaluator", "evaluator", decision, verdict),
        )
        trace_id = digest({"request": request, "policy": policy,
                           "spans": [asdict(s) for s in spans], "state": state.fingerprint})[:12]
        return Trace(trace_id, request, policy, spans, decision, verdict,
                     1.0 if verdict == "PASS" else 0.0, None,
                     (perf_counter_ns() - started) / 1e6, state, state.fingerprint,
                     executed + ("apply_policy",) + (("local_refund",) if decision == "approve_refund" else ()),
                     ledger.snapshot())

    def replay_suffix(self, baseline: Trace, new_value: str,
                      cancelled: Callable[[], bool] = lambda: False) -> Trace:
        started = perf_counter_ns()
        state = validate_trace(baseline)
        ledger = EffectLedger.restore(state.ledger_snapshot)
        try:
            return self._suffix(baseline.request, new_value, baseline.spans[:2], state,
                                ledger, started, (), cancelled)
        finally:
            ledger.close()


class ReplayEngine:
    def __init__(self, runner: Callable[[str, str], Trace]):
        self.runner = runner

    def replay(self, baseline: Trace, *, changed_variable: str, new_value: str,
               cancelled: Callable[[], bool] = lambda: False) -> Trace:
        if changed_variable != "policy_version":
            raise ValueError("Only policy_version is supported")
        if not new_value or new_value == baseline.policy_version:
            raise ValueError("Counterfactual must change exactly one variable")
        agent = getattr(self.runner, "__self__", None)
        if not isinstance(agent, RefundAgent):
            raise ValueError("Runner must support captured suffix replay")
        result = agent.replay_suffix(baseline, new_value, cancelled)
        validate_trace(result)
        if result.replay_state != baseline.replay_state or result.spans[:2] != baseline.spans[:2]:
            raise RuntimeError("Replay changed request, dependencies or prefix")
        return result


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
    cost_delta_usd: float | None
    latency_delta_ms: float
    recommendation: Literal["KEEP", "REJECT", "INVESTIGATE"]
    refusal_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compare(baseline: Trace, counterfactual: Trace, *, changed_variable: str,
            old_value: str, new_value: str) -> Experiment:
    if baseline.request != counterfactual.request:
        raise ValueError("Cannot compare traces with different requests")
    left, right = validate_trace(baseline), validate_trace(counterfactual)
    if changed_variable != "policy_version" or old_value != baseline.policy_version or new_value != counterfactual.policy_version or old_value == new_value:
        raise ValueError("Comparison must describe exactly one policy intervention")
    if left != right or baseline.spans[:2] != counterfactual.spans[:2]:
        raise ValueError("Comparison dependencies, oracle or prefix changed")
    same_truth = left.expected_decision is not None
    improved = same_truth and baseline.verdict == "FAIL" and counterfactual.verdict == "PASS"
    recommendation = "KEEP" if improved else (
        "REJECT" if same_truth and baseline.verdict == counterfactual.verdict else "INVESTIGATE")
    divergence = next((b.name for a, b in zip(baseline.spans, counterfactual.spans) if a != b), None)
    return Experiment(baseline.trace_id, counterfactual.trace_id, changed_variable, old_value,
                      new_value, True, True, divergence, counterfactual.score - baseline.score,
                      None, counterfactual.latency_ms - baseline.latency_ms, recommendation,
                      None if same_truth else "Independent scenario truth unavailable")


def run_mvp() -> tuple[Trace, Trace, Experiment]:
    agent = RefundAgent()
    baseline = agent.run("Order 1042 is eligible for a refund", "v1")
    counterfactual = ReplayEngine(agent.run).replay(baseline, changed_variable="policy_version", new_value="v2")
    return baseline, counterfactual, compare(baseline, counterfactual,
        changed_variable="policy_version", old_value="v1", new_value="v2")

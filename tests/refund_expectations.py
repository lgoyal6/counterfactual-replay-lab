"""Independent refund expectations written from the business rules, not the engine.

This module never imports the planner, the engine or the operation registry.
It states what the refund workflow must decide and which local effect intents
it must record, so a bug shared by incremental replay and the full-rerun
oracle (for example in an operation) still fails a test.
"""

RULES = """
eligibility: order missing or delivery unknown -> unknown; not delivered -> ineligible;
  excluded category -> ineligible; days since delivery <= window -> eligible else ineligible
amount: eligible -> amount * (100 - fee) // 100 capped at cap, else 0 (integer cents)
risk v1: 15*prior_refunds + 20 if amount > 20000 + 25 if account < 30 days + 10 if tier new, max 100
risk v2: 12*prior_refunds + 10 if amount > 20000 + 45 if account < 30 days + 10 if tier new, max 100
route: unknown -> review; ineligible or approvals disabled -> deny; risk > max -> manual;
  amount <= auto max -> auto else manual
effects: auto -> refund(amount); manual/review -> review ticket; deny -> denial notice
decision: auto approve_refund (hold_refund if fraud hold enabled and risk >= hold_at), manual manual_review,
  review review_refund, deny deny_refund
"""


def expected_outcome(order, customer, policy, risk_version="v1", fraud_hold=None, order_id="1042"):
    if order is None or order.get("delivered") is None:
        eligibility = "unknown"
    elif order["delivered"] is False or order["category"] in policy["excluded_categories"]:
        eligibility = "ineligible"
    elif order["days_since_delivery"] <= policy["window_days"]:
        eligibility = "eligible"
    else:
        eligibility = "ineligible"
    amount = 0
    if eligibility == "eligible":
        amount = min(order["amount_cents"] * (100 - policy["restocking_fee_pct"]) // 100, policy["cap_cents"])
    if customer is None:
        score = 100
    else:
        w = {"v1": (15, 20, 25, 10), "v2": (12, 10, 45, 10)}[risk_version]
        score = w[0] * customer["prior_refunds"]
        if amount > 20000:
            score += w[1]
        if customer["account_age_days"] < 30:
            score += w[2]
        if customer["tier"] == "new":
            score += w[3]
        score = min(score, 100)
    if eligibility == "unknown":
        route = "review"
    elif eligibility == "ineligible" or not policy["approve_eligible"]:
        route = "deny"
    elif score > policy["max_risk"]:
        route = "manual"
    elif amount <= policy["auto_approve_max_cents"]:
        route = "auto"
    else:
        route = "manual"
    if route == "auto":
        effects = {("refund", order_id, amount)}
        decision = "approve_refund"
        if fraud_hold is not None and score >= fraud_hold:
            decision = "hold_refund"
    elif route in ("manual", "review"):
        effects = {("review_ticket", order_id, route)}
        decision = "manual_review" if route == "manual" else "review_refund"
    else:
        effects = {("denial_notice", order_id, eligibility)}
        decision = "deny_refund"
    return {"decision": decision, "amount_cents": amount if route == "auto" else 0, "effects": effects,
            "route": route}


def intents_to_effects(ledger_values):
    """Normalize stored canonical intents to the tuples used above."""
    import json
    out = set()
    for text in ledger_values:
        intent = json.loads(text)
        if intent["kind"] == "refund":
            out.add(("refund", intent["target"], intent["amount_cents"]))
        elif intent["kind"] == "review_ticket":
            out.add(("review_ticket", intent["target"], intent["reason"]))
        else:
            out.add((intent["kind"], intent["target"], intent["reason"]))
    return out

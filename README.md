# Counterfactual Replay Lab

Counterfactual Replay Lab takes one agent run, changes one recorded variable, replays the same request, and compares the result.

The MVP uses a deterministic refund-support fixture to verify the core contract before live service wiring:

- the request is unchanged;
- the trace prefix before the intervention is unchanged;
- exactly one variable changes;
- the comparison identifies the first divergence;
- the result is KEEP, REJECT, or INVESTIGATE.

## Run the MVP

```bash
python3 -m pytest
PYTHONPATH=src python3 scripts/demo.py
```

The demo writes `artifacts/mvp-experiment.json`.

## Reproduce the larger synthetic evaluation

Freeze a corpus outside the source checkout, then evaluate both paths against the
same stored eligibility and oracle labels:

```bash
REPLAY_EVIDENCE_DIR=$(mktemp -d)
python3 scripts/generate_replay_cases.py --count 10000 --seed 20261101 \
  --output "$REPLAY_EVIDENCE_DIR/cases.json"
PYTHONPATH=src python3 scripts/benchmark_replay.py \
  --cases "$REPLAY_EVIDENCE_DIR/cases.json" \
  --output "$REPLAY_EVIDENCE_DIR/study.json" --repetitions 3 --warmup 1
```

The November 1, 2026 local run scored the original `v1` policy correctly on
5,000 of 10,000 orders and the replayed `v2` policy on 10,000 of 10,000. Replay
preserved all 3,500 denial controls and routed all 1,500 unknown-eligibility
orders to review. No unsafe KEEP recommendation occurred. Each measured replay
used the captured request, prefix and dependency state with an isolated ledger.

The seeded corpus contains unique requests with ten text forms, including
contradictory customer claims, Spanish text, whitespace, quoted identifiers and
longer context. Labels come from stored eligibility, not the agent under test.
The 50/35/15 approve/deny/review mix is declared before execution. The larger
corpus exercises input identity and replay correctness at greater volume, but
still has only three eligibility states. It is not a broader refund-policy,
real-customer, LLM-quality, speedup or cost-savings result. The report retains raw
timings and records this limitation. Repetitions do not count as distinct orders.

## Integration status

The fixture proves replay semantics locally. The `neatlogs_integration.py` adapter uses the documented Python SDK path (`neatlogs.init`, `neatlogs.span`, `neatlogs.flush`, and `neatlogs.shutdown`) only when the installed SDK exposes all of those APIs and `NEATLOGS_API_KEY` is supplied. Without both, the project emits local JSON only and says so explicitly. See `docs/integration-status.md`.

Entire and cfo.ai remain gated on their real project access. The submission must not claim either integration until a real checkpoint and a real cfo.ai plan have been captured.

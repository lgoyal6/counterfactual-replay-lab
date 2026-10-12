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

## Integration status

The fixture proves replay semantics locally. The `neatlogs_integration.py` adapter uses the documented Python SDK path (`neatlogs.init`, `neatlogs.span`, `neatlogs.flush`, and `neatlogs.shutdown`) only when the installed SDK exposes all of those APIs and `NEATLOGS_API_KEY` is supplied. Without both, the project emits local JSON only and says so explicitly. See `docs/integration-status.md`.

Entire and cfo.ai remain gated on their real project access. The submission must not claim either integration until a real checkpoint and a real cfo.ai plan have been captured.

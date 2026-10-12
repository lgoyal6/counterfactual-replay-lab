# Counterfactual Replay Lab build plan

## Verified MVP

- Deterministic refund agent with a seeded stale-policy failure.
- Baseline and counterfactual trace objects.
- One-variable policy replay.
- Prefix and request preservation checks.
- First-divergence detection.
- KEEP, REJECT, and INVESTIGATE recommendation model.
- Invalid-variable and changed-request refusal tests.

## Next integration gates

1. Obtain the neatlogs hackathon workspace key and the SDK release matching the current Python tracing docs.
2. Run neatlogs' official local and authenticated SDK probes.
3. Emit the baseline and counterfactual runs as real neatlogs traces.
4. Add real neatlogs detections and evaluators for stale policy and refund correctness.
5. Capture Entire repository and policy checkpoints for both runs.
6. Create the cfo.ai business plan in the real cfo.ai workspace and reconcile its cost assumptions with measured traces.
7. Add the judge-facing comparison UI and evidence report.
8. Record the demo only after the live traces, checkpoint, and cfo.ai artifact are verified.

## Scope guard

The MVP supports one deterministic policy mutation. Prompt, model, retrieval, and tool-response mutations are follow-up work after the live policy replay passes. The project must refuse experiments involving uncaptured state or more than one changed variable.

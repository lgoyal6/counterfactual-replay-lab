# Integration status

Verified on October 12, 2026:

- The local replay contract passes without network access.
- The published PyPI package `neatlogs==1.1.8` imports successfully.
- That package exposes `init`, but not the `span`, `flush`, or `shutdown` APIs shown in the current neatlogs Python SDK documentation.
- No `NEATLOGS_API_KEY` is present in this environment.
- Therefore live neatlogs export is **blocked and unclaimed**, rather than simulated.

The next integration checkpoint is to obtain the hackathon workspace key and the SDK release that exposes the documented tracing APIs, run the SDK's official local and authenticated probes, then emit the baseline and counterfactual traces from the real agent.

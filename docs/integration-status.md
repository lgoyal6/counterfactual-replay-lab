# Integration status

Verified on October 12, 2026:

- The project now requires Python `>=3.11,<3.14`, matching the current neatlogs SDK support window.
- A fresh Python 3.12 environment installs `neatlogs==1.4.26`.
- `python -m neatlogs doctor --local --json` passes with schema version `2`, `span_count: 4`, and `dropped_spans: 0`.
- The authenticated `python -m neatlogs doctor --probe --json` passes against the project workspace, with the Doctor trace visible and finalized remotely, `readback_span_count: 4`, and no dropped spans.
- The local replay contract and demo pass without network access.
- The project API key is intentionally kept out of the repository and shell history.
- The instrumented project entry point was run with the project key, and the resulting replay trace was observed in the neatlogs dashboard with the baseline and counterfactual payloads, `FAIL` to `PASS` verdicts, and `apply_policy` as the first divergence. The Doctor probe remains separately identified as synthetic pipeline verification.

The remaining product work is optional polish: richer semantic span attributes, linked trace IDs in the UI, and editable interventions beyond the v1 to v2 fixture.

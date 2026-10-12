# Integration status

Verified on October 12, 2026:

- The project now requires Python `>=3.11,<3.14`, matching the current neatlogs SDK support window.
- A fresh Python 3.12 environment installs `neatlogs==1.4.26`.
- `python -m neatlogs doctor --local --json` passes with schema version `2`, `span_count: 4`, and `dropped_spans: 0`.
- The authenticated `python -m neatlogs doctor --probe --json` passes against the project workspace, with the Doctor trace visible and finalized remotely, `readback_span_count: 4`, and no dropped spans.
- The local replay contract and demo pass without network access.
- The project API key is intentionally kept out of the repository and shell history.
- The application's own live export remains **unverified** until the instrumented project entry point is run and its trace is observed in the dashboard. The Doctor probe is synthetic pipeline verification and is reported separately.

The remaining integration checkpoint is verification that the baseline and counterfactual traces from the project appear in the neatlogs dashboard.

# Integration status

Verified on October 12, 2026:

- The project now requires Python `>=3.11,<3.14`, matching the current neatlogs SDK support window.
- A fresh Python 3.12 environment installs `neatlogs==1.4.26`.
- `python -m neatlogs doctor --local --json` passes with schema version `2`, `span_count: 4`, and `dropped_spans: 0`.
- The local replay contract and demo pass without network access.
- Live export remains **unverified** because no `NEATLOGS_API_KEY` is available in the local environment. No network trace is being claimed.

The remaining integration checkpoint is an authenticated probe with the hackathon workspace key, followed by verification that the baseline and counterfactual traces appear in the neatlogs dashboard.

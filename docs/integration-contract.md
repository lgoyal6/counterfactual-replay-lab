# Integration contract

## neatlogs

Instrument the real baseline and counterfactual runs with the neatlogs SDK. Each trace must include request, model and tool spans, policy metadata, evaluator results, cost, latency, and an experiment ID linking the two runs. The final demo must show the traces in neatlogs or use exported trace evidence from a real neatlogs workspace.

## Entire

Record the repository and policy checkpoint used for the baseline and counterfactual. The report must link the changed policy or prompt to the checkpoint that produced the counterfactual.

## cfo.ai

Create the business plan for replay and evaluation as a real cfo.ai artifact. Include pricing, per-experiment cost, model and tool costs, gross margin, and runway scenarios. The repository will link the exported plan and reconcile its assumptions with measured trace costs.

No integration is considered complete when it is represented only by a screenshot, placeholder, or invented API response.

## Local captured-replay boundary

The local fixture records lookup name/version, canonical input and output, logical
order, explicit independent scenario truth, and the ledger snapshot immediately
before policy evaluation. `ReplayEngine` validates the capture and executes only
policy/effect/evaluation after that boundary. A prefix checksum detects accidental
changes; it does not authenticate an untrusted trace author or establish causality.
Only the versioned fixture lookup and policy versions v1, v2 and v3 are supported.
An unfamiliar natural-language request has unknown truth and cannot earn KEEP.
The study supplies frozen truth labels independently of the decision function.

All refund effects remain in a local SQLite ledger. Each replay restores a fresh
ledger from the captured prefix snapshot. A transaction commits an effect and its
idempotency result together, so losing the response after commit cannot create a
second effect on retry. Cancellation observed before the effect prevents it.
After commit, completion wins; this fixture does not roll back a completed refund.
There is no external refund connector, compensation protocol or remote-write
replay. Such effects are refused. Read-only errors, missing state, changed inputs,
versions/order or multiple interventions are also refused.

Trace JSON can be restored with `Trace.from_dict`; missing or inconsistent capture
fails validation. Export and retain private traces only as long as needed for
review, then delete this run's private copies after evidence publication is complete.
No customer data belongs in these synthetic fixtures. Correlate baseline and replay
with trace IDs and the experiment's baseline/counterfactual IDs.

Run the offline demo with `PYTHONPATH=src python3 scripts/demo.py --output PATH`.
`PATH` should point into private run state or an OS temporary directory. Uploads
require `--live-neatlogs` and separately authorized credentials. An ambient API key
alone no longer triggers upload. Cost is null until actually measured; latency is
local wall time. The UI labels these values accordingly.

Start the local consumer with `PYTHONPATH=src python3 scripts/app.py`, bound only
to 127.0.0.1:8765. GET `/api/experiment?policy=v2` is the local health/exercise path;
v3 is the denying control, and unsupported policies return 400. The service has
no persistent production state. Stop it to roll back the running instance; restore
the recorded Git baseline to roll back code after preserving worktree changes.
Test crash/reopen behavior with the temporary file-backed ledger in the test suite.

The comparative study takes 100 frozen synthetic orders with explicit labels,
2 warmups and 20 paired repetitions per order. It compares full local rerun on the
same observations with suffix replay, reporting actual function invocations and
external wall-clock measurements. Report the order-level clustered uncertainty and
any slower axis. Repeated timings are not additional independent scenarios, live
lookup savings, inferred dollar savings or real-user adoption.

The neatlogs workspace readback, Entire checkpoint provenance and cfo.ai exported
business plan above remain separate unverified integrations. Local tests do not
satisfy them. Independent developer reproduction and real consumer adoption also
remain separate gates.

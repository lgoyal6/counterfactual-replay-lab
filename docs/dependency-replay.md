# Dependency-aware counterfactual replay

Use case: an engineer changing a refund policy, a model/tool version or a captured tool response wants to
recompute only the decisions that depend on the change, reuse unaffected work safely, and see the first causal
divergence and the changed local effects before release.

This extends the fixed-prefix replay in `core.py` (which still works unchanged) to versioned dependency graphs.

## Model

`dag.py` defines a `NodeSpec` with a stable id, operation name and version, canonical parameters, ordered data
inputs, control guards (`Guard(node, allowed_values)`), effect kind (`pure`, `observation`, `effect_intent`) and
output schema. `Graph` validates ids, references, effect kinds, parameter canonicality (NaN/infinity rejected)
and size caps, rejects cycles over data and control edges, and fixes one canonical topological order (Kahn's
algorithm, ties broken by node id). A graph is immutable; `verify_frozen()` recomputes its identity so in-place
mutation of a running snapshot fails closed. Operations cannot add dependencies while running
(`DynamicMutationRejected`); a new edge is a new graph version.

Activation: a node runs only if every guard node is active and its output is one of the guard's allowed values.
Inactive data inputs arrive as `None` (so branch merges work). A failed node blocks its dependents; independent
nodes still run.

## Identity and cache keys

A node's key is the digest of: its spec hash (op, op version, params, inputs, guards, effect, schema), the
environment version, the request identity, each data predecessor's result hash (or `inactive`), each guard
predecessor's result hash (or `inactive`), and, for an observation node, the hash of its captured snapshot entry.
Result hashes are computed once when a result is produced, so key work is proportional to the node's fan-in, not
to result sizes. Node id alone or visible text alone never identifies a result.

Observation snapshot entries are keyed by node, tool, tool version, parameters and input hashes. Replay never
calls a live tool: a changed input misses the snapshot and the node fails with `MissingObservation`.

## Planning (`Engine.plan_replay`)

1. Validate the new graph and reload the base run's stored graph and snapshot, checking both against their
   hashes. Refuse a non-terminal base run or a different request identity.
2. Seeds: nodes whose spec hash differs or that are new, observation nodes whose captured entry changed,
   every effect node when the pre-run ledger changed, every node when the environment version changed, and
   every node that failed or was blocked in the base run. Removed nodes need no seed: their successors' input
   lists changed, so their spec hashes changed.
3. Affected set: seeds closed over data and control successors with the graph's indexed adjacency,
   O(V_affected + E_affected). The spec-hash diff and the reuse list are O(V) and are counted in timings.
4. Schedule the affected nodes, plus unaffected effect-intent nodes (commit only), in canonical order.
   Unaffected non-effect records are copied from the base run inside the run-creation transaction.

Modes: `incremental` (with early cutoff), `incremental_nocutoff` (ablation), `prefix` (generalized fixed-prefix:
reuse everything before the first seed in topological order), `memo_value` (joblib-style memo keyed by full
input values) and `full`.

Early cutoff: an affected node whose recomputed key equals its base record's key reuses that record without
running its operation. Activation is part of the key, so a node whose control predicate changed is never reused
just because its data ancestors are unchanged.

## Effects and durability (`store.py`)

Each run owns an isolated ledger in SQLite, seeded from the snapshot's pre-run ledger. An effect-intent node's
intent may be reused, but its commit always happens against the current run's ledger, in one transaction with
the node record. The idempotency key binds request, node, effect kind and target; an existing row with a
different value is an `IdempotencyConflict` (the node fails), an identical one is adopted.

Pure node records are batched (`commit_every`, default 64) and flushed before every effect transaction. A crash
can lose only pure records, which are recomputed deterministically on resume. Terminal runs are immutable:
triggers abort any insert, update or delete against them, and runs, node records and ledger rows are
append-only. WAL with `synchronous=NORMAL` survives process crashes; it does not claim power-loss durability.

Run states: `running` (or crashed), `stopped` (cancellation observed between nodes), `cancelled_in_node` (a
cooperative operation acknowledged cancellation; its work is discarded), `uncertain` (cancellation requested
while a non-cooperative read-only tool was still running past its grace period; its result is never written),
`completed` and `completed_with_failures`. `resume(run_id)` reverifies graph and snapshot hashes, adopts every
committed record (including committed effects) and continues the schedule.

## Explanation (`explain.py`)

`compare_runs` walks the union of both graph versions in deterministic topological order and classifies each
node as unchanged, changed output, activated, deactivated, added, removed, newly failed or recovered. The first
semantic divergence carries its causes: spec field changes, an edited observation, changed data inputs or a
changed control edge. The effect diff compares the two isolated ledgers.

## Oracles and negative controls

* `oracle.py`: an independent recursive full rerun sharing no planner, key, store or ledger code with the
  engine. Every test and every study repetition checks node statuses, result hashes, error classes and the
  ledger against it.
* `tests/refund_expectations.py`: refund rules written directly from the business rules, so a bug shared by
  both arms (for example in an operation) still fails (`test_independent_rules_catch_a_bug_shared_by_engine_and_oracle`).
* The original fixed-prefix `RefundAgent` reproduces the three-state compatibility graph.
* `mutants.py`: stale key ignoring op/version/params, node-id-only key, key ignoring guards, key ignoring
  observations, invalidation omitting control edges, skipping reused effects, and non-atomic non-idempotent
  effect commits. Each is caught by the tests.

## Commands

```bash
PYTHONPATH=src python -m unittest discover -s tests          # or python -m pytest
PYTHONPATH=src python -m counterfactual_replay_lab.cli capture --db /tmp/r.db --snapshot-out /tmp/snap.json
PYTHONPATH=src python -m counterfactual_replay_lab.cli replay --db /tmp/r.db --base <run> \
    --policy '{"auto_approve_max_cents": 10000}' --verify
PYTHONPATH=src python -m counterfactual_replay_lab.cli compare --db /tmp/r.db <base-run> <replay-run>
PYTHONPATH=src python -m counterfactual_replay_lab.cli demo --port 8766
PYTHONPATH=src python -m counterfactual_replay_lab.cli study --protocol protocol.json --phase final --out DIR
PYTHONPATH=src python -m counterfactual_replay_lab.cli recovery --out DIR
PYTHONPATH=src python -m counterfactual_replay_lab.cli report --study DIR --recovery DIR/recovery.json --out report.md
```

## Limits

Local deterministic fixture operations only; `syn.work` burns SHA-256 rounds as a stand-in for compute-bound
tools, and the cancellation tools are sleep-based. No live tools, LLM calls, provider costs, distributed
transactions or external refunds. One writer per store. The study's timings are single-machine CPU
measurements without CPU pinning.

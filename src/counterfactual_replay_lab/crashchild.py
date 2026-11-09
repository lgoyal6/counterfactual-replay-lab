"""Child process for crash/reopen tests: run a replay and die abruptly at a hook.

Usage: python -m counterfactual_replay_lab.crashchild DB BASE_RUN GRAPH_JSON SNAPSHOT_JSON EVENT NODE [MODE] [ENGINE]
os._exit skips every Python cleanup, so SQLite sees a real process crash.
"""
from __future__ import annotations

import os
import sys
import threading

from .dag import Graph
from .engine import Engine
from .mutants import MUTANTS
from .snapshot import Snapshot
from .store import RunStore


def main(argv: list[str]) -> int:
    db, base, graph_path, snap_path, event, node = argv[:6]
    mode = argv[6] if len(argv) > 6 else "incremental"
    engine_name = argv[7] if len(argv) > 7 else "engine"
    graph = Graph.from_json(open(graph_path).read())
    snapshot = Snapshot.from_json(open(snap_path).read())
    cancel = threading.Event()

    def hook(ev: str, name: str | None) -> None:
        if ev == event and (node == "*" or name == node):
            if os.environ.get("CRASH_AFTER_CANCEL"):
                cancel.set()
                return
            os._exit(77)

    store = RunStore(db)
    cls = MUTANTS.get(engine_name, Engine)
    engine = cls(store, hooks=hook, cancel=cancel, commit_every=int(os.environ.get("COMMIT_EVERY", "64")))
    plan = engine.plan_replay(base, graph, snapshot, mode)
    result = engine.execute(plan)
    print(result.run_id, result.status, flush=True)
    if os.environ.get("CRASH_AFTER_CANCEL"):
        os._exit(78)  # crash while the run is stopped with cancellation recorded
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

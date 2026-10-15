"""Export a local baseline, counterfactual trace and their comparison.

core.py produces the experiment; neatlogs_integration.py handles uploads.
Remote export requires an explicit flag as well as authorized credentials.
"""
import argparse
import json
import os
from pathlib import Path
from counterfactual_replay_lab.core import run_mvp

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True, help="Private trace export path")
parser.add_argument("--live-neatlogs", action="store_true", help="Requires separate upload/spend approval")
args = parser.parse_args()
neatlogs = None
if args.live_neatlogs:
    from counterfactual_replay_lab.neatlogs_integration import (
        configure_neatlogs,
        flush_neatlogs,
        traced_functions,
    )

    neatlogs = configure_neatlogs()
    workflow, replay = traced_functions(neatlogs)

    @workflow
    @replay
    def traced_run_mvp():
        return run_mvp()

    run = traced_run_mvp
else:
    run = run_mvp

try:
    baseline, counterfactual, experiment = run()
finally:
    if neatlogs is not None:
        flush_neatlogs(neatlogs)

out = {
    "baseline": baseline.to_dict(),
    "counterfactual": counterfactual.to_dict(),
    "experiment": experiment.to_dict(),
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))

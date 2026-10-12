import json
import os
from pathlib import Path
from counterfactual_replay_lab.core import run_mvp

neatlogs = None
if os.environ.get("NEATLOGS_API_KEY"):
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
path = Path("artifacts")
path.mkdir(exist_ok=True)
(path / "mvp-experiment.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))

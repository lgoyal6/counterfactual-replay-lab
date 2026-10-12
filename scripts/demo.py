import json
from pathlib import Path
from counterfactual_replay_lab.core import run_mvp

baseline, counterfactual, experiment = run_mvp()
out = {
    "baseline": baseline.to_dict(),
    "counterfactual": counterfactual.to_dict(),
    "experiment": experiment.to_dict(),
}
path = Path("artifacts")
path.mkdir(exist_ok=True)
(path / "mvp-experiment.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))

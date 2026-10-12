"""Small, explicit neatlogs adapter.

The adapter only enables network export when NEATLOGS_API_KEY is present. It
never pretends that local JSON output is a neatlogs trace.
"""

from __future__ import annotations

import os
from typing import Any, Callable


def configure_neatlogs() -> Any:
    """Initialize the real neatlogs SDK or fail with an actionable message."""
    try:
        import neatlogs
    except ImportError as exc:
        raise RuntimeError("Install neatlogs with: python -m pip install -U neatlogs") from exc

    api_key = os.environ.get("NEATLOGS_API_KEY")
    if not api_key:
        raise RuntimeError("Set NEATLOGS_API_KEY before enabling live neatlogs export")

    required = ("init", "span", "flush", "shutdown")
    missing = [name for name in required if not hasattr(neatlogs, name)]
    if missing:
        raise RuntimeError(
            "Installed neatlogs SDK is missing: " + ", ".join(missing) +
            ". Use the SDK release that matches the current docs before enabling live export."
        )

    neatlogs.init(api_key=api_key, workflow_name="counterfactual-replay-lab")
    return neatlogs


def traced_functions(neatlogs: Any) -> tuple[Callable[..., Any], Callable[..., Any]]:
    """Return decorators for the workflow and replay spans from the SDK."""
    workflow = neatlogs.span(kind="WORKFLOW", name="counterfactual_experiment")
    replay = neatlogs.span(kind="AGENT", name="counterfactual_replay")
    return workflow, replay


def flush_neatlogs(neatlogs: Any) -> None:
    """Flush short-lived CLI traces so they are not lost on process exit."""
    neatlogs.flush()
    neatlogs.shutdown()

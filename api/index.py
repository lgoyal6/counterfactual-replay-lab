"""Serve the existing deterministic replay app as one Vercel function.
The shared handler runs bounded synthetic refund scenarios without external calls.
Its source package stays in src, matching the repository's local test layout.
"""

from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from scripts.app import Handler  # noqa: E402


class handler(Handler):
    """Expose the shared handler to Vercel's Python entrypoint discovery."""

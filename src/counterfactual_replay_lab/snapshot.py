"""Frozen captured-observation snapshots for dependency replay.

capture() in engine.py records each observation node's live local tool result
under a key binding node id, tool, tool version, parameters and the hashes of
its inputs. Replay reads only these entries; a changed input misses and fails
closed instead of calling a live tool.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from .replay_state import canonical, digest

SNAPSHOT_FORMAT = 1


class SnapshotError(ValueError):
    """The snapshot is malformed, mutated or missing a required observation."""


def observation_key(node_id: str, tool: str, version: str, params: Any, input_hashes: list[str]) -> str:
    """Contract shared with oracle.py, which re-derives it independently."""
    return digest({"node": node_id, "tool": tool, "version": version, "params": params,
                   "inputs": input_hashes})


@dataclass(frozen=True)
class Snapshot:
    request_id: str
    observations: tuple[tuple[str, str, str], ...]  # (obs_key, node_id, output canonical JSON)
    ledger: tuple[tuple[str, str], ...] = ()          # pre-run local effects (idem_key, value)

    def __post_init__(self):
        if not isinstance(self.request_id, str) or not self.request_id:
            raise SnapshotError("Snapshot needs a request identity")
        keys = [k for k, _, _ in self.observations]
        if len(set(keys)) != len(keys) or keys != sorted(keys):
            raise SnapshotError("Snapshot observations must be unique and sorted")
        ledger_keys = [k for k, _ in self.ledger]
        if len(set(ledger_keys)) != len(ledger_keys) or ledger_keys != sorted(ledger_keys):
            raise SnapshotError("Snapshot ledger keys must be unique and sorted")
        for _, _, output in self.observations:
            if canonical(json.loads(output)) != output:
                raise SnapshotError("Observation output is not canonical JSON")

    def to_dict(self) -> dict[str, Any]:
        return {"format": SNAPSHOT_FORMAT, "request_id": self.request_id,
                "observations": [list(o) for o in self.observations],
                "ledger": [list(r) for r in self.ledger]}

    def to_json(self) -> str:
        return canonical(self.to_dict())

    @property
    def snapshot_hash(self) -> str:
        return digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Snapshot":
        if not isinstance(value, dict) or value.get("format") != SNAPSHOT_FORMAT:
            raise SnapshotError("Unsupported snapshot format")
        try:
            return cls(value["request_id"], tuple(tuple(o) for o in value["observations"]),
                       tuple(tuple(r) for r in value.get("ledger", ())))
        except (KeyError, TypeError, ValueError) as error:
            raise SnapshotError(f"Malformed snapshot: {error}") from error

    @classmethod
    def from_json(cls, text: str) -> "Snapshot":
        return cls.from_dict(json.loads(text))

    def index(self) -> dict[str, tuple[str, str]]:
        return {k: (node, out) for k, node, out in self.observations}

    def entries_for(self, node_id: str) -> list[tuple[str, str]]:
        return [(k, out) for k, node, out in self.observations if node == node_id]

    def with_observation(self, node_id: str, output: Any) -> "Snapshot":
        """Counterfactual edit of one captured tool response, as a new frozen snapshot."""
        entries = self.entries_for(node_id)
        if len(entries) != 1:
            raise SnapshotError(f"Node {node_id} has {len(entries)} captured observations; expected 1")
        key = entries[0][0]
        encoded = canonical(output)
        return Snapshot(self.request_id, tuple((k, n, encoded if k == key else o)
                                               for k, n, o in self.observations), self.ledger)

    def diff(self, other: "Snapshot") -> set[str]:
        """Node ids whose captured observation differs between two snapshots."""
        mine, theirs = self.index(), other.index()
        changed = set()
        for key in set(mine) | set(theirs):
            if mine.get(key) != theirs.get(key):
                changed.add((mine.get(key) or theirs.get(key))[0])
        return changed

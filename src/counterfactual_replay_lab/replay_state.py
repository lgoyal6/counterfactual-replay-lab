"""Captured local dependencies and an isolated, transactional effect ledger."""
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import sqlite3
from typing import Any


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class Observation:
    name: str
    version: str
    inputs: str
    output: str
    sequence: int = 0
    error: str | None = None
    effect: str = "read_only"

    @classmethod
    def lookup(cls, request: str, eligibility: str) -> "Observation":
        return cls("lookup_order", "fixture-lookup-v1", canonical({"request": request}), canonical(eligibility))

    def validate(self, request: str) -> None:
        if self.effect != "read_only":
            raise ValueError("Irreversible external effects cannot be replayed")
        if (self.name, self.version, self.inputs, self.sequence) != (
            "lookup_order", "fixture-lookup-v1", canonical({"request": request}), 0
        ):
            raise ValueError("Recorded dependency input, version or order changed")
        if self.error is not None or json.loads(self.output) not in {"eligible", "ineligible", "unknown"}:
            raise ValueError("Recorded dependency output unavailable or invalid")


@dataclass(frozen=True)
class ReplayState:
    request_hash: str
    observations: tuple[Observation, ...]
    ledger_snapshot: tuple[tuple[str, str], ...]
    prefix_hash: str
    expected_decision: str | None
    scenario_id: str

    @property
    def fingerprint(self) -> str:
        return digest(asdict(self))

    def validate(self, request: str, prefix: Any, fingerprint: str) -> None:
        if self.fingerprint != fingerprint:
            raise ValueError("Captured state changed")
        if digest(request) != self.request_hash or digest(prefix) != self.prefix_hash:
            raise ValueError("Original request or replay prefix changed")
        if len(self.observations) != 1:
            raise ValueError("Recorded dependency state missing")
        self.observations[0].validate(request)


class EffectLedger:
    """Local-only effects; a committed idempotency key survives a lost response.

    Each replay gets its own restored ledger. A file path is useful for crash/reopen
    tests; this class never forwards a refund to an external provider.
    """

    def __init__(self, path: str = ":memory:"):
        self.connection = sqlite3.connect(path)
        self.connection.execute("CREATE TABLE IF NOT EXISTS effects (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.connection.commit()

    def snapshot(self) -> tuple[tuple[str, str], ...]:
        return tuple(self.connection.execute("SELECT key, value FROM effects ORDER BY key"))

    @classmethod
    def restore(cls, snapshot: tuple[tuple[str, str], ...]) -> "EffectLedger":
        ledger = cls()
        with ledger.connection:
            ledger.connection.executemany("INSERT INTO effects VALUES (?, ?)", snapshot)
        return ledger

    def apply(self, key: str, value: str, *, crash_after_effect: bool = False) -> str:
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO effects VALUES (?, ?)", (key, value))
            actual = self.connection.execute("SELECT value FROM effects WHERE key=?", (key,)).fetchone()[0]
            if actual != value:
                raise ValueError("Idempotency key reused with different effect")
        if crash_after_effect:
            raise RuntimeError("Injected crash after local effect commit")
        return actual

    def close(self) -> None:
        self.connection.close()

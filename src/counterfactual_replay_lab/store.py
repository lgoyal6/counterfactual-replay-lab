"""SQLite persistence for immutable replay runs and isolated effect ledgers.

engine.py writes node results and effect commits through this module. One run's
ledger rows and its effect node's result row commit in the same transaction, so
a reopened store either has both or neither. Terminal runs are immutable: SQL
triggers abort any later insert, update or delete against them.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

SCHEMA_VERSION = "replay-store-v1"
TERMINAL = ("completed", "completed_with_failures")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS graphs(graph_hash TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(snapshot_hash TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blobs(hash TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs(
  run_id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, graph_hash TEXT NOT NULL,
  snapshot_hash TEXT, base_run TEXT, mode TEXT NOT NULL, status TEXT NOT NULL,
  cancel_requested INTEGER NOT NULL DEFAULT 0, plan TEXT NOT NULL, detail TEXT);
CREATE TABLE IF NOT EXISTS nodes(
  run_id TEXT NOT NULL, node_id TEXT NOT NULL, status TEXT NOT NULL, key TEXT,
  result_hash TEXT, source TEXT NOT NULL, error TEXT, PRIMARY KEY(run_id, node_id));
CREATE TABLE IF NOT EXISTS effects(
  run_id TEXT NOT NULL, idem_key TEXT NOT NULL, value TEXT NOT NULL, node_id TEXT,
  origin TEXT NOT NULL, PRIMARY KEY(run_id, idem_key));
CREATE TABLE IF NOT EXISTS memo(vkey TEXT PRIMARY KEY, status TEXT NOT NULL, result_hash TEXT);
CREATE TRIGGER IF NOT EXISTS runs_immutable BEFORE UPDATE ON runs
  WHEN OLD.status IN ('completed','completed_with_failures')
  BEGIN SELECT RAISE(ABORT, 'immutable terminal run'); END;
CREATE TRIGGER IF NOT EXISTS runs_no_delete BEFORE DELETE ON runs
  BEGIN SELECT RAISE(ABORT, 'runs are append-only evidence'); END;
CREATE TRIGGER IF NOT EXISTS nodes_no_insert_terminal BEFORE INSERT ON nodes
  WHEN (SELECT status FROM runs WHERE run_id=NEW.run_id) IN ('completed','completed_with_failures')
  BEGIN SELECT RAISE(ABORT, 'immutable terminal run'); END;
CREATE TRIGGER IF NOT EXISTS nodes_no_update_terminal BEFORE UPDATE ON nodes
  WHEN (SELECT status FROM runs WHERE run_id=OLD.run_id) IN ('completed','completed_with_failures')
  BEGIN SELECT RAISE(ABORT, 'immutable terminal run'); END;
CREATE TRIGGER IF NOT EXISTS nodes_no_delete BEFORE DELETE ON nodes
  BEGIN SELECT RAISE(ABORT, 'node records are append-only'); END;
CREATE TRIGGER IF NOT EXISTS effects_no_insert_terminal BEFORE INSERT ON effects
  WHEN (SELECT status FROM runs WHERE run_id=NEW.run_id) IN ('completed','completed_with_failures')
  BEGIN SELECT RAISE(ABORT, 'immutable terminal run'); END;
CREATE TRIGGER IF NOT EXISTS effects_no_change BEFORE UPDATE ON effects
  BEGIN SELECT RAISE(ABORT, 'ledger rows are append-only'); END;
CREATE TRIGGER IF NOT EXISTS effects_no_delete BEFORE DELETE ON effects
  BEGIN SELECT RAISE(ABORT, 'ledger rows are append-only'); END;
"""


class StoreError(RuntimeError):
    """The store is incompatible, corrupted or a run is in the wrong state."""


class RunStore:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(_SCHEMA)
        row = self.conn.execute("SELECT v FROM meta WHERE k='schema'").fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta VALUES ('schema', ?)", (SCHEMA_VERSION,))
        elif row[0] != SCHEMA_VERSION:
            raise StoreError(f"Incompatible store schema {row[0]!r}; expected {SCHEMA_VERSION!r}")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------------------------------------------------------- transactions
    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    # ------------------------------------------------------------- reads
    def run(self, run_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT run_id, seq, graph_hash, snapshot_hash, base_run, mode, status, cancel_requested, plan, detail "
            "FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise StoreError(f"Unknown run {run_id}")
        keys = ("run_id", "seq", "graph_hash", "snapshot_hash", "base_run", "mode", "status",
                "cancel_requested", "plan", "detail")
        out = dict(zip(keys, row))
        out["plan"] = json.loads(out["plan"])
        out["detail"] = json.loads(out["detail"]) if out["detail"] else None
        return out

    def runs(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT run_id FROM runs ORDER BY seq").fetchall()
        return [self.run(r[0]) for r in rows]

    def graph_body(self, graph_hash: str) -> str:
        row = self.conn.execute("SELECT body FROM graphs WHERE graph_hash=?", (graph_hash,)).fetchone()
        if row is None:
            raise StoreError("Graph body missing")
        return row[0]

    def snapshot_body(self, snapshot_hash: str) -> str:
        row = self.conn.execute("SELECT body FROM snapshots WHERE snapshot_hash=?", (snapshot_hash,)).fetchone()
        if row is None:
            raise StoreError("Snapshot body missing")
        return row[0]

    def blob(self, result_hash: str) -> str:
        row = self.conn.execute("SELECT body FROM blobs WHERE hash=?", (result_hash,)).fetchone()
        if row is None:
            raise StoreError(f"Result blob {result_hash[:12]} missing")
        return row[0]

    def records(self, run_id: str, node_ids: Iterable[str] | None = None) -> dict[str, tuple]:
        """(status, key, result_hash, source, error) per node."""
        if node_ids is None:
            rows = self.conn.execute(
                "SELECT node_id, status, key, result_hash, source, error FROM nodes WHERE run_id=?", (run_id,))
        else:
            rows = self.conn.execute(
                "SELECT node_id, status, key, result_hash, source, error FROM nodes WHERE run_id=? "
                "AND node_id IN (SELECT value FROM json_each(?))", (run_id, json.dumps(list(node_ids))))
        return {r[0]: tuple(r[1:]) for r in rows}

    def ledger(self, run_id: str) -> dict[str, str]:
        return dict(self.conn.execute("SELECT idem_key, value FROM effects WHERE run_id=? ORDER BY idem_key",
                                      (run_id,)))

    def ledger_rows(self, run_id: str) -> list[tuple]:
        return list(self.conn.execute(
            "SELECT idem_key, value, node_id, origin FROM effects WHERE run_id=? ORDER BY idem_key", (run_id,)))

    def cancel_requested(self, run_id: str) -> bool:
        row = self.conn.execute("SELECT cancel_requested FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return bool(row and row[0])

    def next_seq(self) -> int:
        return (self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM runs").fetchone()[0]) + 1

    def size_bytes(self) -> dict[str, int]:
        out = {}
        for table, expr in (("blobs", "length(body)"), ("nodes", "length(node_id)+length(COALESCE(key,''))+"
                            "length(COALESCE(result_hash,''))+length(status)+length(source)"),
                            ("effects", "length(value)+length(idem_key)"), ("graphs", "length(body)"),
                            ("snapshots", "length(body)")):
            out[table] = self.conn.execute(f"SELECT COALESCE(SUM({expr}),0) FROM {table}").fetchone()[0]
        return out

    # ------------------------------------------------------------- writes
    def put_blob(self, result_hash: str, body: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO blobs VALUES (?, ?)", (result_hash, body))

    def request_cancel(self, run_id: str) -> None:
        self.conn.execute("UPDATE runs SET cancel_requested=1 WHERE run_id=? AND status NOT IN "
                          "('completed','completed_with_failures')", (run_id,))

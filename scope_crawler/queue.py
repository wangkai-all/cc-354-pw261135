"""Persistent queue shared by discovery, download and ingest services.

Design notes
------------
* One physical table, one row per task, ``kind`` distinguishes services and
  ``state`` carries the lifecycle: ``pending`` → ``leased`` → ``done``,
  with ``parked`` (temporarily not runnable, e.g. because scope says no),
  ``blocked`` (permanent), ``cancelled`` and ``superseded`` as terminal or
  semi-terminal states.
* Claims are atomic (``BEGIN IMMEDIATE`` + ``UPDATE ... WHERE state='pending'``)
  and guarded by a lease deadline, so a crashed worker's rows become
  claimable again after the lease expires.
* ``dedupe`` is a UNIQUE key in the table.  Callers prefix it with the task
  kind (``download:12``, ``ingest:34``) so one logical unit is never enqueued
  twice, while different services can coexist in the same table.  Re-enqueueing
  a finished unit is only possible after the row is removed or moved back to
  pending (which is how scope changes re-open work).
* Nothing is deleted.  ``state``/``last_error``/``parked_reason`` keep an
  audit trail, which the console exposes.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable

from .db import Store, utc_after, utc_now

PENDING = "pending"
LEASED = "leased"
DONE = "done"
PARKED = "parked"
BLOCKED = "blocked"
CANCELLED = "cancelled"
SUPERSEDED = "superseded"

ACTIVE_STATES = (PENDING, LEASED, PARKED)
RUNNABLE_STATES = (PENDING,)
TERMINAL_STATES = (DONE, CANCELLED, SUPERSEDED, BLOCKED)


class QueueError(RuntimeError):
    pass


@dataclass(frozen=True)
class Task:
    id: int
    kind: str
    dedupe: str
    url_id: int | None
    evidence_id: int | None
    depth: int
    rules_version: int
    attempts: int
    payload: dict[str, Any]
    started_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Task":
        return cls(
            id=row["id"],
            kind=row["kind"],
            dedupe=row["dedupe"],
            url_id=row["url_id"],
            evidence_id=row["evidence_id"],
            depth=row["depth"],
            rules_version=row["rules_version"],
            attempts=row["attempts"],
            payload=json.loads(row["payload"] or "{}"),
            started_at=row["started_at"],
        )


class Queue:
    def __init__(self, store: Store):
        self.store = store

    # -- enqueue -------------------------------------------------------
    def enqueue(
        self,
        kind: str,
        dedupe: str,
        *,
        url_id: int | None = None,
        evidence_id: int | None = None,
        priority: int = 100,
        depth: int = 0,
        rules_version: int = 0,
        payload: dict[str, Any] | None = None,
        not_before: str | None = None,
    ) -> int | None:
        """Insert a task unless an active one with the same key exists.

        Returns the new task id, or ``None`` when deduped.  A *finished* task
        with the same key is left untouched: callers that genuinely want to
        redo work must first call :meth:`reopen` (used by rule changes).
        """
        now = utc_now()
        with self.store.write() as conn:
            existing = conn.execute(
                "SELECT id, state FROM queue WHERE dedupe=?", (dedupe,)
            ).fetchone()
            if existing is not None:
                # Idempotent: a known unit of work is never duplicated.  Redoing
                # finished work is an explicit reopen(), which keeps history.
                return None
            cur = conn.execute(
                "INSERT INTO queue(kind, dedupe, url_id, evidence_id, state, priority, depth,"
                " rules_version, not_before, payload, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    kind,
                    dedupe,
                    url_id,
                    evidence_id,
                    PENDING,
                    priority,
                    depth,
                    rules_version,
                    not_before,
                    json.dumps(payload or {}, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def reopen(
        self,
        kind: str,
        dedupe: str,
        *,
        url_id: int | None = None,
        evidence_id: int | None = None,
        priority: int | None = None,
        payload: dict[str, Any] | None = None,
        rules_version: int | None = None,
    ) -> int:
        """Move an existing row back to pending, or create it if absent.

        Used when scope widens: work that was parked/cancelled becomes runnable
        again without wiping its history.  ``url_id``/``evidence_id`` are
        re-pointed when given: an ingest task keyed by evidence can outlive the
        URL row that first created it (identity merges move ownership to the
        surviving row), and a stale pointer would make the worker skip it.
        """
        now = utc_now()
        with self.store.write() as conn:
            row = conn.execute("SELECT * FROM queue WHERE dedupe=?", (dedupe,)).fetchone()
            if row is None:
                # Insert inside this transaction: calling enqueue() here would
                # open a second BEGIN IMMEDIATE on a new connection and block
                # against our own lock until the busy timeout.
                cur = conn.execute(
                    "INSERT INTO queue(kind, dedupe, url_id, evidence_id, state, priority,"
                    " rules_version, payload, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        kind,
                        dedupe,
                        url_id,
                        evidence_id,
                        PENDING,
                        priority if priority is not None else 100,
                        rules_version or 0,
                        json.dumps(payload or {}, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                return int(cur.lastrowid)
            if row["state"] == LEASED:
                # A worker may be racing us.  Do not steal the lease; record the
                # intent so callers can re-check after the lease expires.
                conn.execute(
                    "UPDATE queue SET url_id=COALESCE(?, url_id),"
                    " evidence_id=COALESCE(?, evidence_id),"
                    " priority=COALESCE(?, priority),"
                    " payload=CASE WHEN ? IS NULL THEN payload ELSE ? END,"
                    " rules_version=COALESCE(?, rules_version), updated_at=? WHERE id=?",
                    (
                        url_id,
                        evidence_id,
                        priority,
                        None if payload is None else json.dumps(payload, ensure_ascii=False),
                        None if payload is None else json.dumps(payload, ensure_ascii=False),
                        rules_version,
                        now,
                        row["id"],
                    ),
                )
                return int(row["id"])
            conn.execute(
                "UPDATE queue SET state=?, url_id=COALESCE(?, url_id),"
                " evidence_id=COALESCE(?, evidence_id),"
                " priority=COALESCE(?, priority),"
                " payload=CASE WHEN ? IS NULL THEN payload ELSE ? END,"
                " rules_version=COALESCE(?, rules_version), last_error='',"
                " parked_reason='', not_before=NULL, attempts=0, started_at=NULL,"
                " updated_at=? WHERE id=?",
                (
                    PENDING,
                    url_id,
                    evidence_id,
                    priority,
                    None if payload is None else json.dumps(payload, ensure_ascii=False),
                    None if payload is None else json.dumps(payload, ensure_ascii=False),
                    rules_version,
                    now,
                    row["id"],
                ),
            )
            return int(row["id"])

    # -- claim / finish ------------------------------------------------
    def claim(self, kind: str, worker: str, *, lease_seconds: float = 60.0) -> Task | None:
        """Atomically claim the next runnable task of *kind*."""
        now = utc_now()
        with self.store.write() as conn:
            row = conn.execute(
                "SELECT * FROM queue WHERE kind=? AND state=?"
                " AND (not_before IS NULL OR not_before<=?)"
                " ORDER BY priority ASC, id ASC LIMIT 1",
                (kind, PENDING, now),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE queue SET state=?, worker=?, lease_until=?, started_at=?,"
                " attempts=attempts+1, updated_at=? WHERE id=? AND state=?",
                (LEASED, worker, utc_after(lease_seconds), now, now, row["id"], PENDING),
            )
            if updated.rowcount != 1:  # pragma: no cover - BEGIN IMMEDIATE serializes
                return None
            fresh = conn.execute("SELECT * FROM queue WHERE id=?", (row["id"],)).fetchone()
            return Task.from_row(fresh)

    def complete(self, task_id: int) -> None:
        self.store.execute(
            "UPDATE queue SET state=?, lease_until=NULL, worker=NULL, updated_at=? WHERE id=?",
            (DONE, utc_now(), task_id),
        )

    def fail(
        self,
        task_id: int,
        error: str,
        *,
        retry_after: float | None = None,
        max_attempts: int = 5,
    ) -> str:
        """Record a failure; retry, or park permanently.  Returns new state."""
        row = self.store.one("SELECT attempts FROM queue WHERE id=?", (task_id,))
        if row is None:
            raise QueueError(f"unknown task {task_id}")
        attempts = int(row["attempts"])
        if retry_after is not None and attempts < max_attempts:
            self.store.execute(
                "UPDATE queue SET state=?, last_error=?, not_before=?, lease_until=NULL,"
                " worker=NULL, updated_at=? WHERE id=?",
                (PENDING, error[:500], utc_after(retry_after), utc_now(), task_id),
            )
            return PENDING
        self.store.execute(
            "UPDATE queue SET state=?, last_error=?, lease_until=NULL, worker=NULL,"
            " updated_at=? WHERE id=?",
            (BLOCKED, error[:500], utc_now(), task_id),
        )
        return BLOCKED

    def park(self, task_id: int, reason: str) -> None:
        self.store.execute(
            "UPDATE queue SET state=?, parked_reason=?, lease_until=NULL, worker=NULL,"
            " updated_at=? WHERE id=?",
            (PARKED, reason[:500], utc_now(), task_id),
        )

    def cancel(self, task_id: int, reason: str) -> None:
        self.store.execute(
            "UPDATE queue SET state=?, parked_reason=?, lease_until=NULL, worker=NULL,"
            " updated_at=? WHERE id=?",
            (CANCELLED, reason[:500], utc_now(), task_id),
        )

    def release(self, task_id: int, *, reason: str = "") -> None:
        """Return a leased task to pending (e.g. scope changed mid-flight)."""
        self.store.execute(
            "UPDATE queue SET state=?, parked_reason=?, lease_until=NULL, worker=NULL,"
            " updated_at=? WHERE id=?",
            (PENDING, reason[:500], utc_now(), task_id),
        )

    def state_of(self, task_id: int) -> str | None:
        row = self.store.one("SELECT state FROM queue WHERE id=?", (task_id,))
        return None if row is None else row["state"]

    def mark_superseded(self, task_id: int, note: str) -> None:
        self.store.execute(
            "UPDATE queue SET state=?, parked_reason=?, lease_until=NULL, worker=NULL,"
            " updated_at=? WHERE id=?",
            (SUPERSEDED, note[:500], utc_now(), task_id),
        )

    # -- queries -------------------------------------------------------
    def by_dedupe(self, kind: str, dedupe: str) -> sqlite3.Row | None:
        return self.store.one("SELECT * FROM queue WHERE kind=? AND dedupe=?", (kind, dedupe))

    def counts(self) -> dict[str, dict[str, int]]:
        rows = self.store.all(
            "SELECT kind, state, COUNT(*) AS n FROM queue GROUP BY kind, state ORDER BY kind, state"
        )
        result: dict[str, dict[str, int]] = {}
        for row in rows:
            result.setdefault(row["kind"], {})[row["state"]] = row["n"]
        return result

    def list(
        self,
        *,
        kind: str | None = None,
        states: Iterable[str] | None = None,
        limit: int = 200,
        url_id: int | None = None,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        if states:
            marks = ",".join("?" for _ in states)
            clauses.append(f"state IN ({marks})")
            params.extend(states)
        if url_id is not None:
            clauses.append("url_id=?")
            params.append(url_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        return self.store.all(
            f"SELECT * FROM queue {where} ORDER BY id DESC LIMIT ?", tuple(params)
        )

    def stale_leases(self, now: str | None = None) -> list[sqlite3.Row]:
        return self.store.all(
            "SELECT * FROM queue WHERE state=? AND lease_until IS NOT NULL AND lease_until<?",
            (LEASED, now or utc_now()),
        )

    def recover_stale(self, now: str | None = None) -> int:
        now = now or utc_now()
        with self.store.write() as conn:
            cur = conn.execute(
                "UPDATE queue SET state=?, lease_until=NULL, worker=NULL,"
                " last_error=CASE WHEN last_error='' THEN 'lease expired' ELSE last_error END,"
                " updated_at=? WHERE state=? AND lease_until IS NOT NULL AND lease_until<?",
                (PENDING, now, LEASED, now),
            )
            return cur.rowcount

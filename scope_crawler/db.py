"""SQLite persistence: schema, settings, and small query helpers.

The database is the single source of truth shared by the discovery service,
the downloader, the catalog/ingest service and the web console.  Every worker
opens its own connection (WAL mode) so the queue can be claimed safely.

Invariants worth knowing before reading the engine:

* ``urls.fetch_url`` is immutable: the spelling first used to make the request.
  ``urls.canonical_url`` is *derived* from it by the active rules (aliases).
  A URL row is never deleted.
* ``evidence`` rows and their files are never deleted or re-pointed.  Scope
  narrowing stops publication; it does not destroy what was fetched.
* ``entries`` hold publication state per primary URL row.  Withholding keeps
  the row, the evidence and the history in ``publication_events``.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=OFF;

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rules_versions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    doc        TEXT NOT NULL,
    note       TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    parent_id  INTEGER
);

CREATE TABLE IF NOT EXISTS changes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,              -- publish | undo
    from_version INTEGER,
    to_version   INTEGER NOT NULL,
    summary      TEXT NOT NULL,              -- JSON
    note         TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    reverted_at  TEXT,
    reverted_by  INTEGER
);

CREATE TABLE IF NOT EXISTS urls (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    fetch_url              TEXT NOT NULL,   -- immutable: first spelling requested
    canonical_url          TEXT NOT NULL,   -- derived by active rules
    column_id              TEXT,
    via_alias              INTEGER NOT NULL DEFAULT 0,
    kind                   TEXT NOT NULL DEFAULT 'page',  -- page | resource
    title                  TEXT NOT NULL DEFAULT '',
    depth                  INTEGER NOT NULL DEFAULT 0,
    first_seen_at          TEXT NOT NULL,
    last_seen_at           TEXT NOT NULL,
    in_scope               INTEGER NOT NULL DEFAULT 1,
    reachable              INTEGER NOT NULL DEFAULT 0,
    is_seed                INTEGER NOT NULL DEFAULT 0,
    via_url_id             INTEGER,
    hops_used              INTEGER NOT NULL DEFAULT 0,
    column_depth           INTEGER NOT NULL DEFAULT 0,
    scope_reason           TEXT NOT NULL DEFAULT 'ok',      -- rules-derived
    fetch_error            TEXT NOT NULL DEFAULT '',        -- request-derived
    rules_version          INTEGER NOT NULL DEFAULT 0,
    last_evaluated_version INTEGER NOT NULL DEFAULT 0,
    merged_into            INTEGER,
    merged_at              TEXT
);
CREATE INDEX IF NOT EXISTS urls_canonical ON urls(canonical_url);
CREATE UNIQUE INDEX IF NOT EXISTS urls_primary_canonical
    ON urls(canonical_url) WHERE merged_into IS NULL;
CREATE INDEX IF NOT EXISTS urls_scope ON urls(in_scope, reachable);

CREATE TABLE IF NOT EXISTS links (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    from_url_id   INTEGER,
    raw_url       TEXT NOT NULL,
    resolved_url  TEXT,
    canonical_url TEXT,
    to_url_id     INTEGER,
    outcome       TEXT NOT NULL,   -- queued | already_fetched | boundary | scope | ref_denied
    reason        TEXT NOT NULL DEFAULT '',
    column_id     TEXT,
    depth         INTEGER NOT NULL DEFAULT 0,
    hops_used     INTEGER NOT NULL DEFAULT 0,
    column_depth  INTEGER NOT NULL DEFAULT 0,
    hits          INTEGER NOT NULL DEFAULT 1,
    rules_version INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS links_outcome ON links(outcome);
CREATE INDEX IF NOT EXISTS links_from ON links(from_url_id);

CREATE TABLE IF NOT EXISTS evidence (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id       INTEGER NOT NULL,
    requested_at TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    status_code  INTEGER,
    content_type TEXT NOT NULL DEFAULT '',
    sha256       TEXT NOT NULL,
    size         INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    rules_version INTEGER NOT NULL DEFAULT 0,
    fetch_worker TEXT NOT NULL DEFAULT '',
    meta         TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS evidence_url ON evidence(url_id);
CREATE INDEX IF NOT EXISTS evidence_sha ON evidence(sha256);
CREATE INDEX IF NOT EXISTS evidence_requested ON evidence(url_id, requested_at);

CREATE TABLE IF NOT EXISTS entries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id       INTEGER NOT NULL UNIQUE,
    evidence_id  INTEGER,
    title        TEXT NOT NULL DEFAULT '',
    state        TEXT NOT NULL DEFAULT 'pending',  -- published | withheld
    reason       TEXT NOT NULL DEFAULT '',
    published_at TEXT,
    withdrawn_at TEXT,
    rules_version INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS entries_state ON entries(state);

CREATE TABLE IF NOT EXISTS publication_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id      INTEGER NOT NULL,
    url_id        INTEGER NOT NULL,
    action        TEXT NOT NULL,              -- published | withheld
    reason        TEXT NOT NULL DEFAULT '',
    rules_version INTEGER NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS publication_events_entry ON publication_events(entry_id);

CREATE TABLE IF NOT EXISTS queue (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    kind          TEXT NOT NULL,               -- discover | download | ingest
    dedupe        TEXT NOT NULL UNIQUE,
    url_id        INTEGER,
    evidence_id   INTEGER,
    state         TEXT NOT NULL DEFAULT 'pending',
    priority      INTEGER NOT NULL DEFAULT 100,
    depth         INTEGER NOT NULL DEFAULT 0,
    rules_version INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT NOT NULL DEFAULT '',
    parked_reason TEXT NOT NULL DEFAULT '',
    not_before    TEXT,
    lease_until   TEXT,
    started_at    TEXT,
    worker        TEXT,
    payload       TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS queue_claim ON queue(kind, state, priority, id);
CREATE INDEX IF NOT EXISTS queue_state ON queue(state);

CREATE TABLE IF NOT EXISTS inflight (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id        INTEGER NOT NULL,
    task_id       INTEGER,
    rules_version INTEGER NOT NULL,
    requested_at  TEXT NOT NULL,
    final_url     TEXT NOT NULL,
    status_code   INTEGER,
    content_type  TEXT NOT NULL DEFAULT '',
    sha256        TEXT NOT NULL,
    size          INTEGER NOT NULL,
    storage_path  TEXT NOT NULL,
    meta          TEXT NOT NULL DEFAULT '{}',
    state         TEXT NOT NULL DEFAULT 'held',  -- held | committed | abandoned
    created_at    TEXT NOT NULL,
    committed_at  TEXT,
    result        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS inflight_state ON inflight(state);
"""

ENTRY_PUBLISHED = "published"
ENTRY_WITHHELD = "withheld"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_after(seconds: float) -> str:
    stamp = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_stamp(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


class Store:
    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.commit()

    # -- small helpers -------------------------------------------------
    def one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(sql, params).fetchone()

    def all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute(sql, params).fetchall())

    def scalar(self, sql: str, params: tuple = ()) -> Any:
        row = self.one(sql, params)
        return None if row is None else row[0]

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self.write() as conn:
            cur = conn.execute(sql, params)
            return cur.lastrowid

    # -- settings ------------------------------------------------------
    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM settings WHERE key=?", (key,))
        return default if row is None else row["value"]

    def set_setting(self, key: str, value: str) -> None:
        self.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    # -- rules ---------------------------------------------------------
    def active_rules_version(self) -> int | None:
        value = self.get_setting("active_rules_version")
        return int(value) if value is not None else None

    def active_rules_doc(self) -> dict | None:
        version = self.active_rules_version()
        if version is None:
            return None
        return self.rules_doc_for_version(version)

    def rules_doc_for_version(self, version: int) -> dict | None:
        row = self.one("SELECT doc FROM rules_versions WHERE id=?", (version,))
        return None if row is None else json.loads(row["doc"])

    def rules_version_list(self) -> list[sqlite3.Row]:
        return self.all(
            "SELECT id, note, created_at, parent_id FROM rules_versions ORDER BY id DESC"
        )

    def latest_change(self) -> sqlite3.Row | None:
        return self.one("SELECT * FROM changes ORDER BY id DESC LIMIT 1")

    def last_revertable_change(self) -> sqlite3.Row | None:
        return self.one(
            "SELECT * FROM changes WHERE kind='publish' AND reverted_at IS NULL"
            " AND from_version IS NOT NULL ORDER BY id DESC LIMIT 1"
        )

    # -- urls ----------------------------------------------------------
    def primary_by_canonical(self, canonical_url: str) -> sqlite3.Row | None:
        return self.one(
            "SELECT * FROM urls WHERE canonical_url=? AND merged_into IS NULL"
            " ORDER BY id LIMIT 1",
            (canonical_url,),
        )

    def primary_rows(self) -> list[sqlite3.Row]:
        return self.all("SELECT * FROM urls WHERE merged_into IS NULL ORDER BY id")

    def url_row(self, url_id: int) -> sqlite3.Row | None:
        return self.one("SELECT * FROM urls WHERE id=?", (url_id,))

    def cluster_ids(self, url_id: int) -> list[int]:
        """A primary row plus every row merged into it, transitively.

        Merges can chain (A→B on one rule change, B→C on the next), and evidence
        rows keep the url_id of the row that fetched them, so the whole chain
        must be visible for "latest evidence" lookups.
        """
        ids = [url_id]
        seen = {url_id}
        pending = [url_id]
        while pending:
            current = pending.pop()
            for row in self.all(
                "SELECT id FROM urls WHERE merged_into=? ORDER BY id", (current,)
            ):
                member = int(row["id"])
                if member in seen:
                    continue  # defensive: never loop on a corrupt chain
                seen.add(member)
                ids.append(member)
                pending.append(member)
        return sorted(ids)

    def cluster_members(self, rows: list[sqlite3.Row] | None = None) -> dict[int, list[int]]:
        """Map primary id -> list of all row ids in its cluster."""
        rows = rows if rows is not None else self.all("SELECT id, merged_into FROM urls")
        parent_of: dict[int, int] = {
            int(r["id"]): int(r["merged_into"]) for r in rows if r["merged_into"] is not None
        }

        def root_of(url_id: int) -> int:
            seen: set[int] = set()
            while url_id in parent_of:
                if url_id in seen:  # defensive: never loop on a corrupt chain
                    break
                seen.add(url_id)
                url_id = parent_of[url_id]
            return url_id

        groups: dict[int, list[int]] = {}
        for row in rows:
            url_id = int(row["id"])
            groups.setdefault(root_of(url_id), []).append(url_id)
        return {k: list(dict.fromkeys(v)) for k, v in groups.items()}

    # -- evidence ------------------------------------------------------
    def latest_evidence(self, url_ids: int | list[int]) -> sqlite3.Row | None:
        ids = [url_ids] if isinstance(url_ids, int) else list(url_ids)
        if not ids:
            return None
        marks = ",".join("?" for _ in ids)
        return self.one(
            f"SELECT * FROM evidence WHERE url_id IN ({marks})"
            " ORDER BY requested_at DESC, id DESC LIMIT 1",
            tuple(ids),
        )

    def evidence_by_id(self, evidence_id: int | None) -> sqlite3.Row | None:
        if not evidence_id:
            return None
        return self.one("SELECT * FROM evidence WHERE id=?", (evidence_id,))

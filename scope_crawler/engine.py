"""Crawler engine: discovery, download and ingest services.

The three services communicate only through the persistent queue:

``download``  fetch a URL, store the bytes, optionally hold the result, then
              commit it (enqueue ``ingest`` and ``discover``).
``discover``  parse a page that is already stored and apply the active rules to
              every link, enqueueing ``download`` tasks.
``ingest``    decide whether the newest stored evidence may be published.

Scope changes never delete anything.  Applying a rule document:

1. reconciles URL identity (aliases can move a row's canonical URL; a
   collision merges the losing row into the winner, keeping its evidence),
2. recomputes scope from the boundary + columns + exclusions,
3. recomputes reachability from the enabled column seeds over links already
   recorded (and links re-parsed from stored evidence, so a repoened
   cross-column reference is picked up without any network access),
4. republishes/withholds catalog entries,
5. parks download tasks that are now out of scope and reopens download tasks
   that are newly in scope and still lack evidence.

A download can be *held* (``simulate late result``).  When it is delivered
after a rules change it is committed under the then-current rules: it can be
published, withheld, or discarded as older than already-stored evidence, but
its bytes remain in the evidence store either way.
"""

from __future__ import annotations

import html
import json
import os
import sqlite3
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from . import rules as rules_mod
from .db import ENTRY_PUBLISHED, ENTRY_WITHHELD, Store, parse_stamp, utc_now
from .fetch import FetchError, HttpFetcher, RobotsDisallowed
from .queue import (
    BLOCKED,
    CANCELLED,
    LEASED,
    PARKED,
    PENDING,
    Queue,
    Task,
)
from .rules import Decision, RuleSet

MAX_PAGE_DEPTH = 50
HOLD_NEXT_DOWNLOAD = "hold_next_download"
HOLD_DOWNLOAD_FOR = "hold_next_download_for"

SCOPE_REASON_TEXT = {
    rules_mod.OK: "in scope",
    rules_mod.NO_COLUMN: "outside every configured column",
    rules_mod.COLUMN_DISABLED: "column disabled",
    rules_mod.EXCLUDED: "excluded by pattern",
    rules_mod.ALIAS_ESCAPED: "alias escapes the site root",
    rules_mod.ALIAS_CYCLE: "alias cycle",
    rules_mod.INVALID: "invalid URL",
    "cross_column_denied": "cross-column reference not allowed",
    "cross_column_budget": "cross-column hop budget exhausted",
    "ref_depth_exceeded": "reference depth exhausted",
    "source_out_of_scope": "source page is out of scope",
    "unreachable": "no longer reachable from an enabled column",
    "robots_disallowed": "robots.txt disallows this URL",
    "robots_unavailable": "robots.txt is unavailable",
    "access_restricted": "access restricted",
    "http_error": "HTTP error",
    "unsupported_content": "unsupported content type",
    "redirect_out_of_scope": "redirect left the site root",
    "superseded_by_newer_fetch": "a newer fetch superseded this evidence",
    "stale_rules_result": "download finished after its scope was revoked",
    "max_depth": "maximum crawl depth reached",
    "merged": "row merged into another spelling",
}

PAGE_MIMES = ("text/html", "application/xhtml+xml", "text/plain")

# Fetch errors that say "not now", not "never": a later attempt may succeed,
# so a rule change that makes the page relevant again may retry it.  Permanent
# verdicts (``robots_disallowed``, ``access_restricted``) are not included.
TRANSIENT_FETCH_ERRORS = frozenset({"network_error", "http_error", "robots_unavailable"})


# ---------------------------------------------------------------------------
# HTML link extraction
# ---------------------------------------------------------------------------
class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.base: str | None = None
        self.links: list[str] = []
        self.title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {k.lower(): (v or "") for k, v in attrs}
        if tag == "base" and self.base is None and values.get("href"):
            self.base = values["href"]
        elif tag in ("a", "area") and values.get("href"):
            self.links.append(values["href"])
        elif tag == "link" and values.get("href") and "stylesheet" in values.get("rel", "").lower():
            self.links.append(values["href"])
        elif tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)

    @property
    def title(self) -> str:
        return " ".join(" ".join(self.title_parts).split())


def extract_links(text: str, page_url: str) -> tuple[str, list[str]]:
    parser = _LinkParser()
    try:
        parser.feed(text)
    except Exception:  # pragma: no cover - HTMLParser is tolerant
        pass
    base = urljoin(page_url, parser.base) if parser.base else page_url
    seen: set[str] = set()
    links: list[str] = []
    for href in parser.links:
        href = html.unescape(href).strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
            continue
        resolved = urljoin(base, href)
        if resolved in seen:
            continue
        seen.add(resolved)
        links.append(resolved)
    return parser.title, links


def mime_of(content_type: str | None) -> str:
    return (content_type or "").split(";", 1)[0].strip().lower()


def effective_reason(row: sqlite3.Row) -> str:
    """Request-level refusals take precedence over rule-derived status."""
    if row["fetch_error"]:
        return str(row["fetch_error"])
    return str(row["scope_reason"])


# ---------------------------------------------------------------------------
# Evaluation results
# ---------------------------------------------------------------------------
@dataclass
class Node:
    url_id: int
    canonical_url: str
    column_id: str | None
    in_scope: bool
    reachable: bool
    reason: str
    hops_used: int = 0
    column_depth: int = 0
    is_seed: bool = False

    @property
    def publishable(self) -> bool:
        return self.in_scope and self.reachable


@dataclass
class GraphEvaluation:
    nodes: dict[int, Node] = field(default_factory=dict)
    new_targets: dict[str, dict[str, Any]] = field(default_factory=dict)
    canonical_row: dict[str, int] = field(default_factory=dict)
    denied_links: int = 0

    def publishable_ids(self) -> set[int]:
        return {n.url_id for n in self.nodes.values() if n.publishable}

    def reason_for(self, url_id: int) -> str:
        node = self.nodes.get(url_id)
        return node.reason if node else "unreachable"


@dataclass
class Impact:
    diff: dict[str, list[str]]
    current_in_scope: int
    proposed_in_scope: int
    publish: list[dict[str, Any]]
    withdraw: list[dict[str, Any]]
    park_downloads: list[dict[str, Any]]
    enqueue_downloads: list[dict[str, Any]]
    new_urls: list[dict[str, Any]]
    unchanged: int
    denied_references: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "diff": self.diff,
            "current_in_scope": self.current_in_scope,
            "proposed_in_scope": self.proposed_in_scope,
            "publish": self.publish,
            "withdraw": self.withdraw,
            "park_downloads": self.park_downloads,
            "enqueue_downloads": self.enqueue_downloads,
            "new_urls": self.new_urls,
            "unchanged": self.unchanged,
            "denied_references": self.denied_references,
        }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class Crawler:
    def __init__(
        self,
        store: Store,
        storage_dir: str,
        *,
        fetcher: HttpFetcher | None = None,
        worker_id: str | None = None,
    ):
        self.store = store
        self.storage_dir = os.path.abspath(storage_dir)
        os.makedirs(os.path.join(self.storage_dir, "evidence"), exist_ok=True)
        self.queue = Queue(store)
        self.fetcher = fetcher or HttpFetcher()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._lock = threading.RLock()

    # -- rules ---------------------------------------------------------
    def active_rules(self) -> RuleSet | None:
        version = self.store.active_rules_version()
        doc = self.store.active_rules_doc()
        if version is None or doc is None:
            return None
        return RuleSet(doc, version=version)

    def rules_at(self, version: int) -> RuleSet | None:
        doc = self.store.rules_doc_for_version(version)
        if doc is None:
            return None
        return RuleSet(doc, version=version)

    # -- evidence files ------------------------------------------------
    def _evidence_path(self, sha: str) -> str:
        return os.path.join(self.storage_dir, "evidence", sha[:2], sha)

    def _save_bytes(self, body: bytes, sha: str) -> str:
        path = self._evidence_path(sha)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = f"{path}.{uuid.uuid4().hex}.tmp"
            with open(tmp, "wb") as handle:
                handle.write(body)
            os.replace(tmp, path)
        return path

    # -- URL rows ------------------------------------------------------
    def _ensure_row(
        self,
        rules: RuleSet,
        decision: Decision,
        *,
        depth: int,
        via_url_id: int | None,
        hops_used: int,
        column_depth: int,
        is_seed: bool = False,
    ) -> tuple[sqlite3.Row, bool]:
        """Create or refresh the row for a URL's canonical form."""
        canonical = decision.canonical_url
        if canonical is None:
            raise ValueError("cannot create a row for an out-of-boundary URL")
        now = utc_now()
        version = rules.version or 0
        with self.store.write() as conn:
            row = conn.execute(
                "SELECT * FROM urls WHERE canonical_url=? AND merged_into IS NULL"
                " ORDER BY id LIMIT 1",
                (canonical,),
            ).fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE urls SET last_seen_at=?, column_id=?, depth=MIN(depth, ?),"
                    " hops_used=CASE WHEN hops_used=0 THEN ? ELSE hops_used END,"
                    " column_depth=CASE WHEN column_depth=0 THEN ? ELSE column_depth END,"
                    " last_evaluated_version=?, via_alias=? WHERE id=?",
                    (
                        now,
                        decision.column_id,
                        depth,
                        hops_used,
                        column_depth,
                        version,
                        1 if decision.via_alias else 0,
                        row["id"],
                    ),
                )
                fresh = conn.execute("SELECT * FROM urls WHERE id=?", (row["id"],)).fetchone()
                return fresh, False
            cur = conn.execute(
                "INSERT INTO urls(fetch_url, canonical_url, column_id, via_alias, kind,"
                " depth, first_seen_at, last_seen_at, in_scope, reachable, is_seed,"
                " via_url_id, hops_used, column_depth, scope_reason, rules_version,"
                " last_evaluated_version)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    decision.fetch_url or canonical,
                    canonical,
                    decision.column_id,
                    1 if decision.via_alias else 0,
                    "page",
                    depth,
                    now,
                    now,
                    1,
                    1,
                    1 if is_seed else 0,
                    via_url_id,
                    hops_used,
                    column_depth,
                    rules_mod.OK,
                    version,
                    version,
                ),
            )
            fresh = conn.execute("SELECT * FROM urls WHERE id=?", (cur.lastrowid,)).fetchone()
            return fresh, True

    def ensure_seed_rows(self, rules: RuleSet) -> dict[str, int]:
        ids: dict[str, int] = {}
        for column in rules.ordered:
            canonical = rules.boundary.url_for_path(column.path)
            decision = rules.evaluate(canonical)
            row, _ = self._ensure_row(
                rules,
                decision,
                depth=0,
                via_url_id=None,
                hops_used=0,
                column_depth=0,
                is_seed=True,
            )
            ids[column.id] = int(row["id"])
            self.store.execute(
            "UPDATE urls SET is_seed=1 WHERE id=?", (row["id"],)
        )
        return ids

    def _row_for_canonical(self, canonical: str) -> sqlite3.Row | None:
        return self.store.primary_by_canonical(canonical)

    # -- queue helpers -------------------------------------------------
    def _enqueue_download(self, row: sqlite3.Row, *, rules_version: int) -> bool:
        dedupe = f"download:{row['id']}"
        priority = 10 if row["is_seed"] else 100 + int(row["depth"] or 0)
        existing = self.queue.by_dedupe("download", dedupe)
        if existing is None:
            self.queue.enqueue(
                "download",
                dedupe,
                url_id=int(row["id"]),
                priority=priority,
                depth=int(row["depth"] or 0),
                rules_version=rules_version,
                payload={"url_id": int(row["id"])},
            )
            return True
        reopen = existing["state"] in (PARKED, CANCELLED)
        if existing["state"] == BLOCKED and row["fetch_error"] in TRANSIENT_FETCH_ERRORS:
            # A transient failure (network, HTTP error, robots.txt unavailable)
            # blocked the task.  A later rules change must be able to retry it
            # instead of treating the page as permanently un-fetchable.
            reopen = True
            self._set_fetch_error(int(row["id"]), "")
        if reopen:
            self.queue.reopen(
                "download",
                dedupe,
                url_id=int(row["id"]),
                priority=priority,
                rules_version=rules_version,
                payload={"url_id": int(row["id"])},
            )
            return True
        return False

    def _enqueue_ingest(
        self, evidence_id: int, url_id: int, *, rules_version: int, force: bool = False
    ) -> bool:
        dedupe = f"ingest:{evidence_id}"
        existing = self.queue.by_dedupe("ingest", dedupe)
        if existing is None:
            self.queue.enqueue(
                "ingest",
                dedupe,
                url_id=url_id,
                evidence_id=evidence_id,
                priority=20,
                rules_version=rules_version,
                payload={"url_id": url_id, "evidence_id": evidence_id},
            )
            return True
        if force or existing["state"] in (PARKED, CANCELLED):
            # Re-ingesting under new rules must flip publication even when the
            # previous ingest finished, so a forced reopen includes `done`.
            self.queue.reopen(
                "ingest",
                dedupe,
                url_id=url_id,
                evidence_id=evidence_id,
                priority=20,
                rules_version=rules_version,
                payload={"url_id": url_id, "evidence_id": evidence_id},
            )
            return True
        return False

    def _enqueue_discover(
        self, evidence_id: int, url_id: int, *, depth: int, rules_version: int
    ) -> None:
        dedupe = f"discover:{evidence_id}"
        existing = self.queue.by_dedupe("discover", dedupe)
        if existing is None:
            self.queue.enqueue(
                "discover",
                dedupe,
                url_id=url_id,
                evidence_id=evidence_id,
                priority=30,
                depth=depth,
                rules_version=rules_version,
                payload={"url_id": url_id, "evidence_id": evidence_id},
            )
        elif existing["state"] in (PARKED, CANCELLED):
            # The page was discovered while its source column was out of
            # scope; widening must re-run discovery from the stored bytes, and
            # re-point it if an identity merge moved the owning row.
            self.queue.reopen(
                "discover",
                dedupe,
                url_id=url_id,
                evidence_id=evidence_id,
                priority=30,
                rules_version=rules_version,
                payload={"url_id": url_id, "evidence_id": evidence_id},
            )

    # -- discovery -----------------------------------------------------
    def _read_page(
        self, row: sqlite3.Row, evidence: sqlite3.Row
    ) -> tuple[str, list[str]] | None:
        path = evidence["storage_path"]
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as handle:
                text = handle.read().decode("utf-8", errors="replace")
        except OSError:
            return None
        # Relative links must resolve against the spelling that actually served
        # the bytes.  For a merged row that is the evidence owner's fetch_url
        # (an alias may sit at a different depth), not the surviving primary's.
        base = row["fetch_url"] or row["canonical_url"]
        owner = self.store.url_row(int(evidence["url_id"]))
        if owner is not None and (owner["fetch_url"] or owner["canonical_url"]):
            base = owner["fetch_url"] or owner["canonical_url"]
        return extract_links(text, base)

    def _record_link(
        self,
        *,
        from_url_id: int,
        raw_url: str,
        resolved_url: str,
        canonical_url: str | None,
        to_url_id: int | None,
        outcome: str,
        reason: str,
        column_id: str | None,
        depth: int,
        hops_used: int,
        column_depth: int,
        rules_version: int,
    ) -> None:
        now = utc_now()
        with self.store.write() as conn:
            row = conn.execute(
                "SELECT id FROM links WHERE from_url_id=? AND raw_url=?",
                (from_url_id, raw_url),
            ).fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE links SET resolved_url=?, canonical_url=?, to_url_id=?,"
                    " outcome=?, reason=?, column_id=?, depth=MIN(depth, ?), hits=hits+1,"
                    " rules_version=?, last_seen_at=? WHERE id=?",
                    (
                        resolved_url,
                        canonical_url,
                        to_url_id,
                        outcome,
                        reason,
                        column_id,
                        depth,
                        rules_version,
                        now,
                        row["id"],
                    ),
                )
            else:
                conn.execute(
                    "INSERT INTO links(from_url_id, raw_url, resolved_url, canonical_url,"
                    " to_url_id, outcome, reason, column_id, depth, hops_used, column_depth,"
                    " hits, rules_version, first_seen_at, last_seen_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        from_url_id,
                        raw_url,
                        resolved_url,
                        canonical_url,
                        to_url_id,
                        outcome,
                        reason,
                        column_id,
                        depth,
                        hops_used,
                        column_depth,
                        1,
                        rules_version,
                        now,
                        now,
                    ),
                )

    def _discover_page(
        self, rules: RuleSet, row: sqlite3.Row, evidence: sqlite3.Row
    ) -> dict[str, int]:
        parsed = self._read_page(row, evidence)
        if parsed is None:
            return {"links": 0, "queued": 0, "denied": 0}
        title, links = parsed
        if title and title[:500] != row["title"]:
            self.store.execute("UPDATE urls SET title=? WHERE id=?", (title[:500], int(row["id"])))
            self.store.execute(
                "UPDATE entries SET title=? WHERE url_id=?", (title[:500], int(row["id"]))
            )
        from_col = row["column_id"]
        hops = int(row["hops_used"] or 0)
        col_depth = int(row["column_depth"] or 0)
        depth = int(row["depth"] or 0)
        version = rules.version or 0
        base = row["fetch_url"] or row["canonical_url"]
        queued = denied = 0
        for raw in links:
            resolved = urljoin(base, raw)
            decision = rules.evaluate(resolved)
            outcome, reason, target_id = self._classify_and_store(
                rules,
                from_col=from_col,
                decision=decision,
                hops=hops,
                col_depth=col_depth,
                depth=depth,
                via_url_id=int(row["id"]),
            )
            if outcome == "queued":
                queued += 1
            if outcome in ("boundary", "scope", "ref_denied"):
                denied += 1
            self._record_link(
                from_url_id=int(row["id"]),
                raw_url=raw,
                resolved_url=resolved,
                canonical_url=decision.canonical_url,
                to_url_id=target_id,
                outcome=outcome,
                reason=reason,
                column_id=decision.column_id,
                depth=depth + 1,
                hops_used=hops,
                column_depth=col_depth,
                rules_version=version,
            )
        self.store.execute(
            "UPDATE urls SET last_evaluated_version=?, rules_version=? WHERE id=?",
            (version, version, int(row["id"])),
        )
        return {"links": len(links), "queued": queued, "denied": denied}

    def _classify_and_store(
        self,
        rules: RuleSet,
        *,
        from_col: str | None,
        decision: Decision,
        hops: int,
        col_depth: int,
        depth: int,
        via_url_id: int | None,
    ) -> tuple[str, str, int | None]:
        if not decision.in_scope:
            outcome = "boundary" if decision.outside_boundary else "scope"
            return outcome, decision.reason, None
        allowed, why = rules.transition(
            from_col, decision.column_id, hops_used=hops, column_depth=col_depth
        )
        if not allowed:
            return "ref_denied", why, None
        next_hops, next_depth = rules.next_context(
            from_col, decision.column_id, hops_used=hops, column_depth=col_depth
        )
        # MAX_PAGE_DEPTH bounds the *page* depth of the crawl, not the
        # per-column reference depth (which max_ref_depth and the hop budget
        # already bound).  Same-column chains advance page depth by one per
        # link, so this is what stops a long cycle inside one column.
        if depth + 1 > MAX_PAGE_DEPTH:
            return "scope", "max_depth", None
        assert decision.column_id is not None
        row, created = self._ensure_row(
            rules,
            decision,
            depth=depth + 1,
            via_url_id=via_url_id,
            hops_used=next_hops,
            column_depth=next_depth,
        )
        target_id = int(row["id"])
        if self.store.latest_evidence(self.store.cluster_ids(target_id)) is None:
            self._enqueue_download(row, rules_version=rules.version or 0)
            return "queued", rules_mod.OK, target_id
        return "already_fetched", rules_mod.OK, target_id

    def run_discover(self, task: Task) -> str:
        rules = self.active_rules()
        if rules is None:
            self.queue.park(task.id, "no active rules")
            return "parked"
        row = self.store.url_row(int(task.url_id or 0))
        evidence = self.store.evidence_by_id(task.evidence_id)
        if row is None or row["merged_into"] or evidence is None:
            self.queue.complete(task.id)
            return "skipped"
        decision = rules.evaluate(row["canonical_url"])
        if not decision.in_scope or not row["reachable"]:
            self.queue.park(task.id, decision.reason if not decision.in_scope else "unreachable")
            return "parked"
        stats = self._discover_page(rules, row, evidence)
        self.queue.complete(task.id)
        return f"links={stats['links']} queued={stats['queued']} denied={stats['denied']}"

    # -- download ------------------------------------------------------
    def run_download(self, task: Task, *, hold: bool | None = None) -> str:
        rules = self.active_rules()
        if rules is None:
            self.queue.park(task.id, "no active rules")
            return "parked"
        row = self.store.url_row(int(task.url_id or 0))
        if row is None or row["merged_into"]:
            self.queue.complete(task.id)
            return "skipped"
        decision = rules.evaluate(row["fetch_url"])
        if not decision.in_scope:
            # Scope wins over an in-flight task: never request a URL the
            # current rules exclude.  A fetch that already happened is
            # delivered through `inflight` and adjudicated on commit.
            self.queue.park(task.id, decision.reason)
            return "parked"
        if decision.canonical_url is None:
            self.queue.fail(task.id, "URL left the site boundary", retry_after=None)
            return "blocked"

        def guard(candidate: str) -> None:
            # Checked before the request is sent, for the initial URL and every
            # redirect hop.  A hop outside the site is never contacted.
            if rules.evaluate(candidate).canonical_url is None:
                raise FetchError(
                    f"redirect left the site boundary: {candidate}",
                    reason="redirect_out_of_scope",
                )

        try:
            result = self.fetcher.fetch(decision.canonical_url, guard=guard)
        except RobotsDisallowed as exc:
            self._set_fetch_error(int(row["id"]), "robots_disallowed")
            self._record_refusal(int(row["id"]), "robots_disallowed", rules.version or 0)
            self.queue.fail(task.id, str(exc), retry_after=None)
            return "blocked"
        except FetchError as exc:
            self._set_fetch_error(int(row["id"]), exc.reason)
            if exc.reason in ("access_restricted", "unsupported_content", "redirect_out_of_scope"):
                self._record_refusal(int(row["id"]), exc.reason, rules.version or 0)
                self.queue.fail(task.id, str(exc), retry_after=None)
                return "blocked"
            # Transient failures stay retryable; the fetch error is recorded but
            # a later success clears it.  An unreadable robots.txt is
            # transient too: the retry re-reads the policy, and robots.txt is
            # never bypassed while it is unavailable.
            retry = (
                0.05
                if exc.reason in ("network_error", "http_error", "robots_unavailable")
                else None
            )
            self.queue.fail(task.id, str(exc), retry_after=retry, max_attempts=2)
            return "failed"
        final_decision = rules.evaluate(result.final_url)
        if final_decision.canonical_url is None:
            # Defensive: the per-hop guard already refuses this, but a custom
            # fetcher may not have honoured it.
            self._set_fetch_error(int(row["id"]), "redirect_out_of_scope")
            self._record_refusal(int(row["id"]), "redirect_out_of_scope", rules.version or 0)
            self.queue.fail(
                task.id, f"redirect left the site boundary: {result.final_url}", retry_after=None
            )
            return "blocked"
        storage_path = self._save_bytes(result.body, result.sha256)
        should_hold = hold
        if should_hold is None:
            should_hold = self.store.get_setting(HOLD_NEXT_DOWNLOAD) == "1"
            if should_hold:
                target = self.store.get_setting(HOLD_DOWNLOAD_FOR) or ""
                if target and target != str(row["fetch_url"]) and target != str(row["canonical_url"]):
                    # The hold was requested for one specific page (the
                    # console does this).  A different page is not the late
                    # result the user asked for, and must not consume it.
                    should_hold = False
            if should_hold:
                self.store.set_setting(HOLD_NEXT_DOWNLOAD, "0")
                self.store.set_setting(HOLD_DOWNLOAD_FOR, "")
        meta = {
            "started_rules_version": task.rules_version,
            "finished_rules_version": rules.version or 0,
            "redirects": result.redirects,
            "final_url": result.final_url,
            "held": bool(should_hold),
        }
        # Every result is staged first, then committed.  Holding simply leaves
        # it staged; this is the same code path a delayed delivery takes.
        inflight_id = self.store.execute(
            "INSERT INTO inflight(url_id, task_id, rules_version, requested_at, final_url,"
            " status_code, content_type, sha256, size, storage_path, meta, state, created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(row["id"]),
                task.id,
                task.rules_version,
                result.requested_at,
                result.final_url,
                result.status,
                result.content_type,
                result.sha256,
                len(result.body),
                storage_path,
                json.dumps(meta, ensure_ascii=False),
                "held",
                utc_now(),
            ),
        )
        self.queue.complete(task.id)
        if should_hold:
            self._log("download held", url=row["fetch_url"], inflight=inflight_id)
            return f"held inflight={inflight_id}"
        outcome = self.commit_inflight(inflight_id)
        return f"committed {outcome} inflight={inflight_id}"

    def _set_scope(self, url_id: int, in_scope: bool, reason: str, version: int) -> None:
        self.store.execute(
            "UPDATE urls SET in_scope=?, scope_reason=?, last_evaluated_version=? WHERE id=?",
            (1 if in_scope else 0, reason, version, url_id),
        )

    def _set_fetch_error(self, url_id: int, reason: str) -> None:
        """Record a request-level refusal/error separately from rule scope.

        Keeping the two apart matters: re-evaluating rules must not silently
        clear (or invent) a robots/403 finding, and a transient network error
        must not permanently mark a URL out of scope.
        """
        self.store.execute(
            "UPDATE urls SET fetch_error=? WHERE id=?", (reason, url_id)
        )

    def _record_refusal(self, url_id: int, reason: str, version: int) -> None:
        """Show a permanent refusal in the catalog without fabricating evidence."""
        row = self.store.url_row(url_id)
        if row is None:
            return
        self._set_entry(
            url_id,
            None,
            publish=False,
            reason=reason,
            version=version,
            title=row["title"],
        )

    def _extract_title(self, url_id: int, storage_path: str) -> None:
        """Fill the title before ingest so the published entry is complete."""
        if not storage_path or not os.path.exists(storage_path):
            return
        try:
            with open(storage_path, "rb") as handle:
                text = handle.read().decode("utf-8", errors="replace")
        except OSError:
            return
        row = self.store.url_row(url_id)
        if row is None:
            return
        title, _links = extract_links(text, row["fetch_url"] or row["canonical_url"])
        if not title:
            return
        title = title[:500]
        self.store.execute("UPDATE urls SET title=? WHERE id=?", (title, url_id))
        self.store.execute("UPDATE entries SET title=? WHERE url_id=?", (title, url_id))

    # -- committing a download (immediately or late) -------------------
    def commit_inflight(self, inflight_id: int) -> str:
        """Turn a held fetch into evidence and (maybe) publication.

        This is where a late result meets the *current* rules.  Evidence is
        always preserved.  Publication only happens when the current rules
        still allow the URL and this attempt is not older than stored evidence.
        """
        inf = self.store.one("SELECT * FROM inflight WHERE id=?", (inflight_id,))
        if inf is None:
            raise ValueError(f"unknown inflight attempt {inflight_id}")
        # Claim the attempt atomically: two workers delivering the same late
        # result must not both insert evidence for it.
        with self.store.write() as conn:
            claimed = (
                conn.execute(
                    "UPDATE inflight SET state='committing' WHERE id=? AND state='held'",
                    (inflight_id,),
                ).rowcount
                == 1
            )
        if not claimed:
            inf_now = self.store.one("SELECT state FROM inflight WHERE id=?", (inflight_id,))
            return f"already_{inf_now['state'] if inf_now else 'missing'}"
        try:
            return self._commit_claimed_inflight(inflight_id, inf)
        finally:
            # The record step flips the row to `committed`.  If anything failed
            # before that, the attempt is still `committing`: make it
            # deliverable again instead of stranding it in an internal state.
            self.store.execute(
                "UPDATE inflight SET state='held' WHERE id=? AND state='committing'",
                (inflight_id,),
            )

    def _commit_claimed_inflight(self, inflight_id: int, inf: sqlite3.Row) -> str:
        """Body of :meth:`commit_inflight`; the row is already claimed."""
        row = self.store.url_row(int(inf["url_id"]))
        if row is None:
            # Nothing to attach the bytes to.  Leave the attempt held rather
            # than silently consuming it.
            self.store.execute(
                "UPDATE inflight SET state='held' WHERE id=?", (inflight_id,)
            )
            raise ValueError("inflight row points at a missing URL")
        rules = self.active_rules()
        if rules is None:
            # Not an error: leave the attempt held so a later delivery can
            # still adjudicate it.
            self.store.execute(
                "UPDATE inflight SET state='held' WHERE id=?", (inflight_id,)
            )
            return "no_rules"
        version = rules.version or 0
        decision = rules.evaluate(row["fetch_url"])
        # A late result for a row that a rule change has since merged away
        # still belongs to the surviving identity.  Its evidence is cluster
        # evidence; publication must be adjudicated under the primary row that
        # will actually carry an entry.
        owner_id = int(row["merged_into"]) if row["merged_into"] else int(row["id"])
        cluster = self.store.cluster_ids(owner_id)
        latest = self.store.latest_evidence(cluster)
        superseded = latest is not None and (
            parse_stamp(latest["requested_at"]) >= parse_stamp(inf["requested_at"])
        )
        meta = json.loads(inf["meta"] or "{}")
        meta.update(
            {
                "held_released": True,
                "committed_rules_version": version,
                "superseded": superseded,
                "late": bool(meta.get("held")),
            }
        )
        evidence_id = self.store.execute(
            "INSERT INTO evidence(url_id, requested_at, fetched_at, status_code, content_type,"
            " sha256, size, storage_path, rules_version, fetch_worker, meta)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(row["id"]),
                inf["requested_at"],
                utc_now(),
                inf["status_code"],
                inf["content_type"],
                inf["sha256"],
                inf["size"],
                inf["storage_path"],
                version,
                self.worker_id,
                json.dumps(meta, ensure_ascii=False),
            ),
        )
        # A late result for a row that a rule change has since merged away
        # still belongs to the surviving identity.  Its evidence is cluster
        # evidence; publication must be adjudicated under the primary row that
        # will actually carry an entry.
        with self.store.write() as conn:
            conn.execute(
                "UPDATE urls SET last_seen_at=?, last_evaluated_version=?, fetch_error=''"
                " WHERE id IN (?,?)",
                (utc_now(), version, int(row["id"]), owner_id),
            )
            conn.execute(
                "UPDATE inflight SET state='committed', committed_at=?, result=? WHERE id=?",
                (utc_now(), "", inflight_id),
            )
        if mime_of(inf["content_type"]) in PAGE_MIMES:
            self._extract_title(int(row["id"]), inf["storage_path"])
        result: str
        if superseded:
            result = "superseded"
        elif not decision.in_scope:
            self._withhold(
                int(row["id"]), decision.reason, version, evidence_id=evidence_id
            )
            result = f"withheld:{decision.reason}"
        else:
            # Reachability is refreshed by the reachability pass; a late result
            # for a URL that is no longer reachable will be withheld by ingest.
            self._enqueue_ingest(evidence_id, owner_id, rules_version=version)
            if mime_of(inf["content_type"]) in PAGE_MIMES:
                self._enqueue_discover(
                    evidence_id,
                    owner_id,
                    depth=int(row["depth"] or 0),
                    rules_version=version,
                )
            result = "queued"
        self.store.execute(
            "UPDATE inflight SET result=? WHERE id=?", (result, inflight_id)
        )
        return result

    def deliver_held(self, *, limit: int = 50) -> list[dict[str, Any]]:
        delivered: list[dict[str, Any]] = []
        for inf in self.store.all(
            "SELECT * FROM inflight WHERE state='held' ORDER BY id LIMIT ?", (limit,)
        ):
            outcome = self.commit_inflight(int(inf["id"]))
            row = self.store.url_row(int(inf["url_id"]))
            delivered.append(
                {
                    "inflight_id": int(inf["id"]),
                    "url_id": int(inf["url_id"]),
                    "url": row["canonical_url"] if row else "",
                    "outcome": outcome,
                    "requested_at": inf["requested_at"],
                }
            )
        return delivered

    # -- ingest --------------------------------------------------------
    def run_ingest(self, task: Task) -> str:
        rules = self.active_rules()
        if rules is None:
            self.queue.park(task.id, "no active rules")
            return "parked"
        url_id = int(task.url_id or 0)
        row = self.store.url_row(url_id)
        evidence = self.store.evidence_by_id(task.evidence_id)
        if row is None or row["merged_into"] or evidence is None:
            self.queue.complete(task.id)
            return "skipped"
        cluster = self.store.cluster_ids(url_id)
        latest = self.store.latest_evidence(cluster)
        decision = rules.evaluate(row["fetch_url"])
        publish, reason = self._publishability(row, evidence, latest, decision)
        title = row["title"] or ""
        if latest is not None and int(latest["id"]) == int(evidence["id"]):
            owner = self.store.url_row(int(latest["url_id"]))
            if owner is not None and owner["title"]:
                title = owner["title"]
        self._set_entry(
            url_id,
            int(evidence["id"]),
            publish=publish,
            reason=reason,
            version=rules.version or 0,
            title=title,
        )
        self.queue.complete(task.id)
        return "published" if publish else f"withheld:{reason}"

    def _publishability(
        self,
        row: sqlite3.Row,
        evidence: sqlite3.Row,
        latest: sqlite3.Row | None,
        decision: Decision,
    ) -> tuple[bool, str]:
        if not decision.in_scope:
            return False, decision.reason
        if row["fetch_error"] and row["fetch_error"] not in TRANSIENT_FETCH_ERRORS:
            # A permanent refusal (robots/403/unsupported) invalidates the URL.
            # A transient error does not invalidate bytes already stored: the
            # evidence still describes what the site served, and the fetch will
            # be retried (see _apply_scope_decisions) rather than withdrawing it.
            return False, row["fetch_error"]
        if not row["reachable"]:
            return False, "unreachable"
        if latest is not None and int(latest["id"]) != int(evidence["id"]):
            return False, "superseded_by_newer_fetch"
        if evidence["status_code"] is None or int(evidence["status_code"]) >= 400:
            return False, "http_error"
        mime = mime_of(evidence["content_type"])
        if mime and mime not in PAGE_MIMES:
            return False, "unsupported_content"
        return True, rules_mod.OK

    def _set_entry(
        self,
        url_id: int,
        evidence_id: int,
        *,
        publish: bool,
        reason: str,
        version: int,
        title: str = "",
    ) -> None:
        now = utc_now()
        desired = ENTRY_PUBLISHED if publish else ENTRY_WITHHELD
        with self.store.write() as conn:
            row = conn.execute("SELECT * FROM entries WHERE url_id=?", (url_id,)).fetchone()
            if row is None:
                cur = conn.execute(
                    "INSERT INTO entries(url_id, evidence_id, title, state, reason,"
                    " published_at, withdrawn_at, rules_version, updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        url_id,
                        evidence_id,
                        title,
                        desired,
                        reason,
                        now if publish else None,
                        None if publish else now,
                        version,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO publication_events(entry_id, url_id, action, reason,"
                    " rules_version, created_at) VALUES(?,?,?,?,?,?)",
                    (
                        int(cur.lastrowid),
                        url_id,
                        "published" if publish else "withheld",
                        reason,
                        version,
                        now,
                    ),
                )
                return
            if row["state"] == desired:
                conn.execute(
                    "UPDATE entries SET evidence_id=?, title=?, reason=?, rules_version=?,"
                    " updated_at=? WHERE id=?",
                    (evidence_id, title, reason, version, now, row["id"]),
                )
                return
            conn.execute(
                "UPDATE entries SET evidence_id=?, title=?, state=?, reason=?,"
                " published_at=CASE WHEN ? THEN COALESCE(published_at, ?) ELSE published_at END,"
                " withdrawn_at=CASE WHEN ? THEN ? ELSE NULL END, rules_version=?, updated_at=?"
                " WHERE id=?",
                (
                    evidence_id,
                    title,
                    desired,
                    reason,
                    1 if publish else 0,
                    now,
                    1 if not publish else 0,
                    now,
                    version,
                    now,
                    row["id"],
                ),
            )
            conn.execute(
                "INSERT INTO publication_events(entry_id, url_id, action, reason,"
                " rules_version, created_at) VALUES(?,?,?,?,?,?)",
                (
                    row["id"],
                    url_id,
                    "published" if publish else "withheld",
                    reason,
                    version,
                    now,
                ),
            )

    def _withhold(self, url_id: int, reason: str, version: int, *, evidence_id: int | None = None) -> None:
        row = self.store.one("SELECT * FROM entries WHERE url_id=?", (url_id,))
        if row is None:
            # Never published: there is nothing to withdraw.  Creating a
            # withheld entry here would fabricate catalog state for a page the
            # catalog never listed (merged rows route late commits through the
            # surviving primary before reaching this point).
            return
        current_evidence = evidence_id or row["evidence_id"] or 0
        self._set_entry(
            url_id,
            int(current_evidence),
            publish=False,
            reason=reason,
            version=version,
            title=row["title"],
        )

    # -- reachability graph -------------------------------------------
    def _link_sources(
        self,
        rows: dict[int, sqlite3.Row],
        clusters: dict[int, list[int]],
    ) -> dict[int, list[str]]:
        """Raw link spellings per primary row: recorded links + fresh parses.

        Parsing stored evidence is what lets a widened rule set pick up links
        that were previously rejected, without re-downloading anything.
        """
        sources: dict[int, list[str]] = {url_id: [] for url_id in rows}
        # A merged row's links were recorded under the row that discovered them.
        # Map them onto the surviving primary so a rename does not silently
        # orphan outgoing discoveries.
        owner_of: dict[int, int] = {}
        for primary, members in clusters.items():
            for member in members:
                owner_of[int(member)] = int(primary)
        for link in self.store.all("SELECT * FROM links ORDER BY id"):
            from_id = int(link["from_url_id"] or 0)
            owner = owner_of.get(from_id, from_id)
            sources.setdefault(owner, []).append(link["raw_url"])
        for url_id, row in rows.items():
            latest = self.store.latest_evidence(clusters.get(url_id, [url_id]))
            if latest is None or mime_of(latest["content_type"]) not in PAGE_MIMES:
                continue
            parsed = self._read_page(row, latest)
            if parsed is not None:
                sources[url_id].extend(parsed[1])
        return {k: list(dict.fromkeys(v)) for k, v in sources.items()}

    def evaluate_graph(self, rules: RuleSet, *, include_new: bool = True) -> GraphEvaluation:
        """Recompute scope + reachability under *rules*.  No writes, no network."""
        rows_list = self.store.primary_rows()
        rows = {int(r["id"]): r for r in rows_list}
        clusters = self.store.cluster_members()
        decision_cache: dict[int, Decision] = {}
        canonical_of: dict[int, str | None] = {}
        canonical_row: dict[str, int] = {}
        for url_id, row in rows.items():
            decision = rules.evaluate(row["fetch_url"])
            decision_cache[url_id] = decision
            canonical_of[url_id] = decision.canonical_url
            if decision.canonical_url is not None:
                current = canonical_row.get(decision.canonical_url)
                if current is None or self._prefer_row(rows[url_id], rows[current]):
                    canonical_row[decision.canonical_url] = url_id

        evaluation = GraphEvaluation(canonical_row=canonical_row)
        # Non-seed rows get a pre-computed (possibly unreachable) node.
        for url_id, row in rows.items():
            decision = decision_cache[url_id]
            canonical = canonical_of[url_id]
            node = Node(
                url_id=url_id,
                canonical_url=canonical or row["canonical_url"],
                column_id=decision.column_id,
                in_scope=decision.in_scope,
                reachable=False,
                reason=decision.reason if not decision.in_scope else "unreachable",
            )
            evaluation.nodes[url_id] = node

        queue: deque[tuple[int, int, int]] = deque()
        for column in rules.ordered:
            canonical = rules.boundary.url_for_path(column.path)
            url_id = canonical_row.get(canonical)
            if url_id is None:
                # A configured column root with no row is new work, not an
                # existing page: report it for fetching.  A disabled column is
                # never fetched, so it is not a target even though
                # apply_rules will still create its (unreachable) seed row.
                if include_new and column.enabled:
                    evaluation.new_targets.setdefault(
                        canonical,
                        {"url": canonical, "column_id": column.id, "is_seed": True},
                    )
                continue
            decision = decision_cache[url_id]
            node = evaluation.nodes[url_id]
            node.column_id = decision.column_id or column.id
            node.in_scope = decision.in_scope
            node.reachable = decision.in_scope
            node.reason = decision.reason
            node.is_seed = True
            if decision.in_scope:
                queue.append((url_id, 0, 0))

        sources = self._link_sources(rows, clusters)
        while queue:
            url_id, hops, col_depth = queue.popleft()
            node = evaluation.nodes[url_id]
            if not node.publishable:
                continue
            row = rows[url_id]
            base = row["fetch_url"] or row["canonical_url"]
            for raw in sources.get(url_id, []):
                resolved = urljoin(base, raw)
                decision = rules.evaluate(resolved)
                if not decision.in_scope:
                    evaluation.denied_links += 1
                    continue
                allowed, _why = rules.transition(
                    node.column_id, decision.column_id, hops_used=hops, column_depth=col_depth
                )
                if not allowed:
                    evaluation.denied_links += 1
                    continue
                if decision.canonical_url is None:
                    continue
                next_hops, next_depth = rules.next_context(
                    node.column_id, decision.column_id, hops_used=hops, column_depth=col_depth
                )
                if int(row["depth"] or 0) + 1 > MAX_PAGE_DEPTH:
                    continue
                target_id = canonical_row.get(decision.canonical_url)
                if target_id is None:
                    if include_new:
                        evaluation.new_targets.setdefault(
                            decision.canonical_url,
                            {
                                "url": decision.canonical_url,
                                "column_id": decision.column_id,
                                "via_url_id": url_id,
                                "depth": int(row["depth"] or 0) + 1,
                            },
                        )
                    continue
                target = evaluation.nodes[target_id]
                if target.reachable:
                    continue
                target.reachable = True
                target.in_scope = True
                target.reason = rules_mod.OK
                target.column_id = decision.column_id
                target.hops_used = next_hops
                target.column_depth = next_depth
                queue.append((target_id, next_hops, next_depth))
        return evaluation

    @staticmethod
    def _prefer_row(candidate: sqlite3.Row, current: sqlite3.Row) -> bool:
        """Deterministic winner among rows whose canonical forms collide."""
        return (int(candidate["via_alias"] or 0), int(candidate["id"])) < (
            int(current["via_alias"] or 0),
            int(current["id"]),
        )

    def _persist_graph(self, rules: RuleSet, evaluation: GraphEvaluation) -> None:
        version = rules.version or 0
        rows = self.store.primary_rows()
        now = utc_now()
        with self.store.write() as conn:
            for row in rows:
                url_id = int(row["id"])
                node = evaluation.nodes.get(url_id)
                decision = rules.evaluate(row["fetch_url"])
                canonical = decision.canonical_url or row["canonical_url"]
                if not decision.in_scope:
                    in_scope = reachable = False
                    reason = decision.reason
                elif node is not None and node.publishable:
                    in_scope = reachable = True
                    reason = rules_mod.OK
                else:
                    in_scope = reachable = False
                    reason = "unreachable"
                conn.execute(
                    "UPDATE urls SET canonical_url=?, column_id=?, via_alias=?, in_scope=?,"
                    " reachable=?, scope_reason=?, hops_used=?, column_depth=?,"
                    " last_evaluated_version=?, rules_version=?, last_seen_at=? WHERE id=?",
                    (
                        canonical,
                        decision.column_id,
                        1 if decision.via_alias else 0,
                        1 if in_scope else 0,
                        1 if reachable else 0,
                        reason,
                        node.hops_used if node else 0,
                        node.column_depth if node else 0,
                        version,
                        version,
                        now,
                        url_id,
                    ),
                )

    # -- identity reconciliation --------------------------------------
    def _reconcile_identities(self, rules: RuleSet) -> list[tuple[int, int]]:
        """Recompute canonical URLs; merge or un-merge rows as rules dictate.

        Identity is *derived* state.  An alias or a moved column path can make
        two separately fetched spellings collide (merge), and a later rule
        change — including an undo — can make them distinct again (un-merge).
        Evidence stays attached to the row that fetched it in both directions,
        so neither operation destroys raw bytes.
        """
        version = rules.version or 0
        rows = self.store.all("SELECT * FROM urls ORDER BY id")
        decisions = {int(r["id"]): rules.evaluate(r["fetch_url"]) for r in rows}
        desired: dict[int, str] = {
            url_id: decision.canonical_url
            for url_id, decision in decisions.items()
            if decision.canonical_url is not None
        }
        # Group every row (merged ones included: fetch_url is immutable) by the
        # canonical form the *new* rules assign to it.
        groups: dict[str, list[int]] = {}
        for url_id, canonical in desired.items():
            groups.setdefault(canonical, []).append(url_id)
        winner_of: dict[int, int] = {}
        for members in groups.values():
            members.sort(key=lambda i: (1 if decisions[i].via_alias else 0, i))
            winner = members[0]
            for member in members:
                winner_of[member] = winner

        merges: list[tuple[int, int]] = []
        unmerges: list[int] = []
        repoints: list[tuple[int, int]] = []
        for row in rows:
            url_id = int(row["id"])
            current = row["merged_into"]
            wanted = winner_of.get(url_id)
            if wanted is not None and wanted != url_id:
                if current is None:
                    merges.append((url_id, wanted))
                elif int(current) != wanted:
                    repoints.append((url_id, wanted))
            elif current is not None and wanted == url_id:
                # A collision from an earlier rule set no longer exists: this
                # row is a primary identity again.  Rows whose new decision is
                # None (out of boundary / no column) are left merged, since
                # they cannot be given a distinct canonical form.
                unmerges.append(url_id)
        for loser, primary in merges:
            self._merge_rows(loser, primary, version)
        for url_id, wanted in repoints:
            self.store.execute(
                "UPDATE urls SET merged_into=? WHERE id=?", (wanted, url_id)
            )

        # Canonical rewriting, un-merging and index recreation share one
        # transaction.  An un-merge plus a simultaneous canonical change can
        # collide in an intermediate state even though the end state is
        # consistent, so the uniqueness index is dropped for the duration and
        # recreated at the end (rolling the whole thing back on violation).
        with self.store.write() as conn:
            conn.execute("DROP INDEX IF EXISTS urls_primary_canonical")
            for url_id, canonical in desired.items():
                decision = decisions[url_id]
                conn.execute(
                    "UPDATE urls SET canonical_url=?, column_id=?, via_alias=?,"
                    " last_evaluated_version=? WHERE id=?",
                    (
                        canonical,
                        decision.column_id,
                        1 if decision.via_alias else 0,
                        version,
                        url_id,
                    ),
                )
            for url_id in unmerges:
                conn.execute(
                    "UPDATE urls SET merged_into=NULL, merged_at=NULL,"
                    " scope_reason='unreachable', in_scope=0, reachable=0 WHERE id=?",
                    (url_id,),
                )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS urls_primary_canonical"
                " ON urls(canonical_url) WHERE merged_into IS NULL"
            )
        for url_id in unmerges:
            self._log("unmerged", url=url_id)
        return merges

    def _merge_rows(self, loser: int, primary: int, version: int) -> None:
        if loser == primary:
            return
        now = utc_now()
        with self.store.write() as conn:
            # Evidence keeps its url_id (the loser) so provenance is intact;
            # lookups use the whole cluster.
            loser_entry = conn.execute("SELECT * FROM entries WHERE url_id=?", (loser,)).fetchone()
            primary_entry = conn.execute("SELECT * FROM entries WHERE url_id=?", (primary,)).fetchone()
            if loser_entry is not None and primary_entry is None:
                conn.execute("UPDATE entries SET url_id=? WHERE id=?", (primary, loser_entry["id"]))
                conn.execute("UPDATE publication_events SET url_id=? WHERE entry_id=?", (primary, loser_entry["id"]))
            elif loser_entry is not None:
                if (
                    loser_entry["state"] == ENTRY_PUBLISHED
                    and primary_entry["state"] != ENTRY_PUBLISHED
                ):
                    conn.execute(
                        "UPDATE entries SET state='published', evidence_id=?, reason=?,"
                        " published_at=?, withdrawn_at=NULL, rules_version=?, updated_at=?"
                        " WHERE id=?",
                        (
                            loser_entry["evidence_id"],
                            loser_entry["reason"],
                            loser_entry["published_at"] or now,
                            version,
                            now,
                            primary_entry["id"],
                        ),
                    )
                conn.execute(
                    "UPDATE entries SET state='withheld', reason='merged', withdrawn_at=?,"
                    " rules_version=?, updated_at=? WHERE id=?",
                    (now, version, now, loser_entry["id"]),
                )
            conn.execute(
                "UPDATE links SET to_url_id=? WHERE to_url_id=?", (primary, loser)
            )
            # Anything previously merged into the loser now belongs to the new
            # primary.  Re-pointing keeps clusters flat (and read paths cheap)
            # instead of building chains that grow with each rule change.
            conn.execute(
                "UPDATE urls SET merged_into=? WHERE merged_into=?", (primary, loser)
            )
            conn.execute(
                "UPDATE urls SET merged_into=?, merged_at=?, in_scope=0, reachable=0,"
                " scope_reason='merged' WHERE id=?",
                (primary, now, loser),
            )
            conn.execute(
                "UPDATE queue SET state=?, parked_reason=?, lease_until=NULL, worker=NULL,"
                " updated_at=? WHERE url_id=? AND state IN (?,?,?)",
                (CANCELLED, f"merged into url {primary}", now, loser, PENDING, LEASED, PARKED),
            )
        self._log("merged", loser=loser, primary=primary)

    # -- applying rules ------------------------------------------------
    def apply_rules(self, doc: dict[str, Any], *, note: str = "") -> dict[str, Any]:
        with self._lock:
            rules = RuleSet(doc)
            return self._apply_ruleset(rules, note=note, kind="publish", from_version=None)

    def _apply_ruleset(
        self,
        rules: RuleSet,
        *,
        note: str,
        kind: str,
        from_version: int | None,
    ) -> dict[str, Any]:
        old_version = self.store.active_rules_version()
        summary = self.preview_ruleset(rules)
        now = utc_now()
        with self.store.write() as conn:
            cur = conn.execute(
                "INSERT INTO rules_versions(doc, note, created_at, parent_id) VALUES(?,?,?,?)",
                (
                    json.dumps(rules.to_dict(), ensure_ascii=False, indent=2),
                    note,
                    now,
                    old_version,
                ),
            )
            version = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('active_rules_version', ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(version),),
            )
        rules.version = version
        summary["version"] = version
        summary["from_version"] = old_version

        self.ensure_seed_rows(rules)
        self._reconcile_identities(rules)
        evaluation = self.evaluate_graph(rules)
        self._persist_graph(rules, evaluation)
        self._replay_discovery(rules, evaluation)
        evaluation = self.evaluate_graph(rules)
        self._persist_graph(rules, evaluation)
        self._apply_scope_decisions(rules, evaluation)

        with self.store.write() as conn:
            cur = conn.execute(
                "INSERT INTO changes(kind, from_version, to_version, summary, note, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (
                    kind,
                    from_version if from_version is not None else old_version,
                    version,
                    json.dumps(summary, ensure_ascii=False),
                    note,
                    utc_now(),
                ),
            )
            change_id = int(cur.lastrowid)
        summary["change_id"] = change_id
        return summary

    def _replay_discovery(self, rules: RuleSet, evaluation: GraphEvaluation) -> None:
        """Re-run discovery from stored evidence for every reachable page."""
        for url_id, node in sorted(evaluation.nodes.items()):
            if not node.publishable:
                continue
            row = self.store.url_row(url_id)
            if row is None:
                continue
            cluster = self.store.cluster_ids(url_id)
            evidence = self.store.latest_evidence(cluster)
            if evidence is None or mime_of(evidence["content_type"]) not in PAGE_MIMES:
                continue
            self._discover_page(rules, row, evidence)

    def _apply_scope_decisions(self, rules: RuleSet, evaluation: GraphEvaluation) -> None:
        version = rules.version or 0
        rows = self.store.primary_rows()
        for row in rows:
            url_id = int(row["id"])
            node = evaluation.nodes.get(url_id)
            publishable = bool(node and node.publishable)
            cluster = self.store.cluster_ids(url_id)
            evidence = self.store.latest_evidence(cluster)
            if publishable and evidence is not None:
                self._enqueue_ingest(
                    int(evidence["id"]), url_id, rules_version=version, force=True
                )
                if row["fetch_error"] in TRANSIENT_FETCH_ERRORS:
                    # Stored bytes are still publishable, but the last request
                    # failed transiently.  Retry it in the background; the
                    # reopened task clears the error once it succeeds.
                    self._enqueue_download(row, rules_version=version)
                else:
                    self._park_downloads_for(url_id, park=False)
                continue
            if publishable and evidence is None:
                if row["fetch_error"] and row["fetch_error"] not in TRANSIENT_FETCH_ERRORS:
                    # Nothing was stored (robots/403/unsupported).  Surface it in
                    # the withheld catalog so the refusal is visible, but never
                    # invent evidence for it.
                    self._set_entry(
                        url_id,
                        0,
                        publish=False,
                        reason=row["fetch_error"],
                        version=version,
                        title=row["title"],
                    )
                else:
                    # No evidence and either no error or a transient one: the
                    # page still needs a fetch.  `_enqueue_download` reopens a
                    # transiently blocked task (clearing its error), so a rules
                    # change is genuinely able to retry it.
                    self._enqueue_download(row, rules_version=version)
                continue
            self._park_downloads_for(url_id, park=True)
            entry = self.store.one("SELECT * FROM entries WHERE url_id=?", (url_id,))
            if entry is not None and entry["state"] == ENTRY_PUBLISHED:
                reason = "unreachable"
                if node is not None and not node.in_scope:
                    reason = node.reason
                else:
                    decision = rules.evaluate(row["fetch_url"])
                    if not decision.in_scope:
                        reason = decision.reason
                self._withhold(url_id, reason, version, evidence_id=entry["evidence_id"])

    def _park_downloads_for(self, url_id: int, *, park: bool) -> None:
        if not park:
            return
        self.store.execute(
            "UPDATE queue SET state=?, parked_reason='out_of_scope', lease_until=NULL,"
            " worker=NULL, updated_at=? WHERE kind='download' AND url_id=? AND state IN (?,?)",
            (PARKED, utc_now(), url_id, PENDING, LEASED),
        )

    # -- preview -------------------------------------------------------
    def preview_rules(self, doc: dict[str, Any]) -> dict[str, Any]:
        return self.preview_ruleset(RuleSet(doc))

    def preview_ruleset(self, rules: RuleSet) -> dict[str, Any]:
        current = self.active_rules()
        current_eval = self.evaluate_graph(current) if current else GraphEvaluation()
        proposed = self.evaluate_graph(rules)
        current_ids = current_eval.publishable_ids()
        proposed_ids = proposed.publishable_ids()
        rows = {int(r["id"]): r for r in self.store.primary_rows()}
        entries = {int(e["url_id"]): e for e in self.store.all("SELECT * FROM entries")}
        clusters = self.store.cluster_members()
        evidence = {
            url_id: self.store.latest_evidence(members)
            for url_id, members in clusters.items()
        }
        publish: list[dict[str, Any]] = []
        withdraw: list[dict[str, Any]] = []
        for url_id in sorted(proposed_ids):
            entry = entries.get(url_id)
            if entry is not None and entry["state"] == ENTRY_PUBLISHED:
                continue
            row = rows.get(url_id)
            if row is None:
                continue
            evidence_row = evidence.get(url_id)
            publish.append(
                {
                    "url_id": url_id,
                    "url": row["canonical_url"],
                    "column_id": proposed.nodes[url_id].column_id,
                    "action": (
                        "republish_from_evidence"
                        if evidence_row is not None
                        else "fetch_then_publish"
                    ),
                    "reason": entry["reason"] if entry else "",
                }
            )
        for url_id in sorted(current_ids - proposed_ids):
            entry = entries.get(url_id)
            if entry is None or entry["state"] != ENTRY_PUBLISHED:
                continue
            row = rows.get(url_id)
            if row is None:
                continue
            withdraw.append(
                {
                    "url_id": url_id,
                    "url": row["canonical_url"],
                    "column_id": row["column_id"],
                    "reason": proposed.reason_for(url_id),
                }
            )
        park_downloads: list[dict[str, Any]] = []
        for task in self.store.all(
            "SELECT q.*, u.canonical_url FROM queue q LEFT JOIN urls u ON u.id=q.url_id"
            " WHERE q.kind='download' AND q.state IN ('pending','parked','leased')"
        ):
            url_id = task["url_id"]
            if url_id is None:
                continue
            url_id = int(url_id)
            if url_id in proposed_ids:
                continue
            row = rows.get(url_id)
            park_downloads.append(
                {
                    "task_id": int(task["id"]),
                    "url_id": url_id,
                    "url": row["canonical_url"] if row else task["canonical_url"],
                    "state": task["state"],
                    "reason": proposed.reason_for(url_id),
                }
            )
        enqueue_downloads: list[dict[str, Any]] = []
        seen_dl: set[str] = set()
        for url_id in sorted(proposed_ids):
            if evidence.get(url_id) is not None:
                continue
            row = rows.get(url_id)
            if row is None:
                continue
            key = row["fetch_url"]
            if key in seen_dl:
                continue
            seen_dl.add(key)
            enqueue_downloads.append(
                {
                    "url_id": url_id,
                    "url": row["canonical_url"],
                    "column_id": proposed.nodes[url_id].column_id,
                    "action": "fetch",
                }
            )
        new_urls = []
        for canonical, info in sorted(proposed.new_targets.items()):
            if canonical in seen_dl:
                continue
            seen_dl.add(canonical)
            new_urls.append(info)
        unchanged = len(current_ids & proposed_ids)
        if current is not None:
            diff = current.describe_changes(rules)
        else:
            diff = {"added": [c.id for c in rules.ordered], "removed": [], "changed": []}
        impact = Impact(
            diff=diff,
            current_in_scope=len(current_ids),
            proposed_in_scope=len(proposed_ids),
            publish=publish,
            withdraw=withdraw,
            park_downloads=park_downloads,
            enqueue_downloads=enqueue_downloads + new_urls,
            new_urls=new_urls,
            unchanged=unchanged,
            denied_references=proposed.denied_links,
        )
        return impact.as_dict()

    # -- undo ----------------------------------------------------------
    def undo_last_change(self, *, note: str = "") -> dict[str, Any]:
        with self._lock:
            change = self.store.last_revertable_change()
            if change is None:
                raise ValueError("no rules change can be undone")
            if self.store.active_rules_version() != int(change["to_version"]):
                raise ValueError(
                    "only the most recent rules change can be undone; publish or undo "
                    "newer changes first"
                )
            previous_doc = self.store.rules_doc_for_version(int(change["from_version"]))
            if previous_doc is None:
                raise ValueError("the previous rules version is missing")
            rules = RuleSet(previous_doc)
            summary = self._apply_ruleset(
                rules,
                note=note or f"undo change #{change['id']}",
                kind="undo",
                from_version=int(change["to_version"]),
            )
            with self.store.write() as conn:
                conn.execute(
                    "UPDATE changes SET reverted_at=?, reverted_by=? WHERE id=?",
                    (utc_now(), summary["change_id"], int(change["id"])),
                )
            summary["reverted_change_id"] = int(change["id"])
            return summary

    # -- workers -------------------------------------------------------
    def run_once(self) -> Task | None:
        for kind in ("ingest", "download", "discover"):
            task = self.queue.claim(kind, self.worker_id)
            if task is None:
                continue
            try:
                if kind == "ingest":
                    self.run_ingest(task)
                elif kind == "download":
                    self.run_download(task)
                else:
                    self.run_discover(task)
            except Exception as exc:  # keep the worker alive; record the error
                self.queue.fail(
                    task.id, f"{type(exc).__name__}: {exc}", retry_after=0.1, max_attempts=1
                )
            return task
        return None

    def run_until_idle(self, *, max_tasks: int = 2000) -> int:
        self.queue.recover_stale()
        processed = 0
        idle = 0
        while processed < max_tasks:
            task = self.run_once()
            if task is None:
                idle += 1
                if idle >= 2:
                    break
                continue
            idle = 0
            processed += 1
        return processed

    # -- misc ----------------------------------------------------------
    def _log(self, message: str, **fields: Any) -> None:
        entry = {"event": message, **fields}
        try:
            with open(os.path.join(self.storage_dir, "events.jsonl"), "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass

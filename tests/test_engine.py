"""End-to-end tests: real local HTTP server, real queue, real SQLite."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from scope_crawler import rules as rules_mod
from scope_crawler.db import ENTRY_PUBLISHED, ENTRY_WITHHELD, Store, utc_now
from scope_crawler.engine import Crawler, mime_of
from scope_crawler.fetch import FetchError, HttpFetcher
from scope_crawler.queue import BLOCKED, DONE, PARKED, PENDING, Queue
from scope_crawler.demosite import Site, instrument


@pytest.fixture()
def site():
    with Site() as server:
        yield server


@pytest.fixture()
def env(site, tmp_path):
    store = Store(str(tmp_path / "crawl.db"))
    store.init()
    crawler = Crawler(store, str(tmp_path / "storage"), fetcher=HttpFetcher(timeout=3))
    doc = rules_mod.default_document(site.site_root)
    doc["columns"].append(
        {
            "id": "store",
            "title": "商店",
            "path": "/site/store/",
            "enabled": False,
            "allow_refs_to": [],
            "max_ref_depth": 0,
        }
    )
    doc["exclude_patterns"] = ["/site/news/private/*"]
    crawler.apply_rules(doc, note="initial")
    return store, crawler, doc


def titles(store: Store, state: str = ENTRY_PUBLISHED) -> dict[str, str]:
    rows = store.all(
        "SELECT u.canonical_url, e.title, e.state FROM entries e JOIN urls u ON u.id=e.url_id"
        " WHERE e.state=?",
        (state,),
    )
    return {r["canonical_url"]: r["title"] for r in rows}


def urls_in_scope(store: Store) -> set[str]:
    return {
        r["canonical_url"]
        for r in store.all("SELECT canonical_url FROM urls WHERE in_scope=1 AND reachable=1")
    }


def test_initial_crawl_publishes_in_scope_pages(env):
    store, crawler, _doc = env
    processed = crawler.run_until_idle()
    assert processed > 0
    published = titles(store)
    paths = {u.split("/site", 1)[1] for u in published}
    assert "/news/" in paths
    assert "/news/2024/spring.html" in paths
    assert "/news/2024/summer.html" in paths
    assert "/docs/" in paths
    assert "/docs/guide.html" in paths
    # Alias page is stored under the canonical news path exactly once.
    assert not any("/latest/" in u for u in published)
    row = store.one(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/site/news/2024/summer.html'"
    )
    assert row is not None
    assert store.scalar(
        "SELECT COUNT(*) FROM urls WHERE canonical_url LIKE '%/2024/summer.html'"
    ) == 1
    assert store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id=?", (int(row["id"]),)
    ) == 1
    # Excluded, disabled column, outside root, restricted, robots.
    assert not any("private" in p for p in paths)
    assert not any("store" in p for p in paths)
    assert not any("outside" in p for p in paths)
    assert "/news/restricted" not in paths
    assert "/docs/secret.html" not in paths
    # Titles were parsed from the fetched HTML.
    assert published[next(u for u in published if u.endswith("/news/2024/spring.html"))] == "春天"


def test_boundary_links_are_recorded_not_followed(env):
    store, crawler, _doc = env
    crawler.run_until_idle()
    boundary = store.all("SELECT * FROM links WHERE outcome='boundary'")
    reasons = {r["reason"] for r in boundary}
    assert "outside_prefix" in reasons
    assert "outside_origin" in reasons  # elsewhere.invalid
    # Nothing outside the root ever got a URL row.
    assert store.scalar("SELECT COUNT(*) FROM urls WHERE canonical_url LIKE '%/outside/%'") == 0


def test_redirect_out_of_boundary_is_refused_before_the_request(env, site):
    store, crawler, _doc = env
    counted = instrument()
    site.httpd.RequestHandlerClass = counted
    crawler.run_until_idle()
    # The redirect target is outside /site/ and must never have been requested.
    assert "/outside/" not in counted.counter
    row = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/site/news/bounce'")
    assert row is not None
    assert row["fetch_error"] == "redirect_out_of_scope"
    # The refusal stops publication but is visible in the catalog.
    entry = store.one("SELECT * FROM entries WHERE url_id=?", (row["id"],))
    assert entry is not None and entry["state"] == ENTRY_WITHHELD


def test_cross_column_reference_is_allowed_for_news(env):
    store, crawler, _doc = env
    crawler.run_until_idle()
    denied = store.all("SELECT * FROM links WHERE outcome='ref_denied'")
    # news -> docs is allowed; docs -> news is not (docs.allow_refs_to is empty).
    denied_targets = {r["resolved_url"] for r in denied}
    assert any("/site/news/" in u for u in denied_targets)
    assert not any("/site/docs/" in u for u in denied_targets)


def test_narrowing_withholds_but_keeps_evidence(env, site):
    store, crawler, doc = env
    crawler.run_until_idle()
    before = titles(store)
    evidence_before = store.scalar("SELECT COUNT(*) FROM evidence")
    files_before = sum(
        len(files) for _root, _dirs, files in os.walk(os.path.join(crawler.storage_dir, "evidence"))
    )
    assert evidence_before > 0

    narrowed = json.loads(json.dumps(doc))
    for column in narrowed["columns"]:
        if column["id"] == "news":
            column["enabled"] = False
    summary = crawler.apply_rules(narrowed, note="disable news")
    crawler.run_until_idle()

    after = titles(store)
    news_before = {u for u in before if "/news/" in u}
    news_after = {u for u in after if "/news/" in u}
    assert news_before and not news_after
    # Evidence and files survive narrowing.
    assert store.scalar("SELECT COUNT(*) FROM evidence") >= evidence_before
    files_after = sum(
        len(files) for _root, _dirs, files in os.walk(os.path.join(crawler.storage_dir, "evidence"))
    )
    assert files_after >= files_before
    # Withheld entries keep their rows and record why.
    withheld = {
        r["canonical_url"]: r["reason"]
        for r in store.all(
            "SELECT u.canonical_url, e.reason FROM entries e JOIN urls u ON u.id=e.url_id"
            " WHERE e.state=?",
            (ENTRY_WITHHELD,),
        )
    }
    assert any("/news/" in u for u in withheld)


def test_widening_backfills_only_new_parts(env, site):
    counted = instrument()
    site.httpd.RequestHandlerClass = counted
    store, crawler, doc = env
    crawler.run_until_idle()
    first_counts = dict(counted.counter)

    widened = json.loads(json.dumps(doc))
    for column in widened["columns"]:
        if column["id"] == "store":
            column["enabled"] = True
        if column["id"] == "news":
            column["allow_refs_to"] = ["docs", "store"]
    summary = crawler.apply_rules(widened, note="enable store")
    crawler.run_until_idle()

    widened_paths = titles(store)
    store_paths = {u.split("/site", 1)[1] for u in widened_paths if "/store/" in u}
    assert "/store/" in store_paths
    assert "/store/item.html" in store_paths

    # Already-fetched pages were not downloaded again.
    second_counts = counted.counter
    for path, n in first_counts.items():
        if path in second_counts:
            assert second_counts[path] == n, f"{path} was re-fetched"
    assert summary["diff"]["changed"]
    assert "/site/store/" in "".join(u for u in widened_paths)


def test_scope_change_parks_and_reopens_queue_tasks(env):
    store, crawler, doc = env
    queue = crawler.queue
    # Enqueue but do not run a download for a page in a column we will disable.
    crawler.run_until_idle()
    pending = store.all("SELECT * FROM queue WHERE state=?", (PENDING,))
    assert not pending

    narrowed = json.loads(json.dumps(doc))
    for column in narrowed["columns"]:
        if column["id"] == "docs":
            column["enabled"] = False
    crawler.apply_rules(narrowed, note="disable docs")
    # No doc downloads are left runnable.
    doc_rows = store.all(
        "SELECT q.state FROM queue q JOIN urls u ON u.id=q.url_id"
        " WHERE q.kind='download' AND u.column_id='docs'"
    )
    assert doc_rows
    # Nothing is left runnable: done, parked by the scope change, or permanently
    # blocked because robots.txt forbids it.
    assert all(r["state"] in (DONE, PARKED, BLOCKED) for r in doc_rows)
    assert not any(r["state"] == PENDING for r in doc_rows)

    restored = json.loads(json.dumps(doc))
    crawler.apply_rules(restored, note="restore docs")
    crawler.run_until_idle()
    assert any("/docs/" in u for u in titles(store))


def test_preview_reports_impact_without_applying(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    version = store.active_rules_version()
    published_before = titles(store)

    narrowed = json.loads(json.dumps(doc))
    for column in narrowed["columns"]:
        if column["id"] == "news":
            column["enabled"] = False
    preview = crawler.preview_rules(narrowed)
    assert store.active_rules_version() == version
    assert titles(store) == published_before
    assert preview["proposed_in_scope"] < preview["current_in_scope"]
    assert preview["withdraw"], "preview should list pages that would be withdrawn"
    assert all("/news/" in item["url"] for item in preview["withdraw"])
    # Every listed id must resolve to a column in the proposed graph; the
    # console dereferences it directly.
    proposed = crawler.evaluate_graph(rules_mod.RuleSet(narrowed))
    for item in preview["publish"]:
        assert proposed.nodes[item["url_id"]].column_id is not None


def test_preview_does_not_advertise_a_disabled_new_column(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    extended = json.loads(json.dumps(doc))
    extended["columns"].append(
        {
            "id": "blog",
            "title": "博客",
            "path": "/site/blog/",
            "enabled": False,
            "allow_refs_to": [],
            "max_ref_depth": 0,
        }
    )
    preview = crawler.preview_rules(extended)
    assert preview["new_urls"] == []
    assert not any(item.get("column_id") == "blog" for item in preview["enqueue_downloads"])

    # Enabling it does make the column root new work to fetch.
    extended["columns"][-1]["enabled"] = True
    preview = crawler.preview_rules(extended)
    assert [item["column_id"] for item in preview["new_urls"]] == ["blog"]


def test_out_of_scope_links_are_listed_for_console(env):
    store, crawler, _doc = env
    crawler.run_until_idle()
    boundary = store.all(
        "SELECT l.*, u.canonical_url AS source FROM links l LEFT JOIN urls u ON u.id=l.from_url_id"
        " WHERE l.outcome IN ('boundary','scope','ref_denied')"
    )
    assert boundary
    assert all(r["source"] for r in boundary)


def test_late_download_after_scope_narrowing_is_withheld(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    # Hold the next download, then run until the hold is used.
    store.set_setting("hold_next_download", "1")
    # Force a fresh download of a page that will be put out of scope.
    target = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    assert target is not None
    crawler._enqueue_download(target, rules_version=store.active_rules_version() or 0)
    # The queue is done for this URL, so enqueue a new download task explicitly.
    existing = crawler.queue.by_dedupe("download", f"download:{target['id']}")
    if existing is None or existing["state"] == DONE:
        crawler.queue.enqueue(
            "download",
            f"download:{target['id']}:late",
            url_id=int(target["id"]),
            priority=5,
            rules_version=store.active_rules_version() or 0,
            payload={"url_id": int(target["id"])},
        )
    crawler.run_until_idle()
    held = store.all("SELECT * FROM inflight WHERE state='held'")
    assert held, "the download should be held for later delivery"

    narrowed = json.loads(json.dumps(doc))
    for column in narrowed["columns"]:
        if column["id"] == "docs":
            column["enabled"] = False
    crawler.apply_rules(narrowed, note="disable docs before late delivery")
    crawler.run_until_idle()

    delivered = crawler.deliver_held()
    assert delivered
    crawler.run_until_idle()
    entry = store.one(
        "SELECT e.* FROM entries e JOIN urls u ON u.id=e.url_id WHERE u.canonical_url LIKE '%/docs/guide.html'"
    )
    assert entry is not None
    assert entry["state"] == ENTRY_WITHHELD
    # The bytes were still preserved as evidence.
    assert store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id=?", (int(target["id"]),)
    ) >= 1


def test_transient_failure_can_be_retried_but_a_denial_is_not(env):
    store, crawler, _doc = env
    crawler.run_until_idle()
    guide = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    secret = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/secret.html'")
    assert secret["fetch_error"] == "robots_disallowed"

    # Pretend the guide download hit a network error and was blocked.
    store.execute(
        "UPDATE urls SET fetch_error='network_error' WHERE id=?", (int(guide["id"]),)
    )
    store.execute(
        "UPDATE queue SET state='blocked' WHERE dedupe=?",
        (f"download:{guide['id']}",),
    )
    assert crawler._enqueue_download(store.url_row(int(guide["id"])), rules_version=1)
    assert store.scalar(
        "SELECT state FROM queue WHERE dedupe=?", (f"download:{guide['id']}",)
    ) == PENDING
    assert store.url_row(int(guide["id"]))["fetch_error"] == ""

    # A robots.txt denial (or a 403) is a permanent verdict and stays blocked.
    store.execute(
        "UPDATE queue SET state='blocked' WHERE dedupe=?",
        (f"download:{secret['id']}",),
    )
    assert not crawler._enqueue_download(store.url_row(int(secret["id"])), rules_version=1)
    assert store.scalar(
        "SELECT state FROM queue WHERE dedupe=?", (f"download:{secret['id']}",)
    ) == "blocked"


def test_a_rules_change_retries_a_transiently_blocked_download(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    guide = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    restricted = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/news/restricted'")
    dedupe = f"download:{guide['id']}"

    # Make the guide look like its only attempt hit a transient network error:
    # no stored bytes, a recorded transient error, and a blocked task.
    store.execute("DELETE FROM evidence WHERE url_id=?", (int(guide["id"]),))
    store.execute("DELETE FROM entries WHERE url_id=?", (int(guide["id"]),))
    store.execute(
        "UPDATE urls SET fetch_error='network_error' WHERE id=?", (int(guide["id"]),)
    )
    store.execute("UPDATE queue SET state='blocked' WHERE dedupe=?", (dedupe,))

    # Publishing a rule set re-evaluates every in-scope page.  A transient
    # failure is retryable, so this alone must reopen the download; a permanent
    # refusal (403/robots) must stay blocked.
    crawler.apply_rules(doc, note="retry transient")
    assert store.scalar("SELECT state FROM queue WHERE dedupe=?", (dedupe,)) == PENDING
    assert store.url_row(int(guide["id"]))["fetch_error"] == ""
    restricted_task = store.one(
        "SELECT state FROM queue WHERE dedupe=?", (f"download:{restricted['id']}",)
    )
    assert restricted_task["state"] == BLOCKED

    crawler.run_until_idle()
    entry = store.one("SELECT * FROM entries WHERE url_id=?", (int(guide["id"]),))
    assert entry is not None and entry["state"] == ENTRY_PUBLISHED


def test_a_transient_error_does_not_withdraw_stored_evidence(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    guide = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    assert store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id=?", (int(guide["id"]),)
    ) == 1
    assert store.scalar(
        "SELECT state FROM entries WHERE url_id=?", (int(guide["id"]),)
    ) == ENTRY_PUBLISHED

    # A failed *refresh* is transient: the bytes already stored still describe
    # what the site served, so publication must survive, and the fetch is
    # retried rather than the entry being withdrawn.
    store.execute(
        "UPDATE urls SET fetch_error='network_error' WHERE id=?", (int(guide["id"]),)
    )
    store.execute(
        "UPDATE queue SET state='blocked' WHERE dedupe=?", (f"download:{guide['id']}",)
    )
    crawler.apply_rules(doc, note="publish after a failed refresh")
    assert store.scalar(
        "SELECT state FROM entries WHERE url_id=?", (int(guide["id"]),)
    ) == ENTRY_PUBLISHED
    assert store.scalar(
        "SELECT state FROM queue WHERE dedupe=?", (f"download:{guide['id']}",)
    ) == PENDING

    crawler.run_until_idle()
    # The retry succeeded on the local site, so the transient error is gone.
    assert store.url_row(int(guide["id"]))["fetch_error"] == ""
    assert store.scalar(
        "SELECT state FROM entries WHERE url_id=?", (int(guide["id"]),)
    ) == ENTRY_PUBLISHED


def test_late_download_for_a_merged_row_publishes_under_the_primary(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    target = store.one(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/site/docs/' AND merged_into IS NULL"
    )
    assert target is not None
    crawler.queue.reopen(
        "download",
        f"download:{target['id']}",
        url_id=int(target["id"]),
        priority=1,
        rules_version=store.active_rules_version() or 0,
        payload={"url_id": int(target["id"])},
    )
    task = crawler.queue.claim("download", "test-worker")
    while task is not None and int(task.url_id or 0) != int(target["id"]):
        crawler.queue.complete(task.id)
        task = crawler.queue.claim("download", "test-worker")
    assert task is not None
    assert crawler.run_download(task, hold=True).startswith("held")

    # A rule change merges this row away before the result is delivered.
    merged_doc = json.loads(json.dumps(doc))
    for column in merged_doc["columns"]:
        if column["id"] == "news":
            column["aliases"] = ["/site/latest/", "/site/docs/"]
        elif column["id"] == "docs":
            column["path"] = "/site/news/docs/"
            column["aliases"] = []
    crawler.apply_rules(merged_doc, note="merge docs into news")
    crawler.run_until_idle()
    merged = store.url_row(int(target["id"]))
    assert merged["merged_into"] is not None
    primary_id = int(merged["merged_into"])

    assert crawler.deliver_held()
    crawler.run_until_idle()
    # The late bytes are cluster evidence and the surviving row publishes the
    # newest version; the merged row must not swallow the delivery.
    newest = store.one(
        "SELECT * FROM evidence WHERE url_id IN (?,?) ORDER BY requested_at DESC, id DESC LIMIT 1",
        (int(target["id"]), primary_id),
    )
    entry = store.one("SELECT * FROM entries WHERE url_id=?", (primary_id,))
    assert entry is not None
    assert entry["state"] == ENTRY_PUBLISHED
    assert int(entry["evidence_id"]) == int(newest["id"])


def test_committing_one_late_result_twice_stores_it_once(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    target = store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/site/docs/guide.html'")
    crawler.queue.reopen(
        "download",
        f"download:{target['id']}",
        url_id=int(target["id"]),
        priority=1,
        rules_version=store.active_rules_version() or 0,
        payload={"url_id": int(target["id"])},
    )
    task = crawler.queue.claim("download", "test-worker")
    while task is not None and int(task.url_id or 0) != int(target["id"]):
        crawler.queue.complete(task.id)
        task = crawler.queue.claim("download", "test-worker")
    crawler.run_download(task, hold=True)
    inflight_id = int(store.scalar("SELECT id FROM inflight WHERE state='held'"))
    evidence_before = store.scalar("SELECT COUNT(*) FROM evidence")
    assert crawler.commit_inflight(inflight_id) != "already_committed"
    assert crawler.commit_inflight(inflight_id) == "already_committed"
    assert store.scalar("SELECT COUNT(*) FROM evidence") == evidence_before + 1


def test_page_depth_limit_bounds_the_crawl(env, monkeypatch):
    from scope_crawler import engine as engine_mod

    store, crawler, _doc = env
    monkeypatch.setattr(engine_mod, "MAX_PAGE_DEPTH", 1)
    crawler.run_until_idle()
    published = {
        u.split("/site", 1)[1] for u in titles(store) if "/site" in u
    }
    # The seeds (depth 0) and their directly linked pages (depth 1) run; links
    # discovered from those pages (depth 2) are refused by the page-depth cap.
    assert "/news/2024/spring.html" in published
    assert "/docs/guide.html" in published
    crawled = store.scalar(
        "SELECT COUNT(*) FROM links WHERE outcome='scope' AND reason='max_depth'"
    )
    assert crawled > 0


def test_alias_change_merges_rows_and_keeps_evidence(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    evidence_before = store.scalar("SELECT COUNT(*) FROM evidence")
    docs_home = store.one(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/site/docs/' AND merged_into IS NULL"
    )
    assert docs_home is not None

    # Re-home the docs column and turn its old path into a news alias.  Every
    # stored URL whose spelling sits under the old path now canonicalises to
    # the news home, so the identity rows collide.
    remapped = json.loads(json.dumps(doc))
    for column in remapped["columns"]:
        if column["id"] == "news":
            column["aliases"] = ["/site/latest/", "/site/docs/"]
        elif column["id"] == "docs":
            column["path"] = "/site/news/docs/"
            column["aliases"] = []
    crawler.apply_rules(remapped, note="docs path becomes a news alias")

    merged = store.url_row(int(docs_home["id"]))
    assert merged["merged_into"] is not None
    news_home = store.one(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/site/news/' AND merged_into IS NULL"
    )
    assert int(merged["merged_into"]) == int(news_home["id"])
    # A rename merges identities; it never deletes the raw evidence.
    assert store.scalar("SELECT COUNT(*) FROM evidence") >= evidence_before
    assert store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id=?", (int(docs_home["id"]),)
    ) >= 1
    # The merged row is withheld, not silently dropped.
    entry = store.one("SELECT * FROM entries WHERE url_id=?", (int(docs_home["id"]),))
    assert entry is None or entry["state"] == ENTRY_WITHHELD
    # Its queue rows are cancelled rather than left runnable.
    assert not store.scalar(
        "SELECT COUNT(*) FROM queue WHERE url_id=? AND state IN ('pending','leased')",
        (int(docs_home["id"]),),
    )
    # The columns still divide the site without overlap.
    root = crawler.active_rules().doc["site_root"]
    assert crawler.active_rules().evaluate(root + "docs/guide.html").column_id == "news"


def test_undo_of_an_identity_merge_reuses_evidence(env, site):
    store, crawler, doc = env
    counted = instrument()
    site.httpd.RequestHandlerClass = counted
    crawler.run_until_idle()
    evidence_before = store.scalar("SELECT COUNT(*) FROM evidence")
    requests_before = sum(counted.counter.values())
    assert evidence_before > 0 and requests_before > 0

    # Turn the docs path into a news alias: every docs page collides with its
    # equivalent news identity and gets merged away.
    merged_doc = json.loads(json.dumps(doc))
    for column in merged_doc["columns"]:
        if column["id"] == "news":
            column["aliases"] = ["/site/latest/", "/site/docs/"]
        elif column["id"] == "docs":
            column["path"] = "/site/news/docs/"
            column["aliases"] = []
    crawler.apply_rules(merged_doc, note="merge docs into news")
    crawler.run_until_idle()
    assert store.scalar("SELECT COUNT(*) FROM urls WHERE merged_into IS NOT NULL") > 0
    requests_after_merge = sum(counted.counter.values())
    evidence_after_merge = store.scalar("SELECT COUNT(*) FROM evidence")

    crawler.undo_last_change()
    crawler.run_until_idle()
    # Undo is a derived-state operation: the original docs rows become primary
    # again and their stored evidence republishes.  (The temporary seed row the
    # merge rule created for the moved path stays as a duplicate identity and is
    # merged into the original docs home, which is exactly what the rules say.)
    docs_home = store.one(
        "SELECT * FROM urls WHERE fetch_url LIKE '%/site/docs/' AND merged_into IS NULL"
    )
    assert docs_home is not None
    # No row that actually fetched something stays merged after the undo: the
    # only merged remnant may be a duplicate seed for a path that already has a
    # primary row (never fetched, so no evidence to lose).
    assert store.scalar(
        "SELECT COUNT(*) FROM urls u WHERE u.merged_into IS NOT NULL"
        " AND EXISTS (SELECT 1 FROM evidence e WHERE e.url_id=u.id)"
    ) == 0
    assert store.scalar("SELECT COUNT(*) FROM evidence") == evidence_after_merge
    assert sum(counted.counter.values()) == requests_after_merge
    docs_paths = {
        u.split("/site", 1)[1]
        for u in titles(store)
        if u.split("/site", 1)[1].startswith("/docs/")
    }
    assert "/docs/guide.html" in docs_paths


def test_undo_restores_previous_scope(env):
    store, crawler, doc = env
    crawler.run_until_idle()
    published_before = titles(store)
    narrowed = json.loads(json.dumps(doc))
    for column in narrowed["columns"]:
        if column["id"] == "news":
            column["enabled"] = False
    crawler.apply_rules(narrowed, note="disable news")
    crawler.run_until_idle()
    assert not any("/news/" in u for u in titles(store))
    crawler.undo_last_change()
    crawler.run_until_idle()
    assert titles(store) == published_before


def test_cross_column_recursion_is_bounded(env):
    store, crawler, doc = env
    # Allow both directions, keep the hop budget small.
    for column in doc["columns"]:
        column["allow_refs_to"] = ["news", "docs"]
    for column in doc["columns"]:
        column["allow_refs_to"] = [c for c in column["allow_refs_to"] if c != column["id"]]
    doc["max_cross_column_hops"] = 2
    crawler.apply_rules(doc, note="bidirectional refs")
    crawler.run_until_idle()
    # Every fetch count stays finite despite cycles in the site.
    evidence = store.all("SELECT url_id, COUNT(*) AS n FROM evidence GROUP BY url_id")
    assert evidence
    assert all(r["n"] <= 3 for r in evidence)

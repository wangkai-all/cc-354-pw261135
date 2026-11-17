"""Console tests: they exercise the same HTTP routes a user clicks through."""

from __future__ import annotations

import json
import re

import pytest

from scope_crawler.app import Console, create_app


@pytest.fixture()
def client(tmp_path):
    console = Console(str(tmp_path))
    app = create_app(console)
    app.config.update(TESTING=True)
    with app.test_client() as test_client:
        test_client.console = console
        yield test_client
    console.site.stop()


def post_doc(client, doc: dict, path: str = "/preview", **extra):
    payload = {"doc": json.dumps(doc, ensure_ascii=False), **extra}
    return client.post(path, data=payload, follow_redirects=True)


def active_doc(client) -> dict:
    return client.console.crawler.active_rules().to_dict()


def crawl(client) -> None:
    client.post("/run", data={"next": "queue"}, follow_redirects=True)


def test_overview_and_rules_render(client):
    assert client.get("/").status_code == 200
    rules = client.get("/rules")
    assert rules.status_code == 200
    assert "范围采集控制台" in rules.get_data(as_text=True)
    assert client.get("/queue").status_code == 200


def test_full_flow_crawl_then_narrow_then_widen(client):
    con = client.console
    client.post("/run")
    published = con.store.scalar(
        "SELECT COUNT(*) FROM entries WHERE state='published'"
    )
    assert published > 0

    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "news":
            column["enabled"] = False

    preview = post_doc(client, doc)
    html = preview.get_data(as_text=True)
    assert "规则预览" in html
    # Preview must not mutate anything.
    assert con.crawler.active_rules().to_dict()["columns"][0]["enabled"] is True
    assert "将停止发布" in html

    publish = post_doc(client, doc, path="/publish")
    assert publish.status_code == 200
    assert con.crawler.active_rules().to_dict()["columns"][0]["enabled"] is False
    news_published = con.store.scalar(
        "SELECT COUNT(*) FROM entries e JOIN urls u ON u.id=e.url_id"
        " WHERE e.state='published' AND u.column_id='news'"
    )
    assert news_published == 0

    # Widen again: entries come back from stored evidence.
    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "news":
            column["enabled"] = True
    post_doc(client, doc, path="/publish")
    restored = con.store.scalar(
        "SELECT COUNT(*) FROM entries e JOIN urls u ON u.id=e.url_id"
        " WHERE e.state='published' AND u.column_id='news'"
    )
    assert restored > 0


def test_boundary_page_lists_outside_links(client):
    client.post("/run")
    html = client.get("/boundaries").get_data(as_text=True)
    assert "/outside/" in html
    assert "elsewhere.invalid" in html
    boundary_only = client.get("/boundaries?outcome=boundary").get_data(as_text=True)
    assert "/site/news/" in boundary_only


def test_undo_endpoint_restores_rules(client):
    client.post("/run")
    before = client.console.store.active_rules_version()
    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "docs":
            column["enabled"] = False
    post_doc(client, doc, path="/publish")
    response = client.post("/undo", follow_redirects=True)
    assert response.status_code == 200
    rules = client.console.crawler.active_rules()
    assert rules.columns["docs"].enabled is True
    assert client.console.store.active_rules_version() != before


def test_late_delivery_flow_from_console(client):
    con = client.console
    client.post("/run")
    guide = con.store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    assert guide is not None

    hold = client.post("/late/hold", data={"url_id": guide["id"]}, follow_redirects=True)
    assert hold.status_code == 200
    held = con.store.all("SELECT * FROM inflight WHERE state='held'")
    assert len(held) == 1

    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "docs":
            column["enabled"] = False
    post_doc(client, doc, path="/publish")

    deliver = client.post("/late/deliver", follow_redirects=True)
    assert deliver.status_code == 200
    html = deliver.get_data(as_text=True)
    assert "已交付晚到结果" in html
    entry = con.store.one("SELECT * FROM entries WHERE url_id=?", (guide["id"],))
    assert entry["state"] == "withheld"
    assert con.store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id=?", (guide["id"],)
    ) >= 1


def test_late_hold_refuses_out_of_scope_url(client):
    con = client.console
    client.post("/run")
    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "docs":
            column["enabled"] = False
    post_doc(client, doc, path="/publish")
    guide = con.store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/guide.html'")
    response = client.post(
        "/late/hold", data={"url_id": guide["id"]}, follow_redirects=True
    )
    html = response.get_data(as_text=True)
    assert "不在范围内" in html
    assert con.store.scalar("SELECT COUNT(*) FROM inflight WHERE state='held'") == 0


def test_invalid_rules_show_an_error_not_a_crash(client):
    bad = {"site_root": client.console.site.site_root, "columns": []}
    response = post_doc(client, bad)
    assert response.status_code == 200
    assert "规则无效" in response.get_data(as_text=True)
    response = client.post("/preview", data={"doc": "{not json"})
    assert response.status_code == 200
    assert "规则无效" in response.get_data(as_text=True)


def test_api_preview_and_state(client):
    client.post("/run")
    state = client.get("/api/state").get_json()
    assert state["counts"]["published"] > 0
    doc = active_doc(client)
    for column in doc["columns"]:
        if column["id"] == "news":
            column["enabled"] = False
    response = client.post("/api/preview", json={"doc": doc})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["impact"]["withdraw"]
    assert client.post("/api/preview", json={"nope": 1}).status_code == 400


def test_detail_page_shows_evidence_and_history(client):
    con = client.console
    client.post("/run")
    row = con.store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/news/2024/spring.html'")
    html = client.get(f"/url/{row['id']}").get_data(as_text=True)
    assert "证据" in html
    assert "发布历史" in html
    assert "春天" in html


def test_alias_does_not_duplicate_catalog_entries(client):
    con = client.console
    client.post("/run")
    summer = con.store.all(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/2024/summer.html' AND merged_into IS NULL"
    )
    assert len(summer) == 1
    evidence = con.store.all(
        "SELECT * FROM evidence WHERE url_id=?", (summer[0]["id"],)
    )
    assert len(evidence) == 1


def test_restricted_and_robots_urls_are_never_bypassed(client):
    con = client.console
    client.post("/run")
    restricted = con.store.one(
        "SELECT * FROM urls WHERE canonical_url LIKE '%/news/restricted'"
    )
    assert restricted["fetch_error"] == "access_restricted"
    # The URL stays in scope by rules; it simply cannot be fetched.
    assert restricted["scope_reason"] == "ok"
    secret = con.store.one("SELECT * FROM urls WHERE canonical_url LIKE '%/docs/secret.html'")
    assert secret["fetch_error"] == "robots_disallowed"
    assert con.store.scalar(
        "SELECT COUNT(*) FROM evidence WHERE url_id IN (?,?)",
        (restricted["id"], secret["id"]),
    ) == 0
    entry = con.store.one("SELECT * FROM entries WHERE url_id=?", (restricted["id"],))
    assert entry is not None and entry["state"] == "withheld"

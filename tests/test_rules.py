"""Unit tests for URL boundaries and rule documents (no network)."""

from __future__ import annotations

import pytest

from scope_crawler import rules as R
from scope_crawler.urls import (
    BoundaryError,
    SiteBoundary,
    clean_path,
)


ROOT = "http://example.test/site/"


@pytest.fixture()
def boundary():
    return SiteBoundary.parse(ROOT)


@pytest.mark.parametrize(
    "path",
    [
        "/site/",
        "/site/news/",
        "/site/news/a.html",
        "/site/news/../docs/",
    ],
)
def test_inside_paths_classify_as_ok_or_targeted(boundary, path):
    _canonical, reason, _display = boundary.classify(f"http://example.test{path}")
    assert reason == "ok"


def test_query_is_not_followed_by_default(boundary):
    assert boundary.classify("http://example.test/site/news?x=1")[1] == "query_not_followed"


@pytest.mark.parametrize(
    "url,reason",
    [
        ("http://example.test/outside/", "outside_prefix"),
        ("http://example.test/site", "outside_prefix"),
        ("http://other.test/site/", "outside_origin"),
        ("https://example.test/site/", "outside_origin"),
        ("http://example.test:8080/site/", "outside_origin"),
        ("http://example.test/site/../../etc/passwd", "escaping_path"),
        ("ftp://example.test/site/", "bad_scheme"),
        ("http://user:pass@example.test/site/", "userinfo"),
    ],
)
def test_outside_or_unsafe_urls_are_rejected(boundary, url, reason):
    canonical, actual, _display = boundary.classify(url)
    assert canonical is None
    assert actual == reason


def test_query_rules_are_configurable():
    strict = SiteBoundary.parse(ROOT)
    assert strict.classify("http://example.test/site/a?q=1")[1] == "query_not_followed"
    loose = SiteBoundary.parse(ROOT, follow_query=True)
    canonical, reason, _ = loose.classify("http://example.test/site/a?b=2&a=1")
    assert reason == "ok"
    assert canonical.endswith("/site/a?a=1&b=2")


def test_encoded_traversal_is_refused(boundary):
    assert boundary.classify("http://example.test/site/%2e%2e/%2e%2e/etc")[1] in (
        "outside_prefix",
        "escaping_path",
    )
    assert boundary.classify("http://example.test/site/a%2fb")[1] == "escaping_path"


def test_clean_path_preserves_trailing_slash():
    assert clean_path("/site/news/") == "/site/news/"
    assert clean_path("/site/news") == "/site/news"
    assert clean_path("/site/./a/../b/") == "/site/b/"
    with pytest.raises(BoundaryError):
        clean_path("/../etc")


def _doc(**overrides):
    doc = R.default_document(ROOT)
    doc.update(overrides)
    return doc


def test_alias_resolves_to_canonical_path_and_url():
    rules = R.RuleSet(_doc())
    decision = rules.evaluate("http://example.test/site/latest/x.html")
    assert decision.canonical_url == "http://example.test/site/news/x.html"
    assert decision.fetch_url == "http://example.test/site/latest/x.html"
    assert decision.via_alias


def test_alias_rewrite_does_not_double_encode():
    # Column paths/aliases are decoded; the canonical URL must re-quote once.
    rules = R.RuleSet(_doc())
    decision = rules.evaluate("http://example.test/site/latest/%E4%B8%AD.html")
    assert decision.canonical_url == "http://example.test/site/news/%E4%B8%AD.html"
    assert decision.via_alias
    assert not rules.evaluate("http://example.test/site/latest/a%20b.html").canonical_url.endswith(
        "%2520b.html"
    )


def test_exclude_patterns_match_through_an_alias():
    rules = R.RuleSet(_doc(exclude_patterns=["/site/news/private/*"]))
    # The pattern is written against canonical paths; an alias spelling must
    # still be excluded after canonicalisation.
    decision = rules.evaluate("http://example.test/site/latest/private/x.html")
    assert decision.canonical_url == "http://example.test/site/news/private/x.html"
    assert not decision.in_scope


def test_exclude_written_on_an_alias_also_applies_to_the_canonical_path():
    # An alias is another spelling of the same page.  Excluding a page under
    # its alias must not leave the same page publishable under its canonical
    # path (or vice versa) depending on which spelling was discovered first.
    rules = R.RuleSet(_doc(exclude_patterns=["/site/latest/secret/*"]))
    alias_spelling = rules.evaluate("http://example.test/site/latest/secret/a.html")
    canonical_spelling = rules.evaluate("http://example.test/site/news/secret/a.html")
    assert not alias_spelling.in_scope
    assert not canonical_spelling.in_scope
    assert canonical_spelling.reason == R.EXCLUDED
    assert rules.doc["exclude_patterns"] == ["/site/news/secret/*"]


def test_alias_exclude_rewrite_is_segment_aware():
    # /site/latestx/ is a different path, not the alias /site/latest/.
    rules = R.RuleSet(_doc(exclude_patterns=["/site/latestx/*"]))
    assert rules.doc["exclude_patterns"] == ["/site/latestx/*"]
    assert rules.evaluate("http://example.test/site/news/x.html").in_scope
    # The literal path is still excluded by the literal pattern.
    literal = rules.evaluate("http://example.test/site/latestx/x.html")
    assert not literal.in_scope and literal.reason == R.EXCLUDED
    # A pattern naming the alias without its trailing slash still maps.
    bare = R.RuleSet(_doc(exclude_patterns=["/site/latest"]))
    assert bare.doc["exclude_patterns"] == ["/site/news"]
    assert not bare.evaluate("http://example.test/site/news/x.html").in_scope


def test_alias_cannot_overlap_another_column():
    doc = _doc()
    doc["columns"][1]["aliases"] = ["/site/news/"]
    with pytest.raises(R.RuleError, match="overlaps"):
        R.RuleSet(doc)


def test_alias_cannot_escape_the_root():
    doc = _doc()
    doc["columns"][0]["aliases"] = ["/outside/"]
    with pytest.raises(R.RuleError):
        R.RuleSet(doc)


def test_alias_cannot_be_the_root():
    doc = _doc()
    doc["columns"][0]["aliases"] = ["/"]
    with pytest.raises(R.RuleError):
        R.RuleSet(doc)


def test_column_paths_must_be_unique():
    doc = _doc()
    doc["columns"][1]["path"] = "/site/news/"
    with pytest.raises(R.RuleError, match="share path"):
        R.RuleSet(doc)


def test_unknown_reference_target_is_rejected():
    doc = _doc()
    doc["columns"][0]["allow_refs_to"] = ["ghost"]
    with pytest.raises(R.RuleError, match="unknown column"):
        R.RuleSet(doc)


def test_transition_budget_prevents_unbounded_recursion():
    rules = R.RuleSet(_doc(max_cross_column_hops=1))
    ok, _ = rules.transition("news", "docs", hops_used=0, column_depth=0)
    assert ok
    denied, reason = rules.transition("news", "docs", hops_used=1, column_depth=0)
    assert not denied
    assert reason == "cross_column_budget"
    denied, reason = rules.transition("docs", "news", hops_used=0, column_depth=0)
    assert not denied
    assert reason == "cross_column_denied"


def test_max_ref_depth_is_enforced():
    rules = R.RuleSet(_doc())
    ok, _ = rules.transition("news", "docs", hops_used=0, column_depth=1)
    assert ok
    denied, reason = rules.transition("news", "docs", hops_used=0, column_depth=2)
    assert not denied
    assert reason == "ref_depth_exceeded"


def test_excludes_win_over_columns():
    rules = R.RuleSet(_doc(exclude_patterns=["/site/news/private/*"]))
    decision = rules.evaluate("http://example.test/site/news/private/x.html")
    assert not decision.in_scope
    assert decision.reason == "excluded"


def test_excludes_apply_after_query_handling():
    rules = R.RuleSet(
        _doc(exclude_patterns=["/site/news/private/*"], follow_query=True)
    )
    decision = rules.evaluate("http://example.test/site/news/private/x.html?v=1")
    assert not decision.in_scope
    # The query survives canonicalisation but does not defeat the pattern.
    assert decision.reason == "excluded"


def test_excludes_must_stay_inside_root():
    with pytest.raises(R.RuleError):
        R.RuleSet(_doc(exclude_patterns=["/elsewhere/*"]))


def test_nested_column_longest_prefix_wins():
    doc = _doc()
    doc["columns"].append(
        {
            "id": "news-archive",
            "title": "归档",
            "path": "/site/news/archive/",
            "enabled": True,
            "allow_refs_to": [],
            "max_ref_depth": 0,
        }
    )
    rules = R.RuleSet(doc)
    assert rules.evaluate("http://example.test/site/news/archive/2001.html").column_id == "news-archive"
    assert rules.evaluate("http://example.test/site/news/other.html").column_id == "news"


def test_aliases_are_stable_across_repeated_evaluations():
    rules = R.RuleSet(_doc())
    first = rules.evaluate("http://example.test/site/latest/x.html")
    second = rules.evaluate(first.canonical_url or "")
    assert first.canonical_url == second.canonical_url
    assert not second.via_alias


def test_column_path_may_equal_the_site_root():
    # The root itself is a page too.  Rewriting its path must not append a
    # slash that the boundary then rejects as "escaped", which would make the
    # only seed row unreachable.
    doc = _doc()
    doc["columns"] = [
        {
            "id": "all",
            "title": "all",
            "path": "/site/",
            "aliases": [],
            "enabled": True,
            "allow_refs_to": [],
            "max_ref_depth": 1,
        }
    ]
    rules = R.RuleSet(doc)
    root_decision = rules.evaluate(ROOT)
    assert root_decision.in_scope
    assert root_decision.reason == R.OK
    assert rules.evaluate(ROOT + "news/a.html").in_scope


def test_document_fingerprint_tracks_semantic_changes():
    base = R.RuleSet(_doc())
    same = R.RuleSet(_doc())
    assert base.fingerprint() == same.fingerprint()
    changed = _doc()
    changed["columns"][0]["enabled"] = False
    assert R.RuleSet(changed).fingerprint() != base.fingerprint()


def test_rule_error_is_readable_for_bad_input():
    with pytest.raises(R.RuleError):
        R.from_json("{not json")
    with pytest.raises(R.RuleError):
        R.parse({"columns": []})

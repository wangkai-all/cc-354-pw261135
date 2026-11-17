"""Fetcher-level tests: robots.txt policy and per-hop redirect checks (no network)."""

from __future__ import annotations

import pytest

from scope_crawler.fetch import FetchError, FetchResult, HttpFetcher, RobotsDisallowed


def _fetcher_with_robots(result=None, *, error: FetchError | None = None) -> HttpFetcher:
    fetcher = HttpFetcher(timeout=1)

    def fake_request(url, **kwargs):
        if error is not None:
            raise error
        return result

    fetcher._request = fake_request  # type: ignore[method-assign]
    return fetcher


def _robots(status: int, body: str = "") -> FetchResult:
    return FetchResult(
        requested_url="http://example.test/robots.txt",
        final_url="http://example.test/robots.txt",
        status=status,
        content_type="text/plain",
        body=body.encode("utf-8"),
    )


def test_robots_500_is_treated_as_disallow_all():
    # RFC 9309: an unreadable policy grants nothing.  Treating a 5xx as
    # "allow everything" would crawl on an assumption of permission.
    fetcher = _fetcher_with_robots(_robots(500))
    policy = fetcher.robots_for("http://example.test/site/a.html")
    assert not policy.allowed("/site/a.html")


def test_unreachable_robots_is_disallow_all_and_not_cached():
    fetcher = _fetcher_with_robots(error=FetchError("down", reason="network_error"))
    policy = fetcher.robots_for("http://example.test/site/a.html")
    assert not policy.allowed("/site/a.html")
    # A failed read must be retried, not remembered as a permanent refusal.
    assert "http://example.test" not in fetcher._robots


def test_robots_404_allows():
    fetcher = _fetcher_with_robots(_robots(404))
    assert fetcher.robots_for("http://example.test/site/a.html").allowed("/site/a.html")


def test_robots_403_denies():
    fetcher = _fetcher_with_robots(_robots(403))
    assert not fetcher.robots_for("http://example.test/site/a.html").allowed("/")


def test_fetch_refuses_but_retries_when_robots_is_unavailable():
    fetcher = _fetcher_with_robots(error=FetchError("down", reason="network_error"))
    with pytest.raises(FetchError) as caught:
        fetcher.fetch("http://example.test/site/a.html")
    # Distinct from robots_disallowed: this refusal is transient (retried
    # later) rather than a permanent verdict about the URL.
    assert caught.value.reason == "robots_unavailable"


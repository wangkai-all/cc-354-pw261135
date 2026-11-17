"""HTTP fetching used by the download worker.

Only plain ``urllib`` is used.  The fetcher is intentionally polite:

* a stable User-Agent identifying the tool,
* short timeouts,
* a maximum body size,
* redirects followed manually so every hop can be boundary-checked,
* robots.txt consulted once per origin and cached,
* no cookies, no credentials, no auth headers.

The crawler is for sites you are allowed to crawl; it never tries to work
around access controls, and any redirect that leaves the site root is refused.
"""

from __future__ import annotations

import hashlib
import http.client
import socket
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlsplit

USER_AGENT = "ScopeCrawler/0.1 (local demo; respects robots.txt)"
MAX_REDIRECTS = 5
MAX_BODY_BYTES = 5 * 1024 * 1024

PAGE_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "text/plain",
)
ASSET_TYPES = (
    "text/css",
    "application/javascript",
    "text/javascript",
    "application/json",
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/svg+xml",
    "application/pdf",
)


class FetchError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "fetch_error", status: int | None = None):
        super().__init__(message)
        self.reason = reason
        self.status = status


class RobotsDisallowed(FetchError):
    def __init__(self, url: str):
        super().__init__(f"robots.txt disallows {url}", reason="robots_disallowed", status=None)


@dataclass
class FetchResult:
    requested_url: str
    final_url: str
    status: int
    content_type: str
    body: bytes
    sha256: str = ""
    redirects: list[str] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    fetched_at: str = ""
    requested_at: str = ""

    def __post_init__(self) -> None:
        if not self.sha256:
            self.sha256 = hashlib.sha256(self.body).hexdigest()
        if not self.fetched_at:
            self.fetched_at = (
                datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        if not self.requested_at:
            self.requested_at = self.fetched_at

    @property
    def mime(self) -> str:
        return self.content_type.split(";", 1)[0].strip().lower()

    @property
    def is_page(self) -> bool:
        return self.mime in PAGE_TYPES

    @property
    def is_asset(self) -> bool:
        return self.mime in ASSET_TYPES

    @property
    def is_restricted(self) -> bool:
        return self.status in (401, 403, 407, 451)

    def text(self) -> str:
        charset = "utf-8"
        for part in self.content_type.split(";")[1:]:
            key, _, value = part.strip().partition("=")
            if key.lower() == "charset" and value:
                charset = value.strip('"')
                break
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None  # we follow redirects ourselves for boundary checks


@dataclass
class RobotsPolicy:
    disallow: list[str] = field(default_factory=list)
    allow: list[str] = field(default_factory=list)
    fetched: bool = False
    unreachable: bool = False

    @classmethod
    def parse(cls, body: str) -> "RobotsPolicy":
        policy = cls(fetched=True)
        applies = False
        for raw in body.splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            field_name, _, value = line.partition(":")
            field_name = field_name.strip().lower()
            value = value.strip()
            if field_name == "user-agent":
                token = value.lower()
                # Our own group, or the wildcard group only when no specific
                # group is present.  Being conservative, honour both.
                applies = token in ("*", "scopecrawler")
            elif applies and field_name == "disallow" and value:
                policy.disallow.append(value)
            elif applies and field_name == "allow" and value:
                policy.allow.append(value)
        return policy

    def allowed(self, path: str) -> bool:
        """Longest-match robots rule wins; ties favour ``allow``."""
        best: tuple[int, bool] | None = None
        for pattern in self.allow:
            if _robots_match(path, pattern):
                if best is None or len(pattern) >= best[0]:
                    best = (len(pattern), True)
        for pattern in self.disallow:
            if _robots_match(path, pattern):
                if best is None or len(pattern) > best[0]:
                    best = (len(pattern), False)
        return True if best is None else best[1]


def _robots_match(path: str, pattern: str) -> bool:
    if not pattern:
        return False
    if pattern.endswith("$") and "*" not in pattern:
        return path == pattern[:-1]
    if "*" not in pattern:
        return path.startswith(pattern)
    import fnmatch

    return fnmatch.fnmatchcase(path, pattern.replace("$", ""))


class HttpFetcher:
    def __init__(
        self,
        *,
        timeout: float = 5.0,
        max_body: int = MAX_BODY_BYTES,
        user_agent: str = USER_AGENT,
    ):
        self.timeout = timeout
        self.max_body = max_body
        self.user_agent = user_agent
        self._robots: dict[str, RobotsPolicy] = {}
        self._build_opener()

    def _build_opener(self) -> None:
        handlers: list[urllib.request.BaseHandler] = [
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
        ]
        if hasattr(ssl, "create_default_context"):
            context = ssl.create_default_context()
            handlers.append(urllib.request.HTTPSHandler(context=context))
        self._urllib_opener = urllib.request.build_opener(*handlers)

    # -- robots --------------------------------------------------------
    def robots_for(self, url: str) -> RobotsPolicy:
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin in self._robots:
            return self._robots[origin]
        robots_url = f"{origin}/robots.txt"
        try:
            def same_origin(candidate: str) -> None:
                parts_c = urlsplit(candidate)
                if f"{parts_c.scheme}://{parts_c.netloc}" != origin:
                    raise FetchError(
                        f"robots.txt redirect left its origin: {candidate}",
                        reason="bad_redirect",
                    )

            result = self._request(
                robots_url, max_body=512 * 1024, is_robots=True, guard=same_origin
            )
            if result.status == 200 and (result.is_page or result.mime == "text/plain"):
                policy = RobotsPolicy.parse(result.text())
            elif result.status in (401, 403):
                policy = RobotsPolicy(disallow=["/"], fetched=True)
            elif result.status >= 500:
                # RFC 9309: a server error means the policy is unknown; be
                # conservative and treat the origin as disallowed rather than
                # assuming permission that was never granted.
                policy = RobotsPolicy(disallow=["/"], fetched=True, unreachable=True)
            else:
                policy = RobotsPolicy(fetched=True)
        except FetchError:
            # Same rule for an unreachable robots.txt: never crawl on an
            # assumption of permission when the access policy could not be read.
            policy = RobotsPolicy(disallow=["/"], fetched=True, unreachable=True)
        if not policy.unreachable:
            # A failed read is not cached: the next attempt may succeed.
            self._robots[origin] = policy
        return policy

    # -- public API ----------------------------------------------------
    def fetch(
        self,
        url: str,
        *,
        check_robots: bool = True,
        guard: Callable[[str], None] | None = None,
    ) -> FetchResult:
        """Fetch *url*, following redirects only within the caller's boundary.

        ``guard`` is consulted for **every** hop before the request is made; it
        is how the crawler prevents a redirect from ever touching a URL outside
        the site root.  robots.txt is likewise re-checked per hop, so a redirect
        cannot smuggle a request past a disallow rule.
        """
        if check_robots:
            policy = self.robots_for(url)
            if policy.unreachable:
                # We could not read the policy.  Refuse for now, but as a
                # *transient* failure: the next attempt re-reads robots.txt
                # instead of treating the URL as permanently forbidden.
                raise FetchError(
                    f"robots.txt unavailable for {url}", reason="robots_unavailable"
                )
            if not policy.allowed(urlsplit(url).path or "/"):
                raise RobotsDisallowed(url)
        result = self._request(url, check_robots=check_robots, guard=guard)
        if result.is_restricted:
            raise FetchError(
                f"HTTP {result.status} for {url}", reason="access_restricted", status=result.status
            )
        if result.status >= 400:
            raise FetchError(f"HTTP {result.status} for {url}", reason="http_error", status=result.status)
        if not (result.is_page or result.is_asset):
            raise FetchError(
                f"unsupported content type {result.mime!r}", reason="unsupported_content",
                status=result.status,
            )
        if len(result.body) > self.max_body:
            raise FetchError("response too large", reason="too_large", status=result.status)
        return result

    def _request(
        self,
        url: str,
        *,
        max_body: int | None = None,
        is_robots: bool = False,
        check_robots: bool = False,
        guard: Callable[[str], None] | None = None,
    ) -> FetchResult:
        limit = max_body or self.max_body
        current = url
        redirects: list[str] = []
        from datetime import datetime, timezone

        requested_at = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        first = True
        for _ in range(MAX_REDIRECTS + 1):
            parts = urlsplit(current)
            if parts.scheme not in ("http", "https"):
                raise FetchError(f"unsupported scheme {parts.scheme!r}", reason="bad_scheme")
            if not first and guard is not None:
                # A redirect target is a *new* request.  The caller's boundary
                # guard must clear it before anything else: the very next step
                # is a robots.txt read (itself a request), so even that must
                # wait until the hop is known to be in bounds.
                guard(current)
            if not first:
                # Every redirect hop is a fresh request and must clear
                # robots.txt before it is sent.
                if check_robots:
                    policy = self.robots_for(current)
                    if policy.unreachable:
                        raise FetchError(
                            f"robots.txt unavailable for {current}",
                            reason="robots_unavailable",
                        )
                    if not policy.allowed(parts.path or "/"):
                        raise RobotsDisallowed(current)
            first = False
            request = urllib.request.Request(
                current,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
                },
                method="GET",
            )
            try:
                response = self._urllib_opener.open(request, timeout=self.timeout)
            except urllib.error.HTTPError as exc:
                status = exc.code
                body = b""
                try:
                    body = exc.read(limit + 1)
                except Exception:  # pragma: no cover - body already consumed
                    body = b""
                if status in (301, 302, 303, 307, 308):
                    location = exc.headers.get("Location", "")
                    if not location:
                        raise FetchError("redirect without Location", reason="bad_redirect") from exc
                    from urllib.parse import urljoin

                    current = urljoin(current, location)
                    redirects.append(current)
                    continue
                return FetchResult(
                    requested_url=url,
                    final_url=current,
                    status=status,
                    content_type=exc.headers.get("Content-Type", "") if exc.headers else "",
                    body=body,
                    redirects=redirects,
                    headers=dict(exc.headers.items()) if exc.headers else {},
                    requested_at=requested_at,
                )
            except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, http.client.HTTPException) as exc:
                raise FetchError(f"network error for {current}: {exc}", reason="network_error") from exc
            with response:
                status = response.status
                content_type = response.headers.get("Content-Type", "")
                if status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location", "")
                    if not location:
                        raise FetchError("redirect without Location", reason="bad_redirect")
                    from urllib.parse import urljoin

                    current = urljoin(current, location)
                    redirects.append(current)
                    continue
                body = response.read(limit + 1)
                if len(body) > limit:
                    raise FetchError("response too large", reason="too_large", status=status)
                return FetchResult(
                    requested_url=url,
                    final_url=current,
                    status=status,
                    content_type=content_type,
                    body=body,
                    redirects=redirects,
                    headers=dict(response.headers.items()),
                    requested_at=requested_at,
                )
        raise FetchError(f"too many redirects for {url}", reason="too_many_redirects")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

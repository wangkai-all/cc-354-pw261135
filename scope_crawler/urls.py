"""URL normalization and site-boundary checks.

A crawl target is an HTTP(S) origin plus an optional path prefix (the "site
root").  A URL is *inside* the boundary when:

* its scheme is http/https,
* its host and effective port equal the root's host and port,
* its decoded path equals the root path or starts with root path + "/",
* and (when ``follow_query`` is false) its query is empty.

Normalization is deliberately conservative: it never rewrites the path in a
way that changes meaning, refuses dot segments that escape the root, and
rejects percent-encoded slashes (which some servers treat as separators).
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit

DEFAULT_PORTS = {"http": 80, "https": 443}

# Reasons returned by :meth:`SiteBoundary.classify`.
OK = "ok"
EMPTY = "empty"
BAD_SCHEME = "bad_scheme"
OUTSIDE_ORIGIN = "outside_origin"
OUTSIDE_PREFIX = "outside_prefix"
ESCAPING_PATH = "escaping_path"
USERINFO = "userinfo"
BAD_PORT = "bad_port"
QUERY_NOT_FOLLOWED = "query_not_followed"
BAD_CHARS = "bad_chars"

OUTSIDE_REASONS = frozenset(
    {BAD_SCHEME, OUTSIDE_ORIGIN, OUTSIDE_PREFIX, ESCAPING_PATH, USERINFO, BAD_PORT, QUERY_NOT_FOLLOWED}
)


class BoundaryError(ValueError):
    """Raised when a configured site root or a path is not usable."""


def clean_path(path: str) -> str:
    """Normalize a URL path: drop empty/`.` segments, resolve `..`, decode.

    A trailing slash is preserved because it is semantically significant for
    relative link resolution ("/section/" vs "/section").

    Raises :class:`BoundaryError` when `..` escapes above the root or when a
    segment decodes to something containing a separator or control characters.
    """
    raw_path = path or "/"
    trailing = raw_path.endswith("/") or raw_path.endswith("/.") or raw_path.endswith("/..")
    segments: list[str] = []
    for raw in raw_path.split("/"):
        if raw in ("", "."):
            continue
        try:
            decoded = unquote(raw, errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:  # pragma: no cover - defensive
            raise BoundaryError("undecodable path") from exc
        if decoded in ("", "."):
            continue
        if decoded == "..":
            if not segments:
                raise BoundaryError("path escapes the site root")
            segments.pop()
            continue
        if "/" in decoded or "\\" in decoded:
            raise BoundaryError("encoded path separator")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in decoded):
            raise BoundaryError("control character in path")
        segments.append(decoded)
    cleaned = "/" + "/".join(segments)
    if trailing and cleaned != "/":
        cleaned += "/"
    return cleaned


def _quote_path(path: str) -> str:
    return quote(path or "/", safe="/:@!$&'()*+,;=-._~")


def _canonical_query(query: str) -> str:
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    return urlencode(sorted(pairs), doseq=True)


@dataclass(frozen=True)
class SiteBoundary:
    scheme: str
    host: str
    port: int
    root_path: str
    follow_query: bool = False

    @classmethod
    def parse(cls, root: str, *, follow_query: bool = False) -> "SiteBoundary":
        parts = urlsplit((root or "").strip())
        if parts.scheme not in DEFAULT_PORTS:
            raise BoundaryError(f"unsupported scheme: {parts.scheme or '(none)'}")
        if not parts.hostname:
            raise BoundaryError("site root has no host")
        if parts.query or parts.fragment:
            raise BoundaryError("site root must not carry a query or fragment")
        try:
            port = parts.port or DEFAULT_PORTS[parts.scheme]
        except ValueError as exc:
            raise BoundaryError(f"invalid port: {parts.port}") from exc
        try:
            path = clean_path(parts.path or "/")
        except BoundaryError as exc:
            raise BoundaryError(f"invalid site root path: {exc}") from exc
        return cls(parts.scheme, parts.hostname.lower(), port, path, follow_query)

    def origin(self) -> str:
        netloc = self.host
        if self.port != DEFAULT_PORTS[self.scheme]:
            netloc = f"{self.host}:{self.port}"
        return f"{self.scheme}://{netloc}"

    def classify(self, url: str, *, base: str | None = None) -> tuple[str | None, str, str | None]:
        """Return ``(canonical_url, reason, display_absolute)``.

        ``canonical_url`` is ``None`` whenever the URL is outside the boundary
        or otherwise not fetchable under the current rules.  ``reason`` is one
        of the module-level reason constants.  ``display_absolute`` is the
        absolute form of the input (useful for audits) when one can be built.
        """
        raw = (url or "").strip()
        if not raw:
            return None, EMPTY, None
        joined = urljoin(base, raw) if base else raw
        parts = urlsplit(joined)
        display = joined if parts.scheme in DEFAULT_PORTS else raw
        if parts.scheme not in DEFAULT_PORTS:
            return None, BAD_SCHEME, display
        if (parts.hostname or "").lower() != self.host:
            return None, OUTSIDE_ORIGIN, display
        if parts.username or parts.password:
            return None, USERINFO, display
        try:
            port = parts.port or DEFAULT_PORTS[parts.scheme]
        except ValueError:
            return None, BAD_PORT, display
        if port != self.port or parts.scheme != self.scheme:
            return None, OUTSIDE_ORIGIN, display
        if parts.query and not self.follow_query:
            return None, QUERY_NOT_FOLLOWED, display
        try:
            # clean_path decodes exactly once per segment, so %2e%2e and %2f
            # are both detected rather than silently normalised away.
            path = clean_path(parts.path or "/")
        except BoundaryError:
            return None, ESCAPING_PATH, display
        if not self.contains_path(path):
            return None, OUTSIDE_PREFIX, display
        query = _canonical_query(parts.query) if self.follow_query else ""
        canonical = f"{self.origin()}{_quote_path(path)}"
        if query:
            canonical += f"?{query}"
        return canonical, OK, display

    def normalize(self, url: str, *, base: str | None = None) -> str | None:
        return self.classify(url, base=base)[0]

    def contains_path(self, path: str) -> bool:
        """Segment-aware prefix test.

        A root written as ``/site/`` deliberately does *not* match ``/site``:
        the two are distinct resources and the latter usually redirects.  This
        keeps boundary decisions predictable.
        """
        root = self.root_path
        if root == "/":
            return True
        if root.endswith("/"):
            return path == root or path.startswith(root)
        return path == root or path.startswith(root + "/")

    def url_for_path(self, path: str) -> str:
        canonical, reason, _ = self.classify(f"{self.origin()}{_quote_path(path)}")
        if canonical is None:
            raise BoundaryError(f"path {path!r} is outside the site root ({reason})")
        return canonical

    def relative_path(self, url: str) -> str | None:
        """Path below the site root, used to mirror files on disk."""
        canonical = self.normalize(url)
        if canonical is None:
            return None
        path = urlsplit(canonical).path
        rel = path[len(self.root_path) :].lstrip("/")
        return rel or "index.html"

    def __str__(self) -> str:
        return self.origin() + self.root_path

"""Rule documents: which sub-columns are in scope, and how links may travel.

A rule document is a plain JSON object.  Shape (all keys optional unless noted):

```
{
  "site_root": "http://127.0.0.1:8000/site/",   # required, crawl boundary
  "follow_query": false,                         # treat ?a=b as distinct pages
  "max_cross_column_hops": 2,                    # budget per discovery branch
  "columns": [
    {
      "id": "news",                              # required, stable identifier
      "title": "新闻",
      "path": "/site/news/",                     # required, inside site_root
      "aliases": ["/site/latest/"],              # alternate spellings
      "enabled": true,
      "allow_refs_to": ["docs"],                 # permitted cross-column targets
      "max_ref_depth": 1                         # how deep inside this column refs may start
    }
  ],
  "exclude_patterns": ["/site/private/*"]
}
```

Semantics the rest of the system relies on:

* Assignment is by the *longest* matching column path/alias prefix; ties are
  broken by document order.  This makes nested sub-columns predictable.
* Every URL under the site root must belong to a column, otherwise it is out
  of scope with reason ``no_column``.
* Aliases are alternate spellings of a column's own path.  They canonicalise
  to the column's path so one resource is stored once.  To keep that mapping
  stable (and to make alias recursion impossible), an alias may not overlap a
  column path, its own column path, or another alias.  The canonical form is
  re-evaluated to a fixed point at runtime and a cycle is reported, never
  followed.
* A link from column A to column B is followed only when B is enabled and
  listed in A's ``allow_refs_to``.  Discovery carries a cross-column hop
  counter and a per-column depth; ``max_cross_column_hops`` and
  ``max_ref_depth`` bound A→B→A→B... so references cannot recurse without
  bound, and never leave the site root.
* ``exclude_patterns`` win over column membership and are checked against both
  the requested path and the canonical path.
"""

from __future__ import annotations

import copy
import fnmatch
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urlsplit

from .urls import BoundaryError, SiteBoundary, clean_path

# Evaluation reasons -------------------------------------------------
OK = "ok"
NO_COLUMN = "no_column"
COLUMN_DISABLED = "column_disabled"
EXCLUDED = "excluded"
ALIAS_ESCAPED = "alias_escaped"
ALIAS_CYCLE = "alias_cycle"
INVALID = "invalid"

DENIAL_REASONS = frozenset(
    {
        NO_COLUMN,
        COLUMN_DISABLED,
        EXCLUDED,
        ALIAS_ESCAPED,
        ALIAS_CYCLE,
        INVALID,
        "cross_column_denied",
        "cross_column_budget",
        "ref_depth_exceeded",
        "source_out_of_scope",
        "redirect_out_of_scope",
        "access_restricted",
        "robots_disallowed",
        "http_error",
        "unsupported_content",
    }
)

DEFAULT_MAX_CROSS_COLUMN_HOPS = 2
DEFAULT_MAX_REF_DEPTH = 1
MAX_CANONICAL_STEPS = 4


class RuleError(ValueError):
    """A rule document is malformed or internally inconsistent."""


@dataclass(frozen=True)
class Column:
    id: str
    title: str
    path: str
    aliases: tuple[str, ...] = ()
    enabled: bool = True
    allow_refs_to: tuple[str, ...] = ()
    max_ref_depth: int = DEFAULT_MAX_REF_DEPTH

    def prefixes(self) -> tuple[str, ...]:
        return (self.path, *self.aliases)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "path": self.path,
            "aliases": list(self.aliases),
            "enabled": self.enabled,
            "allow_refs_to": list(self.allow_refs_to),
            "max_ref_depth": self.max_ref_depth,
        }


@dataclass(frozen=True)
class Decision:
    """Result of applying a rule set to one URL."""

    canonical_url: str | None
    fetch_url: str | None
    in_scope: bool
    reason: str
    column_id: str | None = None
    matched_prefix: str | None = None
    via_alias: bool = False

    @property
    def outside_boundary(self) -> bool:
        return self.canonical_url is None

    def as_dict(self) -> dict[str, Any]:
        return {
            "canonical_url": self.canonical_url,
            "fetch_url": self.fetch_url,
            "in_scope": self.in_scope,
            "reason": self.reason,
            "column_id": self.column_id,
            "matched_prefix": self.matched_prefix,
            "via_alias": self.via_alias,
        }


def _norm_prefix(value: Any, boundary: SiteBoundary, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuleError(f"{where}: path must be a non-empty string")
    probe = f"{boundary.origin()}{value if value.startswith('/') else '/' + value}"
    canonical, reason, _ = boundary.classify(probe)
    if canonical is None or reason != "ok":
        raise RuleError(f"{where}: path {value!r} is not inside the site root ({reason})")
    path = urlsplit(canonical).path
    if path != "/" and not path.endswith("/"):
        path += "/"
    return path


def _overlaps(a: str, b: str) -> bool:
    """True when two directory prefixes are equal or one contains the other."""
    ar, br = a.rstrip("/"), b.rstrip("/")
    return ar == br or ar.startswith(br + "/") or br.startswith(ar + "/")


def _prefix_match(path: str, prefix: str) -> str | None:
    """Return the remainder if *path* is at or below *prefix*, else ``None``."""
    if path == prefix or path == prefix.rstrip("/"):
        return ""
    if path.startswith(prefix):
        return path[len(prefix) :]
    return None


def validate_document(doc: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a rule document, returning a fresh dict.

    Raises :class:`RuleError` with a human-readable message.  The returned
    document is the canonical form used for storage, diffing and fingerprints.
    """
    if not isinstance(doc, dict):
        raise RuleError("rule document must be an object")
    root = doc.get("site_root")
    if not isinstance(root, str) or not root.strip():
        raise RuleError("site_root is required")
    follow_query = bool(doc.get("follow_query", False))
    try:
        boundary = SiteBoundary.parse(root, follow_query=follow_query)
    except BoundaryError as exc:
        raise RuleError(f"site_root: {exc}") from exc

    hops = doc.get("max_cross_column_hops", DEFAULT_MAX_CROSS_COLUMN_HOPS)
    if not isinstance(hops, int) or isinstance(hops, bool) or hops < 0:
        raise RuleError("max_cross_column_hops must be a non-negative integer")

    raw_columns = doc.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise RuleError("columns must be a non-empty list")

    columns: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_columns):
        where = f"columns[{index}]"
        if not isinstance(raw, dict):
            raise RuleError(f"{where}: must be an object")
        column_id = raw.get("id")
        if not isinstance(column_id, str) or not column_id.strip():
            raise RuleError(f"{where}: id is required")
        column_id = column_id.strip()
        if column_id in seen_ids:
            raise RuleError(f"{where}: duplicate column id {column_id!r}")
        seen_ids.add(column_id)
        title = raw.get("title") or column_id
        if not isinstance(title, str):
            raise RuleError(f"{where}.title must be a string")
        path = _norm_prefix(raw.get("path"), boundary, f"{where}.path")
        aliases_raw = raw.get("aliases") or []
        if not isinstance(aliases_raw, list):
            raise RuleError(f"{where}.aliases must be a list")
        aliases: list[str] = []
        for a_index, alias in enumerate(aliases_raw):
            norm = _norm_prefix(alias, boundary, f"{where}.aliases[{a_index}]")
            if norm in aliases:
                raise RuleError(f"{where}.aliases[{a_index}]: duplicate alias")
            aliases.append(norm)
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise RuleError(f"{where}.enabled must be a boolean")
        refs_raw = raw.get("allow_refs_to") or []
        if not isinstance(refs_raw, list) or not all(isinstance(r, str) for r in refs_raw):
            raise RuleError(f"{where}.allow_refs_to must be a list of column ids")
        max_ref = raw.get("max_ref_depth", DEFAULT_MAX_REF_DEPTH)
        if not isinstance(max_ref, int) or isinstance(max_ref, bool) or max_ref < 0:
            raise RuleError(f"{where}.max_ref_depth must be a non-negative integer")
        columns.append(
            {
                "id": column_id,
                "title": title,
                "path": path,
                "aliases": aliases,
                "enabled": enabled,
                "allow_refs_to": list(dict.fromkeys(refs_raw)),
                "max_ref_depth": max_ref,
            }
        )

    # Alias stability: an alias is a spelling of one column, never a doorway
    # into another.  It may not equal/contain/sit under a column path, and
    # aliases may not overlap each other; otherwise canonical rewriting could
    # oscillate or silently merge two columns.
    all_aliases: list[tuple[str, str]] = []
    for column in columns:
        for alias in column["aliases"]:
            if alias == "/":
                raise RuleError(f"column {column['id']!r}: alias cannot be the site root")
            for other in columns:
                if _overlaps(alias, other["path"]):
                    raise RuleError(
                        f"column {column['id']!r}: alias {alias!r} overlaps path "
                        f"{other['path']!r} of column {other['id']!r}; aliases must be "
                        "disjoint from every column path"
                    )
            for previous, owner in all_aliases:
                if _overlaps(alias, previous):
                    raise RuleError(
                        f"column {column['id']!r}: alias {alias!r} overlaps alias "
                        f"{previous!r} of column {owner!r}"
                    )
            all_aliases.append((alias, column["id"]))
    # Equal column paths are also ambiguous.
    seen_paths: dict[str, str] = {}
    for column in columns:
        if column["path"] in seen_paths:
            raise RuleError(
                f"columns {seen_paths[column['path']]!r} and {column['id']!r} share path "
                f"{column['path']!r}"
            )
        seen_paths[column["path"]] = column["id"]

    for column in columns:
        for target in column["allow_refs_to"]:
            if target not in seen_ids:
                raise RuleError(
                    f"column {column['id']!r} allows refs to unknown column {target!r}"
                )
            if target == column["id"]:
                raise RuleError(f"column {column['id']!r} cannot reference itself")

    excludes_raw = doc.get("exclude_patterns") or []
    if not isinstance(excludes_raw, list) or not all(isinstance(p, str) for p in excludes_raw):
        raise RuleError("exclude_patterns must be a list of strings")
    # An alias is another spelling of the same page, not a separate resource.
    # A pattern written against an alias must therefore exclude the page under
    # its canonical path too; otherwise the same page would be excluded or
    # published depending on which spelling discovered it.  Rewrite the
    # pattern onto the column path once, at validation time.
    alias_targets = {
        alias: column["path"] for column in columns for alias in column["aliases"]
    }
    excludes = [
        _canonical_pattern(_normalize_pattern(p, boundary, i), alias_targets)
        for i, p in enumerate(excludes_raw)
    ]

    normalized = {
        "site_root": str(boundary),
        "follow_query": follow_query,
        "max_cross_column_hops": hops,
        "columns": columns,
        "exclude_patterns": excludes,
    }
    return normalized


def _normalize_pattern(pattern: str, boundary: SiteBoundary, index: int) -> str:
    if not pattern.startswith("/"):
        raise RuleError(f"exclude_patterns[{index}]: patterns must start with '/'")
    if any(ch in pattern for ch in ("\x00", "\\")):
        raise RuleError(f"exclude_patterns[{index}]: unsupported characters")
    head = pattern.split("*", 1)[0] or "/"
    try:
        clean = clean_path(head)
    except BoundaryError as exc:
        raise RuleError(f"exclude_patterns[{index}]: {exc}") from exc
    if not boundary.contains_path(clean):
        raise RuleError(f"exclude_patterns[{index}]: pattern is outside the site root")
    return pattern


def _canonical_pattern(pattern: str, alias_targets: dict[str, str]) -> str:
    """Rewrite a pattern that starts at an alias onto the column path.

    Prefix matching is segment-aware, exactly like ``_prefix_match``: an alias
    ``/site/latest/`` covers ``/site/latest/x`` and ``/site/latest/``, not a
    lookalike such as ``/site/latestx/``.
    """
    head = pattern.split("*", 1)[0]
    for alias, path in alias_targets.items():
        if head == alias or head.startswith(alias):
            return path + pattern[len(alias) :]
        # A pattern naming the alias directory without its trailing slash is
        # the same directory; only an exact match counts, so a lookalike such
        # as /site/latestx is left alone.
        if head == alias.rstrip("/"):
            return path.rstrip("/") + pattern[len(alias.rstrip("/")) :]
    return pattern


@dataclass
class RuleSet:
    doc: dict[str, Any]
    version: int | None = None
    boundary: SiteBoundary = field(init=False)
    columns: dict[str, Column] = field(init=False)
    ordered: list[Column] = field(init=False)
    excludes: tuple[str, ...] = field(init=False)
    max_cross_column_hops: int = field(init=False)

    def __post_init__(self) -> None:
        self.doc = validate_document(self.doc)
        self.boundary = SiteBoundary.parse(
            self.doc["site_root"], follow_query=self.doc["follow_query"]
        )
        self.ordered = [Column(**raw) for raw in self.doc["columns"]]
        self.columns = {c.id: c for c in self.ordered}
        self.excludes = tuple(self.doc["exclude_patterns"])
        self.max_cross_column_hops = self.doc["max_cross_column_hops"]

    # -- persistence helpers ------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.doc)

    def fingerprint(self) -> str:
        canonical = json.dumps(self.doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def with_version(self, version: int) -> "RuleSet":
        clone = RuleSet(self.to_dict())
        clone.version = version
        return clone

    # -- evaluation ----------------------------------------------------
    def evaluate(self, url: str, *, base: str | None = None) -> Decision:
        """Apply the rules to one URL, canonicalising aliases to a fixed point."""
        first = self._evaluate_once(url, base=base)
        if first.canonical_url is None:
            return first
        fetch_url = first.fetch_url
        via_alias = first.via_alias
        current = first
        seen = {first.canonical_url}
        for _ in range(MAX_CANONICAL_STEPS):
            nxt = self._evaluate_once(current.canonical_url)
            if nxt.canonical_url is None:
                # Canonical target itself left the boundary: refuse rather than
                # pretending the alias is fine.
                return Decision(
                    None, fetch_url, False, nxt.reason, nxt.column_id, nxt.matched_prefix, via_alias
                )
            if nxt.canonical_url == current.canonical_url:
                return Decision(
                    nxt.canonical_url,
                    fetch_url,
                    nxt.in_scope,
                    nxt.reason,
                    nxt.column_id,
                    nxt.matched_prefix,
                    via_alias or nxt.via_alias,
                )
            if nxt.canonical_url in seen:
                return Decision(None, fetch_url, False, ALIAS_CYCLE, None, None, via_alias)
            seen.add(nxt.canonical_url)
            via_alias = via_alias or nxt.via_alias
            current = nxt
        return Decision(None, fetch_url, False, ALIAS_CYCLE, None, None, via_alias)

    def _evaluate_once(self, url: str, *, base: str | None = None) -> Decision:
        canonical, reason, _display = self.boundary.classify(url, base=base)
        if canonical is None:
            return Decision(None, None, False, reason or INVALID)
        fetch_url = canonical
        encoded_path = urlsplit(canonical).path
        # ``classify`` returns a percent-encoded path; column paths, aliases and
        # exclude patterns are stored decoded.  Rewriting must therefore work on
        # the decoded form — quoting the encoded form would double-encode
        # non-ASCII/space characters into a URL that does not exist.
        try:
            path = clean_path(encoded_path)
        except BoundaryError:  # pragma: no cover - classify already accepted it
            path = encoded_path

        if self._is_excluded(path) or self._is_excluded(encoded_path):
            return Decision(canonical, fetch_url, False, EXCLUDED)

        match = self._match_column(path)
        if match is None:
            return Decision(canonical, fetch_url, False, NO_COLUMN)
        column, prefix, via_alias = match
        canonical_path = column.path + path[len(prefix) :]
        if not self.boundary.contains_path(canonical_path):
            return Decision(
                canonical, fetch_url, False, ALIAS_ESCAPED, column.id, prefix, via_alias
            )
        if via_alias or canonical_path != path:
            canonical = self.boundary.url_for_path(canonical_path)
            query = urlsplit(fetch_url).query
            if query:
                canonical += f"?{query}"
        if self._is_excluded(clean_path(urlsplit(canonical).path)):
            return Decision(canonical, fetch_url, False, EXCLUDED, column.id, prefix, via_alias)
        if not column.enabled:
            return Decision(
                canonical, fetch_url, False, COLUMN_DISABLED, column.id, prefix, via_alias
            )
        return Decision(canonical, fetch_url, True, OK, column.id, prefix, via_alias)

    def _is_excluded(self, path: str) -> bool:
        for pattern in self.excludes:
            if fnmatch.fnmatchcase(path, pattern):
                return True
            base = pattern.rstrip("/")
            if "*" not in pattern and (path == base or path.startswith(base + "/")):
                return True
        return False

    def _match_column(self, path: str) -> tuple[Column, str, bool] | None:
        best: tuple[int, int, Column, str, bool] | None = None
        for order, column in enumerate(self.ordered):
            for index, prefix in enumerate(column.prefixes()):
                remainder = _prefix_match(path, prefix)
                if remainder is None:
                    continue
                rank = (-len(prefix), order)
                if best is None or rank < best[:2]:
                    best = (*rank, column, prefix, index > 0)
        if best is None:
            return None
        return best[2], best[3], best[4]

    def column_for(self, canonical_or_url: str) -> Column | None:
        decision = self.evaluate(canonical_or_url)
        if decision.column_id is None:
            return None
        return self.columns.get(decision.column_id)

    # -- cross-column travel ------------------------------------------
    def transition(
        self,
        from_column: str | None,
        to_column: str | None,
        *,
        hops_used: int,
        column_depth: int,
    ) -> tuple[bool, str]:
        """Whether a discovered link may be followed, and why.

        ``hops_used`` counts cross-column transitions already taken on this
        branch.  ``column_depth`` counts links since the branch entered its
        current column.  Same-column links are unrestricted (bounded by the
        crawl depth); cross-column links need an explicit allowance, remaining
        hop budget and a still-open per-column reference depth.
        """
        if to_column is None:
            return False, NO_COLUMN
        target = self.columns.get(to_column)
        if target is None:
            return False, NO_COLUMN
        if not target.enabled:
            return False, COLUMN_DISABLED
        if from_column is None or from_column == to_column:
            return True, OK
        source = self.columns.get(from_column)
        if source is None:
            return False, "cross_column_denied"
        if to_column not in source.allow_refs_to:
            return False, "cross_column_denied"
        if hops_used >= self.max_cross_column_hops:
            return False, "cross_column_budget"
        if column_depth > source.max_ref_depth:
            return False, "ref_depth_exceeded"
        return True, OK

    def next_context(
        self, from_column: str | None, to_column: str | None, *, hops_used: int, column_depth: int
    ) -> tuple[int, int]:
        """Hop/depth context for a newly enqueued child of an allowed link."""
        if from_column is not None and from_column != to_column:
            return hops_used + 1, 0
        return hops_used, column_depth + 1

    # -- seeds ---------------------------------------------------------
    def seed_urls(self) -> list[str]:
        return [self.boundary.url_for_path(c.path) for c in self.ordered if c.enabled]

    def enabled_columns(self) -> Iterable[Column]:
        return (c for c in self.ordered if c.enabled)

    # -- diffs ---------------------------------------------------------
    def describe_changes(self, other: "RuleSet") -> dict[str, list[str]]:
        """Structural diff used for summaries and the console preview."""
        added: list[str] = []
        removed: list[str] = []
        changed: list[str] = []
        for column in other.ordered:
            mine = self.columns.get(column.id)
            if mine is None:
                added.append(column.id)
            elif mine != column:
                changed.append(column.id)
        for column in self.ordered:
            if column.id not in other.columns:
                removed.append(column.id)
        return {"added": added, "removed": removed, "changed": changed}


def parse(doc: dict[str, Any], *, version: int | None = None) -> RuleSet:
    rules = RuleSet(doc)
    rules.version = version
    return rules


def from_json(text: str, *, version: int | None = None) -> RuleSet:
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuleError(f"invalid JSON: {exc}") from exc
    return parse(doc, version=version)


def default_document(site_root: str) -> dict[str, Any]:
    """A small, valid starting document (used by the demo and tests)."""
    return {
        "site_root": site_root,
        "follow_query": False,
        "max_cross_column_hops": 2,
        "columns": [
            {
                "id": "news",
                "title": "新闻",
                "path": "/site/news/",
                "aliases": ["/site/latest/"],
                "enabled": True,
                "allow_refs_to": ["docs"],
                "max_ref_depth": 1,
            },
            {
                "id": "docs",
                "title": "文档",
                "path": "/site/docs/",
                "aliases": [],
                "enabled": True,
                "allow_refs_to": [],
                "max_ref_depth": 1,
            },
        ],
        "exclude_patterns": [],
    }

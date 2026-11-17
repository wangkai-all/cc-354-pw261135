"""Command line entry points.

``python -m scope_crawler.cli serve``   start the demo site + console
``python -m scope_crawler.cli crawl``   run workers against a rules file
``python -m scope_crawler.cli preview`` preview a rules file without applying it
``python -m scope_crawler.cli export``  write the published catalog as JSON
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .db import ENTRY_PUBLISHED, ENTRY_WITHHELD, Store
from .engine import Crawler
from .fetch import HttpFetcher
from .rules import RuleError, from_json


def _open(args: argparse.Namespace) -> Store:
    store = Store(os.path.join(args.data, "crawl.db"))
    store.init()
    return store


def _crawler(args: argparse.Namespace, store: Store) -> Crawler:
    return Crawler(
        store,
        os.path.join(args.data, "storage"),
        fetcher=HttpFetcher(timeout=float(args.timeout)),
    )


def cmd_serve(args: argparse.Namespace) -> int:
    from .app import Console, create_app

    console = Console(args.data, port=args.port)
    app = create_app(console)
    print(f"demo site: {console.site.base}/site/")
    print(f"console:   http://127.0.0.1:{args.port}/")
    app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)
    return 0


def cmd_crawl(args: argparse.Namespace) -> int:
    store = _open(args)
    crawler = _crawler(args, store)
    with open(args.rules, encoding="utf-8") as handle:
        text = handle.read()
    try:
        rules = from_json(text)
    except RuleError as exc:
        print(f"invalid rules: {exc}", file=sys.stderr)
        return 2
    if store.active_rules_version() is None:
        crawler.apply_rules(rules.to_dict(), note="cli initial")
    elif args.publish:
        crawler.apply_rules(rules.to_dict(), note=args.note or "cli publish")
    processed = crawler.run_until_idle(max_tasks=args.max_tasks)
    print(f"processed {processed} tasks")
    return 0


def cmd_preview(args: argparse.Namespace) -> int:
    store = _open(args)
    crawler = _crawler(args, store)
    with open(args.rules, encoding="utf-8") as handle:
        text = handle.read()
    try:
        rules = from_json(text)
    except RuleError as exc:
        print(f"invalid rules: {exc}", file=sys.stderr)
        return 2
    impact = crawler.preview_ruleset(rules)
    json.dump(impact, sys.stdout, ensure_ascii=False, indent=2)
    print()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    store = _open(args)
    state = args.state
    rows = store.all(
        "SELECT e.state, e.title, e.reason, u.canonical_url, u.column_id,"
        " e.evidence_id, e.updated_at FROM entries e JOIN urls u ON u.id=e.url_id"
        " WHERE e.state=? ORDER BY u.canonical_url",
        (state,),
    )
    payload: list[dict[str, Any]] = [
        {
            "url": row["canonical_url"],
            "column_id": row["column_id"],
            "title": row["title"],
            "state": row["state"],
            "reason": row["reason"],
            "evidence_id": row["evidence_id"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        print(f"wrote {len(payload)} entries to {args.out}")
    else:
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
        print()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scope-crawler", description=__doc__)
    parser.add_argument(
        "--data",
        default=os.environ.get("SCOPE_DATA", "demo-data"),
        help="data directory (default: $SCOPE_DATA or demo-data)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the demo site and web console")
    serve.add_argument("--port", type=int, default=8080)
    serve.set_defaults(func=cmd_serve)

    crawl = sub.add_parser("crawl", help="process the queue using a rules file")
    crawl.add_argument("--rules", required=True)
    crawl.add_argument("--publish", action="store_true", help="apply the rules file first")
    crawl.add_argument("--note", default="")
    crawl.add_argument("--max-tasks", type=int, default=5000)
    crawl.add_argument("--timeout", type=float, default=6.0)
    crawl.set_defaults(func=cmd_crawl)

    preview = sub.add_parser("preview", help="print the impact of a rules file")
    preview.add_argument("--rules", required=True)
    preview.add_argument("--timeout", type=float, default=6.0)
    preview.set_defaults(func=cmd_preview)

    export = sub.add_parser("export", help="write catalog entries as JSON")
    export.add_argument("--state", choices=[ENTRY_PUBLISHED, ENTRY_WITHHELD], default=ENTRY_PUBLISHED)
    export.add_argument("--out")
    export.set_defaults(func=cmd_export)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

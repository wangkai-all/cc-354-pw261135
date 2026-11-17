"""Runnable worker services.

The assignment describes three cooperating services.  They share one SQLite
database and one durable queue, and each service only ever claims its own task
kind, so they can run as separate processes (or threads) safely:

    python -m scope_crawler.worker download --data demo-data
    python -m scope_crawler.worker discover --data demo-data
    python -m scope_crawler.worker ingest   --data demo-data

Because claims are atomic and leases expire, any worker can be killed and
restarted without losing or duplicating work.
"""

from __future__ import annotations

import argparse
import os
import signal
import threading
import time
from typing import Callable

from .db import Store
from .engine import Crawler
from .fetch import HttpFetcher
from .queue import Task

KINDS = ("download", "discover", "ingest")


def _handler(crawler: Crawler, kind: str) -> Callable[[Task], object]:
    return {
        "download": crawler.run_download,
        "discover": crawler.run_discover,
        "ingest": crawler.run_ingest,
    }[kind]


def run_service(
    crawler: Crawler,
    kind: str,
    *,
    stop: threading.Event | None = None,
    idle_sleep: float = 0.25,
) -> None:
    handler = _handler(crawler, kind)
    stop = stop or threading.Event()
    while not stop.is_set():
        task = crawler.queue.claim(kind, crawler.worker_id)
        if task is None:
            time.sleep(idle_sleep)
            continue
        try:
            handler(task)
        except Exception as exc:  # noqa: BLE001 - keep the service alive
            crawler.queue.fail(
                task.id, f"{type(exc).__name__}: {exc}", retry_after=0.5, max_attempts=3
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scope-crawler-worker")
    parser.add_argument("kind", choices=KINDS)
    parser.add_argument("--data", default="demo-data")
    parser.add_argument("--timeout", type=float, default=6.0)
    parser.add_argument("--once", action="store_true", help="drain and exit")
    args = parser.parse_args(argv)

    store = Store(os.path.join(args.data, "crawl.db"))
    store.init()
    crawler = Crawler(
        store,
        os.path.join(args.data, "storage"),
        fetcher=HttpFetcher(timeout=args.timeout),
        worker_id=f"{args.kind}-{os.getpid()}",
    )
    if crawler.active_rules() is None:
        print("no active rules: publish a rule document first", flush=True)
        return 2

    stop = threading.Event()

    def _handle(_signum, _frame) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    if args.once:
        while crawler.run_once() is not None:
            pass
        return 0
    run_service(crawler, args.kind, stop=stop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

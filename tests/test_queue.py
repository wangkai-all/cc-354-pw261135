"""Queue semantics: atomic claims, leases, recovery, park/reopen."""

from __future__ import annotations

import pytest

from scope_crawler.db import Store
from scope_crawler.queue import (
    BLOCKED,
    CANCELLED,
    DONE,
    LEASED,
    PARKED,
    PENDING,
    Queue,
    QueueError,
)


@pytest.fixture()
def queue(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    store.init()
    return Queue(store)


def test_enqueue_is_idempotent_by_dedupe_key(queue):
    first = queue.enqueue("download", "download:1", url_id=1)
    assert first is not None
    assert queue.enqueue("download", "download:1", url_id=1) is None
    assert queue.enqueue("download", "download:2", url_id=2) is not None
    # Dedupe keys are global: the same key cannot exist under two kinds.
    assert queue.enqueue("ingest", "download:1", url_id=1) is None


def test_claim_leases_and_hides_row(queue):
    task_id = queue.enqueue("download", "download:1", url_id=1)
    claimed = queue.claim("download", "w1", lease_seconds=60)
    assert claimed is not None and claimed.id == task_id
    assert queue.claim("download", "w1") is None
    assert queue.state_of(task_id) == LEASED


def test_finish_and_failure_paths(queue):
    task_id = queue.enqueue("download", "download:1", url_id=1)
    queue.claim("download", "w1")
    queue.complete(task_id)
    assert queue.state_of(task_id) == DONE

    task_id = queue.enqueue("download", "download:2", url_id=2)
    queue.claim("download", "w1")
    state = queue.fail(task_id, "boom", retry_after=0)
    assert state == PENDING
    queue.claim("download", "w1")
    state = queue.fail(task_id, "boom again", retry_after=None)
    assert state == BLOCKED


def test_stale_lease_is_recovered(queue, monkeypatch):
    task_id = queue.enqueue("download", "download:1", url_id=1)
    queue.claim("download", "w1", lease_seconds=0)
    recovered = queue.recover_stale("2999-01-01T00:00:00Z")
    assert recovered == 1
    assert queue.state_of(task_id) == PENDING


def test_park_then_reopen_keeps_history(queue):
    task_id = queue.enqueue("download", "download:1", url_id=1)
    queue.park(task_id, "out_of_scope")
    assert queue.state_of(task_id) == PARKED
    queue.reopen("download", "download:1", rules_version=3)
    assert queue.state_of(task_id) == PENDING
    row = queue.by_dedupe("download", "download:1")
    assert row["rules_version"] == 3


def test_reopen_does_not_steal_a_lease(queue):
    task_id = queue.enqueue("download", "download:1", url_id=1)
    queue.claim("download", "w1", lease_seconds=60)
    queue.reopen("download", "download:1", rules_version=9)
    assert queue.state_of(task_id) == LEASED


def test_reopen_repoints_a_task_to_the_surviving_url(queue):
    # An ingest keyed by evidence outlives the URL row that created it: after
    # an identity merge the surviving row owns that evidence.
    task_id = queue.enqueue(
        "ingest", "ingest:7", url_id=2, evidence_id=7, payload={"url_id": 2}
    )
    queue.park(task_id, "out_of_scope")
    queue.reopen(
        "ingest",
        "ingest:7",
        url_id=1,
        evidence_id=7,
        payload={"url_id": 1, "evidence_id": 7},
    )
    row = queue.by_dedupe("ingest", "ingest:7")
    assert row["state"] == PENDING
    assert row["url_id"] == 1
    assert row["evidence_id"] == 7


def test_priority_ordering(queue):
    queue.enqueue("download", "low", priority=100)
    queue.enqueue("download", "high", priority=5)
    first = queue.claim("download", "w1")
    assert first is not None and first.dedupe == "high"


def test_counts_and_listing(queue):
    queue.enqueue("download", "a")
    queue.enqueue("ingest", "b")
    task = queue.claim("download", "w1")
    queue.complete(task.id)
    counts = queue.counts()
    assert counts["download"][DONE] == 1
    assert counts["ingest"][PENDING] == 1
    assert queue.list(kind="ingest")[0]["dedupe"] == "b"

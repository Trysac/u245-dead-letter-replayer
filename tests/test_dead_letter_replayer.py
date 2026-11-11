"""Tests for dead_letter_replayer."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dead_letter_replayer import (
    DeadLetter,
    Ledger,
    ReplayError,
    ReplayFilter,
    ReplayReport,
    replay,
)


def make_msg(
    id: str,
    topic: str,
    error: str = "",
    *,
    headers: dict[str, str] | None = None,
    attempts: int = 1,
    failed_at: datetime | None = None,
) -> DeadLetter:
    return DeadLetter(
        id=id,
        topic=topic,
        payload=b"payload",
        headers=headers or {},
        error=error,
        failed_at=failed_at or datetime.now(timezone.utc),
        attempts=attempts,
    )


class FakeSource:
    """In-memory source for testing."""

    def __init__(self, messages: list[DeadLetter], *, stub_source=None) -> None:
        self.messages = messages
        self.cursor_counter = 0
        self.deleted: list[str] = []
        self.list_calls: list[tuple[str | None, int]] = []
        self.stub_source = stub_source

    def list(self, cursor: str | None, limit: int) -> tuple[list[DeadLetter], str | None]:
        self.list_calls.append((cursor, limit))
        # Simulate paging: if no cursor, start at 0; else parse integer cursor.
        start = int(cursor) if cursor is not None else 0
        if start >= len(self.messages):
            return [], None
        end = min(start + limit, len(self.messages))
        page = self.messages[start:end]
        next_cursor = str(end) if end < len(self.messages) else None
        # Simulate a stalled source if configured
        if self.stub_source == "stall" and next_cursor is not None:
            # Return same cursor as previous; we track by saving last non-None cursor.
            # We'll use a flag to force same cursor on second call.
            pass
        return page, next_cursor

    def delete(self, id: str) -> None:
        self.deleted.append(id)


class FakePublisher:
    """Records publish calls and can optionally raise."""

    def __init__(
        self, raise_for: str | None = None, *, raise_type: type[BaseException] = Exception
    ) -> None:
        self.calls: list[tuple[str, bytes, dict[str, str]]] = []
        self.raise_for = raise_for
        self.raise_type = raise_type

    def publish(self, topic: str, payload: bytes, headers: dict[str, str]) -> None:
        if self.raise_for is not None and (
            topic == self.raise_for or headers.get("x-replay-of") == self.raise_for
        ):
            raise self.raise_type("publish failed")
        self.calls.append((topic, payload, headers))


class FakeClock:
    """Controllable monotonic clock."""

    def __init__(self, start: float = 0.0) -> None:
        self.current = start

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


class FakeSleep:
    """Records sleep durations."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, duration: float) -> None:
        self.calls.append(duration)


class RecordingSource(FakeSource):
    """Source that records delete calls in a shared event log."""

    def __init__(self, messages: list[DeadLetter], events: list[tuple[str, str]]) -> None:
        super().__init__(messages)
        self.events = events

    def delete(self, id: str) -> None:
        self.events.append(("delete", id))
        super().delete(id)


class RecordingPublisher(FakePublisher):
    """Publisher that records publish attempts in a shared event log."""

    def __init__(self, events: list[tuple[str, str]], raise_for: str | None = None) -> None:
        super().__init__(raise_for=raise_for)
        self.events = events

    def publish(self, topic: str, payload: bytes, headers: dict[str, str]) -> None:
        self.events.append(("publish", headers["x-replay-of"]))
        super().publish(topic, payload, headers)


def test_filter_topics_and_error():
    m1 = make_msg("m1", "orders", "TimeoutError: db")
    m2 = make_msg("m2", "orders", "ValueError: bad sku")
    m3 = make_msg("m3", "payments", "TimeoutError: psp")
    f = ReplayFilter(topics=("orders",), error_pattern="^TimeoutError")
    assert f.matches(m1)
    assert not f.matches(m2)
    assert not f.matches(m3)


def test_filter_since_until():
    base = datetime.now(timezone.utc)
    m = make_msg("m", "t", failed_at=base + timedelta(seconds=10))
    f = ReplayFilter(since=base)
    assert f.matches(m)
    f = ReplayFilter(until=base + timedelta(seconds=20))
    assert f.matches(m)
    f = ReplayFilter(since=base + timedelta(seconds=20))
    assert not f.matches(m)
    f = ReplayFilter(until=base)
    assert not f.matches(m)


def test_filter_headers():
    m = make_msg("m", "t", headers={"a": "1", "b": "2"})
    f = ReplayFilter(headers={"a": "1"})
    assert f.matches(m)
    f = ReplayFilter(headers={"a": "2"})
    assert not f.matches(m)
    f = ReplayFilter(headers={"a": "1", "c": "3"})
    assert not f.matches(m)


def test_filter_max_attempts():
    m = make_msg("m", "t", attempts=3)
    f = ReplayFilter(max_attempts=2)
    assert not f.matches(m)
    f = ReplayFilter(max_attempts=3)
    assert f.matches(m)


def test_filter_invalid_pattern():
    with pytest.raises(ReplayError) as exc_info:
        ReplayFilter(error_pattern="(")
    assert exc_info.value.code == "invalid_filter"
    with pytest.raises(ReplayError):
        ReplayFilter(error_pattern="a" * 201)


def test_filter_invalid_headers():
    headers = {f"key{i}": "value" for i in range(21)}
    with pytest.raises(ReplayError):
        ReplayFilter(headers=headers)


def test_basic_replay():
    m1 = make_msg("m1", "orders", "TimeoutError: db")
    m2 = make_msg("m2", "orders", "ValueError: bad sku")
    m3 = make_msg("m3", "payments", "TimeoutError: psp")
    source = FakeSource([m1, m2, m3])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter(topics=("orders",), error_pattern="^TimeoutError")
    report = replay(source, publisher, filter=f, ledger=ledger)
    assert report.scanned == 3
    assert report.matched == 1
    assert report.replayed == 1
    assert report.skipped_already == 0
    assert report.skipped_transform == 0
    assert report.failed == 0
    assert report.would_replay == 0
    assert report.stopped is False
    assert report.limited is False
    assert len(publisher.calls) == 1
    topic, payload, headers = publisher.calls[0]
    assert topic == "orders"
    assert payload == b"payload"
    assert headers["x-replay-of"] == "m1"
    assert headers["x-replay-count"] == "1"


def test_idempotency_with_ledger():
    m1 = make_msg("m1", "orders", "TimeoutError: db")
    source = FakeSource([m1])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    report1 = replay(source, publisher, filter=f, ledger=ledger)
    assert report1.replayed == 1
    # second run
    source2 = FakeSource([m1])
    publisher2 = FakePublisher()
    ledger2 = ledger  # same ledger
    report2 = replay(source2, publisher2, filter=f, ledger=ledger2)
    assert report2.skipped_already == 1
    assert report2.replayed == 0
    assert len(publisher2.calls) == 0
    # force replay
    report3 = replay(source2, publisher2, filter=f, ledger=ledger2, force=True)
    assert report3.replayed == 1
    assert len(publisher2.calls) == 1
    headers = publisher2.calls[0][2]
    assert headers["x-replay-count"] == "2"


def test_dry_run():
    m1 = make_msg("m1", "orders", "TimeoutError: db")
    source = FakeSource([m1])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    report = replay(source, publisher, filter=f, ledger=ledger, dry_run=True)
    assert report.would_replay == 1
    assert report.replayed == 0
    assert len(publisher.calls) == 0
    assert len(ledger._counts) == 0  # private, but okay for test


def test_rate_limiting_with_fake_clock_and_sleep():
    messages = [make_msg(f"m{i}", "t", f"error {i}") for i in range(3)]
    source = FakeSource(messages)
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    clock = FakeClock(0.0)
    sleeper = FakeSleep()
    report = replay(
        source,
        publisher,
        filter=f,
        ledger=ledger,
        rate_per_second=2,
        clock=clock,
        sleep=sleeper,
    )
    assert report.replayed == 3
    # Expected publishes at times 0, 0.5, 1.0
    # The clock is used before and after publish; we check sleep calls.
    assert sleeper.calls == [0.5, 0.5]


def test_publisher_failure_continue():
    m1 = make_msg("m1", "orders", "ok")
    m2 = make_msg("m2", "orders", "fail")
    m3 = make_msg("m3", "orders", "ok")
    events: list[tuple[str, str]] = []
    source = RecordingSource([m1, m2, m3], events)
    publisher = RecordingPublisher(events, raise_for="m2")
    ledger = Ledger()
    f = ReplayFilter(topics=("orders",))
    report = replay(source, publisher, filter=f, ledger=ledger, delete_after=True)
    assert report.replayed == 2
    assert report.failed == 1
    assert report.stopped is False
    assert report.limited is False
    # m2 should not be in ledger
    assert ledger.count("m2") == 0
    # m2 was never deleted; successful ids are deleted right after their publish
    assert source.deleted == ["m1", "m3"]
    assert events == [
        ("publish", "m1"),
        ("delete", "m1"),
        ("publish", "m2"),
        ("publish", "m3"),
        ("delete", "m3"),
    ]
    # stop_on_error variant: later messages are not attempted
    events2: list[tuple[str, str]] = []
    source2 = RecordingSource([m1, m2, m3], events2)
    publisher2 = RecordingPublisher(events2, raise_for="m2")
    ledger2 = Ledger()
    report2 = replay(
        source2,
        publisher2,
        filter=f,
        ledger=ledger2,
        delete_after=True,
        stop_on_error=True,
    )
    assert report2.replayed == 1
    assert report2.failed == 1
    assert report2.stopped is True
    assert report2.limited is False
    assert len(publisher2.calls) == 1
    assert ledger2.count("m3") == 0
    assert source2.deleted == ["m1"]


def test_delete_after_and_max_messages():
    m1 = make_msg("m1", "t", "ok")
    m2 = make_msg("m2", "t", "ok")
    m3 = make_msg("m3", "t", "ok")
    events: list[tuple[str, str]] = []
    source = RecordingSource([m1, m2, m3], events)
    publisher = RecordingPublisher(events)
    ledger = Ledger()
    f = ReplayFilter()
    report = replay(
        source,
        publisher,
        filter=f,
        ledger=ledger,
        delete_after=True,
        max_messages=2,
    )
    assert report.replayed == 2
    assert report.limited is True
    assert source.deleted == ["m1", "m2"]  # deleted after each success, in order
    assert events == [
        ("publish", "m1"),
        ("delete", "m1"),
        ("publish", "m2"),
        ("delete", "m2"),
    ]
    # third message not processed
    assert len(publisher.calls) == 2


def test_empty_source():
    source = FakeSource([])
    publisher = FakePublisher()
    report = replay(source, publisher, filter=ReplayFilter(), ledger=Ledger())
    assert report == ReplayReport(0, 0, 0, 0, 0, 0, 0, False, False)
    assert source.list_calls == [(None, 100)]


def test_transform_cannot_change_id():
    m = make_msg("m1", "t", "ok")
    source = FakeSource([m])
    publisher = FakePublisher()
    ledger = Ledger()

    def transform(msg: DeadLetter) -> DeadLetter:
        return DeadLetter(
            id="other",
            topic=msg.topic,
            payload=msg.payload,
            headers=msg.headers,
            error=msg.error,
            failed_at=msg.failed_at,
            attempts=msg.attempts,
        )

    report = replay(
        source,
        publisher,
        filter=ReplayFilter(),
        ledger=ledger,
        transform=transform,
        delete_after=True,
    )
    assert report.replayed == 1
    assert publisher.calls[0][2]["x-replay-of"] == "m1"
    assert ledger.count("m1") == 1
    assert ledger.count("other") == 0
    assert source.deleted == ["m1"]


def test_source_stalled():
    class StalledSource:
        def __init__(self) -> None:
            self.calls = 0

        def list(self, cursor: str | None, limit: int) -> tuple[list[DeadLetter], str | None]:
            if self.calls == 0:
                self.calls += 1
                return [make_msg("m1", "t")], "cursor1"
            else:
                # return same cursor again
                return [make_msg("m2", "t")], "cursor1"

        def delete(self, id: str) -> None:
            pass

    source = StalledSource()
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    with pytest.raises(ReplayError) as exc_info:
        replay(source, publisher, filter=f, ledger=ledger)
    assert exc_info.value.code == "source_stalled"


def test_ledger_persistence(tmp_path: Path) -> None:
    path = tmp_path / "ledger.jsonl"
    ledger = Ledger(path)
    assert ledger.count("a") == 0
    new_count = ledger.record("a", 123.456)
    assert new_count == 1
    # reopen
    ledger2 = Ledger(path)
    assert ledger2.count("a") == 1
    ledger2.record("a", 200.0)
    assert ledger2.count("a") == 2
    # file contains two lines
    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    entry = json.loads(lines[1])
    assert entry["id"] == "a"
    assert entry["count"] == 2
    assert entry["replayed_at"] == 200.0


def test_ledger_corrupt_line(tmp_path: Path) -> None:
    path = tmp_path / "ledger.jsonl"
    path.write_text('{"id": "a", "count": 1, "replayed_at": 1.0}\ninvalid\n', encoding="utf-8")
    with pytest.raises(ReplayError) as exc_info:
        Ledger(path)
    assert exc_info.value.code == "corrupt_ledger"


def test_ledger_full():
    ledger = Ledger(max_entries=1)
    ledger.record("a", 1.0)
    with pytest.raises(ReplayError) as exc_info:
        ledger.record("b", 2.0)
    assert exc_info.value.code == "ledger_full"


def test_on_event_callback():
    m1 = make_msg("m1", "orders", "ok")
    m2 = make_msg("m2", "orders", "fail")
    source = FakeSource([m1, m2])
    publisher = FakePublisher(raise_for="m2")
    ledger = Ledger()
    events: list[dict[str, object]] = []

    def cb(event: dict[str, object]) -> None:
        events.append(event)

    f = ReplayFilter()
    replay(source, publisher, filter=f, ledger=ledger, on_event=cb)
    replayed_events = [e for e in events if e["event"] == "replayed"]
    failed_events = [e for e in events if e["event"] == "publish_failed"]
    assert len(replayed_events) == 1
    assert replayed_events[0]["id"] == "m1"
    assert replayed_events[0]["topic"] == "orders"
    assert "payload" not in replayed_events[0]
    assert "headers" not in replayed_events[0]
    assert "error" not in replayed_events[0]
    assert len(failed_events) == 1
    assert failed_events[0]["id"] == "m2"
    assert failed_events[0]["topic"] == "orders"
    assert "payload" not in failed_events[0]
    assert "headers" not in failed_events[0]
    assert "error" not in failed_events[0]


def test_transform_changes_topic():
    m = make_msg("m1", "old", "ok")
    source = FakeSource([m])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()

    def transform(msg: DeadLetter) -> DeadLetter | None:
        if msg.topic == "old":
            return DeadLetter(
                id=msg.id,
                topic="new",
                payload=msg.payload,
                headers=msg.headers,
                error=msg.error,
                failed_at=msg.failed_at,
                attempts=msg.attempts,
            )
        return msg

    report = replay(source, publisher, filter=f, ledger=ledger, transform=transform)
    assert report.replayed == 1
    assert publisher.calls[0][0] == "new"


def test_transform_returns_none_skips():
    m = make_msg("m1", "t", "ok")
    source = FakeSource([m])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    report = replay(source, publisher, filter=f, ledger=ledger, transform=lambda _: None)
    assert report.skipped_transform == 1
    assert report.replayed == 0
    assert len(publisher.calls) == 0


def test_invalid_options():
    source = FakeSource([])
    publisher = FakePublisher()
    ledger = Ledger()
    f = ReplayFilter()
    with pytest.raises(ReplayError) as exc_info:
        replay(source, publisher, filter=f, ledger=ledger, page_size=0)
    assert exc_info.value.code == "invalid_options"
    with pytest.raises(ReplayError):
        replay(source, publisher, filter=f, ledger=ledger, max_messages=-1)
    with pytest.raises(ReplayError):
        replay(source, publisher, filter=f, ledger=ledger, rate_per_second=0)

"""Replay engine."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, replace

from .errors import ReplayError
from .ledger import Ledger
from .models import DeadLetter, DeadLetterSource, Publisher, ReplayFilter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplayReport:
    """Summary of a replay run."""

    scanned: int
    matched: int
    replayed: int
    skipped_already: int
    skipped_transform: int
    failed: int
    would_replay: int
    stopped: bool
    limited: bool


def replay(
    source: DeadLetterSource,
    publisher: Publisher,
    *,
    filter: ReplayFilter,
    ledger: Ledger,
    dry_run: bool = False,
    force: bool = False,
    max_messages: int = 1_000,
    page_size: int = 100,
    rate_per_second: float | None = None,
    delete_after: bool = False,
    stop_on_error: bool = False,
    transform: Callable[[DeadLetter], DeadLetter | None] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    on_event: Callable[[dict[str, object]], None] | None = None,
) -> ReplayReport:
    """Replay dead-lettered messages from `source` to `publisher`.

    See module documentation for detailed semantics.
    """
    if page_size <= 0:
        raise ReplayError("invalid_options", "page_size must be positive")
    if max_messages < 0:
        raise ReplayError("invalid_options", "max_messages must be non-negative")
    if rate_per_second is not None and rate_per_second <= 0:
        raise ReplayError("invalid_options", "rate_per_second must be positive")

    scanned = 0
    matched = 0
    replayed = 0
    skipped_already = 0
    skipped_transform = 0
    failed = 0
    would_replay = 0
    stopped = False
    limited = False

    cursor: str | None = None
    last_returned_cursor: str | None = None
    last_publish_time: float | None = None

    while True:
        messages, new_cursor = source.list(cursor, page_size)
        if new_cursor is not None and new_cursor == last_returned_cursor:
            raise ReplayError("source_stalled", "source returned same cursor twice")
        last_returned_cursor = new_cursor
        cursor = new_cursor

        for message in messages:
            scanned += 1
            if not filter.matches(message):
                continue
            matched += 1

            if not force and ledger.count(message.id) > 0:
                skipped_already += 1
                if on_event:
                    on_event({"event": "skipped_already", "id": message.id, "topic": message.topic})
                continue

            original_id = message.id
            if transform is not None:
                transformed = transform(message)
                if transformed is None:
                    skipped_transform += 1
                    if on_event:
                        on_event(
                            {
                                "event": "skipped_transform",
                                "id": original_id,
                                "topic": message.topic,
                            }
                        )
                    continue
                message = replace(transformed, id=original_id)

            attempted_count = replayed + failed + would_replay
            if attempted_count >= max_messages:
                limited = True
                if on_event:
                    on_event({"event": "limit_reached"})
                break

            if dry_run:
                would_replay += 1
                if on_event:
                    on_event({"event": "would_replay", "id": original_id, "topic": message.topic})
                continue

            # Actual publish
            if rate_per_second is not None and last_publish_time is not None:
                interval = 1.0 / rate_per_second
                wait_until = last_publish_time + interval
                now = clock()
                if now < wait_until:
                    sleep(wait_until - now)
            try:
                headers = dict(message.headers)
                headers["x-replay-of"] = original_id
                headers["x-replay-count"] = str(ledger.count(original_id) + 1)
                publisher.publish(message.topic, message.payload, headers)
            except Exception:
                failed += 1
                logger.warning("publish failed id=%s topic=%s", original_id, message.topic)
                if on_event:
                    on_event({"event": "publish_failed", "id": original_id, "topic": message.topic})
                if stop_on_error:
                    stopped = True
                    break
                continue

            # Success
            replayed += 1
            logger.info("replayed id=%s topic=%s", original_id, message.topic)
            if on_event:
                on_event({"event": "replayed", "id": original_id, "topic": message.topic})
            ledger.record(original_id, time.time())
            if delete_after:
                source.delete(original_id)
            if rate_per_second is not None:
                last_publish_time = clock()

        if limited or stopped:
            break
        if cursor is None:
            break
        if not messages and cursor is None:
            # Empty page with no cursor means end of stream
            break

    return ReplayReport(
        scanned=scanned,
        matched=matched,
        replayed=replayed,
        skipped_already=skipped_already,
        skipped_transform=skipped_transform,
        failed=failed,
        would_replay=would_replay,
        stopped=stopped,
        limited=limited,
    )

"""Core data models and protocols."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

from .errors import ReplayError


@dataclass(frozen=True)
class DeadLetter:
    """A message from the dead letter source."""

    id: str
    topic: str
    payload: bytes
    headers: Mapping[str, str]
    error: str
    failed_at: datetime
    attempts: int


@dataclass(frozen=True)
class ReplayFilter:
    """Filter criteria for selecting messages to replay.

    Empty `topics` means all topics. `error_pattern` is matched with `re.search`
    against the first 2000 characters of `DeadLetter.error`. `since` is inclusive,
    `until` is exclusive. `headers` must all be present and equal. `max_attempts`
    limits the number of attempts a message has had.
    """

    topics: tuple[str, ...] = ()
    error_pattern: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    max_attempts: int | None = None
    _compiled_error: re.Pattern[str] | None = field(
        init=False, repr=False, compare=False, default=None
    )

    def __post_init__(self) -> None:
        # Validate error_pattern
        if self.error_pattern is not None:
            if len(self.error_pattern) > 200:
                raise ReplayError("invalid_filter", "error_pattern must be at most 200 characters")
            try:
                compiled = re.compile(self.error_pattern)
            except re.error as exc:
                raise ReplayError("invalid_filter", f"invalid error_pattern: {exc}") from exc
            object.__setattr__(self, "_compiled_error", compiled)
        else:
            object.__setattr__(self, "_compiled_error", None)
        # Validate headers
        if len(self.headers) > 20:
            raise ReplayError("invalid_filter", "headers must have at most 20 conditions")
        # Validate max_attempts
        if self.max_attempts is not None and self.max_attempts < 0:
            raise ReplayError("invalid_filter", "max_attempts must be non-negative")

    def matches(self, message: DeadLetter) -> bool:
        """Return True if the message satisfies this filter."""
        if self.topics and message.topic not in self.topics:
            return False
        if self._compiled_error is not None:
            search_text = message.error[:2000]
            if not self._compiled_error.search(search_text):
                return False
        if self.since is not None and message.failed_at < self.since:
            return False
        if self.until is not None and message.failed_at >= self.until:
            return False
        if self.headers and not all(message.headers.get(k) == v for k, v in self.headers.items()):
            return False
        return not (self.max_attempts is not None and message.attempts > self.max_attempts)


@runtime_checkable
class DeadLetterSource(Protocol):
    """Protocol for a source of dead-letter messages."""

    def list(self, cursor: str | None, limit: int) -> tuple[list[DeadLetter], str | None]:
        """Return up to `limit` messages and an optional cursor for the next page."""
        ...

    def delete(self, id: str) -> None:
        """Delete a message from the source."""
        ...


@runtime_checkable
class Publisher(Protocol):
    """Protocol for publishing replayed messages."""

    def publish(self, topic: str, payload: bytes, headers: Mapping[str, str]) -> None:
        """Publish a message to the given topic."""
        ...

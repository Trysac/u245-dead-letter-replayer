"""Replay ledger for idempotency."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .errors import ReplayError


class Ledger:
    """Tracks replay counts and timestamps.

    If `path` is provided, entries are appended as JSON lines and flushed on each
    record. The ledger refuses to add more than `max_entries` distinct ids to
    preserve idempotency guarantees.
    """

    def __init__(self, path: Path | None = None, *, max_entries: int = 1_000_000) -> None:
        if max_entries <= 0:
            raise ReplayError("invalid_options", "max_entries must be positive")
        self.path = path
        self.max_entries = max_entries
        self._counts: dict[str, int] = {}
        self._last_replayed_at: dict[str, float] = {}
        if path is not None:
            self._load()

    def _load(self) -> None:
        path = self.path
        if path is None:
            return
        with open(path, "a+", encoding="utf-8") as f:
            f.seek(0)
            line_no = 0
            for raw in f:
                line_no += 1
                line = raw.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ReplayError("corrupt_ledger", f"corrupt ledger line {line_no}") from exc
                if not isinstance(entry, dict):
                    raise ReplayError(
                        "corrupt_ledger", f"corrupt ledger line {line_no}: not a mapping"
                    )
                if "id" not in entry:
                    raise ReplayError(
                        "corrupt_ledger", f"corrupt ledger line {line_no}: missing id"
                    )
                if "count" not in entry:
                    raise ReplayError(
                        "corrupt_ledger", f"corrupt ledger line {line_no}: missing count"
                    )
                if "replayed_at" not in entry:
                    raise ReplayError(
                        "corrupt_ledger", f"corrupt ledger line {line_no}: missing replayed_at"
                    )
                id_ = entry["id"]
                count = entry["count"]
                replayed_at = entry["replayed_at"]
                if (
                    not isinstance(id_, str)
                    or not isinstance(count, int)
                    or not isinstance(replayed_at, (int, float))
                ):
                    raise ReplayError(
                        "corrupt_ledger", f"corrupt ledger line {line_no}: invalid types"
                    )
                if len(self._counts) >= self.max_entries and id_ not in self._counts:
                    raise ReplayError("ledger_full", "ledger has reached max_entries distinct ids")
                self._counts[id_] = count
                self._last_replayed_at[id_] = replayed_at

    def count(self, id: str) -> int:
        """Return how many times `id` has been recorded."""
        return self._counts.get(id, 0)

    def record(self, id: str, at: float) -> int:
        """Record a replay for `id` at timestamp `at` and return the new count.

        Raises:
            ReplayError: with code ``ledger_full`` if adding a new id would exceed ``max_entries``.
        """
        if id not in self._counts:
            if len(self._counts) >= self.max_entries:
                raise ReplayError("ledger_full", "ledger has reached max_entries distinct ids")
            self._counts[id] = 0
        self._counts[id] += 1
        self._last_replayed_at[id] = at
        if self.path is not None:
            entry = {"id": id, "count": self._counts[id], "replayed_at": at}
            line = json.dumps(entry, separators=(",", ":")) + "\n"
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
        return self._counts[id]

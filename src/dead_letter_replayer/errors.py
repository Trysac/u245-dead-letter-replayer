"""Error types for dead_letter_replayer."""

from __future__ import annotations

from typing import Literal

ReplayErrorCode = Literal[
    "invalid_filter",
    "source_stalled",
    "corrupt_ledger",
    "ledger_full",
    "invalid_options",
]


class ReplayError(Exception):
    """Base error for the library.

    Attributes:
        code: Machine-readable error code.
    """

    def __init__(self, code: ReplayErrorCode, message: str = "") -> None:
        self.code = code
        super().__init__(message if message else code)

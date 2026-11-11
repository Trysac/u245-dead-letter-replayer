"""Safe replay of dead-lettered messages.

This package provides a broker-agnostic mechanism to replay messages from a
dead letter source to a publisher, with filtering, dry-run, rate limiting,
and a ledger to ensure idempotency.
"""

from __future__ import annotations

import logging

from .errors import ReplayError, ReplayErrorCode
from .ledger import Ledger
from .models import DeadLetter, DeadLetterSource, Publisher, ReplayFilter
from .replay import ReplayReport, replay

logging.getLogger("dead_letter_replayer").addHandler(logging.NullHandler())

__all__ = [
    "DeadLetter",
    "DeadLetterSource",
    "Ledger",
    "Publisher",
    "ReplayError",
    "ReplayErrorCode",
    "ReplayFilter",
    "ReplayReport",
    "__version__",
    "replay",
]

__version__ = "0.1.0"

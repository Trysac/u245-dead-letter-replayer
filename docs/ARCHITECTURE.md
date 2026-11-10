# Architecture

## Dead Letter Replayer

The library is designed around three core abstractions:

1. **Source and Publisher ports** (`DeadLetterSource`, `Publisher`) – adapters
   connect any broker or queue system. The core never depends on a specific
   broker.
2. **Ledger** – tracks replay counts and timestamps, either in-memory or as a
   JSON-lines file. It enforces a maximum number of distinct IDs to guarantee
   idempotency is never silently lost.
3. **Replay engine** – implements filtering, dry-run, rate limiting,
   transforms, and error handling.

## Design Decisions

- **Frozen dataclasses** for `DeadLetter` and `ReplayFilter` to enforce
  immutability and make the filtering logic pure and easily testable.
- **Error handling** uses a single `ReplayError` exception with a
  machine-readable `code` so callers can programmatically distinguish between
  invalid filters, stalled sources, corrupt ledgers, and ledger capacity
  issues.
- **Ledger records are appended and flushed** on each successful replay to
  minimize data loss. The file is opened in append mode; a corrupt line raises
  `ReplayError` with the line number.
- **No eviction** in the ledger when `max_entries` is reached – the operation
  fails with `ledger_full` instead of dropping old entries, because that could
  lead to accidental duplicate replays.
- **Rate limiting** uses an injected monotonic clock and sleep function so it
  is deterministic in tests and does not block the event loop.
- **Event callback** (`on_event`) receives only safe fields (`id`, `topic`,
  `event`) – never payload bytes, headers, or error text.

## Testing Strategy

Unit tests cover every exported function and class, including all error paths.
Conformance tests (if any) are tagged with `pytest.mark.conformance` and run
only in CI where external dependencies are available.

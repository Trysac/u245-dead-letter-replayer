# trysac-dead-letter-replayer

![CI](https://github.com/Trysac/u245-dead-letter-replayer/workflows/CI/badge.svg)
![Python Versions](https://img.shields.io/pypi/pyversions/trysac-dead-letter-replayer.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

`dead_letter_replayer` replays messages from a dead letter source to a publisher. It filters by topic, error pattern, failure time, headers and attempts; supports dry runs, rate limiting, optional deletion after publish, transforms, and a JSONL-backed ledger that prevents repeated replays unless `force=True`. The core has no broker dependency: adapters implement `DeadLetterSource` and `Publisher`.

## Installation

```bash
pip install trysac-dead-letter-replayer
```

The runtime code imports only the Python standard library. The import name is `dead_letter_replayer`.

## Usage

```python
from dead_letter_replayer import Ledger, ReplayFilter, replay

source = MyDeadLetterSource()  # implements DeadLetterSource
publisher = MyPublisher()  # implements Publisher
ledger = Ledger("replay-ledger.jsonl")
filter_ = ReplayFilter(topics=("orders",), error_pattern="^TimeoutError")
report = replay(source, publisher, filter=filter_, ledger=ledger, dry_run=True)
print(report)
```

## Public API

- `DeadLetter`: frozen dataclass with `id`, `topic`, `payload` (bytes), `headers` (mapping of str to str), `error`, `failed_at` and `attempts`.
- `ReplayFilter`: frozen dataclass with `topics=()`, `error_pattern=None`, `since=None`, `until=None`, `headers={}`, `max_attempts=None`. Empty `topics` means all topics. `error_pattern` is compiled once and matched with `re.search` against the first 2000 characters of `DeadLetter.error`; at most 200 pattern characters. `since` is inclusive, `until` is exclusive. All `headers` conditions must be present and equal; at most 20 conditions. `max_attempts` rejects messages whose `attempts` is greater than the value.
- `DeadLetterSource`: runtime-checkable protocol with `list(cursor, limit) -> tuple[list[DeadLetter], str | None]` and `delete(id) -> None`.
- `Publisher`: runtime-checkable protocol with `publish(topic, payload, headers) -> None`.
- `Ledger(path=None, *, max_entries=1_000_000)`: in-memory when `path` is `None`; otherwise loads and appends JSON lines with `id`, `count`, `replayed_at`, flushing and fsyncing each record. `count(id)` returns 0 for unknown ids. `record(id, at)` increments the count and raises `ReplayError('ledger_full')` when adding a new id would exceed `max_entries`; it never evicts entries.
- `replay(source, publisher, *, filter, ledger, dry_run=False, force=False, max_messages=1_000, page_size=100, rate_per_second=None, delete_after=False, stop_on_error=False, transform=None, clock=time.monotonic, sleep=time.sleep, on_event=None) -> ReplayReport`: processes pages from `source.list` until the returned cursor is `None`. A repeated non-`None` cursor raises `ReplayError('source_stalled')`. For each message: count `scanned`; skip if `filter.matches` is false; count `matched`; skip with `skipped_already` when the ledger already has the id and `force` is false; apply `transform` (returning `None` gives `skipped_transform`; the original id is preserved); count `would_replay` in dry-run mode; stop with `limited=True` when `replayed + failed + would_replay` reaches `max_messages`; wait before each publish after the first when `rate_per_second` is set; publish with the message headers plus `x-replay-of` and `x-replay-count` (these override existing keys); on success record the ledger, optionally call `source.delete`, and count `replayed`; on a publisher exception count `failed` and either continue or stop with `stopped=True` when `stop_on_error` is true.
- `ReplayReport`: frozen dataclass with `scanned`, `matched`, `replayed`, `skipped_already`, `skipped_transform`, `failed`, `would_replay`, `stopped` and `limited`.
- `ReplayError`: exception with a machine-readable `code` attribute. Codes are `invalid_filter`, `source_stalled`, `corrupt_ledger`, `ledger_full` and `invalid_options`.

### Errors and limits

`ReplayFilter` raises `ReplayError('invalid_filter')` for an error pattern longer than 200 characters, an invalid regular expression, more than 20 header conditions, or a negative `max_attempts`. `Ledger` raises `ReplayError('invalid_options')` when `max_entries` is not positive and `ReplayError('corrupt_ledger')` with the line number when an existing line is not a mapping or lacks valid `id`, `count` or `replayed_at` values. `replay()` raises `ReplayError('invalid_options')` when `page_size` is not positive, `max_messages` is negative, or `rate_per_second` is present and not positive. Publisher exceptions that subclass `Exception` are counted in `failed` and are not re-raised; other `BaseException` types such as `KeyboardInterrupt` propagate. `on_event` receives only `event`, `id` and `topic` keys for message events, and only `event` for `limit_reached`; it never receives payload bytes, headers or error text. Logs contain ids and topics only.

## Why not a one-off script or broker console redrive

A one-off script or broker console redrive does not provide the combination implemented here: `ReplayFilter` selects messages, `dry_run` reports `would_replay` without publishing or changing the ledger, `rate_per_second` paces publishes, and `Ledger` makes reruns skip already-replayed ids unless `force=True`. The library does not replace broker-specific redrive tools; adapters implement `DeadLetterSource` and `Publisher`.

## Development

Run the test module with pytest:

```bash
python -m pytest tests/test_dead_letter_replayer.py
```

The threat model is in [docs/SECURITY.md](docs/SECURITY.md).

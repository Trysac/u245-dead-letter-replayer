# Security notes

## Threat model

`dead_letter_replayer` is a library. It does not authenticate to a broker, encrypt data, or provide a network service. Security depends on the adapters passed to `replay()` and on protecting the ledger file.

### Defended

- Double replay through the ledger: `replay()` skips ids already recorded when `force=False`; `force=True` is explicit. `Ledger.record` refuses a new id at `max_entries` with `ReplayError('ledger_full')` instead of evicting old entries.
- Corrupt ledger input: an existing line that is not a mapping or lacks valid `id`, `count` or `replayed_at` types raises `ReplayError('corrupt_ledger')` with the line number.
- Invalid filter input: patterns longer than 200 characters, invalid regular expressions, more than 20 header conditions, and negative `max_attempts` raise `ReplayError('invalid_filter')`.
- Invalid options: non-positive `page_size`, negative `max_messages`, non-positive `rate_per_second`, and non-positive `max_entries` raise `ReplayError('invalid_options')`.
- Stalled sources: a source that returns the same non-`None` cursor twice raises `ReplayError('source_stalled')`.
- Publisher failures: exceptions that subclass `Exception` are counted in `failed`, logged with id and topic only, and are not re-raised; `stop_on_error=True` stops the run with `stopped=True`. Other `BaseException` types such as `KeyboardInterrupt` propagate.
- Event and log exposure: `on_event` receives `event`, `id` and `topic` for message events and only `event` for `limit_reached`; logs contain ids and topics only, never payload bytes, headers or error text.

### Not defended

- Broker authentication, authorization, TLS or delivery guarantees; adapters own them.
- Confidentiality or integrity of payloads; the library passes bytes unchanged except for an explicit `transform` callable.
- Concurrent writers or processes sharing one ledger file; there is no file locking. Each successful record is appended, flushed and fsynced, but interleaved writers are not coordinated.
- Malicious or buggy adapters; `source.list`, `source.delete` and `publisher.publish` run in-process.
- Failures in `source.delete` after a successful publish; `replay()` does not catch them.
- Disk full, permission errors or other ledger append failures; they are not converted to `ReplayError`.
- Untrusted `transform`, `clock`, `sleep` or `on_event` callables; they execute in-process.

## Limits

- `Ledger.max_entries` defaults to 1,000,000 distinct ids and must be positive. At the limit a new id raises `ledger_full`; existing ids can still be recorded.
- `ReplayFilter.error_pattern` is limited to 200 characters and is matched against the first 2000 characters of `DeadLetter.error`.
- `ReplayFilter.headers` is limited to 20 conditions.
- `replay()` defaults: `max_messages=1_000`, `page_size=100`; `max_messages` counts `replayed + failed + would_replay`.
- `rate_per_second` must be positive when set; the first publish does not wait, and each later publish waits until the previous publish time plus `1 / rate_per_second`.
- The ledger writes one JSON object per record using `id`, `count` and `replayed_at`; `replayed_at` is written with `time.time()`.

## Publishing

The provided source tree contains no publishing workflow or credentials. Release steps are outside the library code.

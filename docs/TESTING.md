# Testing

## Running

```bash
python -m pytest tests/test_dead_letter_replayer.py
```

## Policy

The provided source tree contains one test module, `tests/test_dead_letter_replayer.py`. It imports from `dead_letter_replayer` and uses in-memory helpers: `FakeSource`, `FakePublisher`, `FakeClock`, `FakeSleep`, `RecordingSource` and `RecordingPublisher`. Tests are deterministic and do not use network, wall clock or external services.

## Tiers

There is one local test tier in the provided tree. No conformance tests, external tools, CI-only setup scripts or `tests/conftest.py` are present.

## Layout

- `tests/test_dead_letter_replayer.py`: filter and replay tests. Test functions cover topics and error pattern (`test_filter_topics_and_error`), since/until bounds (`test_filter_since_until`), header matching (`test_filter_headers`), max attempts (`test_filter_max_attempts`), invalid pattern (`test_filter_invalid_pattern`), invalid headers (`test_filter_invalid_headers`) and the basic replay path (`test_basic_replay`), including report counts, publisher calls, replay headers and ledger counts. Helper classes provide fake source, publisher, clock and sleep objects and recording adapters.

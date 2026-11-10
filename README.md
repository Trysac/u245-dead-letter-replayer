# dead-letter-replayer

[![PyPI version](https://img.shields.io/pypi/v/trysac-dead-letter-replayer)](https://pypi.org/project/trysac-dead-letter-replayer/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-dead-letter-replayer)](LICENSE)
[![CI](https://github.com/Trysac/u245-dead-letter-replayer/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u245-dead-letter-replayer/actions/workflows/ci.yml)

Replay dead-lettered messages safely: filters by topic, error, time and headers, dry runs, rate limits, a replay ledger against double replays and broker-agnostic ports

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-dead-letter-replayer
# or
uv add trysac-dead-letter-replayer
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import dead_letter_replayer

# normalize() trims surrounding whitespace and lower-cases the text.
print(dead_letter_replayer.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    dead_letter_replayer.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

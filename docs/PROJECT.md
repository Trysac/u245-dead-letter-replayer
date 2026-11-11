# Project record

## Objective

Replay dead-lettered messages safely: filter by topic, error pattern, failure time, headers and attempts; support dry runs and rate limits; keep a replay ledger that prevents double replays; remain broker-agnostic through the `DeadLetterSource` and `Publisher` protocols.

## Scope

- In scope: the `dead_letter_replayer` package, its public types, protocols, ledger and `replay()` function.
- In scope: filtering by topics, error pattern, failure time, headers and attempts; dry runs; rate limiting; transforms; optional deletion after publish; event callbacks; a ledger in memory or as JSON lines.
- In scope: standard-library-only runtime code.
- Out of scope: broker clients such as Kafka, SQS or RabbitMQ adapters; they implement the two protocols.
- Out of scope: payload editing UIs.
- Out of scope: concurrent replays sharing one ledger file.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays 'pending measurement'.

| Metric | Value |
|---|---|
| Tokens (input / output) | 1151051 / 73010 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.0995 |

## Factory run u245 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | deepseek-v4-pro | yes | 1821 | 18455 | 0 | 3200 | 0.0378 |
| 2026-11-10 | fix-1 | deepseek-flash | no | 20448 | 7506 | 0 | 177408 | 0.0081 |
| 2026-11-10 | audit | deepseek-flash | no | 33423 | 26151 | 0 | 527488 | 0.0223 |
| 2026-11-10 | fix-2 | deepseek-flash | no | 15213 | 7470 | 0 | 267264 | 0.0076 |
| 2026-11-10 | docs | deepseek-flash | no | 104530 | 13428 | 0 | 256 | 0.0237 |

Total: $0.0995. Prices: deepseek line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).

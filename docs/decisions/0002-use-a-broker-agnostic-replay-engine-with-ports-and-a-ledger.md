# Use a broker-agnostic replay engine with ports and a ledger

Status: accepted (generated, pending human review)

## Context

Replaying dead-lettered messages often leads to double replays, no filtering, and no rate limiting. A reusable library should not depend on a specific broker.

## Decision

Implement a core replay function that consumes a `DeadLetterSource` and a `Publisher` (ports) and uses an append-only ledger to record replays, enforcing idempotency. Filtering and dry-run are built in.

## Consequences

Positive: portable across brokers, safe reruns, testable core. Negative: adapters must be written for each broker; the ledger must be stored externally for durability.

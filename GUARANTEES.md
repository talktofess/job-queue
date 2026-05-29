# Queue Guarantees

This is the contract the queue holds itself to. Every clause has a test.

## Delivery semantics: at-least-once

A job is delivered to a handler **one or more times**. We do not attempt
at-most-once (which loses work on crashes). Combined with idempotent handlers
(below), the observable result is **exactly-once effect**.

Why one-or-more: a worker can crash *after* its handler succeeds but *before*
it acks. The lease then expires and the job is redelivered. This is correct and
expected — handlers must tolerate it.

## Ordering

- **No global ordering guarantee.** Priorities and retries reorder work.
- **Strict per-key serialization.** For jobs sharing a non-empty `key` (e.g.
  `user_id`), **at most one runs at a time**, preserving per-entity order.
  Implemented by refusing to claim a job whose `key` already has a running job.

## Visibility timeout (lease)

On claim, a job is marked `running` with `locked_until = now + lease`. While
`running` it is invisible to other claimers. A worker processing a long job
**heartbeats** to extend the lease (every `lease/3`). If the worker dies, the
lease lapses.

## Worker crash mid-job

The job stays `running` with an expired `locked_until`. The **reaper** returns
it to `pending`; another worker reclaims it. No ack was sent, so it re-runs —
hence the idempotency requirement. **No job is lost.**

## Repeated failure

On handler error, if `attempts < max_attempts` the job returns to `pending`
with `available_at = now + backoff(attempts)` (exponential + jitter). Once
`attempts >= max_attempts` it moves to `dead` (the dead-letter queue) for
inspection / manual requeue.

## Idempotency

- **Enqueue-side:** an `idempotency_key` is `UNIQUE`; a producer retrying
  `enqueue` does not create a duplicate job.
- **Consumer-side (exactly-once effect):** a handler records its effect and
  marks the key processed in the **same transaction**; a redelivery hits the
  unique conflict and no-ops.

## Concurrency correctness

With N workers claiming concurrently, **no job is processed by two workers at
once** on the happy path. On Postgres this is guaranteed by
`SELECT … FOR UPDATE SKIP LOCKED`; in the in-memory store by a mutex.

## What we explicitly do NOT guarantee

- Global FIFO ordering.
- Exactly-once *delivery* (only exactly-once *effect*, via idempotency).
- Survival of total storage loss (single-Postgres durability; the Raft stretch
  goal addresses node failure).

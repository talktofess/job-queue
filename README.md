# Distributed Job Queue

A job/task queue built from the primitives in **Go + Postgres**. The point isn't
to reimplement Celery — it's to *demonstrate the guarantees a queue provides and
the trade-offs behind them*. The value is in the failure handling, which is
designed first: see **[GUARANTEES.md](GUARANTEES.md)**.

> Implements the scope in [`../03-distributed-job-queue.md`](../03-distributed-job-queue.md).
> The serving logic sits behind a `Store` interface: an **in-memory** store runs
> the logic + concurrency + chaos tests offline (and the demo), while the
> **Postgres** store provides the real `SELECT … FOR UPDATE SKIP LOCKED`
> semantics and the chaos demo.

## Guarantees (the contract)

At-least-once delivery · exactly-once *effect* via idempotency · strict per-key
ordering · lease-based visibility timeout · crash recovery via a reaper · retries
with exponential backoff + jitter · dead-letter queue. Full text in
[GUARANTEES.md](GUARANTEES.md); each clause has a test.

## Architecture

```
producer ─enqueue─▶ Postgres jobs table ◀─claim (SKIP LOCKED + lease)─ worker pool
                          ▲                                              │ ack/nack/dead
              reaper ─────┘ requeue expired leases (crash recovery)      │ heartbeat→extend lease
```

| Component | Package | Notes |
|---|---|---|
| Domain types | `internal/job` | Job, State, NewJob |
| Store seam | `internal/store` | interface; `memory/` + `postgres/` impls |
| Queue policy | `internal/queue` | retry-vs-dead, backoff, metrics |
| Worker | `internal/worker` | claim→run→ack/nack loop, lease heartbeat |
| Reaper | `internal/reaper` | reclaims expired leases |
| Metrics | `internal/metrics`, `internal/prommetrics` | interface + Prometheus impl |
| CLIs | `cmd/{demo,worker,enqueue,queuectl}` | |

## Run it offline right now (no Postgres, no Docker)

Needs only the Go toolchain. The in-memory store backs everything.

```powershell
cd "job-queue"
go test ./internal/... ./test/...   # unit + concurrency + chaos tests
go run ./cmd/demo                    # the money shot, in one process
```

`cmd/demo` shows all four failure behaviors end-to-end — a "crashed" worker's
job reclaimed by the reaper and finished elsewhere, a flaky job retried with
backoff then completed, and a permanently-failing job dead-lettered:

```
CRASH: ghost-worker claimed job 1 then died without acking (lease 800ms)
worker-1 job 5 failed (attempt 1/5): transient error on attempt 1
worker-0 job 5 failed (attempt 2/5): transient error on attempt 2
worker-0 job 5 done
worker-0 job 6 failed (attempt 3/3): permanent failure
REAPER: reaper reclaimed expired jobs
worker-0 job 1 done
FINAL: done=5 dead=1 pending=0 running=0
  dead-letter: job 6 queue=deadly attempts=3 last_error="permanent failure"
```

## Run it for real (Postgres) — the SKIP LOCKED path + chaos demo

```bash
docker compose up --build         # Postgres + worker pool + Prometheus + Grafana
export QUEUE_DSN="postgres://queue:queue@localhost:5432/queue?sslmode=disable"

go run ./cmd/enqueue -dsn "$QUEUE_DSN" -payload '{"task":"hello"}'
go run ./cmd/queuectl -dsn "$QUEUE_DSN" stats
go run ./cmd/queuectl -dsn "$QUEUE_DSN" list -state dead
go run ./cmd/queuectl -dsn "$QUEUE_DSN" requeue -id 42
```

**Chaos demo (the money shot):** start the worker pool, enqueue a slow job, then
`kill -9` the worker holding it. The lease expires, the reaper returns the job to
`pending`, and another worker finishes it. Nothing is lost.

**SKIP LOCKED correctness** is proven by the Postgres integration test:

```bash
TEST_DATABASE_URL="$QUEUE_DSN" go test -tags integration ./test/...
# 500 jobs, 16 concurrent claimers, asserts each job is claimed exactly once
```

## Observability

`docker compose up` provisions Prometheus + a Grafana "Job Queue" dashboard
(http://localhost:3000): queue depth by state, claim/retry/dead/reap rates,
handler-duration percentiles, and oldest-pending-age. The worker exposes
`/metrics` on `:2112`.

## CI

`.github/workflows/ci.yml`: a **unit** job (`go vet` + in-memory tests, no
services) and an **integration** job that spins up a Postgres service and runs
the `-tags integration` tests.

## Design notes

- **`Store` is the seam.** Queue/worker/reaper depend only on the interface;
  `get`-style construction picks the implementation. Core packages are
  dependency-free (pgx/prometheus are isolated in `postgres`/`prommetrics`).
- **`SELECT … FOR UPDATE SKIP LOCKED`** is the heart: concurrent claimers lock
  their candidate row and skip locked rows, so no two workers claim the same job
  with no application-level coordination.
- **Lease + reaper = crash recovery.** A held job has `locked_until` in the
  future; a healthy worker heartbeats to extend it; a dead worker stops, the
  lease lapses, the reaper requeues it.
- **Honest boundary:** the in-memory store proves the *logic* (mutex-guarded);
  the *database-level* concurrency guarantee is proven only against Postgres.

## Stretch (not yet built)

Horizontal partitioning by key · Raft-replicated queue metadata · deeper
exactly-once-effect via transactional outbox (the `processed` table is the hook).

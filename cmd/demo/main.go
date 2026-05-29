// Command demo runs the whole queue in one process on the in-memory store, with
// no Postgres or Docker, to show the guarantees end-to-end:
//   - normal jobs flow through workers
//   - a flaky job retries with backoff then succeeds
//   - a permanently-failing job lands in the dead-letter queue
//   - a "crashed" worker's job is reclaimed by the reaper and finished elsewhere
//
// Run: go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/reaper"
	"github.com/talktofess/job-queue/internal/store/memory"
	"github.com/talktofess/job-queue/internal/worker"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	st := memory.New()
	q := queue.New(st, backoff.Policy{Base: 150 * time.Millisecond, Cap: time.Second}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := worker.NewRegistry()
	reg.Register("default", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}))
	// Fails on attempts 1 and 2, succeeds on attempt 3 (Attempts is incremented
	// on each claim, so it reflects the current try).
	reg.Register("flaky", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		if j.Attempts < 3 {
			return fmt.Errorf("transient error on attempt %d", j.Attempts)
		}
		return nil
	}))
	reg.Register("deadly", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return fmt.Errorf("permanent failure")
	}))

	// --- simulate a crashed worker BEFORE the pool starts ---
	crashID, _ := q.Enqueue(ctx, job.NewJob{Queue: "default", Payload: []byte(`{"task":"crash-victim"}`)})
	ghost, _ := q.Claim(ctx, "ghost-worker", 800*time.Millisecond) // claims crashID
	log.Printf("CRASH: ghost-worker claimed job %d then died without acking (lease 800ms)", ghost.ID)

	// --- enqueue the rest of the workload ---
	for i := 0; i < 3; i++ {
		q.Enqueue(ctx, job.NewJob{Queue: "default", Payload: []byte(`{"task":"normal"}`)})
	}
	q.Enqueue(ctx, job.NewJob{Queue: "flaky", Payload: []byte(`{"task":"flaky"}`)})
	q.Enqueue(ctx, job.NewJob{Queue: "deadly", MaxAttempts: 3, Payload: []byte(`{"task":"deadly"}`)})

	// --- start reaper + worker pool ---
	rp := reaper.New(st, 200*time.Millisecond, nil, func(msg string) { log.Printf("REAPER: %s", msg) })
	go rp.Run(ctx)

	opts := worker.DefaultOptions()
	opts.Lease = time.Second
	opts.PollInterval = 100 * time.Millisecond
	opts.OnEvent = func(msg string) { log.Print(msg) }
	go worker.RunPool(ctx, 2, func(i int) *worker.Worker {
		return worker.New(fmt.Sprintf("worker-%d", i), q, reg, opts)
	})

	log.Printf("watching job %d get reclaimed by the reaper and finished by a live worker...", crashID)
	time.Sleep(3 * time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	st2, _ := q.Stats(ctx)
	fmt.Printf("\nFINAL: done=%d dead=%d pending=%d running=%d\n",
		st2.Done, st2.Dead, st2.Pending, st2.Running)
	dead, _ := q.List(ctx, job.StateDead, 10)
	for _, d := range dead {
		fmt.Printf("  dead-letter: job %d queue=%s attempts=%d last_error=%q\n",
			d.ID, d.Queue, d.Attempts, d.LastError)
	}
}

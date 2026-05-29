// Package qtest holds integration-style tests that drive the whole stack.
package qtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/store/memory"
	"github.com/talktofess/job-queue/internal/worker"
)

// Many workers, many jobs: every job must run exactly once and none be lost.
func TestNoLossNoDoubleProcessing(t *testing.T) {
	const M = 200
	const N = 8
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := memory.New()
	q := queue.New(s, backoff.Default(), nil)
	for i := 0; i < M; i++ {
		q.Enqueue(ctx, job.NewJob{Queue: "default", Payload: []byte("{}")})
	}

	var mu sync.Mutex
	counts := make(map[job.ID]int)
	reg := worker.NewRegistry()
	reg.Register("default", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		mu.Lock()
		counts[j.ID]++
		mu.Unlock()
		return nil
	}))

	opts := worker.DefaultOptions()
	opts.Lease = 30 * time.Second
	opts.PollInterval = 5 * time.Millisecond

	poolCtx, stopPool := context.WithCancel(ctx)
	go worker.RunPool(poolCtx, N, func(i int) *worker.Worker {
		return worker.New(string(rune('A'+i)), q, reg, opts)
	})

	deadline := time.After(8 * time.Second)
	for {
		st, _ := q.Stats(ctx)
		if st.Done == M {
			break
		}
		select {
		case <-deadline:
			stopPool()
			t.Fatalf("timeout: only %d/%d done", st.Done, M)
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopPool()

	mu.Lock()
	defer mu.Unlock()
	if len(counts) != M {
		t.Fatalf("expected %d distinct jobs processed, got %d", M, len(counts))
	}
	for id, c := range counts {
		if c != 1 {
			t.Fatalf("job %d processed %d times (expected exactly once)", id, c)
		}
	}
}

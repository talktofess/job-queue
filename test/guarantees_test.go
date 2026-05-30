package qtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/store/memory"
	"github.com/talktofess/job-queue/internal/worker"
)

// Guarantee under load: jobs sharing a key are never processed concurrently,
// even with many workers — per-key serialization holds end-to-end.
func TestPerKeyNeverConcurrentUnderLoad(t *testing.T) {
	const M = 20
	const N = 8
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := memory.New()
	q := queue.New(s, backoff.Default(), nil)
	for i := 0; i < M; i++ {
		q.Enqueue(ctx, job.NewJob{Key: "user-1", Payload: []byte("k")})
	}

	var active, maxActive int64
	reg := worker.NewRegistry()
	reg.Register("default", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		cur := atomic.AddInt64(&active, 1)
		for {
			old := atomic.LoadInt64(&maxActive)
			if cur <= old || atomic.CompareAndSwapInt64(&maxActive, old, cur) {
				break
			}
		}
		time.Sleep(3 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return nil
	}))

	opts := worker.DefaultOptions()
	opts.Lease = 30 * time.Second
	opts.PollInterval = 2 * time.Millisecond

	poolCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); worker.RunPool(poolCtx, N, func(i int) *worker.Worker {
		return worker.New(string(rune('A'+i)), q, reg, opts)
	}) }()

	deadline := time.After(8 * time.Second)
	for {
		st, _ := q.Stats(ctx)
		if st.Done == M {
			break
		}
		select {
		case <-deadline:
			stop()
			wg.Wait()
			t.Fatalf("timeout: %d/%d done", st.Done, M)
		case <-time.After(5 * time.Millisecond):
		}
	}
	stop()
	wg.Wait()

	if got := atomic.LoadInt64(&maxActive); got != 1 {
		t.Fatalf("max concurrent same-key handlers = %d, want 1 (per-key serialization broken)", got)
	}
}

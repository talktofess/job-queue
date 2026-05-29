// Package worker runs the claim -> handle -> ack/nack loop, heartbeats the
// lease while a job runs, and stops gracefully on context cancellation.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
)

type Options struct {
	Lease        time.Duration // visibility timeout per claim
	PollInterval time.Duration // sleep when the queue is empty
	JobTimeout   time.Duration // 0 => no per-job timeout
	OnEvent      func(string)  // optional structured-log hook
}

func DefaultOptions() Options {
	return Options{Lease: 30 * time.Second, PollInterval: 200 * time.Millisecond, JobTimeout: 0}
}

type Worker struct {
	id   string
	q    *queue.Queue
	reg  *Registry
	opts Options
}

func New(id string, q *queue.Queue, reg *Registry, opts Options) *Worker {
	if opts.Lease == 0 {
		opts.Lease = DefaultOptions().Lease
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = DefaultOptions().PollInterval
	}
	return &Worker{id: id, q: q, reg: reg, opts: opts}
}

func (w *Worker) logf(format string, args ...any) {
	if w.opts.OnEvent != nil {
		w.opts.OnEvent(fmt.Sprintf(format, args...))
	}
}

// Run loops until ctx is cancelled (graceful shutdown). A hard kill (kill -9)
// skips this path entirely — the lease then expires and the reaper recovers
// the job, which is exactly the crash-recovery guarantee.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		j, err := w.q.Claim(ctx, w.id, w.opts.Lease)
		if err != nil {
			w.logf("worker %s claim error: %v", w.id, err)
			if sleep(ctx, w.opts.PollInterval) != nil {
				return nil
			}
			continue
		}
		if j == nil {
			if sleep(ctx, w.opts.PollInterval) != nil {
				return nil
			}
			continue
		}
		w.process(ctx, j)
	}
}

func (w *Worker) process(ctx context.Context, j *job.Job) {
	h, ok := w.reg.Get(j.Queue)
	if !ok {
		_ = w.q.Fail(ctx, j, fmt.Errorf("no handler registered for queue %q", j.Queue))
		return
	}

	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go w.heartbeat(hbCtx, j)

	runCtx := ctx
	if w.opts.JobTimeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, w.opts.JobTimeout)
		defer cancel()
	}

	start := time.Now()
	if err := h.Handle(runCtx, j); err != nil {
		w.logf("worker %s job %d failed (attempt %d/%d): %v", w.id, j.ID, j.Attempts, j.MaxAttempts, err)
		_ = w.q.Fail(ctx, j, err)
		return
	}
	_ = w.q.Ack(ctx, j, time.Since(start))
	w.logf("worker %s job %d done in %s", w.id, j.ID, time.Since(start))
}

func (w *Worker) heartbeat(ctx context.Context, j *job.Job) {
	t := time.NewTicker(w.opts.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Use Background so a cancelled run-ctx doesn't abort the extension.
			_ = w.q.ExtendLease(context.Background(), j.ID, w.id, time.Now().Add(w.opts.Lease))
		}
	}
}

// RunPool runs n workers concurrently, returning when ctx is cancelled.
func RunPool(ctx context.Context, n int, mk func(i int) *Worker) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = mk(i).Run(ctx)
		}(i)
	}
	wg.Wait()
}

// sleep returns ctx.Err() if the context is cancelled during the wait.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

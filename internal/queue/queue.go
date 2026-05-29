// Package queue is the policy layer over a Store: it decides retry-vs-dead,
// computes backoff, and records metrics. The worker and CLIs use this, not the
// raw Store.
package queue

import (
	"context"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/metrics"
	"github.com/talktofess/job-queue/internal/store"
)

type Queue struct {
	store   store.Store
	backoff backoff.Policy
	rec     metrics.Recorder
}

func New(s store.Store, b backoff.Policy, r metrics.Recorder) *Queue {
	if r == nil {
		r = metrics.Nop{}
	}
	return &Queue{store: s, backoff: b, rec: r}
}

func (q *Queue) Enqueue(ctx context.Context, nj job.NewJob) (job.ID, error) {
	return q.store.Enqueue(ctx, nj)
}

func (q *Queue) Claim(ctx context.Context, workerID string, lease time.Duration) (*job.Job, error) {
	j, err := q.store.Claim(ctx, workerID, lease)
	if err == nil && j != nil {
		q.rec.JobClaimed()
	}
	return j, err
}

func (q *Queue) Ack(ctx context.Context, j *job.Job, took time.Duration) error {
	q.rec.JobCompleted(took)
	return q.store.Ack(ctx, j.ID)
}

// Fail applies the retry-or-dead policy after a handler error.
func (q *Queue) Fail(ctx context.Context, j *job.Job, runErr error) error {
	if j.Attempts >= j.MaxAttempts {
		q.rec.JobDead()
		return q.store.Dead(ctx, j.ID, runErr.Error())
	}
	q.rec.JobRetried()
	retryAt := time.Now().Add(q.backoff.For(j.Attempts))
	return q.store.Nack(ctx, j.ID, retryAt, runErr.Error())
}

func (q *Queue) ExtendLease(ctx context.Context, id job.ID, workerID string, until time.Time) error {
	return q.store.ExtendLease(ctx, id, workerID, until)
}

func (q *Queue) Stats(ctx context.Context) (store.Stats, error) { return q.store.Stats(ctx) }

func (q *Queue) List(ctx context.Context, state job.State, limit int) ([]job.Job, error) {
	return q.store.List(ctx, state, limit)
}

func (q *Queue) Requeue(ctx context.Context, id job.ID) error { return q.store.Requeue(ctx, id) }

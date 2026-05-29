// Package store defines the storage seam. The queue, worker, and reaper depend
// only on this interface; an in-memory and a Postgres implementation satisfy it.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/talktofess/job-queue/internal/job"
)

// ErrNotFound is returned when an operation targets a missing job.
var ErrNotFound = errors.New("job not found")

// Stats is a point-in-time snapshot used for metrics and the admin CLI.
type Stats struct {
	Pending          int
	Running          int
	Done             int
	Dead             int
	OldestPendingAge time.Duration
}

// Store is the persistence contract for the queue.
type Store interface {
	// Enqueue inserts a job. If NewJob.IdempotencyKey is set and already exists,
	// it returns the existing job's ID without inserting a duplicate.
	Enqueue(ctx context.Context, j job.NewJob) (job.ID, error)

	// Claim atomically selects one runnable job (pending, available, no running
	// job shares its key), marks it running, sets locked_until = now+lease, and
	// increments attempts. Returns (nil, nil) when nothing is runnable.
	Claim(ctx context.Context, workerID string, lease time.Duration) (*job.Job, error)

	Ack(ctx context.Context, id job.ID) error
	Nack(ctx context.Context, id job.ID, retryAt time.Time, errMsg string) error
	Dead(ctx context.Context, id job.ID, errMsg string) error
	ExtendLease(ctx context.Context, id job.ID, workerID string, until time.Time) error

	// ReapExpired returns running jobs whose lease elapsed back to pending and
	// reports how many were reclaimed.
	ReapExpired(ctx context.Context, now time.Time) (int, error)

	Stats(ctx context.Context) (Stats, error)
	List(ctx context.Context, state job.State, limit int) ([]job.Job, error)
	Requeue(ctx context.Context, id job.ID) error

	Close() error
}

// Package memory is an in-memory Store for unit/concurrency tests and the
// offline demo. No-double-claim is enforced with a mutex; the real DB-level
// concurrency guarantee is proven against the Postgres store.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/store"
)

type Store struct {
	mu   sync.Mutex
	seq  job.ID
	jobs map[job.ID]*job.Job
	idem map[string]job.ID
}

func New() *Store {
	return &Store{jobs: make(map[job.ID]*job.Job), idem: make(map[string]job.ID)}
}

func (s *Store) Enqueue(ctx context.Context, nj job.NewJob) (job.ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if nj.IdempotencyKey != "" {
		if id, ok := s.idem[nj.IdempotencyKey]; ok {
			return id, nil
		}
	}
	now := time.Now()
	avail := nj.AvailableAt
	if avail.IsZero() {
		avail = now
	}
	maxAtt := nj.MaxAttempts
	if maxAtt == 0 {
		maxAtt = job.DefaultMaxAttempts
	}
	q := nj.Queue
	if q == "" {
		q = "default"
	}
	s.seq++
	id := s.seq
	s.jobs[id] = &job.Job{
		ID:             id,
		Queue:          q,
		Payload:        nj.Payload,
		State:          job.StatePending,
		Priority:       nj.Priority,
		MaxAttempts:    maxAtt,
		Key:            nj.Key,
		IdempotencyKey: nj.IdempotencyKey,
		AvailableAt:    avail,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if nj.IdempotencyKey != "" {
		s.idem[nj.IdempotencyKey] = id
	}
	return id, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	runningKeys := make(map[string]bool)
	for _, j := range s.jobs {
		if j.State == job.StateRunning && j.Key != "" {
			runningKeys[j.Key] = true
		}
	}

	// Deterministic order: priority desc, then available_at, then id.
	candidates := make([]*job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if j.State != job.StatePending || j.AvailableAt.After(now) {
			continue
		}
		if j.Key != "" && runningKeys[j.Key] {
			continue
		}
		candidates = append(candidates, j)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(a, b int) bool {
		x, y := candidates[a], candidates[b]
		if x.Priority != y.Priority {
			return x.Priority > y.Priority
		}
		if !x.AvailableAt.Equal(y.AvailableAt) {
			return x.AvailableAt.Before(y.AvailableAt)
		}
		return x.ID < y.ID
	})

	j := candidates[0]
	j.State = job.StateRunning
	j.LockedBy = workerID
	j.LockedUntil = now.Add(lease)
	j.Attempts++
	j.UpdatedAt = now
	cp := *j
	return &cp, nil
}

func (s *Store) mutate(id job.ID, fn func(*job.Job)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	fn(j)
	j.UpdatedAt = time.Now()
	return nil
}

func (s *Store) Ack(ctx context.Context, id job.ID) error {
	return s.mutate(id, func(j *job.Job) {
		j.State = job.StateDone
		j.LockedBy = ""
		j.LockedUntil = time.Time{}
	})
}

func (s *Store) Nack(ctx context.Context, id job.ID, retryAt time.Time, errMsg string) error {
	return s.mutate(id, func(j *job.Job) {
		j.State = job.StatePending
		j.AvailableAt = retryAt
		j.LastError = errMsg
		j.LockedBy = ""
		j.LockedUntil = time.Time{}
	})
}

func (s *Store) Dead(ctx context.Context, id job.ID, errMsg string) error {
	return s.mutate(id, func(j *job.Job) {
		j.State = job.StateDead
		j.LastError = errMsg
		j.LockedBy = ""
		j.LockedUntil = time.Time{}
	})
}

func (s *Store) ExtendLease(ctx context.Context, id job.ID, workerID string, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	if j.State != job.StateRunning || j.LockedBy != workerID {
		return store.ErrNotFound
	}
	j.LockedUntil = until
	j.UpdatedAt = time.Now()
	return nil
}

func (s *Store) ReapExpired(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.jobs {
		if j.State == job.StateRunning && j.LockedUntil.Before(now) {
			j.State = job.StatePending
			j.LockedBy = ""
			j.LockedUntil = time.Time{}
			j.UpdatedAt = now
			n++
		}
	}
	return n, nil
}

func (s *Store) Stats(ctx context.Context) (store.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st store.Stats
	now := time.Now()
	var oldest time.Time
	for _, j := range s.jobs {
		switch j.State {
		case job.StatePending:
			st.Pending++
			if oldest.IsZero() || j.AvailableAt.Before(oldest) {
				oldest = j.AvailableAt
			}
		case job.StateRunning:
			st.Running++
		case job.StateDone:
			st.Done++
		case job.StateDead:
			st.Dead++
		}
	}
	if !oldest.IsZero() {
		st.OldestPendingAge = now.Sub(oldest)
	}
	return st, nil
}

func (s *Store) List(ctx context.Context, state job.State, limit int) ([]job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]job.Job, 0)
	for _, j := range s.jobs {
		if j.State == state {
			out = append(out, *j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) Requeue(ctx context.Context, id job.ID) error {
	return s.mutate(id, func(j *job.Job) {
		j.State = job.StatePending
		j.Attempts = 0
		j.AvailableAt = time.Now()
		j.LockedBy = ""
		j.LockedUntil = time.Time{}
		j.LastError = ""
	})
}

func (s *Store) Close() error { return nil }

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/store/memory"
)

func newQ() (*Queue, *memory.Store) {
	s := memory.New()
	return New(s, backoff.Policy{Base: 100 * time.Millisecond, Cap: time.Second, Jitter: 0}, nil), s
}

func TestFailRetriesBeforeMaxAttempts(t *testing.T) {
	ctx := context.Background()
	q, s := newQ()
	q.Enqueue(ctx, job.NewJob{MaxAttempts: 5, Payload: []byte("{}")})
	j, _ := q.Claim(ctx, "w", time.Minute) // attempts -> 1
	if err := q.Fail(ctx, j, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	pending, _ := s.List(ctx, job.StatePending, 10)
	if len(pending) != 1 {
		t.Fatalf("expected job back to pending, got %d", len(pending))
	}
	if !pending[0].AvailableAt.After(time.Now()) {
		t.Fatal("retry should be scheduled in the future (backoff)")
	}
	if pending[0].LastError == "" {
		t.Fatal("last_error should be recorded")
	}
}

func TestFailDeadLettersAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	q, s := newQ()
	q.Enqueue(ctx, job.NewJob{MaxAttempts: 1, Payload: []byte("{}")})
	j, _ := q.Claim(ctx, "w", time.Minute) // attempts -> 1 == max
	if err := q.Fail(ctx, j, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	dead, _ := s.List(ctx, job.StateDead, 10)
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead-lettered job, got %d", len(dead))
	}
}

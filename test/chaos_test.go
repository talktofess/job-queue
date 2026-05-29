package qtest

import (
	"context"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/reaper"
	"github.com/talktofess/job-queue/internal/store/memory"
)

// A worker claims a job then "crashes" (never acks, lease lapses). The reaper
// reclaims it and another worker finishes it. Nothing lost.
func TestCrashedWorkerJobIsReclaimed(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	q := queue.New(s, backoff.Default(), nil)
	id, _ := q.Enqueue(ctx, job.NewJob{Payload: []byte("{}")})

	ghost, _ := q.Claim(ctx, "ghost", 50*time.Millisecond)
	if ghost == nil || ghost.ID != id {
		t.Fatalf("ghost failed to claim job: %+v", ghost)
	}
	// While held, no other worker can take it.
	if other, _ := q.Claim(ctx, "live", time.Minute); other != nil {
		t.Fatalf("job should be invisible while leased, got %+v", other)
	}

	rp := reaper.New(s, time.Hour, nil, nil)
	n, err := rp.Once(ctx, time.Now().Add(time.Second)) // lease has lapsed
	if err != nil || n != 1 {
		t.Fatalf("reaper reclaimed %d (err=%v), want 1", n, err)
	}

	rescued, _ := q.Claim(ctx, "live", time.Minute)
	if rescued == nil || rescued.ID != id {
		t.Fatalf("rescued claim failed: %+v", rescued)
	}
	if err := q.Ack(ctx, rescued, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(ctx)
	if st.Done != 1 {
		t.Fatalf("expected job done after recovery, stats=%+v", st)
	}
}

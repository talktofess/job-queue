package memory

import (
	"context"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/job"
)

func TestEnqueueClaimAck(t *testing.T) {
	ctx := context.Background()
	s := New()
	id, err := s.Enqueue(ctx, job.NewJob{Queue: "default", Payload: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Claim(ctx, "w1", time.Minute)
	if err != nil || j == nil {
		t.Fatalf("claim: %v %v", j, err)
	}
	if j.ID != id || j.State != job.StateRunning || j.Attempts != 1 {
		t.Fatalf("unexpected claimed job: %+v", j)
	}
	if err := s.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	if next, _ := s.Claim(ctx, "w1", time.Minute); next != nil {
		t.Fatalf("expected no more jobs, got %+v", next)
	}
}

func TestPerKeySerialization(t *testing.T) {
	ctx := context.Background()
	s := New()
	s.Enqueue(ctx, job.NewJob{Key: "user-1", Payload: []byte("{}")})
	s.Enqueue(ctx, job.NewJob{Key: "user-1", Payload: []byte("{}")})

	first, _ := s.Claim(ctx, "w1", time.Minute)
	if first == nil {
		t.Fatal("expected first claim")
	}
	if blocked, _ := s.Claim(ctx, "w2", time.Minute); blocked != nil {
		t.Fatal("second same-key job must not be claimable while one runs")
	}
	s.Ack(ctx, first.ID)
	if second, _ := s.Claim(ctx, "w2", time.Minute); second == nil {
		t.Fatal("second job should be claimable after first is acked")
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	ctx := context.Background()
	s := New()
	id1, _ := s.Enqueue(ctx, job.NewJob{IdempotencyKey: "abc", Payload: []byte("{}")})
	id2, _ := s.Enqueue(ctx, job.NewJob{IdempotencyKey: "abc", Payload: []byte("{}")})
	if id1 != id2 {
		t.Fatalf("idempotent enqueue created duplicate: %d vs %d", id1, id2)
	}
	st, _ := s.Stats(ctx)
	if st.Pending != 1 {
		t.Fatalf("expected 1 pending, got %d", st.Pending)
	}
}

func TestReapReclaimsExpiredLease(t *testing.T) {
	ctx := context.Background()
	s := New()
	s.Enqueue(ctx, job.NewJob{Payload: []byte("{}")})
	j, _ := s.Claim(ctx, "doomed", 50*time.Millisecond)
	if j == nil {
		t.Fatal("claim failed")
	}
	n, _ := s.ReapExpired(ctx, time.Now().Add(time.Second)) // lease in the past
	if n != 1 {
		t.Fatalf("expected 1 reclaimed, got %d", n)
	}
	if again, _ := s.Claim(ctx, "rescuer", time.Minute); again == nil {
		t.Fatal("reclaimed job should be claimable again")
	}
}

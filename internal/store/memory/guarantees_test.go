package memory

import (
	"context"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/job"
)

// Guarantee: higher priority is claimed first; ties broken by availability.
func TestPriorityClaimOrder(t *testing.T) {
	ctx := context.Background()
	s := New()
	s.Enqueue(ctx, job.NewJob{Priority: 1, Payload: []byte("low")})
	s.Enqueue(ctx, job.NewJob{Priority: 5, Payload: []byte("high")})
	s.Enqueue(ctx, job.NewJob{Priority: 3, Payload: []byte("mid")})

	var got []string
	for i := 0; i < 3; i++ {
		j, err := s.Claim(ctx, "w", time.Minute)
		if err != nil || j == nil {
			t.Fatalf("claim %d: %v %v", i, j, err)
		}
		got = append(got, string(j.Payload))
	}
	want := []string{"high", "mid", "low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("priority order = %v, want %v", got, want)
		}
	}
}

// Guarantee: a delayed/scheduled job is invisible until available_at.
func TestDelayedJobNotClaimedUntilAvailable(t *testing.T) {
	ctx := context.Background()
	s := New()
	s.Enqueue(ctx, job.NewJob{AvailableAt: time.Now().Add(time.Hour), Payload: []byte("later")})

	if j, _ := s.Claim(ctx, "w", time.Minute); j != nil {
		t.Fatalf("delayed job should not be claimable yet, got %q", string(j.Payload))
	}

	s.Enqueue(ctx, job.NewJob{Payload: []byte("now")})
	j, _ := s.Claim(ctx, "w", time.Minute)
	if j == nil || string(j.Payload) != "now" {
		t.Fatalf("expected the available job, got %v", j)
	}
}

// Guarantee: at most one job per key is in the running state at a time.
func TestPerKeySerializationAtClaim(t *testing.T) {
	ctx := context.Background()
	s := New()
	for i := 0; i < 3; i++ {
		s.Enqueue(ctx, job.NewJob{Key: "user-1", Payload: []byte("k")})
	}
	first, _ := s.Claim(ctx, "w1", time.Minute)
	if first == nil {
		t.Fatal("expected first claim")
	}
	if blocked, _ := s.Claim(ctx, "w2", time.Minute); blocked != nil {
		t.Fatal("second same-key job must not be claimable while one runs")
	}
	if err := s.Ack(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if next, _ := s.Claim(ctx, "w2", time.Minute); next == nil {
		t.Fatal("next same-key job should be claimable after ack")
	}
}

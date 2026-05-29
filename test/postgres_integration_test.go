//go:build integration

// Postgres-backed proof of SKIP LOCKED correctness. Run with a database:
//   TEST_DATABASE_URL=postgres://queue:queue@localhost:5432/queue?sslmode=disable \
//     go test -tags integration ./test/...
package qtest

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/store/postgres"
)

func TestPostgresNoDoubleClaimUnderConcurrency(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run Postgres integration tests")
	}
	ctx := context.Background()
	s, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	const M = 500
	for i := 0; i < M; i++ {
		if _, err := s.Enqueue(ctx, job.NewJob{Payload: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	claimed := make(map[job.ID]int)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				j, err := s.Claim(ctx, id, time.Minute)
				if err != nil || j == nil {
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
				_ = s.Ack(ctx, j.ID)
			}
		}(string(rune('a' + w)))
	}
	wg.Wait()

	if len(claimed) != M {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), M)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("job %d claimed %d times — SKIP LOCKED violated", id, c)
		}
	}
}

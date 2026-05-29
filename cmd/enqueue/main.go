// Command enqueue is a producer CLI (Postgres-backed).
//
//	go run ./cmd/enqueue -dsn "$QUEUE_DSN" -queue default -payload '{"hello":"world"}'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/store/postgres"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("QUEUE_DSN"), "Postgres DSN")
	q := flag.String("queue", "default", "queue name")
	payload := flag.String("payload", "{}", "JSON payload")
	priority := flag.Int("priority", 0, "priority (higher first)")
	key := flag.String("key", "", "per-key serialization key")
	idem := flag.String("idem", "", "idempotency key")
	delay := flag.Duration("delay", 0, "delay before the job becomes available")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("set -dsn or QUEUE_DSN")
	}
	ctx := context.Background()
	st, err := postgres.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	qq := queue.New(st, backoff.Default(), nil)
	nj := job.NewJob{
		Queue: *q, Payload: []byte(*payload), Priority: *priority,
		Key: *key, IdempotencyKey: *idem,
	}
	if *delay > 0 {
		nj.AvailableAt = time.Now().Add(*delay)
	}
	id, err := qq.Enqueue(ctx, nj)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	fmt.Printf("enqueued job %d\n", id)
}

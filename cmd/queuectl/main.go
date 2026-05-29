// Command queuectl is the admin CLI (Postgres-backed): inspect and operate the
// queue without touching SQL by hand.
//
//	go run ./cmd/queuectl -dsn "$QUEUE_DSN" stats
//	go run ./cmd/queuectl -dsn "$QUEUE_DSN" list -state dead
//	go run ./cmd/queuectl -dsn "$QUEUE_DSN" requeue -id 42
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/store/postgres"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("QUEUE_DSN"), "Postgres DSN")
	flag.Parse()
	args := flag.Args()
	if *dsn == "" {
		log.Fatal("set -dsn or QUEUE_DSN")
	}
	if len(args) == 0 {
		log.Fatal("usage: queuectl [stats|list|requeue] ...")
	}

	ctx := context.Background()
	st, err := postgres.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer st.Close()
	q := queue.New(st, backoff.Default(), nil)

	switch args[0] {
	case "stats":
		s, err := q.Stats(ctx)
		must(err)
		fmt.Printf("pending=%d running=%d done=%d dead=%d oldest_pending=%s\n",
			s.Pending, s.Running, s.Done, s.Dead, s.OldestPendingAge)
	case "list":
		fs := flag.NewFlagSet("list", flag.ExitOnError)
		state := fs.String("state", "pending", "state to list")
		limit := fs.Int("limit", 50, "max rows")
		fs.Parse(args[1:])
		jobs, err := q.List(ctx, job.State(*state), *limit)
		must(err)
		for _, j := range jobs {
			fmt.Printf("#%d queue=%s state=%s attempts=%d/%d key=%q err=%q\n",
				j.ID, j.Queue, j.State, j.Attempts, j.MaxAttempts, j.Key, j.LastError)
		}
	case "requeue":
		fs := flag.NewFlagSet("requeue", flag.ExitOnError)
		id := fs.Int64("id", 0, "job id")
		fs.Parse(args[1:])
		must(q.Requeue(ctx, job.ID(*id)))
		fmt.Printf("requeued job %d\n", *id)
	default:
		log.Fatalf("unknown command %q", args[0])
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Command worker runs a Postgres-backed worker pool + reaper + Prometheus
// metrics endpoint.
//
//	go run ./cmd/worker -dsn "postgres://queue:queue@localhost:5432/queue?sslmode=disable" -workers 4
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/talktofess/job-queue/internal/backoff"
	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/prommetrics"
	"github.com/talktofess/job-queue/internal/queue"
	"github.com/talktofess/job-queue/internal/reaper"
	"github.com/talktofess/job-queue/internal/store/postgres"
	"github.com/talktofess/job-queue/internal/worker"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("QUEUE_DSN"), "Postgres DSN")
	workers := flag.Int("workers", 4, "number of worker goroutines")
	lease := flag.Duration("lease", 30*time.Second, "visibility timeout")
	reapEvery := flag.Duration("reap-every", 5*time.Second, "reaper interval")
	metricsAddr := flag.String("metrics-addr", ":2112", "Prometheus metrics listen address")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("set -dsn or QUEUE_DSN")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := postgres.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	rec := prommetrics.New()
	q := queue.New(st, backoff.Default(), rec)

	go rec.PollDepth(ctx, st, 2*time.Second)
	go func() {
		http.Handle("/metrics", rec.Handler())
		log.Printf("metrics on %s/metrics", *metricsAddr)
		_ = http.ListenAndServe(*metricsAddr, nil)
	}()

	rp := reaper.New(st, *reapEvery, rec, func(msg string) { log.Printf("reaper: %s", msg) })
	go rp.Run(ctx)

	reg := worker.NewRegistry()
	// Sample handler: replace with real work. Idempotent + honors ctx.
	reg.Register("default", worker.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		log.Printf("handling job %d: %s", j.ID, string(j.Payload))
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	opts := worker.DefaultOptions()
	opts.Lease = *lease
	opts.OnEvent = func(msg string) { log.Print(msg) }
	log.Printf("starting %d workers", *workers)
	worker.RunPool(ctx, *workers, func(i int) *worker.Worker {
		return worker.New(fmt.Sprintf("worker-%d", i), q, reg, opts)
	})
	log.Println("shut down cleanly")
}

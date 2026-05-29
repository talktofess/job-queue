// Package reaper returns jobs whose lease expired (crashed workers) back to
// pending so another worker can reclaim them.
package reaper

import (
	"context"
	"time"

	"github.com/talktofess/job-queue/internal/metrics"
	"github.com/talktofess/job-queue/internal/store"
)

type Reaper struct {
	store    store.Store
	interval time.Duration
	rec      metrics.Recorder
	onEvent  func(string)
}

func New(s store.Store, interval time.Duration, rec metrics.Recorder, onEvent func(string)) *Reaper {
	if rec == nil {
		rec = metrics.Nop{}
	}
	return &Reaper{store: s, interval: interval, rec: rec, onEvent: onEvent}
}

// Run reaps on a ticker until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			n, err := r.store.ReapExpired(ctx, now)
			if err != nil {
				if r.onEvent != nil {
					r.onEvent("reaper error: " + err.Error())
				}
				continue
			}
			if n > 0 {
				r.rec.JobReaped(n)
				if r.onEvent != nil {
					r.onEvent("reaper reclaimed expired jobs")
				}
			}
		}
	}
}

// Once runs a single reap pass (useful for tests and on-demand admin).
func (r *Reaper) Once(ctx context.Context, now time.Time) (int, error) {
	n, err := r.store.ReapExpired(ctx, now)
	if err == nil && n > 0 {
		r.rec.JobReaped(n)
	}
	return n, err
}

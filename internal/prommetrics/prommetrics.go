// Package prommetrics implements metrics.Recorder with Prometheus and exposes
// a /metrics handler plus a poller that publishes queue-depth gauges.
package prommetrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/talktofess/job-queue/internal/store"
)

type Recorder struct {
	claimed    prometheus.Counter
	retried    prometheus.Counter
	dead       prometheus.Counter
	reaped     prometheus.Counter
	duration   prometheus.Histogram
	depth      *prometheus.GaugeVec
	oldestPend prometheus.Gauge
}

func New() *Recorder {
	r := &Recorder{
		claimed:  prometheus.NewCounter(prometheus.CounterOpts{Name: "jobs_claimed_total", Help: "Jobs claimed"}),
		retried:  prometheus.NewCounter(prometheus.CounterOpts{Name: "jobs_retried_total", Help: "Jobs retried"}),
		dead:     prometheus.NewCounter(prometheus.CounterOpts{Name: "jobs_dead_total", Help: "Jobs dead-lettered"}),
		reaped:   prometheus.NewCounter(prometheus.CounterOpts{Name: "jobs_reaped_total", Help: "Expired leases reclaimed"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "job_duration_seconds", Help: "Handler duration", Buckets: prometheus.DefBuckets}),
		depth:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_depth", Help: "Jobs by state"}, []string{"state"}),
		oldestPend: prometheus.NewGauge(prometheus.GaugeOpts{Name: "oldest_pending_age_seconds", Help: "Age of oldest pending job"}),
	}
	prometheus.MustRegister(r.claimed, r.retried, r.dead, r.reaped, r.duration, r.depth, r.oldestPend)
	return r
}

func (r *Recorder) JobClaimed()                 { r.claimed.Inc() }
func (r *Recorder) JobCompleted(d time.Duration) { r.duration.Observe(d.Seconds()) }
func (r *Recorder) JobRetried()                 { r.retried.Inc() }
func (r *Recorder) JobDead()                    { r.dead.Inc() }
func (r *Recorder) JobReaped(n int)             { r.reaped.Add(float64(n)) }

func (r *Recorder) Handler() http.Handler { return promhttp.Handler() }

// PollDepth periodically publishes queue-depth gauges from Stats.
func (r *Recorder) PollDepth(ctx context.Context, s store.Store, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st, err := s.Stats(ctx)
			if err != nil {
				continue
			}
			r.depth.WithLabelValues("pending").Set(float64(st.Pending))
			r.depth.WithLabelValues("running").Set(float64(st.Running))
			r.depth.WithLabelValues("done").Set(float64(st.Done))
			r.depth.WithLabelValues("dead").Set(float64(st.Dead))
			r.oldestPend.Set(st.OldestPendingAge.Seconds())
		}
	}
}

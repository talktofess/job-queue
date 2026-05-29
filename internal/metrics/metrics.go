// Package metrics defines the recorder interface the core depends on. The
// Prometheus implementation lives in package prommetrics so the core stays
// dependency-free (and unit-testable without external modules).
package metrics

import "time"

type Recorder interface {
	JobClaimed()
	JobCompleted(d time.Duration)
	JobRetried()
	JobDead()
	JobReaped(n int)
}

// Nop is a no-op Recorder, used when metrics are not wired.
type Nop struct{}

func (Nop) JobClaimed()              {}
func (Nop) JobCompleted(time.Duration) {}
func (Nop) JobRetried()              {}
func (Nop) JobDead()                 {}
func (Nop) JobReaped(int)            {}

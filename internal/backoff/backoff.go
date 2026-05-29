// Package backoff computes retry delays: exponential growth with a cap and
// optional jitter to avoid thundering herds.
package backoff

import (
	"math"
	"math/rand"
	"time"
)

type Policy struct {
	Base   time.Duration // delay for the first retry
	Cap    time.Duration // maximum delay
	Jitter float64       // 0..1 fraction of the delay applied as +/- jitter
}

func Default() Policy {
	return Policy{Base: time.Second, Cap: 5 * time.Minute, Jitter: 0.2}
}

// For returns the delay before retry number `attempt` (1-based).
func (p Policy) For(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.Base) * math.Pow(2, float64(attempt-1))
	if cap := float64(p.Cap); p.Cap > 0 && d > cap {
		d = cap
	}
	if p.Jitter > 0 {
		j := d * p.Jitter
		d = d - j + rand.Float64()*2*j
	}
	return time.Duration(d)
}

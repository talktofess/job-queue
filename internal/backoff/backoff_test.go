package backoff

import (
	"testing"
	"time"
)

func TestExponentialGrowthAndCap(t *testing.T) {
	p := Policy{Base: time.Second, Cap: 4 * time.Second, Jitter: 0}
	cases := map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 4 * time.Second}
	for attempt, want := range cases {
		if got := p.For(attempt); got != want {
			t.Errorf("For(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestJitterStaysInBounds(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute, Jitter: 0.2}
	for i := 0; i < 100; i++ {
		got := p.For(1)
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("jittered delay %s out of [0.8s,1.2s]", got)
		}
	}
}

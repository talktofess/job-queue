// Package job holds the queue's domain types.
package job

import "time"

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
	StateDead    State = "dead"
)

// ID is a job identifier.
type ID int64

// Job is a unit of work and its current state.
type Job struct {
	ID             ID
	Queue          string
	Payload        []byte
	State          State
	Priority       int
	Attempts       int
	MaxAttempts    int
	Key            string // per-key serialization; "" means no key
	IdempotencyKey string // enqueue-side dedupe; "" means none
	AvailableAt    time.Time
	LockedBy       string
	LockedUntil    time.Time
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewJob is the input to Enqueue.
type NewJob struct {
	Queue          string
	Payload        []byte
	Priority       int
	MaxAttempts    int // 0 => default (5)
	Key            string
	IdempotencyKey string
	AvailableAt    time.Time // zero => now (use for delayed/scheduled jobs)
}

const DefaultMaxAttempts = 5

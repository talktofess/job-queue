package worker

import (
	"context"

	"github.com/talktofess/job-queue/internal/job"
)

// Handler runs a job. Returning an error triggers retry-or-dead policy.
// Handlers MUST be idempotent: at-least-once delivery means a handler can run
// more than once for the same job.
type Handler interface {
	Handle(ctx context.Context, j *job.Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, j *job.Job) error

func (f HandlerFunc) Handle(ctx context.Context, j *job.Job) error { return f(ctx, j) }

// Registry maps queue names to handlers.
type Registry struct {
	m map[string]Handler
}

func NewRegistry() *Registry { return &Registry{m: make(map[string]Handler)} }

func (r *Registry) Register(queue string, h Handler) { r.m[queue] = h }

func (r *Registry) Get(queue string) (Handler, bool) {
	h, ok := r.m[queue]
	return h, ok
}

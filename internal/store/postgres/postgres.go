// Package postgres is the production Store. Concurrency safety on Claim comes
// from SELECT ... FOR UPDATE SKIP LOCKED: concurrent claimers lock their
// candidate row and skip rows already locked, so no two workers claim the same
// job, with no thundering-herd contention.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/talktofess/job-queue/internal/job"
	"github.com/talktofess/job-queue/internal/store"
)

//go:embed migrations/0001_init.sql
var initSQL string

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Migrate applies the schema (idempotent).
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, initSQL)
	return err
}

const cols = `id, queue, payload, state, priority, attempts, max_attempts,
	key, idempotency_key, available_at, locked_by, locked_until, last_error,
	created_at, updated_at`

func scanJob(row pgx.Row) (*job.Job, error) {
	var (
		j           job.Job
		state       string
		key         sql.NullString
		idem        sql.NullString
		lockedBy    sql.NullString
		lockedUntil sql.NullTime
		lastErr     sql.NullString
	)
	err := row.Scan(
		&j.ID, &j.Queue, &j.Payload, &state, &j.Priority, &j.Attempts, &j.MaxAttempts,
		&key, &idem, &j.AvailableAt, &lockedBy, &lockedUntil, &lastErr,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	j.State = job.State(state)
	j.Key = key.String
	j.IdempotencyKey = idem.String
	j.LockedBy = lockedBy.String
	if lockedUntil.Valid {
		j.LockedUntil = lockedUntil.Time
	}
	j.LastError = lastErr.String
	return &j, nil
}

func (s *Store) Enqueue(ctx context.Context, nj job.NewJob) (job.ID, error) {
	q := nj.Queue
	if q == "" {
		q = "default"
	}
	maxAtt := nj.MaxAttempts
	if maxAtt == 0 {
		maxAtt = job.DefaultMaxAttempts
	}
	avail := nj.AvailableAt
	if avail.IsZero() {
		avail = time.Now()
	}
	var keyArg, idemArg *string
	if nj.Key != "" {
		keyArg = &nj.Key
	}
	if nj.IdempotencyKey != "" {
		idemArg = &nj.IdempotencyKey
	}

	const sqlStr = `
		INSERT INTO jobs (queue, payload, priority, max_attempts, key, idempotency_key, available_at, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
		ON CONFLICT (idempotency_key) DO UPDATE SET updated_at = now()
		RETURNING id`
	var id int64
	err := s.pool.QueryRow(ctx, sqlStr, q, nj.Payload, nj.Priority, maxAtt, keyArg, idemArg, avail).Scan(&id)
	return job.ID(id), err
}

func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*job.Job, error) {
	const sqlStr = `
		WITH claimed AS (
			SELECT id FROM jobs
			WHERE state = 'pending'
			  AND available_at <= now()
			  AND (key IS NULL OR NOT EXISTS (
			        SELECT 1 FROM jobs r WHERE r.key = jobs.key AND r.state = 'running'))
			ORDER BY priority DESC, available_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		   SET state = 'running', locked_by = $1,
		       locked_until = now() + make_interval(secs => $2),
		       attempts = attempts + 1, updated_at = now()
		  FROM claimed
		 WHERE jobs.id = claimed.id
		RETURNING ` + cols
	j, err := scanJob(s.pool.QueryRow(ctx, sqlStr, workerID, lease.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (s *Store) exec(ctx context.Context, sqlStr string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) Ack(ctx context.Context, id job.ID) error {
	return s.exec(ctx, `UPDATE jobs SET state='done', locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, int64(id))
}

func (s *Store) Nack(ctx context.Context, id job.ID, retryAt time.Time, errMsg string) error {
	return s.exec(ctx, `UPDATE jobs SET state='pending', available_at=$2, last_error=$3,
		locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, int64(id), retryAt, errMsg)
}

func (s *Store) Dead(ctx context.Context, id job.ID, errMsg string) error {
	return s.exec(ctx, `UPDATE jobs SET state='dead', last_error=$2,
		locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, int64(id), errMsg)
}

func (s *Store) ExtendLease(ctx context.Context, id job.ID, workerID string, until time.Time) error {
	return s.exec(ctx, `UPDATE jobs SET locked_until=$3, updated_at=now()
		WHERE id=$1 AND locked_by=$2 AND state='running'`, int64(id), workerID, until)
}

func (s *Store) ReapExpired(ctx context.Context, _ time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE jobs SET state='pending', locked_by=NULL,
		locked_until=NULL, updated_at=now() WHERE state='running' AND locked_until < now()`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) Stats(ctx context.Context) (store.Stats, error) {
	const sqlStr = `
		SELECT
			count(*) FILTER (WHERE state='pending'),
			count(*) FILTER (WHERE state='running'),
			count(*) FILTER (WHERE state='done'),
			count(*) FILTER (WHERE state='dead'),
			EXTRACT(EPOCH FROM now() - min(available_at) FILTER (WHERE state='pending'))
		FROM jobs`
	var st store.Stats
	var oldestSec sql.NullFloat64
	err := s.pool.QueryRow(ctx, sqlStr).Scan(&st.Pending, &st.Running, &st.Done, &st.Dead, &oldestSec)
	if err != nil {
		return st, err
	}
	if oldestSec.Valid {
		st.OldestPendingAge = time.Duration(oldestSec.Float64 * float64(time.Second))
	}
	return st, nil
}

func (s *Store) List(ctx context.Context, state job.State, limit int) ([]job.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+cols+` FROM jobs WHERE state=$1 ORDER BY id LIMIT $2`,
		string(state), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (s *Store) Requeue(ctx context.Context, id job.ID) error {
	return s.exec(ctx, `UPDATE jobs SET state='pending', attempts=0, available_at=now(),
		locked_by=NULL, locked_until=NULL, last_error=NULL, updated_at=now() WHERE id=$1`, int64(id))
}

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

DO $$ BEGIN
    CREATE TYPE job_state AS ENUM ('pending','running','done','failed','dead');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT        NOT NULL DEFAULT 'default',
    payload         BYTEA       NOT NULL,
    state           job_state   NOT NULL DEFAULT 'pending',
    priority        INT         NOT NULL DEFAULT 0,
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 5,
    key             TEXT,
    idempotency_key TEXT UNIQUE,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_runnable ON jobs (state, available_at, priority DESC)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (locked_until)
    WHERE state = 'running';
CREATE INDEX IF NOT EXISTS idx_jobs_key_running ON jobs (key)
    WHERE state = 'running';

-- Consumer-side exactly-once-effect: handlers mark a key here in the same tx
-- as their effect; a redelivery hits the UNIQUE conflict and no-ops.
CREATE TABLE IF NOT EXISTS processed (
    idempotency_key TEXT PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

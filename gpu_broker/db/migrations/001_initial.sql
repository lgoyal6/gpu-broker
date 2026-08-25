-- Phase 0. Queue, ledger, and the user-to-resource mapping.
--
-- Money and GPU-hours are TEXT, holding the string form of a Decimal. SQLite's
-- REAL is a binary float and would drift by fractions of a cent over a month of
-- accrual. TEXT sorts wrong but we never sort on it; we aggregate in Python.
--
-- Timestamps are TEXT, ISO-8601 with microseconds, always UTC. Fixed width, so
-- lexical order is chronological order.

CREATE TABLE users (
    user_id          TEXT PRIMARY KEY,
    display_name     TEXT NOT NULL,
    budget_usd       TEXT NOT NULL,
    budget_gpu_hours TEXT NOT NULL,
    is_admin         INTEGER NOT NULL DEFAULT 0 CHECK (is_admin IN (0, 1)),
    created_at       TEXT NOT NULL
);

CREATE TABLE jobs (
    job_id          TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(user_id),
    command         TEXT NOT NULL,
    gpu_type        TEXT NOT NULL,
    requested_hours REAL NOT NULL CHECK (requested_hours > 0),
    currency        TEXT NOT NULL CHECK (currency IN ('USD', 'GPU_HOUR')),
    reserved        TEXT NOT NULL,
    state           TEXT NOT NULL,
    backend         TEXT,
    backend_handle  TEXT,
    submitted_at    TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    exit_code       INTEGER,
    refusal_reason  TEXT,
    updated_at      TEXT NOT NULL
);

CREATE INDEX idx_jobs_state       ON jobs(state);
CREATE INDEX idx_jobs_user        ON jobs(user_id, state);
CREATE INDEX idx_jobs_submitted   ON jobs(submitted_at);

-- A backend handle belongs to exactly one job. This is the constraint that makes
-- reconciliation meaningful: if two rows claimed the same instance, "orphan"
-- would be undecidable.
CREATE UNIQUE INDEX idx_jobs_handle
    ON jobs(backend, backend_handle)
    WHERE backend_handle IS NOT NULL;

-- Append-only. Every state change a job ever made, in order.
CREATE TABLE job_transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL REFERENCES jobs(job_id),
    from_state TEXT,
    to_state   TEXT NOT NULL,
    at         TEXT NOT NULL,
    reason     TEXT
);

CREATE INDEX idx_transitions_job ON job_transitions(job_id, id);

-- Append-only. Balances are derived from this, never stored.
--
-- Deriving rather than storing means a crash can lose at most the transaction
-- in flight, and can never leave a balance that disagrees with its own history.
CREATE TABLE ledger (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id   TEXT REFERENCES jobs(job_id),
    user_id  TEXT NOT NULL REFERENCES users(user_id),
    currency TEXT NOT NULL CHECK (currency IN ('USD', 'GPU_HOUR')),
    kind     TEXT NOT NULL CHECK (kind IN ('RESERVE', 'RELEASE', 'SETTLE')),
    amount   TEXT NOT NULL,
    period   TEXT NOT NULL,
    at       TEXT NOT NULL,
    note     TEXT
);

CREATE INDEX idx_ledger_account ON ledger(user_id, period, currency);
CREATE INDEX idx_ledger_job     ON ledger(job_id);
CREATE INDEX idx_ledger_at      ON ledger(at);
CREATE INDEX idx_ledger_period  ON ledger(period, currency);

CREATE TABLE job_logs (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    at     TEXT NOT NULL,
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr', 'broker')),
    line   TEXT NOT NULL
);

CREATE INDEX idx_logs_job ON job_logs(job_id, id);

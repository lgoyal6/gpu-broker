-- Phase 3: what capacity costs, and what it is actually doing.

-- Prices, refreshed from the AWS pricing API rather than hardcoded.
--
-- Stored rather than fetched on demand: the pricing API is slow, rate limited,
-- and occasionally unavailable, and a broker that cannot price a job is a
-- broker that cannot admit one. Every row records where the number came from
-- and when, so a stale price is visible as a stale price rather than as a
-- confident wrong answer.
CREATE TABLE prices (
    gpu_type       TEXT PRIMARY KEY,
    instance_type  TEXT NOT NULL DEFAULT '',
    region         TEXT NOT NULL DEFAULT '',
    hourly         TEXT NOT NULL,
    currency       TEXT NOT NULL CHECK (currency IN ('USD', 'GPU_HOUR')),
    source         TEXT NOT NULL,
    priced_at      TEXT NOT NULL
);

-- Every utilization sample the broker has taken of a running job.
--
-- Append-only, and kept after the job ends. The point of storing them is that
-- somebody whose job was reclaimed can be shown the evidence; deleting them
-- with the job would mean the reclaim is unauditable exactly when it matters.
CREATE TABLE gpu_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(job_id),
    at          TEXT NOT NULL,
    gpu_percent REAL NOT NULL,
    memory_mb   INTEGER NOT NULL DEFAULT 0,
    source      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_samples_job ON gpu_samples(job_id, id);
CREATE INDEX idx_samples_at  ON gpu_samples(at);

-- What the broker has told somebody, and when.
--
-- A job is never reclaimed without a notification recorded here first. That is
-- the whole safety property: if this table has no row, the reclaim did not
-- happen, and the check is a lookup rather than a promise.
CREATE TABLE notifications (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    TEXT REFERENCES jobs(job_id),
    user_id   TEXT NOT NULL,
    kind      TEXT NOT NULL,
    message   TEXT NOT NULL,
    at        TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    seen_at   TEXT
);

CREATE INDEX idx_notifications_user ON notifications(user_id, id);
CREATE INDEX idx_notifications_job  ON notifications(job_id, id);

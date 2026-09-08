-- Disposable scheduler observations for the operational cost study.
--
-- Durable job state, transitions, and money remain in their existing tables.
-- This table records the scheduler's decision at each real tick so blocked
-- decisions are measurable without scraping logs. It intentionally stores no
-- user identifier, command, display name, or backend handle.
CREATE TABLE schedule_observations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    at                  TEXT NOT NULL,
    decision            TEXT NOT NULL,
    resource_class      TEXT NOT NULL,
    requested_memory_mb INTEGER NOT NULL CHECK (requested_memory_mb >= 0),
    backend              TEXT,
    tier                 TEXT,
    detail               TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_schedule_observations_job ON schedule_observations(job_id, id);
CREATE INDEX idx_schedule_observations_at ON schedule_observations(at);

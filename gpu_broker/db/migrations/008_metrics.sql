-- Phase 7: the numbers, over time.
--
-- Adoption is not stored here and does not need to be. Distinct users, jobs
-- submitted, jobs completed, repeat users, dollars spent and dollars reclaimed
-- are all derivable from `jobs`, `job_transitions` and `ledger`, which have
-- recorded every one of those events since the first deploy. That is what
-- "cannot be backfilled" means: the events had to exist from day one, and they
-- did.
--
-- What is NOT derivable is a series. How deep the queue was at three o'clock
-- last Tuesday is gone unless somebody wrote it down at three o'clock last
-- Tuesday. That is what this table is for.

CREATE TABLE metrics (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name   TEXT NOT NULL,
    at     TEXT NOT NULL,
    value  REAL NOT NULL,
    labels TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_metrics_name_at ON metrics(name, at);
CREATE INDEX idx_metrics_at      ON metrics(at);

-- What the club spent before the broker existed, for the measurement report.
--
-- Normally pulled from Cost Explorer. Stored because Cost Explorer is slow, and
-- because a club whose IAM user lacks ce:GetCostAndUsage can put the numbers in
-- by hand rather than having no baseline at all.
CREATE TABLE baseline (
    period         TEXT PRIMARY KEY,
    instance_hours REAL NOT NULL DEFAULT 0,
    dollars        TEXT NOT NULL DEFAULT '0',
    source         TEXT NOT NULL DEFAULT '',
    recorded_at    TEXT NOT NULL
);

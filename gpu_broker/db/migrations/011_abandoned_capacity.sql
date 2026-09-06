-- Capacity that really billed with no completed work behind it.
--
-- A crash between `backend.launch()` and the store write leaves an instance
-- alive and billing while the job row is still QUEUED. `reap` has always been
-- able to see that machine, name the member who launched it, and compute what
-- it has burned. It had nowhere to write the number down, so the club's ledger
-- said $0.00 for hours that AWS invoiced.
--
-- Three changes, one rebuild. SQLite cannot alter a CHECK constraint or drop a
-- NOT NULL in place, so the table is rebuilt the long way.
--
--   ABANDONED     A fourth kind. Not SETTLE: settled spend bought something a
--                 job produced, abandoned spend bought nothing anybody can
--                 point at. Merging them makes "what did we get for the money"
--                 unanswerable, which is the question the ledger exists to
--                 answer. Both count as spend; only one is defensible.
--
--   resource_handle
--                 Which machine a row is about. `reap` runs on a schedule and
--                 recomputes burn since launch every pass, so the passes
--                 overlap. Recording the delta against what this handle has
--                 already been charged makes a re-run idempotent, the same way
--                 accrual is idempotent against `job_spend`. Without it a daily
--                 reap turns one leaked instance into a month of phantom spend.
--
--   user_id NULL  An untagged instance is capacity the club paid for with
--                 nobody behind it. There is no member to charge and the money
--                 is still gone, so the row carries no user and still lands in
--                 the pool total. Dropping it instead would understate the pool
--                 by exactly the amount most worth looking at.

CREATE TABLE ledger_new (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT REFERENCES jobs(job_id),
    user_id         TEXT REFERENCES users(user_id),
    currency        TEXT NOT NULL CHECK (currency IN ('USD', 'GPU_HOUR')),
    kind            TEXT NOT NULL CHECK (kind IN ('RESERVE', 'RELEASE', 'SETTLE', 'ABANDONED')),
    amount          TEXT NOT NULL,
    period          TEXT NOT NULL,
    at              TEXT NOT NULL,
    note            TEXT,
    resource_handle TEXT
);

INSERT INTO ledger_new (id, job_id, user_id, currency, kind, amount, period, at, note)
SELECT id, job_id, user_id, currency, kind, amount, period, at, note FROM ledger;

DROP TABLE ledger;
ALTER TABLE ledger_new RENAME TO ledger;

CREATE INDEX idx_ledger_account ON ledger(user_id, period, currency);
CREATE INDEX idx_ledger_job     ON ledger(job_id);
CREATE INDEX idx_ledger_at      ON ledger(at);
CREATE INDEX idx_ledger_period  ON ledger(period, currency);

-- The lookup `reap` does on every pass: what has this machine already been
-- charged for.
CREATE INDEX idx_ledger_handle  ON ledger(resource_handle, currency);

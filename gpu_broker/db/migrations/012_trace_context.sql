-- Where a job's W3C trace context lives across the process boundary.
--
-- `gpu web` and `gpu run` are separate processes that only ever meet through a
-- row in `jobs`. That row is the queue hop, so it is also the only place a
-- trace context can travel: the submitting process writes the traceparent it
-- was working under, and the scheduler that later picks the job up makes its
-- own span a child of that one. Without this, the two halves of a submission
-- are two unrelated traces and "where did the request go" has no answer.
--
-- Its own table rather than a column on `jobs` for two reasons. Losing a
-- traceparent is not a correctness failure, so it must not be able to fail a
-- job admission; and the row is disposable, which a column on the ledger-bound
-- jobs table would not be.
CREATE TABLE trace_context (
    job_id      TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
    traceparent TEXT NOT NULL,
    at          TEXT NOT NULL
);

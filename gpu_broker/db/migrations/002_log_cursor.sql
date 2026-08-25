-- How much of a job's backend output the broker has already stored.
--
-- Before this, the offset passed to `Backend.fetch_logs` was the number of log
-- rows on the job. That silently conflated two different sequences: rows the
-- broker wrote itself ("dispatched to ec2/i-...") and rows that came back from
-- the backend. Every broker-authored line pushed the cursor forward by one and
-- skipped a real line of the user's output.
--
-- A column, not a derivation, because it has to survive a restart: on the way
-- back up the broker must resume reading the job's output where it stopped,
-- not from the beginning and not from a guess.

ALTER TABLE jobs ADD COLUMN logs_fetched INTEGER NOT NULL DEFAULT 0;

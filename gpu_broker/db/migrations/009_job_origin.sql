-- Where a job came from: a real club member, the pilot, or seeded demo data.
--
-- Demo data does not live here at all; it goes in a separate database file, so
-- that "is this row real" is never a question the query has to remember to ask.
-- The 'seeded' value exists so that a demo database is self-describing when you
-- are looking at one, not so that seeded rows can share a table with real ones.
--
-- 'pilot' is different. Those are real runs on real hardware, recorded the way
-- a club job is; the tag only says the pool had one user at the time, which is
-- something the public page should say out loud rather than let a reader assume
-- twenty people were queueing.
ALTER TABLE jobs ADD COLUMN origin TEXT NOT NULL DEFAULT 'real';

CREATE INDEX IF NOT EXISTS jobs_origin ON jobs (origin);

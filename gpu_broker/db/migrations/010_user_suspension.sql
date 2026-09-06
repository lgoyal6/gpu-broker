-- Membership, where the thing that spends money can see it.
--
-- Before this, "is this person still in the club" was a question only the web
-- app could answer, and it asked it once, at sign-in. A session lasts fourteen
-- days and `gpu run` never imports FastAPI, so a job queued before somebody was
-- removed was dispatched afterwards on the club's credits.
--
-- Suspension rather than deletion: rows for a member's past jobs point at this
-- table, the ledger has to keep adding up, and somebody removed by mistake on a
-- Friday should get their queue position back rather than lose it.
ALTER TABLE users ADD COLUMN suspended_at     TEXT;
ALTER TABLE users ADD COLUMN suspended_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN suspended_by     TEXT NOT NULL DEFAULT '';

-- The scheduler asks "is this job's owner suspended" for every queued job on
-- every tick. Partial, because the answer is no for almost everybody and an
-- index over the whole table would be mostly empty rows.
CREATE INDEX IF NOT EXISTS users_suspended ON users (user_id) WHERE suspended_at IS NOT NULL;

-- Phase 4: what survives a preemption.

-- A job now has a history rather than a single run. It can be interrupted,
-- requeued, and started again on different hardware, and the broker has to know
-- how many times that has happened and how far it got.

ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN preemptions INTEGER NOT NULL DEFAULT 0;

-- Set once a job has been preempted repeatedly without saving anything. After
-- that it stops being offered spot, because paying twice to redo the same hour
-- costs more than on-demand would have.
ALTER TABLE jobs ADD COLUMN pinned_tier TEXT;

-- The last checkpoint the broker knows about. Duplicated from the checkpoint
-- store on purpose: deciding whether a preempted job made progress happens on
-- every tick, and it must not require a round trip to S3.
ALTER TABLE jobs ADD COLUMN checkpoint_step INTEGER;
ALTER TABLE jobs ADD COLUMN checkpoint_key TEXT;
ALTER TABLE jobs ADD COLUMN checkpoint_at TEXT;

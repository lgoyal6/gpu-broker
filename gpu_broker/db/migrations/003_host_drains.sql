-- Hosts an admin has taken out of service by hand.
--
-- This has to be on disk. Every CLI invocation is a new process, so a drain
-- held in the backend's memory is forgotten the instant `gpu admin drain`
-- returns -- the command appears to work and changes nothing.
--
-- Only manual drains live here. Automatic ones come from the health check and
-- are re-derived on every run, which is the point: a host that fixed itself
-- should come back without anybody remembering to say so.

CREATE TABLE host_drains (
    hostname  TEXT PRIMARY KEY,
    reason    TEXT NOT NULL,
    drained_at TEXT NOT NULL,
    drained_by TEXT NOT NULL DEFAULT ''
);

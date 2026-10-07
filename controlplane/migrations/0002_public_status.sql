-- 0002: the only thing the public status service may read (ADR 0008).
--
-- The status process logs in as a role that is a member of gpub_status, which
-- has SELECT on these two views and nothing else. Aggregates count only pilot
-- and real tenants (ADR 0009), and outcome counts are NULL whenever fewer
-- than three tenants contributed in the window, so a single tenant's activity
-- cannot be read off the page.

CREATE VIEW public_pool_status_v1 AS
WITH window_jobs AS (
    SELECT r.pool_id, j.state, j.tenant_id, j.evidence_class
    FROM jobs j
    -- A retried job has one reservation per attempt; count it once, in the
    -- pool of its last attempt.
    JOIN LATERAL (SELECT pool_id FROM reservations WHERE job_id = j.id
                  ORDER BY created_at DESC, id DESC LIMIT 1) r ON true
    WHERE j.finished_at >= now() - interval '7 days'
      AND j.evidence_class IN ('pilot', 'real')
),
per_pool AS (
    SELECT pool_id,
           count(DISTINCT tenant_id)                           AS tenants_7d,
           count(*) FILTER (WHERE state = 'SUCCEEDED')        AS succeeded_7d,
           count(*) FILTER (WHERE state = 'FAILED')           AS failed_7d,
           count(*) FILTER (WHERE state = 'CANCELLED')        AS cancelled_7d,
           count(*) FILTER (WHERE state = 'EXPIRED')          AS expired_7d,
           count(*) FILTER (WHERE evidence_class = 'pilot')   AS pilot_7d,
           count(*) FILTER (WHERE evidence_class = 'real')    AS real_7d
    FROM window_jobs GROUP BY pool_id
)
SELECT p.name                                                        AS pool,
       p.region                                                      AS region,
       count(w.id) FILTER (WHERE w.state = 'READY')                  AS workers_ready,
       coalesce(sum(w.gpus) FILTER (WHERE w.state <> 'OFFLINE'), 0)  AS gpus_total,
       coalesce(sum(w.gpus_reserved), 0)                             AS gpus_reserved,
       max(w.last_heartbeat)                                         AS newest_heartbeat,
       coalesce(pp.tenants_7d, 0)                                    AS tenants_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.succeeded_7d END AS succeeded_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.failed_7d END    AS failed_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.cancelled_7d END AS cancelled_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.expired_7d END   AS expired_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.pilot_7d END     AS pilot_jobs_7d,
       CASE WHEN coalesce(pp.tenants_7d, 0) >= 3 THEN pp.real_jobs_7d END AS real_jobs_7d,
       now()                                                         AS generated_at
FROM pools p
LEFT JOIN workers w ON w.pool_id = p.id
LEFT JOIN (SELECT pool_id, tenants_7d, succeeded_7d, failed_7d, cancelled_7d, expired_7d, pilot_7d,
                  real_7d AS real_jobs_7d FROM per_pool) pp ON pp.pool_id = p.id
GROUP BY p.name, p.region, pp.tenants_7d, pp.succeeded_7d, pp.failed_7d, pp.cancelled_7d,
         pp.expired_7d, pp.pilot_7d, pp.real_jobs_7d;

CREATE VIEW public_queue_status_v1 AS
WITH q AS (
    SELECT tenant_id, evidence_class FROM jobs
    WHERE state = 'QUEUED' AND evidence_class IN ('pilot', 'real')
)
SELECT count(*)                                           AS queued_jobs,
       count(DISTINCT tenant_id)                          AS queued_tenants,
       CASE WHEN count(DISTINCT tenant_id) >= 3
            THEN count(*) FILTER (WHERE evidence_class = 'pilot') END AS queued_pilot,
       CASE WHEN count(DISTINCT tenant_id) >= 3
            THEN count(*) FILTER (WHERE evidence_class = 'real') END  AS queued_real,
       now()                                              AS generated_at
FROM q;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'gpub_status') THEN
        CREATE ROLE gpub_status NOLOGIN;
    END IF;
END $$;
GRANT SELECT ON public_pool_status_v1, public_queue_status_v1 TO gpub_status;

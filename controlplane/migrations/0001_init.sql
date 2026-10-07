-- 0001: the control plane's source of truth.
--
-- Constraints here are the second line of defence behind the domain package:
-- the domain refuses an illegal state first, and the schema refuses it again
-- if a code path ever skips the domain. Each CHECK and partial unique index
-- below names the invariant it backs.

CREATE TABLE tenants (
    id             text PRIMARY KEY,
    name           text NOT NULL UNIQUE,
    evidence_class text NOT NULL CHECK (evidence_class IN ('simulator', 'seeded', 'pilot', 'real')),
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- ADR 0009: evidence class is fixed at creation, so seeded or simulator rows
-- can never be relabelled as real use.
CREATE FUNCTION tenants_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.evidence_class IS DISTINCT FROM OLD.evidence_class THEN
        RAISE EXCEPTION 'tenant evidence_class is immutable (% -> %)', OLD.evidence_class, NEW.evidence_class
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER tenants_evidence_immutable BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION tenants_evidence_immutable();

CREATE TABLE users (
    id        text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id),
    handle    text NOT NULL,
    role      text NOT NULL CHECK (role IN ('member', 'operator')),
    UNIQUE (tenant_id, handle)
);

CREATE TABLE projects (
    id               text PRIMARY KEY,
    tenant_id        text NOT NULL REFERENCES tenants(id),
    name             text NOT NULL,
    budget_micro_usd bigint NOT NULL CHECK (budget_micro_usd >= 0),
    UNIQUE (tenant_id, name)
);

CREATE TABLE quotas (
    tenant_id       text PRIMARY KEY REFERENCES tenants(id),
    max_queued_jobs integer NOT NULL CHECK (max_queued_jobs > 0),
    max_active_gpus integer NOT NULL CHECK (max_active_gpus > 0)
);

CREATE TABLE pools (
    id                           text PRIMARY KEY,
    name                         text NOT NULL UNIQUE,
    kind                         text NOT NULL CHECK (kind IN ('kubernetes', 'simulated')),
    region                       text NOT NULL,
    price_micro_usd_per_gpu_hour bigint NOT NULL CHECK (price_micro_usd_per_gpu_hour >= 0),
    interruptible                boolean NOT NULL DEFAULT false,
    pue                          double precision NOT NULL DEFAULT 1.0 CHECK (pue >= 1.0)
);

CREATE TABLE workers (
    id             text PRIMARY KEY,
    pool_id        text NOT NULL REFERENCES pools(id),
    name           text NOT NULL,
    gpu_model      text NOT NULL,
    gpus           integer NOT NULL CHECK (gpus > 0),
    gpu_mem_gb     integer NOT NULL CHECK (gpu_mem_gb >= 0),
    runtimes       text[] NOT NULL,
    region         text NOT NULL,
    -- Capacity CAS target (ADR 0004). The CHECK makes overbooking impossible
    -- even for a statement that forgets the predicate.
    gpus_reserved  integer NOT NULL DEFAULT 0 CHECK (gpus_reserved >= 0 AND gpus_reserved <= gpus),
    state          text NOT NULL CHECK (state IN ('READY', 'DRAINING', 'OFFLINE')),
    last_heartbeat timestamptz NOT NULL,
    registered_at  timestamptz NOT NULL DEFAULT now(),
    version        bigint NOT NULL DEFAULT 0,
    UNIQUE (pool_id, name)
);

CREATE TABLE worker_heartbeats (
    worker_id     text NOT NULL REFERENCES workers(id),
    at            timestamptz NOT NULL,
    held_attempts integer NOT NULL,
    PRIMARY KEY (worker_id, at)
);

-- Tokens: only a SHA-256 of the secret is stored (ADR 0008).
CREATE TABLE api_tokens (
    id          text PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('user', 'worker', 'bootstrap')),
    secret_hash bytea NOT NULL,
    user_id     text REFERENCES users(id),
    worker_id   text REFERENCES workers(id),
    pool_id     text REFERENCES pools(id),
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz,
    CHECK ((kind = 'user' AND user_id IS NOT NULL)
        OR (kind = 'worker' AND worker_id IS NOT NULL)
        OR (kind = 'bootstrap' AND pool_id IS NOT NULL))
);

CREATE TABLE jobs (
    id              text PRIMARY KEY,
    tenant_id       text NOT NULL REFERENCES tenants(id),
    project_id      text NOT NULL REFERENCES projects(id),
    user_id         text NOT NULL REFERENCES users(id),
    image           text NOT NULL,
    command         jsonb NOT NULL,
    gpus            integer NOT NULL CHECK (gpus > 0),
    gpu_model       text NOT NULL DEFAULT '',
    min_gpu_mem_gb  integer NOT NULL DEFAULT 0,
    runtime         text NOT NULL DEFAULT '',
    priority        integer NOT NULL CHECK (priority BETWEEN 0 AND 9),
    preemptible     boolean NOT NULL DEFAULT false,
    deadline        timestamptz,
    max_runtime_s   bigint NOT NULL CHECK (max_runtime_s > 0),
    max_delay_s     bigint NOT NULL DEFAULT 0 CHECK (max_delay_s >= 0),
    budget_cap      bigint NOT NULL DEFAULT 0 CHECK (budget_cap >= 0),
    policy_name     text NOT NULL,
    allowed_pools   text[] NOT NULL DEFAULT '{}',
    max_attempts    integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    attempt         integer NOT NULL DEFAULT 0,
    state           text NOT NULL CHECK (state IN ('SUBMITTED', 'QUEUED', 'RESERVED', 'DISPATCHED', 'RUNNING',
                                                   'CANCEL_REQUESTED', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED')),
    not_before      timestamptz,
    submitted_at    timestamptz NOT NULL,
    started_at      timestamptz,
    finished_at     timestamptz,
    updated_at      timestamptz NOT NULL,
    version         bigint NOT NULL DEFAULT 0,
    evidence_class  text NOT NULL
);
CREATE INDEX jobs_queue ON jobs (submitted_at) WHERE state = 'QUEUED';
CREATE INDEX jobs_tenant ON jobs (tenant_id, submitted_at DESC);
CREATE INDEX jobs_active ON jobs (state) WHERE state IN ('RESERVED', 'DISPATCHED', 'RUNNING', 'CANCEL_REQUESTED');

CREATE TABLE job_transitions (
    id             bigserial PRIMARY KEY,
    job_id         text NOT NULL REFERENCES jobs(id),
    from_state     text NOT NULL,
    to_state       text NOT NULL,
    actor_kind     text NOT NULL,
    actor_id       text NOT NULL,
    reason         text NOT NULL CHECK (reason <> ''),
    at             timestamptz NOT NULL,
    correlation_id text NOT NULL
);
CREATE INDEX job_transitions_job ON job_transitions (job_id, id);

CREATE TABLE job_attempts (
    id         text PRIMARY KEY,
    job_id     text NOT NULL REFERENCES jobs(id),
    number     integer NOT NULL CHECK (number >= 1),
    worker_id  text NOT NULL REFERENCES workers(id),
    created_at timestamptz NOT NULL,
    acked_at   timestamptz,
    ended_at   timestamptz,
    outcome    text CHECK (outcome IN ('SUCCEEDED', 'FAILED', 'LOST', 'PREEMPTED', 'CANCELLED', 'EXPIRED', 'TIMED_OUT')),
    exit_code  integer,
    reason     text NOT NULL DEFAULT '',
    -- Set when the control plane wants the worker to stop this attempt:
    -- 'preempt' (a higher-priority job needs the capacity) or 'timeout'.
    -- Cancellation is carried by the job state CANCEL_REQUESTED instead.
    stop_requested text CHECK (stop_requested IN ('preempt', 'timeout')),
    UNIQUE (job_id, number),
    CHECK ((outcome IS NULL) = (ended_at IS NULL))
);
-- At most one open attempt per job.
CREATE UNIQUE INDEX job_attempts_one_open ON job_attempts (job_id) WHERE outcome IS NULL;

-- A closed attempt is immutable: a retry is a new row, never a reopened one.
CREATE FUNCTION job_attempts_closed_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.outcome IS NOT NULL THEN
        RAISE EXCEPTION 'attempt % is closed (%)', OLD.id, OLD.outcome USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER job_attempts_closed_immutable BEFORE UPDATE ON job_attempts
    FOR EACH ROW EXECUTE FUNCTION job_attempts_closed_immutable();

CREATE TABLE attempt_events (
    attempt_id text NOT NULL REFERENCES job_attempts(id),
    seq        bigint NOT NULL CHECK (seq >= 1),
    kind       text NOT NULL,
    payload    jsonb NOT NULL,
    at         timestamptz NOT NULL,
    PRIMARY KEY (attempt_id, seq) -- worker events are deduplicated on this key
);

CREATE TABLE job_artifacts (
    attempt_id  text NOT NULL REFERENCES job_attempts(id),
    name        text NOT NULL,
    size        bigint NOT NULL DEFAULT 0,
    sha256      text NOT NULL DEFAULT '',
    state       text NOT NULL CHECK (state IN ('UPLOADING', 'COMPLETE')),
    object_key  text NOT NULL,
    updated_at  timestamptz NOT NULL,
    PRIMARY KEY (attempt_id, name)
);

CREATE TABLE reservations (
    id             text PRIMARY KEY,
    job_id         text NOT NULL REFERENCES jobs(id),
    attempt_id     text NOT NULL REFERENCES job_attempts(id),
    worker_id      text NOT NULL REFERENCES workers(id),
    pool_id        text NOT NULL REFERENCES pools(id),
    gpus           integer NOT NULL CHECK (gpus > 0),
    price_rate     bigint NOT NULL,
    hold_amount    bigint NOT NULL,
    epoch          bigint NOT NULL,
    created_at     timestamptz NOT NULL,
    released_at    timestamptz,
    release_reason text
);
-- ADR 0004: a job has at most one active reservation.
CREATE UNIQUE INDEX reservations_one_active ON reservations (job_id) WHERE released_at IS NULL;
CREATE INDEX reservations_active_worker ON reservations (worker_id) WHERE released_at IS NULL;

CREATE TABLE leases (
    attempt_id text PRIMARY KEY REFERENCES job_attempts(id),
    worker_id  text NOT NULL REFERENCES workers(id),
    expires_at timestamptz NOT NULL,
    renewed_at timestamptz NOT NULL
);
CREATE INDEX leases_expiry ON leases (expires_at);

CREATE TABLE budget_ledger (
    id         bigserial PRIMARY KEY,
    project_id text NOT NULL REFERENCES projects(id),
    job_id     text NOT NULL REFERENCES jobs(id),
    attempt_id text,
    kind       text NOT NULL CHECK (kind IN ('HOLD', 'SETTLE', 'RELEASE')),
    amount     bigint NOT NULL CHECK (amount > 0),
    at         timestamptz NOT NULL
);
CREATE INDEX budget_ledger_job ON budget_ledger (job_id);
CREATE INDEX budget_ledger_project ON budget_ledger (project_id);

-- A finished job cannot consume future budget. The application checks this
-- with the job row locked; the trigger refuses it again if anything else
-- tries. RELEASE stays legal: closing a finished job's budget is a release.
CREATE FUNCTION budget_ledger_no_spend_after_finish() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s text;
BEGIN
    IF NEW.kind IN ('HOLD', 'SETTLE') THEN
        SELECT state INTO s FROM jobs WHERE id = NEW.job_id;
        IF s IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
            RAISE EXCEPTION 'job % is % and cannot take a %', NEW.job_id, s, NEW.kind USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER budget_ledger_no_spend_after_finish BEFORE INSERT ON budget_ledger
    FOR EACH ROW EXECUTE FUNCTION budget_ledger_no_spend_after_finish();

CREATE TABLE scheduling_policies (
    version    text PRIMARY KEY,
    name       text NOT NULL,
    spec       jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE decisions (
    id             bigserial PRIMARY KEY,
    job_id         text NOT NULL REFERENCES jobs(id),
    tenant_id      text NOT NULL REFERENCES tenants(id),
    tick_id        text NOT NULL,
    policy_version text NOT NULL REFERENCES scheduling_policies(version),
    action         text NOT NULL CHECK (action IN ('PLACE', 'DELAY', 'WAIT', 'PREEMPT', 'EXPIRE')),
    reason         text NOT NULL,
    fallback       boolean NOT NULL DEFAULT false,
    body           jsonb NOT NULL,
    input_digest   text NOT NULL,
    at             timestamptz NOT NULL
);
CREATE INDEX decisions_job ON decisions (job_id, id);
CREATE INDEX decisions_at ON decisions (at);

CREATE TABLE carbon_snapshots (
    region         text NOT NULL,
    interval_start timestamptz NOT NULL,
    grams_per_kwh  double precision NOT NULL CHECK (grams_per_kwh >= 0),
    source         text NOT NULL,
    observed_at    timestamptz NOT NULL,
    PRIMARY KEY (region, interval_start, source)
);

CREATE TABLE cost_snapshots (
    pool_id                      text NOT NULL REFERENCES pools(id),
    at                           timestamptz NOT NULL,
    price_micro_usd_per_gpu_hour bigint NOT NULL,
    source                       text NOT NULL,
    PRIMARY KEY (pool_id, at)
);

CREATE TABLE audit_events (
    id             bigserial PRIMARY KEY,
    tenant_id      text,
    actor_kind     text NOT NULL,
    actor_id       text NOT NULL,
    action         text NOT NULL,
    target         text NOT NULL,
    at             timestamptz NOT NULL,
    correlation_id text NOT NULL
);

CREATE TABLE outbox_events (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    key          text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    attempts     integer NOT NULL DEFAULT 0,
    locked_until timestamptz,
    locked_by    text,
    delivered_at timestamptz
);
CREATE INDEX outbox_pending ON outbox_events (topic, id) WHERE delivered_at IS NULL;

CREATE TABLE idempotency_keys (
    principal  text NOT NULL,
    key        text NOT NULL,
    method     text NOT NULL,
    path       text NOT NULL,
    body_hash  text NOT NULL,
    status     integer NOT NULL,
    response   jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (principal, key)
);

-- One row; the epoch is the fencing token (ADR 0004).
CREATE TABLE scheduler_leader (
    id          integer PRIMARY KEY CHECK (id = 1),
    epoch       bigint NOT NULL,
    holder      text NOT NULL,
    acquired_at timestamptz NOT NULL
);
INSERT INTO scheduler_leader (id, epoch, holder, acquired_at) VALUES (1, 0, 'none', now());

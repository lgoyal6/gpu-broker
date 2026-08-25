-- Phase 6: named environments.
--
-- The commonest reason a member's job fails is that their environment differs
-- from the box. A named environment is a *spec* -- a base image plus some
-- packages -- and its content hash is the cache key. Two people who ask for the
-- same packages get the same digest and therefore the same build, once.

CREATE TABLE environments (
    name          TEXT PRIMARY KEY,
    digest        TEXT NOT NULL,
    base_image    TEXT NOT NULL DEFAULT '',
    python_version TEXT NOT NULL DEFAULT '',
    requirements  TEXT NOT NULL DEFAULT '',
    apt_packages  TEXT NOT NULL DEFAULT '',
    owner         TEXT NOT NULL DEFAULT '',
    description   TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE INDEX idx_environments_digest ON environments(digest);

-- What has been built, where, and whether it worked.
--
-- Keyed on (digest, target) rather than on the environment name: editing an
-- environment produces a new digest and a new build, and the old one keeps
-- working for jobs that are already running against it.
CREATE TABLE environment_builds (
    digest     TEXT NOT NULL,
    target     TEXT NOT NULL,
    state      TEXT NOT NULL,
    reference  TEXT NOT NULL DEFAULT '',
    detail     TEXT NOT NULL DEFAULT '',
    built_at   TEXT NOT NULL,
    PRIMARY KEY (digest, target)
);

ALTER TABLE jobs ADD COLUMN environment TEXT;

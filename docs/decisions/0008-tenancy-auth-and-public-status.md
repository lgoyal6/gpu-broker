# 0008. Tenancy, authentication, and the read-only public status surface

Status: accepted, 2026-10-06

## Principals

Bearer tokens of the form `gpub_<kind>_<id>_<secret>`. The store keeps
`SHA-256(secret)` only. Kinds: `user` (member of exactly one tenant, role
`member` or `operator`), `worker` (one worker), `bootstrap` (one pool,
registration only).

## Tenant isolation

Every repository method that reads a tenant-owned row takes the tenant id from
the authenticated principal, never from the request body, and filters on it in
SQL. A job, log, artifact or decision belonging to another tenant answers
`404 not_found`, the same response as an id that does not exist, so ids cannot
be probed. The tenant isolation tests make two tenants and assert every
read route returns 404 across the boundary and every list route returns only
the caller's rows.

## Idempotency

Mutating requests from users require `Idempotency-Key`. The key is stored with
the principal, method, path, request body hash and response. Replaying the same
key and body returns the stored response; the same key with a different body
is `422 idempotency_key_reused`. Keys expire after 24 hours.

## Public status

`gpubroker status` is a separate process and Deployment. It connects with a
Postgres role (`gpub_status`) that has `SELECT` on one view,
`public_status_v1`, and nothing else; it physically cannot mutate a job. Its
route table is GET-only, which a test walks. The JSON is built from
`PublicStatusV1`, an allowlist DTO constructed field by field; a test fails if
the DTO gains a field not in the allowlist or a field whose name suggests an
identifier (`id`, `name`, `command`, `tenant`, `host`, `token`). Aggregates are
suppressed (null) for any pool with fewer than three tenants in the window, so
a single tenant's activity cannot be read off the page.

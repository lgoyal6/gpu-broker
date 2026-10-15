# 0014. Commit idempotency receipts with user mutations

Status: accepted, 2026-10-15

The submit, cancel and create-user routes run key lookup, SQL mutation and
response receipt in one PostgreSQL transaction. Lookup claims even an absent
principal/key pair with a transaction advisory lock. A concurrent caller waits
for commit and then replays the receipt or rejects a different request.
Hash collisions serialize unrelated keys without changing their identity.

Each invocation copies the server and service and binds the copy's Store to
the existing transaction. The shared service remains unchanged. These three
handlers perform SQL mutations only; external object writes and provider calls
must remain outside this wrapper. Transaction retries rebuild the request body
and response buffer.

A receipt failure or API death before commit rolls back the mutation. A death
after commit leaves both mutation and response available for replay. Transient
quota or validation errors without a mutation remain retryable. An admission
refusal that records a FAILED job commits its error receipt with that job.

`idempotency_test.go` covers simultaneous same-key jobs/users, receipt-write
failure, fresh-server replay and admission refusal semantics. The opt-in
process suite blocks a real receipt INSERT with a PostgreSQL table lock,
SIGKILLs the API, verifies rollback, and retries across two API restarts.
These controls use local PostgreSQL and processes, without provider execution.

# 0007. Worker agent, dispatch authentication, and the isolation boundary

Status: accepted, 2026-10-06

## Protocol

1. **Register.** The agent presents a pool bootstrap token and its declared
   capabilities (GPU model, count, memory, runtimes, region). The API returns a
   worker id and a worker credential; only the hash is stored.
2. **Heartbeat.** Every `heartbeat_interval` the agent reports liveness and the
   attempts it holds. The response renews those attempts' leases and lists
   attempts the control plane wants cancelled.
3. **Dispatch.** The agent long-polls its dispatch topic. Each dispatch carries
   a lease token: `HMAC-SHA256(dispatch_key, attempt_id | worker_id |
   lease_expiry | spec_digest)`. The agent verifies the MAC and the spec digest
   before doing anything, so a dispatch that was altered in the queue, or
   addressed to another worker, is refused locally.
4. **Acknowledge.** `POST /v1/attempts/{id}/ack` with the lease token. The
   store accepts exactly one ack per attempt (`acked_at IS NULL` predicate);
   a redelivered dispatch is answered `already_acknowledged` and the agent
   drops it. An ack for an attempt whose job is CANCEL_REQUESTED is answered
   `cancel`.
5. **Events.** Structured events carry a per-attempt sequence number and are
   spooled to the agent's state directory before sending, so a restart resends
   them; the API deduplicates on `(attempt_id, seq)`.
6. **Terminal status** is one event type the store accepts once per attempt.
   Duplicates return the stored outcome, so "exactly once" is enforced by the
   database, not by the agent remembering.
7. **Recovery.** On start the agent lists runtime handles labelled with its
   worker id, re-adopts the ones whose lease is still valid, and reports the
   rest as LOST.

Capability enforcement is server-side: the ack is refused if the attempt's
requirements exceed the worker's registered capability, independent of what
the scheduler decided.

## Isolation boundary

There are two runtimes and they have different boundaries. Neither executes a
job command on the agent's host.

- **`sim`** (simulator): never executes the job command. It parses a small
  declarative spec (duration, exit code, artifact bytes, fail-after) and
  sleeps against the injected clock. The command string is stored and shown,
  never run. This is the boundary for credential-free use.
- **`kubernetes`** (production): each attempt is a `batch/v1` Job with the
  user's image and args, `runAsNonRoot`, `allowPrivilegeEscalation: false`,
  all capabilities dropped, read-only root filesystem, `automountServiceAccountToken:
  false`, a seccomp `RuntimeDefault` profile, resource limits including the
  GPU extended resource, `activeDeadlineSeconds` from the job's max runtime,
  and a namespace with a default-deny NetworkPolicy. The agent's own service
  account can create and delete Jobs in that namespace only.

There is no `process`/`docker run` runtime on the agent host. One would make
the agent a remote-code-execution service with the node's privileges.

## Artifacts

Uploads are resumable: the agent `PUT`s chunks at an explicit offset; the
server answers `409` with the committed offset on mismatch, and the agent
resumes from there. Completion carries the SHA-256 of the whole object and is
rejected if it does not match. Object storage is behind `application.ObjectStore`;
the shipped adapter is a filesystem store (a PVC in Kubernetes). An S3 adapter
needs credentials this repository does not have and is listed as an external
step.

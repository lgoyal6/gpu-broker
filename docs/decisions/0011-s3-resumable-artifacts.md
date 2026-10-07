# 0011. Immutable S3 chunks and conditional upload manifests

Status: accepted, 2026-10-07

Supersedes the filesystem-only artifact boundary in ADR 0007.

## Decision

Use the AWS SDK for S3 authentication and conditional requests. An artifact
is an ordered manifest and immutable, SHA-256-addressed chunks. Publishing
a new manifest uses `If-Match` against the version read by the writer, or
`If-None-Match: *` for the first version. The manifest is the committed
upload offset. API replicas share this state without a shared volume.

A successful chunk write followed by a failed manifest update leaves an
unreferenced chunk, not a partially visible artifact. Retrying the chunk
is safe. A competing manifest writer receives the current committed offset.
Readers use one manifest snapshot and verify each chunk before returning
bytes. Each chunk and manifest is bounded; hashing streams chunks rather
than buffering the whole artifact.

## Completion invariant

Completion first verifies the whole digest and atomically seals the object.
A sealed object refuses all further appends. Database completion can then
be retried after an interrupted request without allowing bytes to change
between digest verification and the database update. The filesystem adapter
implements the same seal with a durable sidecar and a per-object file lock.

This closes a concurrency hole in the previous protocol: an upload could
pass its database check, append outside the transaction, and race with a
completion request. A process-local mutex did not fence another API replica.

## Deployment and limits

The filesystem remains a local deployment option. S3 requires a bucket
created by the operator and an identity scoped to GetObject and PutObject
under the configured prefix. Credentials use the SDK's standard provider
chain; Helm can supply an existing Secret. No credentials are committed.
Do not enable anonymous bucket access. Production endpoints use HTTPS.

The S3 layout is internal to this adapter. Existing filesystem objects are
not automatically migrated. Stop uploads and export/copy artifacts before
switching an existing installation. Do not mix adapters behind one API
Service during migration. Unreferenced chunks are retained conservatively;
garbage collection must distinguish live manifests before deleting them.

No provider-specific append or unsupported multipart trick is required.
The S3-compatible test service must implement conditional PutObject, strong
read-after-write behavior, and ETags. CI exercises these semantics against a
pinned local MinIO image. This verifies compatibility locally, not operation
of a user's AWS account, IAM policy, GPU hardware, or production workload.

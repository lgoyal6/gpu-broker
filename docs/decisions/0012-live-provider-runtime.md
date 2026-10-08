# 0012. Live provider runtime boundaries

Status: accepted, 2026-10-08

## Carbon

Use the existing public GB provider rather than requiring a commercial API.
Polling runs independently of reconciliation, with bounded requests and atomic
upserts. Unsupported regions remain missing and trigger explicit fallback.
See ADR 0006 for provenance and freshness.

## AWS spot notices

The optional `worker-agent --spot-provider=aws` uses the AWS SDK's IMDS client
to poll `spot/instance-action` on its own EC2 node, using IMDSv2 without v1
fallback. This is the provider's documented two-minute notice, not a prediction
that spot capacity is available. A 404 means no notice; other failures are
logged and retried without inventing an interruption. Requests have a two-second
deadline. An announced stop, termination, or hibernation latches the agent into
draining, stops active attempts as PREEMPTED, and reports them through the
existing durable spool. It does not provision instances or migrate checkpoints.

The latch is stored in the agent state directory so a process restart cannot
resume claiming on a doomed node. The Helm agent state is an emptyDir, retained
on container restart but not pod replacement. On pod replacement IMDS must
still report the notice; a terminating node is not reusable capacity. An
operator reusing a surviving node must clear the interruption marker explicitly.

The SDK already exists in this module through S3. Kubernetes installations may
instead use AWS Node Termination Handler to cordon and drain nodes. That existing
cordon path is supported and tested; do not run both integrations without
understanding their ordering. Agent pods must reach IMDS, and EC2 metadata must
allow the container hop count. No real EC2 notice can be claimed from Kind.

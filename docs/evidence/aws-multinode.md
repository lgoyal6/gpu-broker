# AWS multi-node capacity evidence

The account's `Running On-Demand G and VT instances` quota in `us-east-1` is
4 vCPUs. A `g4dn.xlarge` consumes all four, so two simultaneous T4 nodes
cannot launch under the current quota.

On 2026-10-11, a request for the smallest useful increase, 8 vCPUs, was
submitted through Service Quotas:

```text
request: ef4f897c6e7542a1a30cb04fb9877236dPvzBq2m
case:    179177310900893
status:  CASE_OPENED
```

The multi-node smoke remains pending AWS approval. Until the quota changes,
the repository's simulator and Kubernetes tests are the available multi-host
evidence; they do not substitute for two simultaneous EC2 nodes.

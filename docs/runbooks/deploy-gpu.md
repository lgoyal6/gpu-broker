# Deploy to real GPU nodes

**Status: not executed by this repository.** No GPU cluster was available.
Everything below is the exact procedure; the Kind profile exercises the same
chart, runtime and protocol with declared GPUs.

## Prerequisites

- Kubernetes 1.30+ with GPU nodes running the NVIDIA device plugin (or GPU
  Operator), so nodes advertise `nvidia.com/gpu` and carry
  `nvidia.com/gpu.present=true`.
- A managed PostgreSQL 16 reachable from the cluster, with a role owning a
  database. The migrate Job creates `gpub_status` and `gpub_status_login`;
  the owner role needs `CREATEROLE` for that (or create them once by hand).
- A NetworkPolicy-enforcing CNI.
- The image pushed by digest.

## Steps

```bash
kubectl label nodes <gpu-node>... gpub.dev/pool=gpu-a100
kubectl create namespace gpub
kubectl -n gpub create secret generic gpu-broker-db \
  --from-literal=url='postgres://owner:...@db:5432/gpub?sslmode=require' \
  --from-literal=status-url='postgres://gpub_status_login:...@db:5432/gpub?sslmode=require' \
  --from-literal=status-password='...'
helm upgrade --install gb infra/helm/gpu-broker -n gpub -f infra/helm/gpu-broker/values-gpu.yaml \
  --set image.digest=sha256:<digest> --wait
kubectl -n gpub exec deploy/gb-api -c api -- gpubroker admin create-tenant \
  --name pilot --evidence pilot --handle <you> --budget-usd 50 --metrics-addr=
```

## Verify

1. `gpuctl doctor` shows one ready worker per labelled GPU node.
2. `gpuctl submit --gpus 1 --image nvidia/cuda:12.6.2-base-ubuntu24.04 -- nvidia-smi`
   succeeds and `gpuctl logs` shows the GPU.
3. The attempt pod has `resources.limits["nvidia.com/gpu"]` equal to `--gpus`,
   and `nvidia-smi` inside it lists only that many devices.
4. Drain one GPU node during a long job; the job is retried on another node
   as attempt 2 with the first attempt LOST (same as the Kind test).
5. Delete the scheduler leader pod; `gbadmin leader` shows a new holder and
   a higher epoch within seconds.

Record the results in `docs/evidence/` as pilot evidence, separately from the
Kind and simulator evidence.

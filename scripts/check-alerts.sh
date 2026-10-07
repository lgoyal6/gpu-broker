#!/usr/bin/env bash
set -euo pipefail
# The chart rules are also a plain Prometheus rule file. No live cluster is needed.
gpub_root=$(cd "$(dirname "$0")/.." && pwd)
docker run --rm --memory 256m --cpus 1 --entrypoint /bin/promtool \
  -v "$gpub_root:/workspace:ro" -w /workspace prom/prometheus:v3.5.0 \
  check rules infra/helm/gpu-broker/alerts.yaml
docker run --rm --memory 256m --cpus 1 --entrypoint /bin/promtool \
  -v "$gpub_root:/workspace:ro" -w /workspace prom/prometheus:v3.5.0 \
  test rules controlplane/tests/monitoring/alerts.test.yaml

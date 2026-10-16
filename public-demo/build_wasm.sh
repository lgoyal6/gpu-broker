#!/usr/bin/env sh
# Build the browser runner from the same Go simulator and pin its runtime file.
# Run from any directory; generated files are served with the static demo.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GOROOT=$(go env GOROOT)
OUT="$ROOT/public-demo/site"

cd "$ROOT/controlplane"
GOOS=js GOARCH=wasm go build -trimpath -ldflags='-s -w' \
  -o "$OUT/gpubroker.wasm" ./cmd/gpubroker-wasm
cp "$GOROOT/lib/wasm/wasm_exec.js" "$OUT/wasm_exec.js"
printf 'Built %s\n' "$OUT/gpubroker.wasm"

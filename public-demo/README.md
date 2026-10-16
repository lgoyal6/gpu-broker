# GPU Broker recorded policy explorer

The export's `source_commit` identifies its historical source snapshot. Rebase
merges can change commit IDs without changing source bytes. `--check` always
requires every current source hash and recorded result to match. It also checks
the historical Git snapshot when that object is available; otherwise it explicitly
reports that the historical object could not be verified.

This static demo compares the native Go simulator's eight policies on four synthetic workloads. It is an interactive recording, not the status page, a reimplementation of the scheduler, or a public job API.

From the repository root, export and check retained data:

```sh
python3 public-demo/export.py
python3 public-demo/export.py --check
python3 -m http.server 8080 --directory public-demo/site --bind 127.0.0.1
```

To regenerate the recording from the existing native engine using CPU only (Go 1.27.1+):

```sh
python3 public-demo/export.py --refresh
python3 public-demo/export.py --check
```

Refresh runs only `gpubroker sim`, seed 42, all policies and workloads. It starts no DB, cloud worker, model inference or job. The exporter drops nondeterministic wall-clock timing data; estimates and adverse outcomes remain. This dataset is simulated, with modelled capacity/prices/power and historical GB carbon data. It is not measured savings, adoption, or live carbon.

Vercel project root: `public-demo`; static output: `site`. No build command, credentials, environment variables or functions are required. This lane starts on free hosting. Publish only after exact-source independent review. No admin, job-submission, AWS or database endpoint is exposed.

## Browser simulator

The page also runs one bounded workload in a Web Worker using the repository's Go simulator compiled to WebAssembly. The worker caps the run at 32 jobs and one simulated day; terminating the worker cancels it. Go checksum-verifies the historical carbon fixture before use. Browser results remain labelled `simulator`, and no GPU, cloud account, provider API or database is contacted.

Build the browser binary and matching Go runtime with:

```sh
./public-demo/build_wasm.sh
```

Then refresh and verify static assets with `python3 public-demo/export.py` and `python3 public-demo/export.py --check` from the repository root. The WASM binary and runtime are generated assets and should be rebuilt with the repository's Go version before publishing.

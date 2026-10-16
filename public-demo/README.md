# GPU Broker recorded policy explorer

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

# 0009. Simulator, seeded demo, pilot, and real-user evidence are never mixed

Status: accepted, 2026-10-06

Every tenant row has `evidence_class` in `{simulator, seeded, pilot, real}`.
It is set at creation and cannot be updated (a trigger rejects the update).

- `gpubroker sim` writes nothing to Postgres; its output is a report file whose
  first field is `"evidence_class": "simulator"`.
- `gpuctl demo seed` creates tenants with `evidence_class = seeded`.
- Pilot tenants are created by an operator with `--pilot`.
- Metrics carry the label `evidence_class`; dashboards default to `real`.
- The public status view counts only `pilot` and `real`, and reports the two
  separately.

Benchmark numbers are labelled with the workload name, hardware, commit, and
the fact that they come from the simulator or a synthetic load generator. No
document in this repository presents a simulator or seeded number as adoption,
cost saving, or capacity evidence.

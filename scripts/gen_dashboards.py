"""Generate the Grafana dashboards shipped in the Helm chart.

The dashboards are code: one panel list per dashboard, rendered to JSON so
every panel uses the same datasource variable and the metric names stay in
one reviewable place. CI regenerates and fails on a diff.

    python scripts/gen_dashboards.py
"""

import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "infra/helm/gpu-broker/dashboards"

DASHBOARDS = {
    "queue-health": ("GPU broker: queue health", [
        ("Queued jobs by tenant", "sum by (tenant) (gpub_queue_depth)", "short"),
        ("Undelivered dispatch events", "sum by (topic) (gpub_outbox_undelivered)", "short"),
        ("Queue to reservation p50 / p99",
         ["histogram_quantile(0.5, sum by (le) (rate(gpub_job_stage_seconds_bucket{stage=\"queue_to_reserve\"}[15m])))",
          "histogram_quantile(0.99, sum by (le) (rate(gpub_job_stage_seconds_bucket{stage=\"queue_to_reserve\"}[15m])))"], "s"),
        ("Reservation to dispatch p99 (SLO 60s)",
         "histogram_quantile(0.99, sum by (le) (rate(gpub_job_stage_seconds_bucket{stage=\"reserve_to_dispatch\"}[15m])))", "s"),
        ("Scheduler tick and per-decision latency p99",
         ["histogram_quantile(0.99, sum by (le) (rate(gpub_scheduler_tick_seconds_bucket[5m])))",
          "histogram_quantile(0.99, sum by (le) (rate(gpub_scheduler_decision_seconds_bucket[5m])))"], "s"),
        ("Decisions by action", "sum by (action) (rate(gpub_scheduler_decisions_total[5m]))", "ops"),
        ("Reservation conflicts", "sum by (reason) (rate(gpub_reservation_conflicts_total[5m]))", "ops"),
        ("Scheduler leader (should be exactly 1)", "sum(gpub_scheduler_is_leader)", "short"),
    ]),
    "pool-capacity": ("GPU broker: pool capacity", [
        ("GPUs reserved / total", ["sum by (pool) (gpub_pool_gpus{state=\"reserved\"})", "sum by (pool) (gpub_pool_gpus{state=\"total\"})"], "short"),
        ("Allocation (reserved / total)", "sum by (pool) (gpub_pool_gpus{state=\"reserved\"}) / clamp_min(sum by (pool) (gpub_pool_gpus{state=\"total\"}), 1)", "percentunit"),
        ("Fragmentation (1 - largest free block / free)", "gpub_pool_fragmentation_ratio", "percentunit"),
    ]),
    "worker-health": ("GPU broker: worker health", [
        ("Heartbeat age", "max by (pool, worker) (gpub_worker_heartbeat_age_seconds)", "s"),
        ("Stale reclaims (leases, reservations)", "sum by (kind) (increase(gpub_stale_reclaims_total[15m]))", "short"),
        ("Artifact failures", "sum by (reason) (rate(gpub_artifact_failures_total[15m]))", "ops"),
        ("Reconciler repairs (should be 0)", "sum by (kind) (increase(gpub_reconciler_repairs_total[1h]))", "short"),
        ("Database transaction retries", "sum by (sqlstate) (rate(gpub_db_tx_retries_total[5m]))", "ops"),
    ]),
    "job-outcomes": ("GPU broker: job outcomes", [
        ("Terminal transitions by state", "sum by (to) (rate(gpub_job_transitions_total{to=~\"SUCCEEDED|FAILED|CANCELLED|EXPIRED\"}[15m]))", "ops"),
        ("Attempt outcomes (LOST/PREEMPTED are retried)", "sum by (outcome) (rate(gpub_attempt_outcomes_total[15m]))", "ops"),
        ("Submit to start p50 / p99",
         ["histogram_quantile(0.5, sum by (le) (rate(gpub_job_stage_seconds_bucket{stage=\"submit_to_start\"}[30m])))",
          "histogram_quantile(0.99, sum by (le) (rate(gpub_job_stage_seconds_bucket{stage=\"submit_to_start\"}[30m])))"], "s"),
        ("Submit availability (SLO 99.5%)", "gpub:submit_availability:ratio_rate5m", "percentunit"),
        ("API 5xx by route", "sum by (route) (rate(gpub_http_requests_total{code=\"5xx\"}[5m]))", "ops"),
    ]),
    "budget-runway": ("GPU broker: budget and EcoShift", [
        ("Project budget available (USD)", "gpub_project_available_usd", "currencyUSD"),
        ("EcoShift fallbacks by reason", "sum by (reason) (increase(gpub_ecoshift_fallback_total[1h]))", "short"),
        ("Carbon reading age by region", "gpub_carbon_reading_age_seconds", "s"),
        ("Estimated carbon saved by delays (g, TDP basis)", "increase(gpub_ecoshift_carbon_saved_grams_total[24h])", "short"),
        ("Decisions by policy", "sum by (policy) (rate(gpub_scheduler_decisions_total[15m]))", "ops"),
    ]),
}


def panel(i, title, exprs, unit):
    exprs = exprs if isinstance(exprs, list) else [exprs]
    return {
        "id": i + 1, "type": "timeseries", "title": title,
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "gridPos": {"h": 8, "w": 12, "x": (i % 2) * 12, "y": (i // 2) * 8},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [{"refId": chr(65 + k), "expr": e, "datasource": {"type": "prometheus", "uid": "${datasource}"}}
                    for k, e in enumerate(exprs)],
    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for uid, (title, panels) in DASHBOARDS.items():
        dash = {
            "uid": f"gpub-{uid}", "title": title, "schemaVersion": 39, "version": 1, "editable": False,
            "time": {"from": "now-6h", "to": "now"}, "refresh": "30s", "tags": ["gpu-broker"],
            "templating": {"list": [{"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Prometheus"}]},
            "panels": [panel(i, *p) for i, p in enumerate(panels)],
        }
        (OUT / f"{uid}.json").write_text(json.dumps(dash, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()

"""Time series, Prometheus, the digest, and the measurement report.

The report is the point of the phase, and the thing worth being careful about is
that it stays capable of saying bad news. A generated report that can only
flatter is a hand-written one with extra steps.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from gpu_broker.digest import build as build_digest
from gpu_broker.digest import fetch_baseline, render, send
from gpu_broker.errors import BrokerError
from gpu_broker.metrics import (
    HEAD_WAIT_HOURS,
    POOL_REMAINING,
    QUEUE_DEPTH,
    RUNNING_JOBS,
    UTILIZATION,
    decode_labels,
    encode_labels,
    prometheus,
)
from gpu_broker.report import markdown


# ------------------------------------------------------------------- series


def test_labels_round_trip():
    assert decode_labels(encode_labels({"gpu": "a10g", "user": "ana"})) == {
        "gpu": "a10g", "user": "ana"
    }


def test_labels_are_sorted_so_a_series_does_not_split_in_two():
    assert encode_labels({"b": "2", "a": "1"}) == encode_labels({"a": "1", "b": "2"})


def test_a_tick_records_the_gauges(broker, users, clock):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.tick()
    assert broker.metrics.series(QUEUE_DEPTH)
    assert broker.metrics.series(RUNNING_JOBS)


def test_the_series_grows_over_time(broker, users, clock):
    for _ in range(4):
        broker.tick()
        clock.advance(minutes=5)
    points = broker.metrics.series(QUEUE_DEPTH)
    assert len(points) == 4
    assert [p.at for p in points] == sorted(p.at for p in points)


def test_queue_depth_reflects_what_is_waiting(broker, users, clock, cloud):
    cloud.capacity = {"a10g": 1}
    for name in ("ana", "bo", "cy"):
        broker.submit(user_id=name, command="x", gpu_type="a10g", hours=2)
        clock.advance(minutes=1)
    broker.tick()

    assert broker.metrics.series(QUEUE_DEPTH)[-1].value == 2
    assert broker.metrics.series(RUNNING_JOBS)[-1].value == 1


def test_the_head_wait_is_the_longest_one(broker, users, clock, cloud):
    cloud.capacity = {"a10g": 0}
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    clock.advance(hours=3)
    broker.submit(user_id="bo", command="x", gpu_type="a10g", hours=1)
    broker.tick()

    assert broker.metrics.series(HEAD_WAIT_HOURS)[-1].value == pytest.approx(3.0, abs=0.1)


def test_utilization_is_recorded_per_job(broker, users, clock, cloud):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4)
    broker.tick()
    clock.advance(minutes=5)
    broker.tick()

    points = broker.metrics.series(UTILIZATION)
    assert points
    assert points[-1].labels["user"] == "ana"


def test_pool_remaining_is_tracked_per_currency(broker, users):
    broker.tick()
    currencies = {p.labels["currency"] for p in broker.metrics.latest(POOL_REMAINING)}
    assert currencies == {"USD", "GPU_HOUR"}


def test_a_metrics_failure_does_not_take_down_a_tick(broker, users, monkeypatch):
    """Losing a data point is a gap in a chart. Losing a tick is a job that did
    not start."""
    import gpu_broker.scheduler as scheduler

    def boom(*args, **kwargs):
        raise RuntimeError("the collector is unwell")

    monkeypatch.setattr(scheduler, "collect_metrics", boom)
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job

    report = broker.tick()
    assert job.job_id in report.dispatched


def test_pruning_removes_old_points(broker, users, clock):
    broker.tick()
    clock.advance(days=100)
    broker.tick()
    removed = broker.metrics.prune(clock.now() - dt.timedelta(days=30))

    assert removed > 0
    assert len(broker.metrics.series(QUEUE_DEPTH)) == 1


# -------------------------------------------------------------- prometheus


def test_the_export_is_well_formed(broker, users):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.tick()
    body = prometheus(broker.metrics)

    assert "# HELP gpu_broker_queue_depth" in body
    assert "# TYPE gpu_broker_queue_depth gauge" in body
    assert body.endswith("\n")
    for line in body.splitlines():
        assert line.startswith("#") or " " in line


def test_labels_are_rendered_the_way_prometheus_expects(broker, users):
    broker.tick()
    body = prometheus(broker.metrics)
    assert 'gpu_broker_free_slots{gpu="a10g"}' in body


def test_only_the_newest_value_of_each_series_is_exported(broker, users, clock):
    for _ in range(3):
        broker.tick()
        clock.advance(minutes=5)
    body = prometheus(broker.metrics)
    assert len([line for line in body.splitlines() if line.startswith("gpu_broker_queue_depth ")]) == 1


def test_a_hostile_label_cannot_break_the_format(broker):
    broker.metrics.record([("queue_depth", 1.0, {"user": 'ana" evil="'})])
    body = prometheus(broker.metrics)
    assert '\\"' in body


# ------------------------------------------------------------------ digest


def test_a_quiet_week_says_so(broker, users):
    digest = build_digest(broker)
    assert digest.quiet
    assert "Nobody ran anything" in render(digest)


def test_the_digest_names_who_used_what(broker, users, clock):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.run_until_idle(step_seconds=120)

    body = render(build_digest(broker))
    assert "**ana**" in body
    assert "1 job" in body


def test_the_digest_reports_reclaims(broker, users, clock, cloud):
    job = broker.submit(user_id="bo", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    cloud.set_utilization(job.job_id, 0.0)
    for _ in range(40):
        clock.advance(minutes=1)
        broker.tick()
    broker.reclaim(force=True)

    digest = build_digest(broker)
    assert digest.reclaimed_jobs == 1
    assert "Reclaimed 1 idle job" in render(digest)


def test_the_digest_welcomes_new_members(broker, users):
    assert "Welcome" in render(build_digest(broker))


def test_the_digest_carries_the_runway(broker, users):
    assert "left" in render(build_digest(broker))


def test_sending_with_no_webhook_says_what_to_do_instead(broker):
    with pytest.raises(BrokerError, match="paste it wherever"):
        send(broker.config, "body")


def test_sending_posts_json_both_slack_and_discord_understand(broker):
    from dataclasses import replace

    class FakeHttp:
        def __init__(self):
            self.posted = None

        def post(self, url, content=None, headers=None):
            self.posted = (url, content)

            class R:
                status_code = 204

            return R()

    client = FakeHttp()
    config = replace(broker.config, digest_webhook="https://hooks.slack.com/services/x")
    send(config, "hello", client=client)

    import json

    body = json.loads(client.posted[1])
    assert body["text"] == "hello"     # slack
    assert body["content"] == "hello"  # discord


def test_a_webhook_that_refuses_is_reported(broker):
    from dataclasses import replace

    class Refusing:
        def post(self, url, content=None, headers=None):
            class R:
                status_code = 403

            return R()

    config = replace(broker.config, digest_webhook="https://example.invalid/hook")
    with pytest.raises(BrokerError, match="HTTP 403"):
        send(config, "hello", client=Refusing())


# ---------------------------------------------------------------- baseline


class FakeCostExplorer:
    def __init__(self, results=None, error=None):
        self.results = results
        self.error = error
        self.calls: list[dict] = []

    def get_cost_and_usage(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return {
            "ResultsByTime": self.results
            if self.results is not None
            else [
                {
                    "TimePeriod": {"Start": "2026-05-01", "End": "2026-06-01"},
                    "Total": {
                        "UnblendedCost": {"Amount": "412.55", "Unit": "USD"},
                        "UsageQuantity": {"Amount": "310.0", "Unit": "Hrs"},
                    },
                }
            ]
        }


def test_the_baseline_comes_back_per_month():
    rows = fetch_baseline(FakeCostExplorer(), "2026-05-01", "2026-06-01")
    assert rows == [("2026-05", 310.0, Decimal("412.55"))]


def test_the_baseline_is_filtered_to_ec2_compute():
    """Otherwise S3 and data transfer get counted as GPU spend, which would make
    the broker look better than it is."""
    client = FakeCostExplorer()
    fetch_baseline(client, "2026-05-01", "2026-06-01")
    values = client.calls[0]["Filter"]["Dimensions"]["Values"]
    assert values == ["Amazon Elastic Compute Cloud - Compute"]


def test_a_missing_permission_says_which_one():
    client = FakeCostExplorer(error=RuntimeError("AccessDeniedException"))
    with pytest.raises(BrokerError, match="ce:GetCostAndUsage"):
        fetch_baseline(client, "2026-05-01", "2026-06-01")


def test_a_stored_baseline_appears_in_the_report(broker, users):
    broker.store.save_baseline("2026-05", 310.0, Decimal("412.55"), "cost-explorer")
    body = markdown(broker.report())
    assert "2026-05" in body
    assert "$412.55" in body


def test_no_baseline_says_how_to_get_one(broker, users):
    assert "put the numbers in by hand" in markdown(broker.report())


# ---------------------------------------------------------------- bucketing


def test_a_series_buckets_into_equal_slices(broker, users, clock):

    start = clock.now()
    for _ in range(12):
        broker.tick()
        clock.advance(minutes=10)

    buckets = broker.metrics.bucketed(QUEUE_DEPTH, start, clock.now(), buckets=6)
    assert len(buckets) == 6
    assert all(b is not None for b in buckets)


def test_a_gap_in_the_record_stays_a_gap(broker, users, clock):
    """`None`, not zero. A pool that was switched off did not have a GPU sitting
    idle -- nothing was running at all, and drawing that as a floor would be a
    different and wrong claim."""

    start = clock.now()
    broker.tick()
    clock.advance(hours=6)          # the broker was down
    broker.tick()

    buckets = broker.metrics.bucketed(QUEUE_DEPTH, start, clock.now(), buckets=6)
    assert buckets[0] is not None
    assert any(b is None for b in buckets), "a six-hour outage rendered as data"


def test_bucketing_an_empty_window_is_not_a_crash(broker, users, clock):
    now = clock.now()
    assert broker.metrics.bucketed(QUEUE_DEPTH, now, now) == []


def test_the_sparkline_breaks_across_a_gap(broker):
    from gpu_broker.web.app import _sparkline

    svg = _sparkline([10.0, 20.0, None, None, 40.0, 50.0])
    assert svg.count("<polyline") == 2, "drew through a gap"


def test_an_isolated_sample_is_drawn_as_a_dot(broker):
    """A pool used twice a day has every sample isolated between gaps. A
    polyline needs two points, so without this the panel renders blank and
    looks like no data rather than sparse data."""
    from gpu_broker.web.app import _sparkline

    svg = _sparkline([None, 40.0, None, 60.0, None])
    assert svg.count("<circle") == 2
    assert "<polyline" not in svg


def test_a_sparkline_with_nothing_to_draw_renders_nothing(broker):
    from gpu_broker.web.app import _sparkline

    assert _sparkline([]) == ""
    assert _sparkline([None, None]) == ""


def test_the_sparkline_respects_a_ceiling(broker):
    """Utilization is drawn against 0-100, not against its own maximum, or a
    pool that peaked at 3% would look busy."""
    from gpu_broker.web.app import _sparkline

    low = _sparkline([1.0, 2.0, 3.0], height=100, ceiling=100.0)
    assert "99" in low or "98" in low, "a 3% peak was drawn near the top"

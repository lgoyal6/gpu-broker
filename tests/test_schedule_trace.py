from __future__ import annotations

import datetime as dt
import json

import pytest

from gpu_broker.demo import seed
from gpu_broker.schedule_trace import (
    TraceError,
    anonymous_user_id,
    build_cost_report,
    export_trace,
    replay_trace,
)
from gpu_broker.states import JobState


def test_seeded_trace_is_anonymized_deterministic_and_replayable(tmp_path):
    state = tmp_path / "demo"
    seed(state, days=8, rng_seed=3)

    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(state, clock=SystemClock())
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    export_trace(broker.store, first)
    export_trace(broker.store, second)

    assert first.read_bytes() == second.read_bytes()
    raw = first.read_text()
    assert "python train.py" not in raw
    assert "demo-1" not in raw
    assert "backend_handle" not in raw
    bundle = json.loads(raw)
    assert bundle["evaluation_limits"]["optimizer_comparison"].startswith("blocked")

    summary = replay_trace(first)
    assert summary.jobs == len(bundle["jobs"])
    assert summary.users == 6
    assert summary.completed > 0
    assert summary.max_active > 0
    assert summary.p95_wait_seconds is not None


def test_small_operational_history_is_refused(broker):
    broker.add_user("only-person")
    broker.submit(
        user_id="only-person",
        command="private training command",
        gpu_type="a10g",
        hours=1,
    )
    with pytest.raises(TraceError, match="too small to anonymize safely"):
        export_trace(broker.store, broker.config.db_path.parent / "trace.json")


def test_replay_rejects_modified_trace(tmp_path):
    state = tmp_path / "demo"
    seed(state, days=2, rng_seed=1)

    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(state, clock=SystemClock())
    trace = tmp_path / "trace.json"
    export_trace(broker.store, trace)
    bundle = json.loads(trace.read_text())
    bundle["jobs"][0]["requested_hours"] = 999
    trace.write_text(json.dumps(bundle))
    with pytest.raises(TraceError, match="digest"):
        replay_trace(trace)


def test_anonymous_user_id_is_stable_and_salt_scoped():
    first = anonymous_user_id("member@example.test", "a-private-salt-1")
    assert first == anonymous_user_id("member@example.test", "a-private-salt-1")
    assert first != anonymous_user_id("member@example.test", "a-private-salt-2")
    assert "member" not in first
    with pytest.raises(TraceError, match="at least 16 bytes"):
        anonymous_user_id("member@example.test", "short")


def test_cost_report_is_aggregate_only_and_replays_same_trace(broker, clock):
    for user_number in range(5):
        user_id = f"private-member-{user_number}"
        broker.add_user(user_id)
        for job_number in range(4):
            submitted = broker.submit(
                user_id=user_id,
                command=f"python secret-{user_number}-{job_number}.py",
                gpu_type="a10g",
                hours=0.1,
            ).job
            broker.store.record_schedule_observation(
                job_id=submitted.job_id,
                decision="DISPATCH",
                resource_class="a10g",
                requested_memory_mb=24_576,
                backend="cloud",
                tier="ondemand",
                detail="test dispatch",
            )
            running = broker.store.transition(
                submitted,
                JobState.ALLOCATING,
                backend="cloud",
                backend_handle=submitted.job_id,
            )
            clock.advance(seconds=1)
            broker.store.transition(running, JobState.COMPLETED, clear_handle=True)

    report = build_cost_report(
        broker.store, broker.config, salt="local-secret-never-exported"
    )
    encoded = json.dumps(report)
    assert report["evidence_boundary"]["jobs"] == 20
    assert report["evidence_boundary"]["anonymous_users"] == 5
    assert report["fifo_replay"]["same_trace_jobs"] == 20
    assert report["instrumentation"]["scheduler_observations"] == 20
    assert "private-member" not in encoded
    assert "python secret" not in encoded
    assert "jobs" not in report, "aggregate export must not grow a per-job payload"


def test_cost_report_refuses_the_real_one_user_trace(broker):
    broker.add_user("one-real-user")
    broker.submit(
        user_id="one-real-user", command="private command", gpu_type="a10g", hours=0.1
    )
    with pytest.raises(TraceError, match="need at least 20 jobs and 5 users"):
        build_cost_report(broker.store, broker.config, salt="local-private-salt")


def test_trace_retention_deletes_only_expired_observations(broker):
    broker.add_user("member")
    job = broker.submit(
        user_id="member", command="python train.py", gpu_type="a10g", hours=0.1
    ).job
    for at in ("2025-01-01T00:00:00.000000+00:00", "2026-01-01T00:00:00.000000+00:00"):
        broker.store.conn.execute(
            "INSERT INTO schedule_observations "
            "(job_id, at, decision, resource_class, requested_memory_mb, detail) "
            "VALUES (?, ?, 'BLOCKED_CAPACITY', 'a10g', 24576, 'none free')",
            (job.job_id, at),
        )
    deleted = broker.store.delete_schedule_observations_before(
        dt.datetime(2025, 6, 1, tzinfo=dt.UTC)
    )
    assert deleted == 1
    assert (
        broker.store.conn.execute(
            "SELECT COUNT(*) FROM schedule_observations"
        ).fetchone()[0]
        == 1
    )
    assert broker.store.get_job(job.job_id).command == "python train.py"


def test_real_tick_records_decision_resource_class_and_memory(broker):
    broker.add_user("member")
    job = broker.submit(
        user_id="member", command="python train.py", gpu_type="a10g", hours=0.1
    ).job
    broker.tick()
    row = broker.store.conn.execute(
        "SELECT * FROM schedule_observations WHERE job_id = ?", (job.job_id,)
    ).fetchone()
    assert row["decision"] == "DISPATCH"
    assert row["resource_class"] == "a10g"
    assert row["requested_memory_mb"] == 24_576
    assert row["backend"] == "cloud"


def test_a_blocked_decision_is_recorded_once_until_it_changes(broker, clock):
    """A job blocked on capacity must not grow one identical row per tick.

    Ticks arrive every few seconds for as long as the daemon runs, so a table
    that gains a row per tick per waiting job grows with wall-clock time rather
    than with scheduling decisions. Dispatches are exempt: a resume after a
    preemption is a new decision even when nothing about it looks different.
    """
    broker.add_user("fixture-a@example.test")
    broker.add_user("fixture-b@example.test")
    # Fill both a10g slots, then a third job waits on capacity. The clock moves
    # between submissions so the queue order is the submission order rather
    # than a job-id coin toss.
    for _ in range(2):
        broker.submit(
            user_id="fixture-a@example.test",
            command="python fill.py",
            gpu_type="a10g",
            hours=1.0,
        )
        clock.advance(seconds=1)
    waiting = broker.submit(
        user_id="fixture-b@example.test",
        command="python wait.py",
        gpu_type="a10g",
        hours=0.1,
    ).job
    for _ in range(4):
        broker.tick()
        clock.advance(seconds=30)
    rows = broker.store.conn.execute(
        "SELECT decision FROM schedule_observations WHERE job_id = ? ORDER BY id",
        (waiting.job_id,),
    ).fetchall()
    assert [row["decision"] for row in rows] == ["BLOCKED_CAPACITY"]

    # Let the slot-holders finish; the waiting job's dispatch is a new decision
    # and is appended.
    for _ in range(80):
        broker.tick()
        clock.advance(minutes=5)
    rows = broker.store.conn.execute(
        "SELECT decision FROM schedule_observations WHERE job_id = ? ORDER BY id",
        (waiting.job_id,),
    ).fetchall()
    assert [row["decision"] for row in rows] == ["BLOCKED_CAPACITY", "DISPATCH"]

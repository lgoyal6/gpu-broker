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


def seed_fixture_club(broker, clock):
    """Twenty synthetic-fixture jobs from five fixture members.

    Fixture data, never operational evidence: it exists only inside this test's
    temporary store, and its identities are deliberately email-shaped so the
    identity-leak scan below has something realistic to catch.
    """
    for user_number in range(5):
        user_id = f"fixture-member-{user_number}@example.test"
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


def test_cost_report_is_aggregate_only_and_replays_same_trace(broker, clock):
    seed_fixture_club(broker, clock)

    report = build_cost_report(
        broker.store, broker.config, salt="local-secret-never-exported"
    )
    encoded = json.dumps(report)
    assert report["evidence_boundary"]["jobs"] == 20
    assert report["evidence_boundary"]["anonymous_users"] == 5
    assert report["fifo_replay"]["same_trace_jobs"] == 20
    assert report["instrumentation"]["scheduler_observations"] == 20
    assert "fixture-member" not in encoded
    assert "@example.test" not in encoded
    assert "python secret" not in encoded
    assert "jobs" not in report, "aggregate export must not grow a per-job payload"


def test_cost_report_is_deterministic_for_the_same_trace(broker, clock):
    """Same trace in, identical metrics out, twice. A replay that shifts between
    runs is a random-number generator wearing a lab coat."""
    seed_fixture_club(broker, clock)

    first = build_cost_report(broker.store, broker.config, salt="a-long-fixture-salt")
    second = build_cost_report(broker.store, broker.config, salt="a-long-fixture-salt")
    assert first == second
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_cost_report_refuses_the_real_one_user_trace(broker):
    """The gate that keeps a one-person trace from becoming a C18 claim.

    The statement must carry the real counts and must say that no verdict is
    emitted, because that refusal is itself the honest measured result until
    20 jobs from 5 users exist.
    """
    broker.add_user("one-real-user")
    broker.submit(
        user_id="one-real-user", command="private command", gpu_type="a10g", hours=0.1
    )
    with pytest.raises(
        TraceError,
        match=r"1 job\(s\) from 1 user\(s\).*no cost-comparison verdict",
    ):
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


def test_deleted_trace_rows_disappear_from_the_export_too(broker, clock):
    """Retention is only real if the export agrees the rows are gone."""
    seed_fixture_club(broker, clock)
    before = build_cost_report(broker.store, broker.config, salt="a-long-fixture-salt")
    assert before["instrumentation"]["scheduler_observations"] == 20

    deleted = broker.store.delete_schedule_observations_before(clock.now())
    assert deleted == 20
    remaining = broker.store.conn.execute(
        "SELECT COUNT(*) FROM schedule_observations"
    ).fetchone()[0]
    assert remaining == 0

    after = build_cost_report(broker.store, broker.config, salt="a-long-fixture-salt")
    assert after["instrumentation"]["scheduler_observations"] == 0
    # Deletion touched the disposable trace and nothing else: the jobs and the
    # money they settled are still there.
    assert after["evidence_boundary"]["jobs"] == 20
    assert after["current_policy"]["completion_rate"] == 1.0


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


def drive_to_quiet(broker, clock, max_ticks=300, minutes=2.0):
    for _ in range(max_ticks):
        broker.tick()
        if not broker.store.queued_jobs() and not broker.store.active_jobs():
            return
        clock.advance(minutes=minutes)
    pytest.fail("the fixture queue never drained")


def test_fifo_replay_reports_all_six_metrics_against_the_observed_policy(
    broker, clock, cloud
):
    """The comparison harness: observed fair-share-plus-age against a FIFO
    replay of the same jobs, reporting cost per completed job, queue time,
    completion rate, abandoned capacity cost, retry cost, and budget refusals.

    Everything here is synthetic fixture data in a temporary store. The report
    it produces is machinery evidence, never a C18 cost claim.
    """
    from decimal import Decimal

    from gpu_broker.money import Currency

    for user_number in range(5):
        user_id = f"fixture-member-{user_number}@example.test"
        broker.add_user(user_id)
        for job_number in range(2):
            broker.submit(
                user_id=user_id,
                command=f"python fixture-{user_number}-{job_number}.py",
                gpu_type="a10g" if job_number == 0 else "t4",
                hours=0.1,
            )
    # One real interruption so a retry costs something.
    broker.tick()
    active = broker.store.active_jobs()
    assert active
    cloud.interrupt(active[0].backend_handle)
    drive_to_quiet(broker, clock)

    # More fixture jobs so the interrupted-and-resumed one still meets the
    # 20-job floor, submitted after the drain so they queue cleanly.
    for user_number in range(5):
        user_id = f"fixture-member-{user_number}@example.test"
        for job_number in range(2, 4):
            broker.submit(
                user_id=user_id,
                command=f"python fixture-{user_number}-{job_number}.py",
                gpu_type="a10g" if job_number == 2 else "t4",
                hours=0.1,
            )
    drive_to_quiet(broker, clock)

    # One budget refusal: an a100 day costs more than the default budget.
    refused = broker.submit(
        user_id="fixture-member-0@example.test",
        command="python too-expensive.py",
        gpu_type="a100",
        hours=10.0,
    )
    assert refused.refusal is not None and refused.refusal.code == "USER_BUDGET_EXCEEDED"

    # One leaked machine, charged as abandoned capacity.
    broker.store.record_abandoned(
        handle="i-fixture-leak",
        job_id=None,
        user_id=None,
        currency=Currency.USD,
        burned=Decimal("3.21"),
        note="fixture leaked instance",
    )

    report = build_cost_report(broker.store, broker.config, salt="a-long-fixture-salt")
    observed = report["current_policy"]
    fifo = report["fifo_replay"]
    usd = observed["cost"]["USD"]

    # All six required metrics, with the fixture making each one non-trivial.
    assert Decimal(usd["cost_per_completed_job"]) > 0
    assert observed["queue_p50_seconds"] is not None
    assert observed["queue_p95_seconds"] is not None
    assert 0 < observed["completion_rate"] < 1  # the refusal never completed
    assert Decimal(usd["abandoned_capacity_cost"]) == Decimal("3.21")
    assert Decimal(usd["estimated_retry_cost"]) > 0
    assert report["instrumentation"]["budget_refusals"] == 1

    # The baseline replays the exact same started jobs and reports queue time.
    assert fifo["same_trace_jobs"] == 20
    assert fifo["queue_p50_seconds"] is not None
    assert fifo["queue_p95_seconds"] is not None
    assert fifo["cost_held_constant"] is True

    # No identity leaks from the fixture into the aggregate output.
    encoded = json.dumps(report)
    assert "fixture-member" not in encoded
    assert "@example.test" not in encoded
    assert "i-fixture-leak" not in encoded


def test_lifecycle_instrumentation_records_every_cost_evidence_field(tmp_path, clock):
    """One job, whole lifecycle: submit, dispatch, interrupt, checkpoint,
    resume, complete. Every field the cost study reads must be on the record:
    scheduler decision, resource class, requested GPU memory, queued/start/end
    timestamps, interruption, checkpoint/resume, completion, abandonment, and
    estimated plus settled cost.
    """
    from decimal import Decimal

    from gpu_broker.backends import FakeBackend
    from gpu_broker.broker import Broker
    from gpu_broker.checkpoint import LocalCheckpointStore
    from gpu_broker.config import load_config
    from gpu_broker.money import Currency

    state_dir = tmp_path / "broker"
    checkpoints = LocalCheckpointStore(tmp_path / "checkpoints")
    spot = FakeBackend(
        "spot",
        clock=clock,
        checkpoints=checkpoints,
        currency=Currency.USD,
        capacity={"a10g": 2},
        startup_seconds=30.0,
        tier="spot",
        state_path=state_dir / "spot.json",
    )
    broker = Broker.open(
        state_dir,
        clock=clock,
        backends=[spot],
        config=load_config(state_dir),
        checkpoints=checkpoints,
    )
    try:
        broker.add_user("fixture-lifecycle@example.test")
        job = broker.submit(
            user_id="fixture-lifecycle@example.test",
            command="python train.py",
            gpu_type="a10g",
            hours=2.0,
            # Room above the 2-hour price: the interrupted first attempt's burn
            # plus the resumed attempt must fit under the ceiling, or the test
            # measures the budget stop instead of the resume.
            budget="10.00",
        ).job
        spot.plan(job.job_id, run_hours=2.0, total_steps=1_000, checkpoint_every=100)

        broker.tick()  # dispatch
        for _ in range(7):  # run ~35 minutes so a checkpoint lands
            clock.advance(minutes=5)
            broker.tick()
        handle = broker.status(job.job_id).backend_handle
        assert handle, "the job never started; nothing to interrupt"
        spot.interrupt(handle)
        broker.tick()  # observe the interruption, collect, requeue

        for _ in range(80):
            if broker.status(job.job_id).state is JobState.COMPLETED:
                break
            clock.advance(minutes=5)
            broker.tick()
        done = broker.status(job.job_id)
        assert done.state is JobState.COMPLETED  # completion

        rows = broker.store.conn.execute(
            "SELECT * FROM schedule_observations WHERE job_id = ? ORDER BY id",
            (job.job_id,),
        ).fetchall()
        dispatches = [row for row in rows if row["decision"] == "DISPATCH"]
        assert len(dispatches) == 2, "first start and the resume are two decisions"
        for row in dispatches:
            assert row["resource_class"] == "a10g"  # resource class
            assert row["requested_memory_mb"] == 24_576  # requested GPU memory
            assert row["backend"] == "spot"
            assert row["tier"] == "spot"

        assert done.submitted_at is not None  # queued timestamp
        assert done.started_at is not None  # start timestamp
        assert done.finished_at is not None  # end timestamp
        assert done.preemptions == 1  # interruption
        assert done.checkpoint_step is not None  # checkpoint
        assert done.attempts == 2  # resume
        assert done.reserved > Decimal("0")  # estimated cost
        assert broker.store.job_spend(job.job_id) > Decimal("0")  # settled cost

        # Abandonment: a leaked machine is chargeable against the same record.
        broker.store.record_abandoned(
            handle="i-fixture-leak",
            job_id=None,
            user_id=None,
            currency=Currency.USD,
            burned=Decimal("1.00"),
            note="fixture leaked instance",
        )
        assert broker.store.abandoned_entries(Currency.USD)
    finally:
        broker.close()

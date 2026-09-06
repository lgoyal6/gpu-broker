"""What stops one member from taking the broker down for everybody else.

The budget is already backpressure -- a queued job holds a reservation, so the
default $25 stops a member at 33 queued a10g jobs. Measured. The hole is that
the reservation is whatever `--budget` says: at `--budget 0.01` the same member
queues 2500, and at `--budget 0.001` on the free lab tier, 6000. A tick has to
score and place every queued job, so that is everybody's tick, not just theirs.

So there are three more bounds, and each one is here because something was
measured first:

  command size        10 MB of `--command` was accepted and stored verbatim.
  queue depth per user 2500 queued jobs at `--budget 0.01`, at 20 kB of
                      database each, taking a tick from 0.8 ms to 140 ms.
  running jobs per user one member took 63 of 64 free slots in a single tick.
  log volume          a job that prints is never told to stop; 20,000 lines
                      grew the database by 10.7 MB, and one line of 5 MB was
                      stored as 5 MB.

Two properties matter as much as the limits themselves, and each has a test:
a refusal has to be *predictable* -- the same submission refused for the same
named reason every time, not a timeout -- and the system has to *recover*, so
the member who hit a limit gets back in as soon as the pressure is off.
"""

from __future__ import annotations

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.config import load_config
from gpu_broker.errors import BrokerError
from gpu_broker.money import Currency
from gpu_broker.states import JobState


@pytest.fixture
def limits_broker(state_dir, clock):
    """A broker with small limits, so a test can reach them in a few lines.

    The defaults are sized for a club; a test that had to submit 33 jobs to
    prove a queue limit exists would be testing arithmetic, not the limit.
    """
    config = load_config(
        state_dir,
        overrides={
            "max_command_bytes": 128,
            "max_queued_jobs_per_user": 4,
            "max_running_jobs_per_user": 2,
            "max_log_lines_per_job": 10,
            "max_log_line_bytes": 32,
        },
    )
    cloud = FakeBackend(
        "cloud",
        clock=clock,
        currency=Currency.USD,
        capacity={"a10g": 8},
        startup_seconds=0.0,
        state_path=state_dir / "cloud.json",
    )
    instance = Broker.open(state_dir, clock=clock, backends=[cloud], config=config)
    instance.add_user("ana")
    instance.add_user("bo")
    yield instance
    instance.close()


def submit(broker, user, hours=0.5, budget="0.05", command="python train.py"):
    return broker.submit(
        user_id=user, command=command, gpu_type="a10g", hours=hours, budget=budget
    )


# ------------------------------------------------------------- request size


def test_an_oversized_command_is_refused_before_anything_is_written(limits_broker):
    """Refused by raising, not by recording a refusal.

    A recorded refusal keeps the command, so recording a 10 MB one is the
    damage the limit exists to prevent.
    """
    before = len(limits_broker.store.list_jobs(user_id="ana"))
    with pytest.raises(BrokerError, match="128"):
        submit(limits_broker, "ana", command="python " + "A" * 10_000)
    assert len(limits_broker.store.list_jobs(user_id="ana")) == before


def test_a_command_at_the_limit_is_still_accepted(limits_broker):
    result = submit(limits_broker, "ana", command="p" * 128)
    assert result.accepted


# ------------------------------------------------------------- queue depth


def test_one_user_cannot_queue_without_limit(limits_broker):
    for _ in range(4):
        assert submit(limits_broker, "ana").accepted

    result = submit(limits_broker, "ana")
    assert not result.accepted
    assert result.refusal.code == "QUEUE_DEPTH_EXCEEDED"
    assert "4" in result.refusal.reason


def test_the_queue_refusal_is_the_same_every_time(limits_broker):
    """Predictable, in the sense that matters: the same submission gets the
    same named refusal, not a timeout that depends on how loaded the box is."""
    for _ in range(4):
        submit(limits_broker, "ana")
    codes = {submit(limits_broker, "ana").refusal.code for _ in range(5)}
    assert codes == {"QUEUE_DEPTH_EXCEEDED"}


def test_a_queue_flood_does_not_refuse_anybody_else(limits_broker):
    for _ in range(6):
        submit(limits_broker, "ana")
    assert submit(limits_broker, "bo").accepted


def test_the_queue_limit_lifts_when_the_jobs_drain(limits_broker):
    """Recovery. A limit that never lets you back in is an outage with a
    friendlier message."""
    for _ in range(4):
        submit(limits_broker, "ana")
    assert not submit(limits_broker, "ana").accepted

    # The overload passes: two of the four leave the queue onto real capacity.
    # Recovery is proportional, not all-or-nothing -- two out means two back in.
    limits_broker.tick()
    assert limits_broker.store.queued_count("ana") == 2

    assert submit(limits_broker, "ana").accepted
    assert submit(limits_broker, "ana").accepted
    assert not submit(limits_broker, "ana").accepted


# ------------------------------------------------- concurrency, per tenant


def test_one_user_cannot_hold_every_gpu_at_once(limits_broker):
    """Eight free slots, one member wanting all of them, a limit of two."""
    jobs = [submit(limits_broker, "ana").job for _ in range(4)]
    report = limits_broker.tick()

    assert len(report.dispatched) == 2
    blocked = set(report.blocked_on_tenant)
    assert {job.job_id for job in jobs} - set(report.dispatched) == blocked


def test_a_tenant_limit_does_not_stop_a_second_user(limits_broker):
    """The point of the limit: the capacity one member cannot have is capacity
    somebody else gets, in the same tick."""
    for _ in range(4):
        submit(limits_broker, "ana")
    theirs = submit(limits_broker, "bo").job

    report = limits_broker.tick()
    assert theirs.job_id in report.dispatched


def test_a_tenant_blocked_job_keeps_its_place_and_runs_later(limits_broker):
    """Recovery at the dispatch boundary. Blocked is not refused: the job waits
    where it is and goes as soon as its owner is under the limit again."""
    jobs = [submit(limits_broker, "ana").job for _ in range(3)]
    first = limits_broker.tick()
    waiting = [job for job in jobs if job.job_id not in first.dispatched]
    assert len(waiting) == 1
    assert limits_broker.status(waiting[0].job_id).state is JobState.QUEUED

    # One of the running jobs finishes, freeing the member's slot.
    limits_broker.cancel(first.dispatched[0], actor="ana")
    second = limits_broker.tick()
    assert waiting[0].job_id in second.dispatched


def test_a_dry_run_reports_the_tenant_limit_too(limits_broker):
    """`plan` and `tick` read the same decisions. If a dry run did not show the
    limit, the first anybody heard of it would be a job that did not start."""
    for _ in range(4):
        submit(limits_broker, "ana")
    actions = [decision.action for decision in limits_broker.plan().decisions]
    assert actions.count("BLOCKED_TENANT") == 2


# --------------------------------------------------------------- log volume


def test_a_long_log_line_is_stored_truncated(limits_broker):
    job = submit(limits_broker, "ana").job
    limits_broker.store.append_log(job.job_id, "stdout", "y" * 5_000)

    stored = limits_broker.store.conn.execute(
        "SELECT line FROM job_logs WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (job.job_id,),
    ).fetchone()["line"]
    assert len(stored) <= 32
    assert stored.endswith("...")


def test_a_chatty_job_stops_being_stored_after_the_cap(limits_broker):
    job = submit(limits_broker, "ana").job
    for index in range(50):
        limits_broker.store.append_log(job.job_id, "stdout", f"line {index}")

    rows = limits_broker.store.conn.execute(
        "SELECT line FROM job_logs WHERE job_id = ? ORDER BY id", (job.job_id,)
    ).fetchall()
    assert len(rows) == 10
    # The last thing stored says why there is no more, so a member reading the
    # page is not left thinking their job stopped printing.
    assert "truncated" in rows[-1]["line"]


def test_the_truncation_notice_is_written_once(limits_broker):
    job = submit(limits_broker, "ana").job
    for index in range(200):
        limits_broker.store.append_log(job.job_id, "stdout", f"line {index}")

    rows = limits_broker.store.conn.execute(
        "SELECT line FROM job_logs WHERE job_id = ? ORDER BY id", (job.job_id,)
    ).fetchall()
    assert len(rows) == 10
    assert sum("truncated" in row["line"] for row in rows) == 1


def test_a_backend_poll_is_capped_the_same_way(limits_broker):
    """The path that actually carries a training script's output is
    `append_backend_logs`, not `append_log`. Capping only the second would cap
    only the broker's own notes."""
    job = submit(limits_broker, "ana").job
    limits_broker.store.append_backend_logs(
        job, [("stdout", f"line {index}") for index in range(50)]
    )
    rows = limits_broker.store.conn.execute(
        "SELECT COUNT(*) AS n FROM job_logs WHERE job_id = ?", (job.job_id,)
    ).fetchone()["n"]
    assert rows == 10


def test_a_capped_poll_still_advances_the_read_cursor(limits_broker):
    """Dropping lines must not make the broker re-read them next tick, which
    would be an infinite poll of the same output."""
    job = submit(limits_broker, "ana").job
    cursor = limits_broker.store.append_backend_logs(
        job, [("stdout", f"line {index}") for index in range(50)]
    )
    assert cursor == 50

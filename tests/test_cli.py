"""The surface a sophomore actually touches.

Adoption is the deliverable, so these tests are about whether the output tells
somebody what to do next, not only whether the exit code is right.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from gpu_broker.cli import app

runner = CliRunner()


@pytest.fixture
def home(tmp_path: Path, monkeypatch) -> Path:
    state = tmp_path / "broker"
    monkeypatch.setenv("GPU_BROKER_HOME", str(state))
    monkeypatch.setenv("GPU_BROKER_USER", "ana")
    return state


def run(*args: str):
    return runner.invoke(app, list(args), catch_exceptions=False)


def flat(text: str) -> str:
    """Collapse whitespace before asserting on prose.

    Rich wraps at the terminal width, so any sentence long enough to be worth
    checking is also long enough to be split across lines.
    """
    return " ".join(text.split())


def submit_job(*extra: str) -> str:
    """Submit a job and return its short id, read from the line the user sees."""
    result = run("submit", "--gpu", "a10g", "--hours", "1", *extra, "--", "python", "x.py")
    assert result.exit_code == 0, result.output
    for line in result.output.splitlines():
        if line.startswith("queued"):
            return line.split()[1]
    raise AssertionError(f"no 'queued' line in:\n{result.output}")


def test_submit_queues_a_job(home):
    result = run("submit", "--gpu", "a10g", "--hours", "4", "--budget", "12", "--", "python", "train.py")
    assert result.exit_code == 0
    assert "queued" in result.output
    assert "$12.00" in result.output


def test_a_first_time_user_is_registered_without_asking_anyone(home):
    """Making twenty people request an account before they can run anything is
    the friction that kills adoption."""
    result = run("submit", "--gpu", "a10g", "--hours", "1", "--", "nvidia-smi")
    assert result.exit_code == 0
    assert "welcome, ana" in result.output
    assert "$25.00" in result.output


def test_the_command_after_the_dashes_is_preserved_verbatim(home):
    run("submit", "--gpu", "a10g", "--hours", "1", "--", "python", "train.py", "--epochs", "10")
    result = run("status")
    assert "python train.py --epochs 10" in result.output.replace("…", "")


def test_submitting_with_no_command_shows_the_exact_line_to_type(home):
    result = run("submit", "--gpu", "a10g", "--hours", "1")
    assert result.exit_code == 1
    assert "gpu submit --gpu a10g --hours 4 --budget 12 -- python train.py" in result.output


def test_an_unknown_gpu_lists_the_ones_that_exist(home):
    result = run("submit", "--gpu", "h100", "--hours", "1", "--", "x")
    assert result.exit_code == 1
    assert "a10g" in result.output and "a6000" in result.output


def test_over_budget_exits_nonzero_and_names_the_shortfall(home):
    run("admin", "add-user", "cy", "--usd", "3")
    result = runner.invoke(
        app, ["submit", "--gpu", "a10g", "--hours", "4", "--", "python", "big.py"],
        env={"GPU_BROKER_USER": "cy"}, catch_exceptions=False,
    )
    assert result.exit_code == 2
    assert "short" in result.output
    assert "$3.00" in result.output


def test_queue_why_shows_the_formula_and_the_terms(home):
    run("submit", "--gpu", "a10g", "--hours", "1", "--", "x")
    result = run("queue", "--why")
    assert result.exit_code == 0
    assert "priority =" in result.output
    assert "share of pool used" in result.output
    assert "decays by half" in result.output


def test_an_empty_queue_says_so_rather_than_printing_an_empty_table(home):
    result = run("queue")
    assert "queue is empty" in result.output


def test_who_reports_holders_and_says_when_the_pool_is_idle(home):
    assert "idle" in run("who").output
    run("submit", "--gpu", "a10g", "--hours", "1", "--", "x")
    run("tick")
    output = run("who").output
    assert "ana" in output and "a10g" in output


def test_budget_separates_the_two_currencies(home):
    run("submit", "--gpu", "a10g", "--hours", "1", "--", "x")
    output = run("budget").output
    assert "dollars" in output and "free gpu-hours" in output
    assert "held" in output


def test_pool_view_separates_running_from_queued_demand(home):
    run("submit", "--gpu", "a10g", "--hours", "1", "--", "x")
    output = run("budget", "--pool").output
    assert "queued" in output and "free to" in output
    assert "does not count against the cap" in output


def test_status_of_one_job_shows_its_history(home):
    job_id = submit_job()
    result = run("status", job_id)
    assert "history" in result.output
    assert "QUEUED" in result.output


def test_a_short_job_id_is_enough(home):
    """Nobody types 32 hex characters."""
    job_id = submit_job()
    assert run("status", job_id[:5]).exit_code == 0


def test_an_unknown_job_id_says_so(home):
    result = run("status", "zzzzzz")
    assert result.exit_code == 1
    assert "no job" in result.output


def test_cancel_returns_the_unspent_budget(home):
    job_id = submit_job("--budget", "12")
    result = run("cancel", job_id)
    assert "cancelled" in result.output
    assert "$12.00 returned" in result.output


def test_you_cannot_cancel_somebody_elses_job(home):
    job_id = submit_job()
    result = runner.invoke(
        app, ["cancel", job_id], env={"GPU_BROKER_USER": "bo"}, catch_exceptions=False
    )
    assert result.exit_code == 1
    assert "belongs to ana" in result.output


def test_logs_print_after_a_job_has_run(home):
    job_id = submit_job()
    run("tick")
    output = run("logs", job_id).output
    assert "dispatched" in output


def test_reconcile_reports_a_clean_system(home):
    result = run("reconcile")
    assert result.exit_code == 0
    assert "no drift" in result.output


def test_reconcile_exits_nonzero_when_there_is_drift(home):
    """So it can be wired into a cron job without parsing output."""
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(home, clock=SystemClock())
    broker.scheduler.backend("fake").leak(job_id="d" * 32, user_id="bo")
    broker.close()

    result = run("reconcile")
    assert result.exit_code == 1
    assert "ORPHAN" in result.output
    assert "nothing was touched" in result.output


def test_tick_says_nothing_to_do_when_idle(home):
    assert "nothing to do" in run("tick").output


def test_admin_can_set_a_budget(home):
    result = run("admin", "add-user", "bo", "--usd", "40", "--gpu-hours", "12")
    assert "$40.00" in result.output and "12.00 gpu-hr" in result.output
    assert "bo" in run("admin", "users").output


# --------------------------------------------------------- phase 1 surface


def test_reap_reports_a_clean_pool(home):
    result = run("reap")
    assert result.exit_code == 0
    assert "nothing to reap" in result.output


def test_reap_finds_an_orphan_names_the_person_and_the_cost(home):
    """The number and the name are the whole point: this is what turns 'the
    credits are gone' into a conversation with somebody."""
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(home, clock=SystemClock())
    broker.scheduler.backend("fake").leak(job_id="e" * 32, user_id="bo")
    broker.close()

    result = run("reap")
    assert result.exit_code == 1
    assert "bo" in result.output
    assert "orphan" in result.output
    assert "Nothing was terminated" in result.output


def test_reap_never_offers_a_way_to_terminate(home):
    """Report, do not terminate, until it has been watched working. There is no
    --force, and this test exists so that adding one is a deliberate act."""
    assert "--force" not in run("reap", "--help").output
    assert "--terminate" not in run("reap", "--help").output
    assert "--yes" not in run("reap", "--help").output


def test_tick_dry_run_changes_nothing(home):
    submit_job()
    result = run("tick", "--dry-run")
    assert result.exit_code == 0
    assert "nothing was launched" in result.output
    assert "launch" in result.output
    # Still queued afterwards.
    assert "queue is empty" not in run("queue").output


def test_tick_dry_run_on_an_empty_queue_says_so(home):
    assert "nothing queued" in run("tick", "--dry-run").output


def test_cancel_dry_run_stops_nothing(home):
    job_id = submit_job("--budget", "8")
    result = run("cancel", job_id, "--dry-run")
    assert result.exit_code == 0
    assert "would cancel" in result.output
    assert "$8.00 would go back" in result.output
    # Really still there.
    assert run("status", job_id).output.count("QUEUED") >= 1


def test_cancel_dry_run_describes_the_machine_it_would_stop(home):
    job_id = submit_job()
    run("tick")
    result = run("cancel", job_id, "--dry-run")
    assert "would terminate" in result.output


def test_the_dry_run_still_respects_ownership(home):
    job_id = submit_job()
    result = runner.invoke(
        app, ["cancel", job_id, "--dry-run"],
        env={"GPU_BROKER_USER": "bo"}, catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "belongs to ana" in result.output


# ----------------------------------------------------------- the local pool


@pytest.fixture
def lab_home(tmp_path: Path, monkeypatch, gpu_host):
    """A state dir configured to use the simulated lab machine, the way a club
    admin would write it."""
    import json

    state = tmp_path / "broker"
    state.mkdir(parents=True)
    spec = gpu_host.server.spec()
    (state / "config.json").write_text(
        json.dumps(
            {
                "backends": ["local"],
                "local": {
                    "hosts": [
                        {"hostname": spec.hostname, "username": spec.username, "port": spec.port}
                    ],
                    "max_jobs_per_gpu": 2,
                    "command_timeout_seconds": 3.0,
                    "poll_timeout_seconds": 2.0,
                },
            }
        )
    )
    monkeypatch.setenv("GPU_BROKER_HOME", str(state))
    monkeypatch.setenv("GPU_BROKER_USER", "ana")
    return state


def test_hosts_says_nothing_is_configured_by_default(home):
    assert "no local pool configured" in run("hosts").output


def test_hosts_shows_the_lab_machine(lab_home):
    output = run("hosts").output
    assert "HEALTHY" in output
    assert "A6000" in output


def test_hosts_flags_a_machine_whose_limits_do_not_bite(lab_home, gpu_host):
    """The loudest thing on the page, because a job on that host can take the
    whole machine down."""
    gpu_host.limits_apply = False
    output = run("hosts", "--refresh").output
    assert "NOT ENFORCED" in flat(output)
    assert "take the whole machine down" in flat(output)
    assert "DRAINING" in output


def test_hosts_explains_a_broken_driver(lab_home, gpu_host):
    gpu_host.nvidia_smi_works = False
    output = run("hosts", "--refresh").output
    assert "DRAINING" in output
    assert "reboot" in flat(output)


def test_a_drained_host_says_running_jobs_are_untouched(lab_home):
    result = run("admin", "drain", "127.0.0.1", "--reason", "swapping a fan")
    assert result.exit_code == 0
    assert "swapping a fan" in flat(result.output)
    assert "already running there are untouched" in flat(result.output)
    assert "DRAINING" in run("hosts").output


def test_undrain_puts_a_host_back(lab_home):
    run("admin", "drain", "127.0.0.1", "--reason", "maintenance")
    assert run("admin", "undrain", "127.0.0.1").exit_code == 0
    assert "HEALTHY" in run("hosts").output


def test_draining_an_unknown_host_is_an_error(lab_home):
    result = run("admin", "drain", "not-a-host")
    assert result.exit_code == 1
    assert "no host" in result.output


def test_a_lab_job_runs_end_to_end_from_the_cli(lab_home, gpu_host):
    result = run("submit", "--gpu", "a6000", "--hours", "1", "--", "python", "train.py")
    assert result.exit_code == 0
    assert "gpu-hr" in result.output, "the free pool was priced in dollars"

    run("tick")
    assert len(gpu_host.active_units()) == 1
    assert "a6000" in run("who").output


def test_the_lab_pool_shows_up_in_gpu_hours_not_dollars(lab_home, gpu_host):
    run("submit", "--gpu", "a6000", "--hours", "2", "--", "python", "train.py")
    output = run("budget").output
    assert "free gpu-hours" in output
    assert "2.25 gpu-hr" in output.replace(",", "")


def test_a_drain_survives_the_process_that_set_it(lab_home, gpu_host):
    """Regression: the drain used to live in the backend's memory, so
    `gpu admin drain` returned successfully and changed nothing. Every CLI
    invocation is a new process; each `run` below is a separate Broker.
    """
    run("admin", "drain", "127.0.0.1", "--reason", "bad fan")

    assert "DRAINING" in run("hosts").output
    assert "bad fan" in flat(run("hosts").output)

    # And it actually stops work landing there, not just the display.
    run("submit", "--gpu", "a6000", "--hours", "1", "--", "python", "train.py")
    run("tick")
    assert gpu_host.active_units() == []

    run("admin", "undrain", "127.0.0.1")
    run("tick")
    assert len(gpu_host.active_units()) == 1


# ------------------------------------------------------- cost and idle (CLI)


def test_prices_says_which_numbers_nobody_refreshed(home):
    output = run("prices").output
    assert "builtin" in output
    assert "typed in by hand" in flat(output)
    assert "$1.01" in output


def test_forecast_on_an_untouched_pool_is_ok(home):
    result = run("forecast")
    assert result.exit_code == 0
    assert "nothing is being spent" in flat(result.output)


def test_forecast_exits_nonzero_when_the_pool_is_nearly_gone(home):
    """So it can be wired into a cron job without parsing output."""
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock
    from gpu_broker.db.connection import transaction
    from gpu_broker.models import LedgerKind
    from gpu_broker.money import Currency, money

    broker = Broker.open(home, clock=SystemClock())
    broker.add_user("ana")
    with transaction(broker.store.conn) as conn:
        broker.store._insert_ledger(
            conn, job_id=None, user_id="ana", currency=Currency.USD,
            kind=LedgerKind.SETTLE, amount=money("480.00"),
            at=broker.clock.now(), note="a busy week",
        )
    broker.close()

    result = run("forecast")
    assert result.exit_code == 1
    assert "CRITICAL" in result.output or "EXHAUSTED" in result.output


def test_forecast_admits_it_is_a_straight_line(home):
    assert "not in this number" in flat(run("forecast").output)


def test_reclaim_on_a_healthy_pool_says_nothing_is_idle(home):
    assert "nothing idle" in run("reclaim").output


def idle_job_on_disk(home, minutes_idle: int = 40):
    """An idle, already-notified job, dated against the wall clock.

    The CLI always uses a SystemClock. Setting this up with a ManualClock would
    write samples dated months away from `now`, the idle window would find none
    of them, and the test would pass or fail for reasons having nothing to do
    with the CLI.
    """
    import datetime as dt

    from gpu_broker.backends import FakeBackend
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock
    from gpu_broker.idle import IDLE_NOTIFICATION
    from gpu_broker.models import Sample
    from gpu_broker.money import Currency
    from gpu_broker.states import JobState

    clock = SystemClock()
    cloud = FakeBackend(
        "fake", clock=clock, currency=Currency.USD, capacity={"a10g": 2},
        startup_seconds=0.0, state_path=home / "fake-backend.json",
    )
    broker = Broker.open(home, clock=clock, backends=[cloud], config=None)
    broker.add_user("ana")
    job = broker.submit(user_id="ana", command="python dead.py", gpu_type="a10g", hours=4).job
    broker.tick()
    broker.store.transition(broker.status(job.job_id), JobState.RUNNING)

    now = clock.now()
    broker.store.record_samples(
        job.job_id,
        [
            Sample(at=now - dt.timedelta(minutes=m), gpu_percent=0.0, memory_mb=128)
            for m in range(9, -1, -1)
        ],
    )
    # Notified, and long enough ago that the grace period has expired.
    with_backdate = broker.store.notify(
        user_id="ana", kind=IDLE_NOTIFICATION, job_id=job.job_id,
        message="Job has used no GPU for 10 minutes.",
    )
    broker.store.conn.execute(
        "UPDATE notifications SET at = ? WHERE id = ?",
        ((now - dt.timedelta(minutes=minutes_idle)).isoformat(timespec="microseconds"),
         with_backdate.notification_id),
    )
    broker.close()
    return job


def test_reclaim_reports_without_killing_and_shows_the_samples(home):
    from gpu_broker.backends import FakeBackend
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock
    from gpu_broker.money import Currency
    from gpu_broker.states import JobState

    job = idle_job_on_disk(home)

    result = run("reclaim")
    assert result.exit_code == 0
    assert "would reclaim" in result.output
    assert "Nothing was killed" in flat(result.output)
    assert "the samples that justify it" in flat(result.output)

    clock = SystemClock()
    cloud = FakeBackend(
        "fake", clock=clock, currency=Currency.USD, capacity={"a10g": 2},
        startup_seconds=0.0, state_path=home / "fake-backend.json",
    )
    reopened = Broker.open(home, clock=clock, backends=[cloud], config=None)
    assert reopened.status(job.job_id).state is JobState.RUNNING
    reopened.close()


def test_reclaim_has_no_way_to_skip_the_notification(home):
    """--force overrides the config switch, not the safety property. Nothing is
    killed without a notification already recorded and a grace period expired."""
    help_text = run("reclaim", "--help").output
    assert "--no-notify" not in help_text
    assert "--skip-grace" not in help_text


def test_status_shows_a_jobs_own_utilization_and_notices(home):
    job = idle_job_on_disk(home)

    output = run("status", job.short_id).output
    assert "gpu utilization" in output
    assert "notices" in output
    assert "used no GPU" in flat(output)


# ----------------------------------------------------- metrics and reporting


def test_metrics_shows_the_last_week(home):
    submit_job()
    run("tick")
    output = run("metrics").output
    assert "jobs queued" in output or "jobs running" in output


def test_metrics_can_print_the_prometheus_body(home):
    run("tick")
    output = run("metrics", "--prometheus").output
    assert "# TYPE gpu_broker_" in output


def test_metrics_can_prune(home):
    run("tick")
    assert "removed" in run("metrics", "--prune").output


def test_the_digest_prints_without_a_webhook(home):
    """A digest that needs a Slack workspace does not exist for a club that uses
    Discord."""
    output = run("digest").output
    assert "GPU pool" in output
    assert "left" in output


def test_sending_without_a_webhook_says_what_to_do_instead(home):
    result = run("digest", "--send")
    assert result.exit_code == 1
    assert "paste it wherever" in flat(result.output)


def test_the_report_is_generated_not_written(home):
    submit_job()
    run("tick")
    output = run("report").output
    assert "what actually happened" in output
    assert "## Adoption" in output
    assert "## Where it loses" in output


def test_the_report_can_be_written_to_a_file(home, tmp_path):
    target = tmp_path / "report.md"
    run("report", "--out", str(target))
    assert "what actually happened" in target.read_text()


def test_the_report_says_work_lost_is_zero_on_a_clean_run(home):
    submit_job("--hours", "0.005")
    run("run")
    assert "Zero." in run("report").output

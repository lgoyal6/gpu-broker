"""What a member may do to somebody else's job, and what happens when they stop
being a member.

`test_web_auth.py` covers getting *in*: forged cookies, stale state, a login
that is not on the list. This file covers the two questions that come after
that, both of which were answered wrongly:

  Can one member touch another's job? Cancelling is checked; reading is
  deliberately open, because the whole point of `gpu who` is that twenty people
  can see what the pool is doing. Both are pinned here so a later change has to
  be a decision rather than an accident.

  Does losing membership actually stop anything? It did not. `Membership.check`
  runs at sign-in and nowhere else, so a session outlives the membership that
  created it by up to fourteen days, and the daemon that dispatches jobs never
  had a notion of membership at all: a job queued before somebody was removed
  still ran, on the club's money, after they were gone.

The second one is why suspension lives in the users table rather than in the
web app. `gpu run` holds the credentials and never imports FastAPI; a check
that only the web app can do is a check the thing that spends money cannot do.
"""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.errors import Unauthorized
from gpu_broker.money import Currency
from gpu_broker.states import JobState
from gpu_broker.web.app import create_app
from gpu_broker.web.auth import STATE_COOKIE, Identity, WebConfig


class StubProvider:
    """Stands in for GitHub. Everything else in the auth path is real."""

    def __init__(self, identity=Identity("ana", "Ana"), org_member=True):
        self.identity = identity
        self.org_member = org_member

    def authorize_url(self, state: str) -> str:
        return f"/auth/callback?code=stub&state={state}"

    def exchange(self, code: str):
        return self.identity, self.org_member


@pytest.fixture
def rig(tmp_path: Path):
    state_dir = tmp_path / "broker"
    config = load_config(state_dir)
    allowlist_file = tmp_path / "allowlist.txt"
    allowlist_file.write_text("ana\nbo\n")
    web = replace(
        WebConfig(),
        github_client_id="cid",
        allowlist_file=str(allowlist_file),
        session_secret="test-secret",
        base_url="http://testserver",
    )
    app = create_app(config=config, web=web, provider=StubProvider())
    return {
        "app": app,
        "config": config,
        "state_dir": state_dir,
        "allowlist_file": allowlist_file,
    }


@pytest.fixture
def client(rig):
    with TestClient(rig["app"], follow_redirects=False) as made:
        yield made


def sign_in(client, login):
    client.app.state.provider.identity = Identity(login, login.title())
    client.get("/auth/login")
    state = client.cookies.get(STATE_COOKIE)
    client.get(f"/auth/callback?code=stub&state={state}")
    return client


def daemon(rig, clock=None):
    """A broker that holds backends -- what `gpu run` would be."""
    clock = clock or ManualClock()
    return Broker.open(
        rig["state_dir"],
        clock=clock,
        config=rig["config"],
        backends=[
            FakeBackend(
                "fake",
                clock=clock,
                currency=Currency.USD,
                capacity={"a10g": 4},
                startup_seconds=0.0,
                state_path=rig["state_dir"] / "fake-backend.json",
            )
        ],
    )


def submit_as(client, command="python train.py"):
    return client.post(
        "/submit",
        data={"command": command, "gpu": "a10g", "hours": "0.5", "budget": "1.00"},
    )


# ------------------------------------------------- one member, another's job


def test_a_member_cannot_cancel_somebody_elses_job(client, rig):
    sign_in(client, "ana")
    job_id = submit_as(client).headers["location"].rsplit("/", 1)[-1]

    sign_in(client, "bo")
    response = client.post(f"/jobs/{job_id}/cancel")
    assert response.status_code == 403
    assert "ana" in response.text

    with daemon(rig) as broker:
        assert broker.status(job_id).state is JobState.QUEUED


def test_a_member_cannot_cancel_from_the_cli_either(rig):
    """The web app is not the only door. The CLI trusts `GPU_BROKER_USER`, and
    the ownership check has to be in front of the broker, not in the page."""
    from typer.testing import CliRunner

    from gpu_broker.cli import app

    with daemon(rig) as broker:
        broker.add_user("ana")
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["cancel", job.job_id, "--state-dir", str(rig["state_dir"])],
        env={"GPU_BROKER_USER": "bo"},
    )
    assert result.exit_code != 0
    with daemon(rig) as broker:
        assert broker.status(job.job_id).state is JobState.QUEUED


def test_the_broker_itself_refuses_a_cancel_by_somebody_else(rig):
    """The check was in the callers, not in the thing that cancels.

    Both doors happened to check ownership before calling `Broker.cancel`, and
    `Broker.cancel` checked nothing -- so the rule was "every future caller
    remembers", which is not a rule. Before this test, `cancel(actor="bo")` on
    ana's job terminated it and refunded her reservation.
    """
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.add_user("bo")
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job

        with pytest.raises(Unauthorized, match="ana"):
            broker.cancel(job.job_id, actor="bo")
        assert broker.status(job.job_id).state is JobState.QUEUED


def test_the_broker_lets_an_officer_cancel_for_somebody(rig):
    """The escape hatch the club needs: an officer stopping a run whose owner
    has gone home. Explicit, and it names who did it in the job's history."""
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.add_user("laksh", is_admin=True)
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job

        cancelled = broker.cancel(job.job_id, actor="laksh")
        assert cancelled.state is JobState.CANCELLED
        assert any("laksh" in (reason or "") for *_, reason in broker.store.history(job.job_id))


def test_a_dispatched_job_cannot_be_cancelled_by_somebody_else(rig):
    """The same rule once a machine exists. This is the expensive direction:
    a cross-user cancel here terminates hardware somebody else is paying for."""
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.add_user("bo")
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job
        broker.tick()
        assert broker.status(job.job_id).state is not JobState.QUEUED

        with pytest.raises(Unauthorized):
            broker.cancel(job.job_id, actor="bo")
        assert not broker.status(job.job_id).is_terminal


def test_reading_another_members_job_is_open_on_purpose(client, rig):
    """Pinned, not fixed. `gpu who` exists so twenty people can see what the
    pool is doing; a job page that only its owner could read would make the
    dashboard a lie. If this ever changes it should be because somebody decided
    to, and this test is what makes them notice."""
    sign_in(client, "ana")
    job_id = submit_as(client).headers["location"].rsplit("/", 1)[-1]

    sign_in(client, "bo")
    assert client.get(f"/jobs/{job_id}").status_code == 200


def test_a_stranger_sees_nothing(client, rig):
    sign_in(client, "ana")
    job_id = submit_as(client).headers["location"].rsplit("/", 1)[-1]

    client.cookies.clear()
    assert client.get(f"/jobs/{job_id}").status_code == 303
    assert client.post(f"/jobs/{job_id}/cancel").status_code == 303
    # 303 to the sign-in page rather than 401, because the app's 401 handler
    # redirects everything but /metrics. Wrong-ish for an EventSource, which
    # cannot follow it -- but it is not 200, which is what this test is for.
    assert client.get(f"/jobs/{job_id}/stream").status_code != 200


# ------------------------------------------------------- losing membership


def test_a_removed_member_cannot_spend_with_a_live_session(client, rig):
    """The session is fourteen days long and membership was checked once, at
    sign-in. Being removed from the club has to bite on the next protected
    action, not on the next sign-in that never comes."""
    sign_in(client, "ana")
    assert submit_as(client).status_code == 303

    rig["allowlist_file"].write_text("bo\n")

    response = submit_as(client)
    assert response.status_code == 403
    with daemon(rig) as broker:
        assert len(broker.store.list_jobs(user_id="ana")) == 1


def test_a_removed_member_cannot_read_with_a_live_session(client, rig):
    sign_in(client, "ana")
    rig["allowlist_file"].write_text("bo\n")
    assert client.get("/").status_code == 403


def test_a_suspended_member_is_refused_at_submission(rig):
    """The broker's own copy of the same idea, so it works from the CLI and
    from a deployment that authenticates against a GitHub org, where the web
    app cannot recheck anything without calling GitHub on every request."""
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.suspend_user("ana", reason="left the club", actor="laksh")

        result = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        )
        assert not result.accepted
        assert result.refusal.code == "USER_SUSPENDED"
        assert "left the club" in result.refusal.reason


def test_a_queued_job_is_not_dispatched_after_its_owner_is_suspended(rig):
    """The one that matters. Authorization at submission is not authorization:
    the job sat in the queue, the member was removed, and the daemon launched
    it anyway on the club's credits."""
    with daemon(rig) as broker:
        broker.add_user("ana")
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job

        broker.suspend_user("ana", reason="left the club", actor="laksh")

        report = broker.tick()
        assert job.job_id not in report.dispatched
        # Its own list, not the tenant-limit one. "Wait, the pool is busy" and
        # "an officer has to put you back" are different answers and a member
        # acting on the wrong one waits for something that will never happen.
        assert job.job_id in report.blocked_on_authorization
        assert job.job_id not in report.blocked_on_tenant
        assert broker.status(job.job_id).state is JobState.QUEUED


def test_a_restored_member_gets_their_queued_job_back(rig):
    """Suspension holds a job, it does not throw it away. Somebody removed by
    mistake on a Friday should not lose six hours of queue position."""
    with daemon(rig) as broker:
        broker.add_user("ana")
        job = broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
        ).job
        broker.suspend_user("ana", reason="mistake", actor="laksh")
        broker.tick()

        broker.restore_user("ana", actor="laksh")
        report = broker.tick()
        assert job.job_id in report.dispatched


def test_suspension_does_not_touch_anybody_else(rig):
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.add_user("bo")
        theirs = broker.submit(
            user_id="bo", command="python train.py", gpu_type="a10g", hours=0.5
        ).job
        broker.suspend_user("ana", reason="left", actor="laksh")

        report = broker.tick()
        assert theirs.job_id in report.dispatched


def test_a_suspended_member_is_refused_by_the_web_app_too(client, rig):
    """The web app and the daemon read the same users table, so an officer only
    has to say this once."""
    sign_in(client, "ana")
    assert submit_as(client).status_code == 303

    with daemon(rig) as broker:
        broker.suspend_user("ana", reason="left the club", actor="laksh")

    assert submit_as(client).status_code == 403


def test_the_log_stream_is_refused_after_membership_is_revoked(client, rig):
    sign_in(client, "ana")
    job_id = submit_as(client).headers["location"].rsplit("/", 1)[-1]
    # Cancelled first, so the job is terminal. Not incidental: `client.stream`
    # does not return until the first byte of body, so if this check regresses
    # and the stream is allowed, a live job would leave the test blocked on a
    # connection that is working exactly as designed. A terminal job's stream
    # sends `done` and closes, so the regression fails here instead of hanging.
    client.post(f"/jobs/{job_id}/cancel")

    rig["allowlist_file"].write_text("bo\n")
    with client.stream("GET", f"/jobs/{job_id}/stream") as response:
        assert response.status_code == 403


def test_a_live_log_stream_ends_when_membership_is_revoked_mid_stream(client, rig):
    """A stream is a long-lived execution boundary, and the only one here. It
    authenticated once at connect and then yielded whatever the job printed for
    as long as the socket stayed open -- so a member removed at 2pm kept reading
    live output until they closed the tab.

    Two threads, both load-bearing. The revocation has to happen *after* the
    connection is accepted, and `client.stream()` does not return until the
    first byte of body, so revoking on this thread would deadlock rather than
    test anything. The second thread is a watchdog: it makes the job terminal,
    which ends the stream by the ordinary route, so a regression fails this test
    in eight seconds instead of hanging the suite -- which is what the missing
    check did when this test was first written.
    """
    import threading

    sign_in(client, "ana")
    job_id = submit_as(client).headers["location"].rsplit("/", 1)[-1]

    def revoke():
        time.sleep(1.0)
        rig["allowlist_file"].write_text("bo\n")

    def watchdog():
        time.sleep(8.0)
        with daemon(rig) as broker:
            job = broker.status(job_id)
            if not job.is_terminal:
                broker.store.transition(job, JobState.CANCELLED, reason="watchdog")

    for worker in (threading.Thread(target=revoke), threading.Thread(target=watchdog)):
        worker.daemon = True
        worker.start()

    events: list[str] = []
    with client.stream("GET", f"/jobs/{job_id}/stream") as response:
        assert response.status_code == 200
        for line in response.iter_lines():
            events.append(line)
            if "revoked" in line or "done" in line or len(events) > 20:
                break

    assert any("revoked" in line for line in events), events

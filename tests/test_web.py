"""The web app, driven through the real routes.

The identity provider is the only thing stubbed. Everything else -- sessions,
membership, templates, the broker underneath -- is what runs in production.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import SystemClock
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.states import JobState
from gpu_broker.web.app import create_app
from gpu_broker.web.auth import SESSION_COOKIE, STATE_COOKIE, Identity, Sessions, WebConfig


class StubProvider:
    """Stands in for GitHub. Everything else in the auth path is real."""

    def __init__(self, identity=Identity("ana", "Ana"), org_member=True, boom=None):
        self.identity = identity
        self.org_member = org_member
        self.boom = boom

    def authorize_url(self, state: str) -> str:
        return f"/auth/callback?code=stub&state={state}"

    def exchange(self, code: str):
        if self.boom:
            raise self.boom
        return self.identity, self.org_member


@pytest.fixture
def rig(tmp_path: Path):
    state_dir = tmp_path / "broker"
    config = load_config(state_dir)
    web = replace(
        WebConfig(),
        github_client_id="cid",
        allowlist=("ana", "bo", "laksh"),
        admins=("laksh",),
        session_secret="test-secret",
        base_url="http://testserver",
    )
    provider = StubProvider()
    app = create_app(config=config, web=web, provider=provider)
    return {
        "app": app, "config": config, "web": web, "provider": provider,
        "state_dir": state_dir,
    }


@pytest.fixture
def client(rig):
    with TestClient(rig["app"], follow_redirects=False) as made:
        yield made


def sign_in(client, rig, login="ana"):
    rig["provider"].identity = Identity(login, login.title())
    client.get("/auth/login")
    state = client.cookies.get(STATE_COOKIE)
    client.get(f"/auth/callback?code=stub&state={state}")
    return client


def daemon(rig, backends=None):
    """A broker that *does* hold backends -- what `gpu run` would be."""
    return Broker.open(
        rig["state_dir"], clock=SystemClock(), config=rig["config"],
        backends=backends if backends is not None else [
            FakeBackend("fake", clock=SystemClock(), currency=Currency.USD,
                        capacity={"a10g": 2}, startup_seconds=0.0,
                        state_path=rig["state_dir"] / "fake-backend.json")
        ],
    )


# ---------------------------------------------------------------- signing in


def test_health_needs_no_sign_in(client):
    assert client.get("/health").json() == {"ok": True}


def test_every_page_needs_a_session(client):
    for path in ("/", "/jobs", "/submit", "/admin"):
        response = client.get(path)
        assert response.status_code == 303, path
        assert response.headers["location"] == "/auth/login"


def test_signing_in_lands_on_the_dashboard(client, rig):
    client.get("/auth/login")
    state = client.cookies.get(STATE_COOKIE)
    response = client.get(f"/auth/callback?code=stub&state={state}")

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert client.cookies.get(SESSION_COOKIE)


def test_a_callback_without_matching_state_is_refused(client):
    """Without this, anybody can hand somebody a link that signs them into an
    account they did not choose."""
    client.get("/auth/login")
    response = client.get("/auth/callback?code=stub&state=not-the-one")

    assert response.status_code == 400
    assert "expired or was not started here" in response.text
    assert not client.cookies.get(SESSION_COOKIE)


def test_a_callback_with_no_state_at_all_is_refused(client):
    response = client.get("/auth/callback?code=stub&state=")
    assert not client.cookies.get(SESSION_COOKIE)
    assert "expired or was not started here" in response.text


def test_somebody_not_in_the_club_is_told_why(client, rig):
    rig["provider"].identity = Identity("mallory")
    rig["provider"].org_member = False
    client.get("/auth/login")
    state = client.cookies.get(STATE_COOKIE)
    response = client.get(f"/auth/callback?code=stub&state={state}")

    assert response.status_code == 403
    assert "not on the club allowlist" in response.text
    assert not client.cookies.get(SESSION_COOKIE)


def test_a_first_sign_in_creates_the_account(client, rig):
    sign_in(client, rig, "bo")
    with daemon(rig) as broker:
        user = broker.store.maybe_user("bo")
        assert user is not None
        assert user.budget_usd == rig["config"].default_budget_usd


def test_signing_out_clears_the_session(client, rig):
    sign_in(client, rig)
    client.get("/auth/logout")
    assert client.get("/").status_code == 303


def test_a_forged_session_cookie_does_not_work(client, rig):
    forged = Sessions("some-other-secret", 3600).issue(Identity("mallory"))
    client.cookies.set(SESSION_COOKIE, forged)
    assert client.get("/").status_code == 303


# ---------------------------------------------------------------- the pages


def test_the_dashboard_shows_the_pool(client, rig):
    sign_in(client, rig)
    body = client.get("/").text
    assert "The pool" in body
    assert "dollars left" in body
    assert "free gpu-hours left" in body


def test_the_dashboard_shows_what_is_running_and_who_holds_it(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "python train.py", "gpu": "a10g", "hours": "2", "budget": ""})
    with daemon(rig) as broker:
        broker.tick()

    body = client.get("/").text
    assert "python train.py" in body or "ana" in body
    assert "Running now" in body


def test_the_dashboard_flags_idle_jobs(client, rig):
    import datetime as dt

    from gpu_broker.idle import IDLE_NOTIFICATION
    from gpu_broker.models import Sample

    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "4", "budget": ""})
    with daemon(rig) as broker:
        broker.tick()
        job = broker.history(user_id="ana")[0]
        broker.store.transition(broker.status(job.job_id), JobState.RUNNING)
        now = broker.clock.now()
        broker.store.record_samples(
            job.job_id,
            [Sample(at=now - dt.timedelta(minutes=m), gpu_percent=0.0) for m in range(9, -1, -1)],
        )
        broker.store.notify(user_id="ana", kind=IDLE_NOTIFICATION, job_id=job.job_id, message="idle")

    assert "holding a GPU without using it" in client.get("/").text


def test_the_queue_explains_its_own_ordering(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    body = client.get("/").text
    assert "fair share plus waiting time" in body


def test_my_jobs_only_shows_mine(client, rig):
    sign_in(client, rig, "ana")
    client.post("/submit", data={"command": "mine.py", "gpu": "a10g", "hours": "1", "budget": ""})
    client.get("/auth/logout")
    sign_in(client, rig, "bo")
    client.post("/submit", data={"command": "theirs.py", "gpu": "a10g", "hours": "1", "budget": ""})

    body = client.get("/jobs").text
    assert "theirs.py" in body
    assert "mine.py" not in body


def test_everyones_jobs_is_one_click_away(client, rig):
    sign_in(client, rig, "ana")
    client.post("/submit", data={"command": "mine.py", "gpu": "a10g", "hours": "1", "budget": ""})
    client.get("/auth/logout")
    sign_in(client, rig, "bo")

    body = client.get("/jobs?everyone=true").text
    assert "mine.py" in body


def test_a_missing_job_is_a_404_page_not_a_stack_trace(client, rig):
    sign_in(client, rig)
    response = client.get("/jobs/deadbeef")
    assert response.status_code == 404
    assert "no job" in response.text


# ------------------------------------------------------------------ submit


def test_submitting_from_the_browser_queues_a_job(client, rig):
    sign_in(client, rig)
    response = client.post(
        "/submit", data={"command": "python train.py --epochs 3", "gpu": "a10g",
                         "hours": "2", "budget": ""}
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/jobs/")

    with daemon(rig) as broker:
        jobs = broker.history(user_id="ana")
        assert len(jobs) == 1
        assert jobs[0].command == "python train.py --epochs 3"
        assert jobs[0].state is JobState.QUEUED


def test_a_refusal_is_shown_on_the_form_not_thrown_away(client, rig):
    sign_in(client, rig)
    response = client.post(
        "/submit", data={"command": "x", "gpu": "a10g", "hours": "4", "budget": "9999"}
    )
    assert response.status_code == 200
    assert "per-job cap" in response.text
    assert 'value="9999"' in response.text, "made them retype it"


def test_a_bad_gpu_type_lists_the_real_ones(client, rig):
    sign_in(client, rig)
    response = client.post(
        "/submit", data={"command": "x", "gpu": "h100", "hours": "1", "budget": ""}
    )
    assert "a10g" in response.text and "a6000" in response.text


def test_the_form_shows_what_you_have_left(client, rig):
    sign_in(client, rig)
    body = client.get("/submit").text
    assert "What you have left this month" in body
    assert "$25.00 left" in body


def test_the_form_tells_you_how_to_survive_a_preemption(client, rig):
    """Adoption is the deliverable, and a checkpointing job is worth more than
    a paragraph in a README nobody opens."""
    sign_in(client, rig)
    assert "Checkpointer" in client.get("/submit").text


# ------------------------------------------------------------------ cancel


def test_cancelling_a_queued_job_happens_immediately(client, rig):
    """No machine is involved, so this page can finish the job itself."""
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    with daemon(rig) as broker:
        job_id = broker.history(user_id="ana")[0].job_id

    client.post(f"/jobs/{job_id}/cancel")
    with daemon(rig) as broker:
        assert broker.status(job_id).state is JobState.CANCELLED


def test_cancelling_a_running_job_is_a_request_the_daemon_acts_on(client, rig):
    """This process holds no credentials by design, so it cannot terminate
    anything. It records the ask; the daemon stops the machine."""
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "4", "budget": ""})
    with daemon(rig) as broker:
        broker.tick()
        job_id = broker.history(user_id="ana")[0].job_id
        assert broker.status(job_id).backend_handle

    client.post(f"/jobs/{job_id}/cancel")

    with daemon(rig) as broker:
        job = broker.status(job_id)
        assert job.cancel_requested == "ana"
        assert not job.is_terminal, "the web app stopped a machine it cannot reach"

        broker.tick()
        assert broker.status(job_id).state is JobState.CANCELLED
        assert broker.scheduler.backend("fake").list_resources() == []


def test_the_page_says_a_cancel_is_pending(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "4", "budget": ""})
    with daemon(rig) as broker:
        broker.tick()
        job_id = broker.history(user_id="ana")[0].job_id
    client.post(f"/jobs/{job_id}/cancel")

    body = client.get(f"/jobs/{job_id}").text
    assert "Cancel requested by ana" in body
    assert "cannot reach it" in body


def test_you_cannot_cancel_somebody_elses_job(client, rig):
    sign_in(client, rig, "ana")
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    with daemon(rig) as broker:
        job_id = broker.history(user_id="ana")[0].job_id

    client.get("/auth/logout")
    sign_in(client, rig, "bo")
    response = client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 403
    assert "belongs to ana" in response.text
    with daemon(rig) as broker:
        assert broker.status(job_id).state is JobState.QUEUED


# ------------------------------------------------------------- live logging


def test_logs_stream_over_sse(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    with daemon(rig) as broker:
        broker.tick()
        job_id = broker.history(user_id="ana")[0].job_id
        broker.store.append_log(job_id, "stdout", "epoch 1 loss 2.31")
        broker.store.transition(broker.status(job_id), JobState.RUNNING)
        broker.store.transition(broker.status(job_id), JobState.COMPLETED)

    with client.stream("GET", f"/jobs/{job_id}/stream?after=0") as stream:
        assert stream.headers["content-type"].startswith("text/event-stream")
        body = "".join(stream.iter_text())

    assert "event: line" in body
    assert "epoch 1 loss 2.31" in body
    assert "event: done" in body
    assert '"state": "COMPLETED"' in body


def test_the_stream_resumes_from_a_cursor(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    with daemon(rig) as broker:
        job_id = broker.history(user_id="ana")[0].job_id
        broker.store.append_log(job_id, "stdout", "first")
        broker.store.append_log(job_id, "stdout", "second")
        rows = broker.store.read_logs(job_id)
        cursor = rows[0][0]
        broker.store.transition(broker.status(job_id), JobState.CANCELLED)

    with client.stream("GET", f"/jobs/{job_id}/stream?after={cursor}") as stream:
        body = "".join(stream.iter_text())

    assert "second" in body
    assert "first" not in body, "re-sent lines the page already has"


def test_the_stream_needs_a_session(client, rig):
    with client.stream("GET", "/jobs/whatever/stream") as stream:
        assert stream.status_code == 303


def test_the_job_page_hands_the_stream_a_starting_cursor(client, rig):
    sign_in(client, rig)
    client.post("/submit", data={"command": "x", "gpu": "a10g", "hours": "1", "budget": ""})
    with daemon(rig) as broker:
        job_id = broker.history(user_id="ana")[0].job_id
        broker.store.append_log(job_id, "stdout", "already on the page")

    body = client.get(f"/jobs/{job_id}").text
    assert 'data-stream="/jobs/' in body
    assert 'data-after="' in body
    assert 'data-after="0"' not in body, "would re-deliver every line already rendered"


# ------------------------------------------------------------------- admin


def test_the_admin_page_is_for_officers(client, rig):
    sign_in(client, rig, "ana")
    response = client.get("/admin")
    assert response.status_code == 403
    assert "club officers" in response.text


def test_an_officer_sees_the_admin_page(client, rig):
    sign_in(client, rig, "laksh")
    body = client.get("/admin").text
    assert "Members" in body and "Pool" in body


def test_an_officer_can_set_a_budget(client, rig):
    sign_in(client, rig, "laksh")
    client.post("/admin/budget", data={"user_id": "laksh", "usd": "120", "gpu_hours": ""})
    with daemon(rig) as broker:
        assert broker.store.get_user("laksh").budget_usd == Decimal("120")


def test_a_member_cannot_set_their_own_budget(client, rig):
    sign_in(client, rig, "ana")
    response = client.post("/admin/budget", data={"user_id": "ana", "usd": "99999"})
    assert response.status_code == 403
    with daemon(rig) as broker:
        assert broker.store.get_user("ana").budget_usd == rig["config"].default_budget_usd


def test_admin_links_are_hidden_from_members(client, rig):
    sign_in(client, rig, "ana")
    assert 'href="/admin"' not in client.get("/").text
    client.get("/auth/logout")
    sign_in(client, rig, "laksh")
    assert 'href="/admin"' in client.get("/").text


# ------------------------------------------------------- the security posture


def test_the_web_app_builds_no_backends(rig):
    """The whole reason this process is safe to expose: no AWS client is
    constructed and no SSH key is read, so nothing here can launch or terminate
    anything."""
    from gpu_broker.web.app import open_broker

    with open_broker(rig["config"]) as instance:
        assert instance.scheduler.backends == []


def test_a_tick_from_the_web_process_launches_nothing(rig):
    from gpu_broker.web.app import open_broker

    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)

    with open_broker(rig["config"]) as instance:
        report = instance.tick()
        assert report.dispatched == ()
        assert instance.store.queued_jobs(), "the job left the queue somehow"


# --------------------------------------------------- metrics and the report


def test_the_metrics_endpoint_is_off_unless_a_token_is_set(client, rig):
    """These series name people and jobs. Prometheus conventionally leaves
    /metrics open; that is not a reason to."""
    response = client.get("/metrics")
    assert response.status_code == 404
    assert "these series name people" in response.text


def test_the_metrics_endpoint_needs_the_token(client, rig, monkeypatch):
    monkeypatch.setenv("GPU_BROKER_METRICS_TOKEN", "s3cret")
    assert client.get("/metrics").status_code == 401
    assert client.get("/metrics", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_a_scraper_is_not_redirected_to_a_sign_in_page(client, rig, monkeypatch):
    """A scraper cannot follow a redirect to a login form, and turning its 401
    into a 303 makes the failure look like success."""
    monkeypatch.setenv("GPU_BROKER_METRICS_TOKEN", "s3cret")
    response = client.get("/metrics")
    assert response.status_code == 401
    assert "location" not in response.headers


def test_metrics_are_served_with_the_right_token(client, rig, monkeypatch):
    monkeypatch.setenv("GPU_BROKER_METRICS_TOKEN", "s3cret")
    with daemon(rig) as broker:
        broker.add_user("ana")
        broker.tick()

    response = client.get("/metrics", headers={"Authorization": "Bearer s3cret"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "# TYPE gpu_broker_queue_depth gauge" in response.text


def test_the_metrics_endpoint_needs_no_session(client, rig, monkeypatch):
    """It is scraped by a machine, not read by a person."""
    monkeypatch.setenv("GPU_BROKER_METRICS_TOKEN", "s3cret")
    assert client.get("/metrics", headers={"Authorization": "Bearer s3cret"}).status_code == 200


def test_the_report_page_needs_a_session(client):
    assert client.get("/report").status_code == 303


def test_the_report_page_shows_where_it_loses(client, rig):
    """Printed next to the good numbers, on the page members actually open."""
    sign_in(client, rig)
    body = client.get("/report").text
    assert "What actually happened" in body
    assert "Where it loses" in body
    assert "Not measured here" in body


def test_the_dashboard_charts_utilization_over_time(client, rig):
    """The build prompt asks the pool dashboard for utilization *over time*, not
    just right now."""
    import datetime as dt

    from gpu_broker.db.connection import transaction
    from gpu_broker.metrics import QUEUE_DEPTH, UTILIZATION

    # Points spread across the window. A real deployment ticks every 30s; six
    # ticks inside one millisecond land in one bucket and draw no line.
    with daemon(rig) as broker:
        broker.add_user("ana")
        now = broker.clock.now()
        with transaction(broker.store.conn) as conn:
            conn.executemany(
                "INSERT INTO metrics (name, at, value, labels) VALUES (?, ?, ?, '')",
                [
                    (name, (now - dt.timedelta(minutes=m)).isoformat(timespec="microseconds"), value)
                    for m in range(30, 720, 30)
                    for name, value in ((UTILIZATION, 40.0 + m / 30), (QUEUE_DEPTH, float(m % 5)))
                ],
            )

    sign_in(client, rig)
    body = client.get("/").text
    assert "Over the last 24 hours" in body
    assert "<polyline" in body, "a dense series rendered no line"
    assert "gpu utilization, mean across running jobs" in body


def test_the_dashboard_omits_the_chart_when_there_is_nothing_to_draw(client, rig):
    """Rather than an empty axis implying the pool sat at zero."""
    sign_in(client, rig)
    assert "Over the last 24 hours" not in client.get("/").text

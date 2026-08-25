"""The unauthenticated status page.

Two things are being defended here. The page must not name anybody or say what
they were running, and the app serving it must not be able to change anything.
Both are asserted against the rendered output and the route table rather than
against the intent of the code, because both are the kind of property that
decays quietly when somebody adds a field.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gpu_broker.config import load_config
from gpu_broker.states import JobState
from gpu_broker.web.app import create_app
from gpu_broker.web.auth import WebConfig
from gpu_broker.web.public import build

SECRET_COMMAND = "python train_the_secret_thing.py --dataset nobody-elses-business"


@pytest.fixture
def busy(broker, clock):
    """A pool with a few people on it and one job of each interesting shape."""
    for name in ("ana", "bo", "cy"):
        broker.add_user(name)

    done = broker.submit(user_id="ana", command=SECRET_COMMAND, gpu_type="a10g", hours=1.0)
    broker.run_until_idle()

    running = broker.submit(user_id="bo", command="python other.py", gpu_type="a10g", hours=4.0)
    for _ in range(3):
        broker.tick()
        clock.sleep(120)

    waiting = broker.submit(user_id="cy", command="python third.py", gpu_type="a100", hours=2.0)
    broker.tick()
    return {"done": done.job, "running": running.job, "waiting": waiting.job}


def test_the_page_never_names_a_member_or_their_work(broker, busy):
    status = build(broker)
    rendered = repr(status)

    for job in busy.values():
        assert job.command not in rendered
        assert job.job_id not in rendered
        assert job.user_id not in rendered
        if job.backend_handle:
            assert job.backend_handle not in rendered


def test_members_are_ordinals_not_hashes_of_their_login(broker, busy):
    """A hash of a GitHub login is reversible when the roster is twenty names."""
    status = build(broker)
    labels = {job.member for job in status.running + status.recent}

    assert labels
    for label in labels:
        assert label.startswith("user-")
        # The suffix carries no information about the name it replaced.
        assert label[len("user-") :].isalpha()
    assert "ana" not in " ".join(labels)


def test_the_same_member_keeps_the_same_label_across_renders(broker, busy):
    first = {job.member for job in build(broker).recent}
    second = {job.member for job in build(broker).recent}
    assert first == second


def test_rendered_html_leaks_nothing(public_client, busy):
    page = public_client.get("/")
    assert page.status_code == 200
    body = page.text

    assert SECRET_COMMAND not in body
    assert "train_the_secret_thing" not in body
    for name in ("ana", "bo", "cy"):
        # Word-ish check: "ana" would match inside other words, so look for the
        # way a username actually renders.
        assert f">{name}<" not in body
    for job in busy.values():
        assert job.job_id not in body
        assert job.short_id not in body


def test_the_json_view_leaks_nothing_either(public_client, busy):
    """It is built from the same object, so it cannot drift wider than the page."""
    payload = public_client.get("/status.json")
    assert payload.status_code == 200
    body = payload.text

    assert SECRET_COMMAND not in body
    for job in busy.values():
        assert job.job_id not in body
        assert job.user_id not in body


def test_public_app_has_no_mutating_routes(public_app):
    """Read-only by construction, not by remembering to guard each handler."""
    for route in public_app.routes:
        methods = getattr(route, "methods", None)
        if methods is None:
            continue
        assert methods <= {"GET", "HEAD"}, f"{route.path} accepts {methods}"


def test_public_app_mounts_no_submit_cancel_or_admin(public_app):
    paths = {getattr(route, "path", "") for route in public_app.routes}
    for forbidden in ("/submit", "/cancel", "/admin", "/auth/login", "/jobs"):
        assert not any(path.startswith(forbidden) for path in paths)


def test_the_page_is_cached_so_a_link_does_not_become_a_scan(public_client, busy, monkeypatch):
    from gpu_broker.web import publicapp

    calls = []
    real = publicapp.build

    def counted(broker, **kwargs):
        calls.append(1)
        return real(broker, **kwargs)

    monkeypatch.setattr(publicapp, "build", counted)

    for _ in range(5):
        assert public_client.get("/").status_code == 200

    assert len(calls) == 1, "the aggregates were rebuilt per request"
    assert "max-age" in public_client.get("/").headers["cache-control"]


def test_status_is_404_until_it_is_turned_on(tmp_path: Path):
    """Publishing what the club spends is opt-in, not a default."""
    config = load_config(tmp_path / "broker")
    web = replace(WebConfig(), session_secret="s", dev_login="ana", public_status=False)
    client = TestClient(create_app(config=config, web=web), raise_server_exceptions=False)
    assert client.get("/status").status_code == 404


def test_status_is_served_when_it_is_turned_on(tmp_path: Path):
    config = load_config(tmp_path / "broker")
    web = replace(WebConfig(), session_secret="s", dev_login="ana", public_status=True)
    client = TestClient(create_app(config=config, web=web))
    page = client.get("/status")
    assert page.status_code == 200
    assert "sign in" not in page.text.lower()


def test_an_empty_pool_says_so_rather_than_rendering_zeroes(broker):
    status = build(broker)
    assert status.quiet
    assert status.jobs_all_time == 0


def test_a_single_user_pool_is_labelled_a_pilot(broker, clock):
    broker.add_user("ana")
    broker.submit(user_id="ana", command="python train.py", gpu_type="a10g", hours=1.0)
    broker.run_until_idle()

    status = build(broker)
    assert status.pilot, "a queue of one is not evidence of a queue and should say so"


def test_a_busy_pool_is_not_labelled_a_pilot(broker, busy):
    assert not build(broker).pilot


def test_seeded_data_is_labelled_demo_and_cannot_be_unlabelled(broker):
    broker.add_user("ana")
    broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=1.0, origin="seeded"
    )
    broker.run_until_idle()

    status = build(broker)
    assert status.demo
    assert not status.pilot, "demo data is not a pilot; it is not real activity at all"


def test_real_data_is_not_labelled_demo(broker, busy):
    assert not build(broker).demo


def test_a_job_that_resumed_counts_once_as_completed_and_once_as_interrupted(broker, clock, cloud):
    broker.add_user("ana")
    # Budget with room for a second attempt. Without it the restart runs into
    # the reservation ceiling and the job fails, which is correct behaviour and
    # the wrong thing for this test to be measuring.
    result = broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=2.0, budget="8"
    )
    cloud.plan(result.job.job_id, total_steps=20, checkpoint_every=2)

    for _ in range(10):
        broker.tick()
        clock.sleep(120)
    handle = broker.store.get_job(result.job.job_id).backend_handle
    cloud.interrupt(handle)
    broker.run_until_idle(step_seconds=60)

    status = build(broker)
    assert broker.store.get_job(result.job.job_id).state is JobState.COMPLETED
    assert status.month.completed == 1
    assert status.month.interrupted == 1
    assert status.month.preempted == 0, "nothing is mid-restart once the queue drained"


def test_capacity_is_split_by_whether_it_costs_money(broker, clock):
    broker.add_user("ana")
    broker.submit(user_id="ana", command="python a.py", gpu_type="a10g", hours=1.0)
    broker.submit(user_id="ana", command="python b.py", gpu_type="a6000", hours=1.0)
    broker.run_until_idle()

    kinds = {backend.name: backend.kind for backend in build(broker).backends}
    assert kinds["cloud"] == "cloud"
    assert kinds["lab"] == "local"


def test_a_gap_in_the_series_stays_a_gap(broker, busy):
    """'Nothing was running' and 'a GPU sat idle' are different claims."""
    status = build(broker)
    assert any(point is None for point in status.utilization_30d)

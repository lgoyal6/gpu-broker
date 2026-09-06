"""What `/status.json` promises to anything that is not this repo.

The README tells people to read it instead of scraping HTML, which makes it a
published interface: somebody's dashboard, a cron job, a Grafana panel. Nothing
here checked that a change to `PublicStatus` did not quietly rename a field out
from under them -- and the payload is built by `dataclasses.asdict`, so renaming
an attribute republishes the whole contract without anybody typing a JSON key.

`fixtures/status_v1.json` is not a hand-written wish. It was captured from a
running `/status.json` at commit f717dde, which is this project before the
authorization and limits work. So the compatibility claim tested here is a real
one: a client written against that build still finds everything it read.

Two directions, because "compatible" is not one property:

  additive     new keys may appear. An old client ignores what it does not know,
               and this file proves the old keys are still there, still typed
               the way they were.

  breaking     a removed or retyped key must fail loudly here, not silently
               produce `None` in somebody's dashboard six weeks later.

The second one has its own test, against a deliberately mutated payload, because
a contract checker that cannot fail is not evidence of anything.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.states import JobState
from gpu_broker.web.publicapp import create_public_app

FIXTURE = Path(__file__).parent / "fixtures" / "status_v1.json"


# --------------------------------------------------------------- the checker


def shape(value, path: str = "") -> dict[str, str]:
    """Every leaf in a JSON document, as path -> type name.

    A list contributes the shape of its first element under `path[]`. That is
    enough for this payload -- every list in it is homogeneous by construction,
    since each comes from one dataclass -- and it keeps an empty list from
    asserting anything, which is what we want: an empty `running[]` says nothing
    about the shape of a running job.
    """
    if isinstance(value, dict):
        found: dict[str, str] = {}
        for key, item in value.items():
            found.update(shape(item, f"{path}.{key}" if path else key))
        return found
    if isinstance(value, list):
        return shape(value[0], f"{path}[]") if value else {}
    return {path: type(value).__name__}


def incompatibilities(old: dict, new: dict) -> list[str]:
    """What an old client would find broken in a new payload.

    `NoneType` on either side is not a mismatch. `runway_days` is a float or
    null depending on whether there is enough history to project one, so a
    client already had to handle both; calling that a break would make the
    checker cry wolf on the first quiet week.
    """
    old_shape, new_shape = shape(old), shape(new)
    broken = []
    for path, was in sorted(old_shape.items()):
        if path not in new_shape:
            broken.append(f"{path}: gone (was {was})")
            continue
        now = new_shape[path]
        if was != now and "NoneType" not in (was, now):
            broken.append(f"{path}: was {was}, is now {now}")
    return broken


# ------------------------------------------------------------------- the app


@pytest.fixture
def published(tmp_path: Path) -> dict:
    """Today's `/status.json`, from a pool with something in it.

    The same scenario the fixture was captured from: one job that finished and
    one that is running, so `recent[]` and `running[]` are both populated and
    their shapes are actually compared rather than skipped as empty.
    """
    root = tmp_path / "broker"
    clock = ManualClock()
    config = load_config(root)
    backend = FakeBackend(
        "cloud",
        clock=clock,
        currency=Currency.USD,
        capacity={"a10g": 4},
        startup_seconds=0.0,
        state_path=root / "cloud.json",
    )
    with Broker.open(root, clock=clock, backends=[backend], config=config) as broker:
        broker.add_user("ana")
        broker.add_user("bo")
        broker.submit(
            user_id="ana", command="python train.py", gpu_type="a10g", hours=0.02
        )
        for _ in range(6):
            broker.tick()
            clock.sleep(120)
        broker.submit(
            user_id="bo", command="python eval.py", gpu_type="a10g", hours=8.0
        )
        broker.tick()
        clock.sleep(60)
        broker.tick()
        assert broker.who(), "the scenario must leave a job holding a machine"
        assert any(
            job.state is JobState.COMPLETED for job in broker.history(limit=50)
        ), "the scenario must leave a finished job"

    app = create_public_app(
        lambda: Broker.open(root, clock=ManualClock(), backends=[], config=config),
        title="contract",
        cache_seconds=0.0,
    )
    with TestClient(app) as client:
        response = client.get("/status.json")
        assert response.status_code == 200
        return response.json()


# ------------------------------------------------------- an additive change


def test_a_client_written_against_the_old_build_still_works(published):
    """The whole point. Since f717dde this project grew a suspension column, a
    new tick-report field and five new config limits -- all additive -- and a
    dashboard written before any of it must not have noticed."""
    old = json.loads(FIXTURE.read_text())
    assert incompatibilities(old, published) == []


def test_the_fields_the_readme_names_are_all_still_published(published):
    """The README tells people what is on the page. These are the ones it
    names, pinned by hand so that a rename has to argue with a human sentence
    as well as with a fixture."""
    for field in (
        "queue_depth",
        "running_now",
        "dollars_spent",
        "dollars_reclaimed",
        "gpu_hours_used",
        "runway_days",
        "utilization_7d",
        "utilization_30d",
        "week",
        "month",
        "backends",
        "demo",
    ):
        assert field in published, field


def test_an_old_client_reading_the_new_payload_gets_numbers_not_none(published):
    """A shape check passes if a field is present and null forever. This is the
    consumer half: the four numbers a dashboard would actually plot, read out of
    today's payload the way a client written against v1 would read them."""
    assert isinstance(published["queue_depth"], int)
    assert isinstance(published["running_now"], int)
    # Money is a string on purpose -- JSON floats lose cents -- so a client
    # parses it. That decision is part of the contract too.
    assert float(published["dollars_spent"]) >= 0
    assert published["week"]["completed"] >= 1


def test_a_new_field_is_not_a_breaking_change(published):
    """Additive, stated as a rule rather than assumed: adding a key to the
    payload must not make the checker complain."""
    grown = dict(published, brand_new_field=[{"nested": 1}])
    assert incompatibilities(json.loads(FIXTURE.read_text()), grown) == []


# -------------------------------------------------------- a breaking change


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda p: p.pop("queue_depth"), "queue_depth: gone"),
        (lambda p: p.update(queue_depth="seven"), "queue_depth: was int, is now str"),
        (lambda p: p.update(dollars_spent=12.5), "dollars_spent: was str, is now float"),
        (
            lambda p: p["week"].pop("completed"),
            "week.completed: gone",
        ),
        (
            lambda p: p["running"][0].pop("gpu_type"),
            "running[].gpu_type: gone",
        ),
    ],
)
def test_a_breaking_change_is_named_not_swallowed(published, mutate, expected):
    """Each of these is a change somebody could make without touching a JSON
    key: renaming a dataclass attribute, changing a Decimal to a float, dropping
    a field from `Outcomes`. The payload is built by `asdict`, so all of them
    reach the wire. This asserts the checker says which one, because "the
    contract test failed" is not something anybody can act on."""
    broken = dict(published)
    broken["week"] = dict(published["week"])
    broken["running"] = [dict(job) for job in published["running"]]
    mutate(broken)

    reported = incompatibilities(json.loads(FIXTURE.read_text()), broken)
    assert any(line.startswith(expected) for line in reported), reported


def test_the_checker_is_not_vacuous():
    """A checker that returns [] for everything would make every test above
    pass. One line, comparing a document to nothing."""
    assert incompatibilities({"a": 1}, {}) == ["a: gone (was int)"]
    assert incompatibilities({"a": 1}, {"a": "1"}) == ["a: was int, is now str"]
    assert incompatibilities({"a": None}, {"a": 1.0}) == []

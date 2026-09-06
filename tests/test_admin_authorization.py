"""Who may act on the pool itself, rather than on one job.

`Broker.cancel` was fixed so the ownership check lives in the method that
terminates a machine, instead of being a rule every future caller had to
remember. The same defect was still open on every other privileged method.
`suspend_user`, `restore_user`, `drain_host` and `undrain_host` all took an
`actor`, wrote it into the record, and never looked at it. `add_user` would set
`is_admin` for anybody who asked.

The last one is the one that matters, because it defeats all the others. An
officer may cancel anybody's job, suspend anybody, and drain any host, so a
member who can set their own `is_admin` already has every power the checks were
meant to protect. Closing the cancel path and leaving the promotion path open is
worth close to nothing, which is what
`test_self_promotion_walks_straight_through_the_cancel_check` is here to say.

The bootstrap is deliberate and is pinned by a test: while the pool has no
officers at all, nobody could pass an officer check, so the first officer can be
made. It closes the moment one exists.
"""

from __future__ import annotations

import pytest

from gpu_broker.broker import Broker
from gpu_broker.errors import Unauthorized
from gpu_broker.states import JobState


@pytest.fixture
def club(broker: Broker) -> Broker:
    """A pool that already has an officer, which is the normal case.

    Every test below is about what a *member* can do here. On a pool with no
    officers there is nothing to escalate to and the bootstrap applies instead.
    """
    broker.add_user("laksh", is_admin=True)
    broker.add_user("ana")
    broker.add_user("bo")
    return broker


# ------------------------------------------------------------ the twin holes


def test_a_member_cannot_suspend_another_member(club: Broker):
    """Before this, `suspend_user(actor='bo')` took ana off the pool and wrote
    bo's name into the record as the officer who did it."""
    with pytest.raises(Unauthorized, match="bo"):
        club.suspend_user("ana", reason="I want her slots", actor="bo")
    assert not club.store.get_user("ana").is_suspended


def test_a_suspended_member_cannot_restore_themselves(club: Broker):
    """The one that makes suspension worth having. A suspension anybody can
    undo is a suggestion, and every C05 test that turns on a held job rests on
    this staying true."""
    club.suspend_user("ana", reason="graduated", actor="laksh")

    with pytest.raises(Unauthorized, match="ana"):
        club.restore_user("ana", actor="ana")
    assert club.store.get_user("ana").is_suspended


def test_a_member_cannot_promote_themselves_to_officer(club: Broker):
    with pytest.raises(Unauthorized, match="bo"):
        club.add_user("bo", is_admin=True, actor="bo")
    assert not club.store.get_user("bo").is_admin


def test_a_member_cannot_demote_the_only_officer(club: Broker):
    """Demotion is the same privilege as promotion pointed the other way: an
    officer-less pool is one where the bootstrap opens again."""
    with pytest.raises(Unauthorized, match="bo"):
        club.add_user("laksh", is_admin=False, actor="bo")
    assert club.store.get_user("laksh").is_admin


def test_self_promotion_walks_straight_through_the_cancel_check(club: Broker):
    """Why the promotion hole made the landed cancel fix worth little.

    `Broker.cancel` refuses a cross-user cancel unless the actor `is_admin`, and
    reads that column out of the users table. Before this fix bo could write
    that column. The refusal was one call away from being nothing.
    """
    job = club.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5
    ).job

    with pytest.raises(Unauthorized):
        club.add_user("bo", is_admin=True, actor="bo")

    # And so the cancel check still means what C05 said it meant.
    with pytest.raises(Unauthorized, match="ana"):
        club.cancel(job.job_id, actor="bo")
    assert club.status(job.job_id).state is JobState.QUEUED


def test_a_member_cannot_drain_a_lab_host(club: Broker):
    """Draining is how you take the free pool away from everybody else.

    The authorization check runs before the "is there a local pool at all"
    check on purpose: a member should be told they are not an officer, not told
    about the club's backend configuration.
    """
    with pytest.raises(Unauthorized, match="bo"):
        club.drain_host("lab-01", "I want the A6000", actor="bo")


def test_a_member_cannot_undrain_a_host(club: Broker):
    """The twin of the one above. A host an officer took out for a fan swap is
    not a host a member may put back."""
    with pytest.raises(Unauthorized, match="bo"):
        club.undrain_host("lab-01", actor="bo")


# ------------------------------------------------- what must still be allowed


def test_an_officer_can_still_suspend_and_restore(club: Broker):
    club.suspend_user("ana", reason="graduated", actor="laksh")
    assert club.store.get_user("ana").is_suspended

    club.restore_user("ana", actor="laksh")
    assert not club.store.get_user("ana").is_suspended


def test_an_officer_can_still_promote_somebody(club: Broker):
    club.add_user("ana", is_admin=True, actor="laksh")
    assert club.store.get_user("ana").is_admin


def test_the_first_officer_can_still_be_created(broker: Broker):
    """The bootstrap. A pool with no officers has nobody who could pass the
    check, so `gpu admin add-user <you> --admin` on day one has to work."""
    broker.add_user("ana")
    assert not broker.store.has_admins()

    broker.add_user("laksh", is_admin=True, actor="laksh")
    assert broker.store.get_user("laksh").is_admin

    # And it closes behind itself.
    with pytest.raises(Unauthorized, match="ana"):
        broker.add_user("ana", is_admin=True, actor="ana")


def test_a_budget_change_is_not_a_privileged_promotion(club: Broker):
    """`add_user` is also how budgets are set, and the web app's officer page
    calls it that way. Only a change to `is_admin` is gated, so a budget edit
    keeps working and does not quietly need an officer in the users table."""
    from decimal import Decimal

    updated = club.add_user("ana", budget_usd=Decimal("40"), actor="ana")
    assert updated.budget_usd == Decimal("40")
    assert not updated.is_admin


def test_the_web_apps_officers_keep_the_power_they_had(club: Broker):
    """The same escape hatch `cancel` has, and for the same reason: the web
    app's officers come from its own config file, not from this table, and it
    would otherwise lose a power it has today. `admin=True` is a caller saying
    it established officer status some other way."""
    with pytest.raises(Unauthorized):
        club.drain_host("lab-01", "maintenance", actor="ivy")

    # Not Unauthorized: it gets past the check and fails on the real thing,
    # which is that no local pool is configured in this rig.
    with pytest.raises(Exception) as caught:
        club.drain_host("lab-01", "maintenance", actor="ivy", admin=True)
    assert not isinstance(caught.value, Unauthorized)
    assert "local pool" in str(caught.value)


def test_an_in_process_call_with_no_identity_is_left_alone(club: Broker):
    """Same convention as `cancel(actor=None)`: no identity asserted means an
    internal caller, not an anonymous one. Every door names its actor; this is
    for the broker calling itself.
    """
    club.suspend_user("ana", reason="graduated", actor=None)
    assert club.store.get_user("ana").is_suspended

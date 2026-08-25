"""Shared fixtures.

Every test runs against a ManualClock and fake backends. No test touches the
network, and none of them sleep. A four-hour job finishes in a microsecond,
which is what makes it reasonable to simulate a hundred of them.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import BrokerConfig, load_config
from gpu_broker.money import Currency


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "broker"


@pytest.fixture
def config(state_dir: Path) -> BrokerConfig:
    return load_config(state_dir)


@pytest.fixture
def cloud(clock: ManualClock, state_dir: Path) -> FakeBackend:
    """Paid capacity. Two a10g slots, one a100, so contention is easy to arrange."""
    return FakeBackend(
        "cloud",
        clock=clock,
        currency=Currency.USD,
        capacity={"t4": 2, "a10g": 2, "l4": 1, "a100": 1},
        startup_seconds=60.0,
        state_path=state_dir / "cloud.json",
    )


@pytest.fixture
def lab(clock: ManualClock, state_dir: Path) -> FakeBackend:
    """The free A6000. Billed in GPU-hours, not dollars."""
    return FakeBackend(
        "lab",
        clock=clock,
        currency=Currency.GPU_HOUR,
        capacity={"a6000": 2},
        startup_seconds=5.0,
        state_path=state_dir / "lab.json",
    )


@pytest.fixture
def broker(
    state_dir: Path, clock: ManualClock, config: BrokerConfig, cloud, lab
) -> Broker:
    instance = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)
    yield instance
    instance.close()


@pytest.fixture
def users(broker: Broker) -> list[str]:
    names = ["ana", "bo", "cy", "di", "eo"]
    for name in names:
        broker.add_user(name)
    return names


def settle_history(
    broker: Broker,
    user_id: str,
    amount: str,
    currency=Currency.USD,
    days_ago: float = 0.0,
    note: str = "prior usage",
):
    """Give a user some consumption history for fair-share to order by.

    Writes a SETTLE straight to the ledger; real usage arrives the same way via
    `Store.settle`, this just skips running a job to produce it.

    `days_ago` backdates it. That matters for more than decay: the ledger row
    lands in whatever billing period it is dated to, so usage from last month
    shapes this month's queue position without consuming this month's budget.
    Which is exactly what "unequal prior usage" means.
    """
    import datetime as dt

    from gpu_broker.db.connection import transaction
    from gpu_broker.models import LedgerKind

    at = broker.clock.now() - dt.timedelta(days=days_ago)
    with transaction(broker.store.conn) as conn:
        broker.store._insert_ledger(
            conn,
            job_id=None,
            user_id=user_id,
            currency=currency,
            kind=LedgerKind.SETTLE,
            amount=Decimal(amount),
            at=at,
            note=note,
        )


# --------------------------------------------------------------- local pool


@pytest.fixture
def gpu_host():
    """A simulated lab machine behind a real SSH server."""
    from ssh_host import FakeGpuHost, SshHostServer

    machine = FakeGpuHost()
    server = SshHostServer(machine)
    machine.server = server
    yield machine
    server.close()


@pytest.fixture
def transport():
    from gpu_broker.local.transport import SshTransport

    made = SshTransport(command_timeout=10.0)
    yield made
    made.close()


@pytest.fixture
def local_config(gpu_host):
    from gpu_broker.local.hosts import LocalConfig

    return LocalConfig(
        hosts=(gpu_host.server.spec(),),
        max_jobs_per_gpu=2,
        default_gpu_memory_mb=16_384,
        default_memory_max_mb=32_768,
        default_cpu_quota_percent=400,
        # Short, so the tests that make a host vanish finish in a second rather
        # than waiting out a production timeout.
        command_timeout_seconds=3.0,
        poll_timeout_seconds=2.0,
    )


@pytest.fixture
def lab_backend(clock, local_config, transport):
    from gpu_broker.backends.local import LocalBackend

    return LocalBackend(clock=clock, config=local_config, transport=transport)


def local_job(job_id: str, user_id: str, command: str = "python train.py", hours: float = 2.0):
    """A job shaped for the lab pool: GPU-hours, not dollars."""
    import datetime as dt

    from gpu_broker.models import Job
    from gpu_broker.money import Currency, money
    from gpu_broker.states import JobState

    now = dt.datetime(2026, 1, 15, 12, tzinfo=dt.timezone.utc)
    return Job(
        job_id=job_id.ljust(32, "0"),
        user_id=user_id,
        command=command,
        gpu_type="a6000",
        requested_hours=hours,
        currency=Currency.GPU_HOUR,
        reserved=money(str(hours)),
        state=JobState.QUEUED,
        submitted_at=now,
        updated_at=now,
    )

"""The facade the CLI and (later) the web app talk to.

Everything above this line is policy split into readable pieces. Everything
below it is storage and backends. This module is the seam, and it is deliberately
thin: if a method here is doing arithmetic, it belongs somewhere else.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from . import admission
from .aws import make_clients
from .backends.base import Backend
from .backends.ec2 import Ec2Backend
from .backends.fake import FakeBackend
from .backends.local import LocalBackend
from .checkpoint import CheckpointStore, LocalCheckpointStore, S3CheckpointStore
from .local.hosts import HostHealth
from .local.transport import SshTransport
from .clock import Clock, SystemClock
from .config import BrokerConfig, load_config
from .db import connect, migrate
from .errors import BrokerError
from .forecast import Forecast, alerts, forecast
from .digest import Digest
from .digest import build as build_digest
from .digest import fetch_baseline
from .idle import IdleVerdict
from .idle import candidates as idle_candidates
from .idle import reclaimable
from .metrics import Metrics
from .models import (
    Balance,
    Drift,
    DryRunPlan,
    Job,
    Priority,
    SubmitResult,
    TickReport,
    User,
)
from .money import Currency, money
from .pricing import PriceBook, RefreshReport
from .reaper import ReapReport, reap
from .report import Report
from .report import build as build_report
from .scheduler import Scheduler
from .states import ACTIVE, JobState
from .store import Store


@dataclass(frozen=True)
class ReclaimReport:
    """What `gpu reclaim` found, and what it did about it."""

    at: dt.datetime
    enabled: bool
    idle: tuple[IdleVerdict, ...] = ()
    ready: tuple[IdleVerdict, ...] = ()
    """Idle, notified, and out of grace. Killed if enabled, listed if not."""
    reclaimed: tuple[str, ...] = ()
    held: tuple[tuple[str, str], ...] = ()

    @property
    def would_reclaim(self) -> tuple[IdleVerdict, ...]:
        return () if self.enabled else self.ready


def _ecr_client(config: BrokerConfig):
    import boto3

    return boto3.client("ecr", region_name=config.aws.region)


def build_checkpoints(config: BrokerConfig) -> CheckpointStore:
    """Where checkpoints go.

    Local by default so a fresh clone works, and refused at config load time if
    spot is in the placement order: a preempted job comes back on a different
    machine, and a directory on the old one is gone with it.
    """
    if config.checkpoint_store == "s3":
        clients = make_clients(config.aws)
        import boto3

        return S3CheckpointStore(
            boto3.client("s3", region_name=config.aws.region),
            config.checkpoint_bucket,
            config.checkpoint_prefix,
        ) if clients else None  # pragma: no cover
    root = Path(config.checkpoint_root) if config.checkpoint_root else config.db_path.parent / "checkpoints"
    return LocalCheckpointStore(root)


def build_backends(
    config: BrokerConfig,
    clock: Clock,
    environments: "Callable[[str], object | None] | None" = None,
) -> list[Backend]:
    """Bring up whatever `config.backends` names.

    Defaults to the fake alone. That is not a placeholder to be replaced later:
    it is how the CLI stays usable on a laptop with no AWS account, which every
    phase has to keep true.
    """
    built: list[Backend] = []
    for name in config.backends:
        if name == "fake":
            built.append(
                FakeBackend(
                    "fake",
                    clock=clock,
                    currency=Currency.USD,
                    # Two seconds, not the sixty a real g5 takes. Nobody trying
                    # the tool for the first time should watch a simulated boot.
                    # Tests configure a realistic boot time against a clock they
                    # control.
                    startup_seconds=2.0,
                    state_path=config.db_path.parent / "fake-backend.json",
                )
            )
        elif name == "local":
            built.append(
                LocalBackend(
                    "local",
                    clock=clock,
                    config=config.local,
                    transport=SshTransport(
                        command_timeout=config.local.command_timeout_seconds
                    ),
                    environments=environments,
                    volumes=config.volumes,
                    environment_root=config.environment_root,
                )
            )
        elif name in ("ec2", "ec2-spot"):
            clients = make_clients(config.aws)
            spot = name == "ec2-spot"
            built.append(
                Ec2Backend(
                    name,
                    clock=clock,
                    aws=config.aws,
                    ec2=clients["ec2"],
                    ssm=clients["ssm"],
                    logs=clients["logs"],
                    spot=spot,
                    checkpoint_bucket=config.checkpoint_bucket,
                    checkpoint_prefix=config.checkpoint_prefix,
                    checkpoint_deadline_seconds=int(config.checkpoint_deadline_seconds),
                    max_launch_failures=config.spot_max_launch_failures,
                    cooldown_seconds=config.spot_cooldown_seconds,
                    environments=environments,
                    volumes=config.volumes,
                    ecr_repository=config.ecr_repository,
                    ecr=_ecr_client(config) if config.ecr_repository else None,
                )
            )
    return built


class Broker:
    def __init__(
        self,
        store: Store,
        backends: list[Backend],
        config: BrokerConfig,
        checkpoints: CheckpointStore | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self.checkpoints = checkpoints or build_checkpoints(config)
        self.scheduler = Scheduler(store, backends, config, self.checkpoints)
        self.clock: Clock = store.clock
        self.prices = PriceBook(config, store.clock)

    # ----------------------------------------------------------------- setup

    @classmethod
    def open(
        cls,
        state_dir: Path | str | None = None,
        *,
        clock: Clock | None = None,
        backends: list[Backend] | None = None,
        config: BrokerConfig | None = None,
        checkpoints: CheckpointStore | None = None,
    ) -> "Broker":
        """Open (and if needed create) a broker against a state directory.

        With no backends given, you get a fake one. That is not a placeholder to
        be replaced later -- it is how the CLI stays usable on a laptop with no
        AWS account, which every phase has to keep true.
        """
        clock = clock or SystemClock()
        config = config or load_config(state_dir)
        conn = connect(config.db_path)
        migrate(conn)
        store = Store(conn, clock, config)
        checkpoints = checkpoints or build_checkpoints(config)
        prices = PriceBook(config, clock)
        prices.load(store.prices())
        if backends is None:
            backends = build_backends(config, clock, environments=store.environment)
        broker = cls(store, backends, config, checkpoints)
        broker.prices = prices
        broker._reapply_drains()
        return broker

    def close(self) -> None:
        for backend in self.scheduler.backends:
            transport = getattr(backend, "transport", None)
            if transport is not None and hasattr(transport, "close"):
                transport.close()
        self.store.conn.close()

    def __enter__(self) -> "Broker":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- submit

    def submit(
        self,
        *,
        user_id: str,
        command: str,
        gpu_type: str,
        hours: float,
        budget: Decimal | float | str | None = None,
        environment: str | None = None,
    ) -> SubmitResult:
        """Admit a job, or refuse it with the number named.

        The order is: validate, then check policy, then write. A refusal is
        recorded as a REFUSED job so it can be looked up later; a typo is not.
        """
        admission.validate_request(self.config, gpu_type, hours)
        gpu = self.config.gpu(gpu_type)
        currency = gpu.currency

        reserved = (
            money(budget)
            if budget is not None
            else admission.default_reservation(self.config, gpu_type, hours)
        )

        self.store.get_user(user_id)  # raises UnknownUser before any write
        if environment and self.store.environment(environment) is None:
            known = [e.name for e in self.store.environments()]
            raise BrokerError(
                f"no environment called {environment!r}. "
                + (f"Known: {', '.join(known)}" if known else "None have been created yet")
            )

        refusal = admission.check_job_cap(self.config, currency, reserved)
        if refusal is None:
            balance = self.store.balance(user_id, currency)
            refusal = admission.check_user_budget(balance, reserved)

        if refusal is not None:
            job = self.store.record_refusal(
                user_id=user_id,
                command=command,
                gpu_type=gpu_type,
                requested_hours=hours,
                currency=currency,
                reserved=reserved,
                reason=refusal.reason,
            )
            return SubmitResult(job=job, refusal=refusal)

        job = self.store.create_job(
            user_id=user_id,
            command=command,
            gpu_type=gpu_type,
            requested_hours=hours,
            currency=currency,
            reserved=reserved,
            environment=environment,
        )
        return SubmitResult(
            job=job,
            warning=admission.ceiling_warning(self.config, gpu_type, hours, reserved),
        )

    def cancel(self, job_id: str, *, actor: str | None = None) -> Job:
        """Stop a job and give back whatever it had not spent."""
        job = self.store.find_job(job_id)
        if job.is_terminal:
            raise BrokerError(
                f"job {job.short_id} is already {job.state.lower()}; nothing to cancel"
            )

        if job.backend and job.backend_handle:
            backend = self.scheduler.backend(job.backend)
            if backend is not None:
                backend.terminate(job.backend_handle, f"cancelled by {actor or job.user_id}")

        return self.store.transition(
            job,
            JobState.CANCELLED,
            reason=f"cancelled by {actor or job.user_id}",
            clear_handle=True,
        )

    # ------------------------------------------------------------------ loop

    def tick(self) -> TickReport:
        return self.scheduler.tick()

    def run_until_idle(
        self,
        *,
        step_seconds: float = 60.0,
        max_ticks: int = 10_000,
        on_tick: "Callable[[TickReport], None] | None" = None,
    ) -> list[TickReport]:
        """Drive the broker until the queue drains.

        With a `ManualClock` this is a simulation that finishes instantly; with a
        `SystemClock` it is the real scheduler loop. Same code either way, which
        is the point of injecting the clock.
        """
        reports: list[TickReport] = []
        for _ in range(max_ticks):
            report = self.tick()
            reports.append(report)
            if on_tick is not None:
                on_tick(report)
            if not self.store.queued_jobs() and not self.store.active_jobs():
                return reports
            self.clock.sleep(step_seconds)
        raise BrokerError(
            f"queue did not drain after {max_ticks} ticks. Something is stuck: "
            "check `gpu queue --why` and `gpu reconcile`."
        )

    def reconcile(self) -> list[Drift]:
        return self.scheduler.reconcile()

    def plan(self) -> DryRunPlan:
        """What a tick would do, doing none of it."""
        return self.scheduler.plan()

    def _reapply_drains(self) -> None:
        """Put the persisted manual drains back onto the backend.

        Every CLI invocation builds a fresh Broker, so this is what makes
        `gpu admin drain` mean anything beyond the process that ran it.
        """
        backend = self.local_backend()
        if backend is None:
            return
        for hostname, reason in self.store.drained_hosts().items():
            if backend.host(hostname) is not None:
                backend.drain(hostname, reason)

    def local_backend(self) -> LocalBackend | None:
        for backend in self.scheduler.backends:
            if isinstance(backend, LocalBackend):
                return backend
        return None

    def hosts(self, refresh: bool = False) -> list[HostHealth]:
        """Health of every lab host. Empty if the local pool is not configured."""
        backend = self.local_backend()
        return backend.health(refresh=refresh) if backend else []

    def drain_host(self, hostname: str, reason: str, actor: str = "") -> None:
        backend = self.local_backend()
        if backend is None:
            raise BrokerError("the local pool is not configured; nothing to drain")
        backend.drain(hostname, reason)  # raises on an unknown host, before we write
        self.store.drain_host(hostname, reason, actor)

    def undrain_host(self, hostname: str) -> None:
        backend = self.local_backend()
        if backend is None:
            raise BrokerError("the local pool is not configured")
        backend.undrain(hostname)
        self.store.undrain_host(hostname)

    # ------------------------------------------------------- cost and idle

    def refresh_prices(self, pricing_client: object | None = None) -> RefreshReport:
        """Pull current on-demand prices from AWS and store them.

        Explicit, never automatic on the hot path: the pricing API is slow and
        occasionally unavailable, and a broker that cannot price a job is a
        broker that cannot admit one.
        """
        if pricing_client is None:
            pricing_client = make_clients(self.config.aws)["pricing"]
        report = self.prices.refresh(pricing_client)
        self.store.save_prices(report.updated)
        return report

    def forecast(self, currency: Currency = Currency.USD) -> Forecast:
        return forecast(self.store, self.config, self.clock, currency)

    def alerts(self) -> list[Forecast]:
        return alerts(self.store, self.config, self.clock)

    def idle_jobs(self) -> list[IdleVerdict]:
        return idle_candidates(self.store, self.config, self.clock)

    def reclaim(self, force: bool = False) -> "ReclaimReport":
        """Act on idle jobs, or say what acting would have done.

        Off by default. Nothing is killed without a notification already in the
        table and a grace period already expired, and the samples that justified
        it stay on disk after the job is gone so the person can audit it.
        """
        now = self.clock.now()
        enabled = self.config.reclaim_enabled or force
        reclaimed: list[str] = []
        held: list[tuple[str, str]] = []
        ready: list[IdleVerdict] = []

        for verdict in self.idle_jobs():
            allowed, why = reclaimable(verdict, self.clock)
            if not allowed:
                held.append((verdict.job.job_id, why))
                continue
            ready.append(verdict)
            if enabled:
                self._reclaim_one(verdict)
                reclaimed.append(verdict.job.job_id)

        return ReclaimReport(
            at=now,
            enabled=enabled,
            idle=tuple(self.idle_jobs()),
            ready=tuple(ready),
            reclaimed=tuple(reclaimed),
            held=tuple(held),
        )

    def _reclaim_one(self, verdict: IdleVerdict) -> None:
        job = verdict.job
        evidence = verdict.evidence()
        self.store.notify(
            user_id=job.user_id,
            kind="reclaimed",
            job_id=job.job_id,
            message=(
                f"Job {job.short_id} was reclaimed after "
                f"{verdict.idle_hours(self.clock.now()):.1f}h idle. "
                f"The samples that justified it:\n{evidence}"
            ),
        )
        self.store.append_log(job.job_id, "broker", f"reclaimed as idle:\n{evidence}")

        if job.backend and job.backend_handle:
            backend = self.scheduler.backend(job.backend)
            if backend is not None:
                backend.terminate(job.backend_handle, "reclaimed as idle")

        self.store.transition(
            job,
            JobState.RECLAIMED,
            reason=f"idle: {verdict.reason}",
            clear_handle=True,
        )

    # ------------------------------------------------- metrics and reporting

    @property
    def metrics(self) -> Metrics:
        return Metrics(self.store)

    def report(self, since: dt.datetime | None = None) -> Report:
        """The measurement report, computed from the record.

        Can be wrong; cannot be flattering. Everything in it comes from the
        ledger, the job history and the utilization samples.
        """
        return build_report(self.store, self.config, self.clock, since)

    def digest(self, days: float = 7.0) -> Digest:
        return build_digest(self, days)

    def refresh_baseline(self, months: int = 6, client: object | None = None) -> list[str]:
        """Pull the club's pre-broker spend from Cost Explorer."""
        if client is None:
            import boto3

            client = boto3.client("ce", region_name="us-east-1")
        now = self.clock.now()
        start = (now - dt.timedelta(days=31 * months)).replace(day=1)
        rows = fetch_baseline(client, start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))
        for period, hours, dollars in rows:
            self.store.save_baseline(period, hours, dollars, "cost-explorer")
        return [period for period, _, _ in rows]

    def reap(self) -> ReapReport:
        """Machines that are alive with no live job behind them. Reports only."""
        return reap(self.store, self.scheduler.backends, self.config, self.clock)

    # ----------------------------------------------------------------- reads

    def queue(self) -> list[tuple[Job, Priority]]:
        return self.scheduler.ordered_queue()

    def status(self, job_id: str) -> Job:
        return self.store.find_job(job_id)

    def job_spend(self, job_id: str) -> Decimal:
        return self.store.job_spend(job_id)

    def budgets(self, user_id: str) -> dict[Currency, Balance]:
        return {
            currency: self.store.balance(user_id, currency) for currency in Currency
        }

    def pool(self) -> dict[Currency, Balance]:
        return {currency: self.store.pool_balance(currency) for currency in Currency}

    def who(self) -> list[Job]:
        """Who is holding what, right now. The question the club actually asks."""
        return sorted(
            self.store.list_jobs(states=ACTIVE),
            key=lambda job: (job.user_id, job.submitted_at),
        )

    def history(self, user_id: str | None = None, limit: int = 50) -> list[Job]:
        jobs = self.store.list_jobs(user_id=user_id)
        return sorted(jobs, key=lambda job: job.submitted_at, reverse=True)[:limit]

    def logs(self, job_id: str, after: int = 0) -> list[tuple[int, dt.datetime, str, str]]:
        job = self.store.find_job(job_id)
        return self.store.read_logs(job.job_id, after_id=after)

    # ---------------------------------------------------------------- admin

    def add_user(self, user_id: str, **kwargs) -> User:
        return self.store.upsert_user(user_id, **kwargs)

    def users(self) -> list[User]:
        return self.store.list_users()

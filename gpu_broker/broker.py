"""The facade the CLI and (later) the web app talk to.

Everything above this line is policy split into readable pieces. Everything
below it is storage and backends. This module is the seam, and it is deliberately
thin: if a method here is doing arithmetic, it belongs somewhere else.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

from . import admission, tracing
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
from .errors import BrokerError, Unauthorized
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

    def _apply_prices(self) -> None:
        """Point every money decision at the price book.

        `refresh_prices` used to update the book and nothing else, so `gpu
        prices` printed the number AWS had just given us while admission,
        accrual, the ceiling check and `reap` all went on reading the table
        typed in by hand in August. A club whose budget is enforced against a
        price nobody is charged does not have a budget.

        The config is frozen and rebuilt rather than mutated, and the rebuilt
        one is handed to everything holding a reference, so there is exactly one
        price in play at any moment.
        """
        book = self.prices.all()
        self.config = replace(
            self.config,
            gpu_types=tuple(
                replace(gpu, hourly_price=book[gpu.name].hourly)
                if gpu.name in book
                else gpu
                for gpu in self.config.gpu_types
            ),
        )
        self.store.config = self.config
        self.scheduler.config = self.config

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
        # Before anything can be admitted. A restart that put the money path
        # back on the builtin table would make every price refresh last only
        # until the next deploy.
        broker._apply_prices()
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
        origin: str = "real",
    ) -> SubmitResult:
        """Admit a job, or refuse it with the number named.

        The order is: validate, then check policy, then write. A refusal is
        recorded as a REFUSED job so it can be looked up later; a typo is not.
        """
        admission.validate_request(self.config, gpu_type, hours, command)
        gpu = self.config.gpu(gpu_type)
        currency = gpu.currency

        reserved = (
            money(budget)
            if budget is not None
            else admission.default_reservation(self.config, gpu_type, hours)
        )

        user = self.store.get_user(user_id)  # raises UnknownUser before any write
        if environment and self.store.environment(environment) is None:
            known = [e.name for e in self.store.environments()]
            raise BrokerError(
                f"no environment called {environment!r}. "
                + (f"Known: {', '.join(known)}" if known else "None have been created yet")
            )

        # Membership first. Somebody who is off the pool should be told that,
        # not told their budget is short -- and a suspended member must not be
        # able to learn the pool's headroom by probing this.
        refusal = admission.check_membership(user)
        if refusal is None:
            refusal = admission.check_job_cap(self.config, currency, reserved)
        if refusal is None:
            refusal = admission.check_queue_depth(
                self.config, self.store.queued_count(user_id)
            )
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
                origin=origin,
            )
            return SubmitResult(job=job, refusal=refusal)

        with tracing.span("queue.enqueue", kind="producer",
                          gpu_type=gpu_type, requested_hours=hours,
                          command=command) as span:
            job = self.store.create_job(
                user_id=user_id,
                command=command,
                gpu_type=gpu_type,
                requested_hours=hours,
                currency=currency,
                reserved=reserved,
                environment=environment,
                origin=origin,
            )
            span.set_attribute("job_id", job.job_id)
            # The queue hop. `gpu run` is a different process and may not be
            # running yet; the row is the only thing that reaches it.
            tracing.record_queue_context(self.store.conn, job.job_id, self.store.clock.now())
        return SubmitResult(
            job=job,
            warning=admission.ceiling_warning(self.config, gpu_type, hours, reserved),
        )

    def _require_officer(self, actor: str | None, what: str, *, admin: bool = False) -> None:
        """The privilege check for everything that acts on the pool itself.

        Same shape as the ownership check inside `cancel`, and here for the same
        reason: `cancel` used to trust every caller to remember, and so did
        `suspend_user`, `restore_user`, `drain_host`, `undrain_host` and
        `add_user`. Fixing one of six is not fixing the rule.

        `admin=True` is the same escape hatch `cancel` has, for a door that
        establishes officer status some other way -- the web app's officers come
        from its own config file, not from this table.

        `actor` of None or "" asserts no identity at all. That is the broker
        calling itself, matching `cancel(actor=None)`, not an anonymous user:
        every door names its actor.

        The bootstrap: while the pool has no officers, nobody could pass this
        check, so `gpu admin add-user <you> --admin` on day one has to work. It
        closes the moment an officer exists, and that is pinned by a test.
        """
        if admin or not actor:
            return
        if not self.store.has_admins():
            return
        acting = self.store.maybe_user(actor)
        if acting is None or not acting.is_admin:
            raise Unauthorized(
                f"{what} is for club officers, and {actor} is not one. Ask an officer"
            )

    def cancel(self, job_id: str, *, actor: str | None = None, admin: bool = False) -> Job:
        """Stop a job and give back whatever it had not spent.

        The ownership check is here rather than only in `gpu cancel` and the web
        app, because this is the method that terminates a machine. Both callers
        happened to check first and this one checked nothing, which made the
        rule "every future caller remembers".

        An officer may cancel for somebody -- a run whose owner has gone home is
        exactly what the club needs to be able to stop -- and the reason written
        into the job's history names who did it.
        """
        job = self.store.find_job(job_id)
        if actor is not None and actor != job.user_id and not admin:
            # `admin=True` is for a door that establishes officer status some
            # other way -- the web app's officers come from its own config, not
            # from this table, and it would otherwise lose a power it has today.
            acting = self.store.maybe_user(actor)
            if acting is None or not acting.is_admin:
                raise Unauthorized(
                    f"job {job.short_id} belongs to {job.user_id}, not {actor}. "
                    "Ask them, or ask an officer"
                )
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

    def drain_host(
        self, hostname: str, reason: str, actor: str = "", *, admin: bool = False
    ) -> None:
        """Stop placing jobs on a lab host.

        The officer check runs before the "is there a local pool" check on
        purpose: a member should be told they are not an officer, rather than
        told about the club's backend configuration.
        """
        self._require_officer(actor, "draining a host", admin=admin)
        backend = self.local_backend()
        if backend is None:
            raise BrokerError("the local pool is not configured; nothing to drain")
        backend.drain(hostname, reason)  # raises on an unknown host, before we write
        self.store.drain_host(hostname, reason, actor)

    def undrain_host(self, hostname: str, actor: str = "", *, admin: bool = False) -> None:
        """Let a host take jobs again. The twin of `drain_host`, and checked.

        It used to take no actor at all, so a host an officer pulled out for a
        fan swap was one any member could put straight back.
        """
        self._require_officer(actor, "undraining a host", admin=admin)
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
        self._apply_prices()
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
        """Machines that are alive with no live job behind them.

        Terminates nothing, as it never has. It does now write down what each
        one has burned, because a number the broker can compute and does not
        record is a number the club cannot act on: the ledger read $0.00 while
        AWS billed for every hour a leaked instance stayed up.

        Bookkeeping, not cleanup. Whether to kill the machine is still a
        person's call.
        """
        report = reap(self.store, self.scheduler.backends, self.config, self.clock)
        for orphan in report.orphans:
            if not orphan.cost_known:
                # No launch time, so `burned` is $0.00 by default rather than by
                # measurement. Writing that down would put "this machine was
                # free" in the ledger, which is a confident wrong answer where
                # the honest one is the report saying the cost is unknown.
                continue
            self.store.record_abandoned(
                handle=orphan.handle,
                job_id=orphan.job_id,
                user_id=orphan.user_id,
                currency=orphan.currency,
                burned=orphan.burned,
                note=f"{orphan.backend}/{orphan.handle} ({orphan.gpu_type}): {orphan.reason}",
            )
        return report

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

    def add_user(
        self, user_id: str, *, actor: str | None = None, admin: bool = False, **kwargs
    ) -> User:
        """Create a member, or change one. Only `is_admin` is officer-only.

        Promotion is the escalation that defeats every other check in this file.
        An officer may cancel anybody's job, suspend anybody and drain any host,
        so a member who can set their own `is_admin` already holds every power
        the other checks protect -- and `cancel` reads exactly this column to
        decide. Demotion is gated too: emptying the officer list is how you
        reopen the bootstrap.

        Budgets are not gated. This is also the method the web app's officer
        page calls to change one, that page has its own officer check, and the
        CLI has no authentication to gate against in the first place.
        """
        if kwargs.get("is_admin") is not None:
            self._require_officer(actor, "changing who is an officer", admin=admin)
        return self.store.upsert_user(user_id, **kwargs)

    def users(self) -> list[User]:
        return self.store.list_users()

    def suspend_user(
        self, user_id: str, *, reason: str, actor: str | None, admin: bool = False
    ) -> User:
        """Take somebody off the pool. Queued work is held, not thrown away.

        Nothing is cancelled here on purpose. A member removed by mistake on a
        Friday keeps their queue position, and an officer who meant it can
        cancel the jobs explicitly -- which is a decision with a name on it
        rather than a side effect of an administrative click.

        `actor` is checked here, not only in `gpu admin suspend`. It was already
        being written into the record as the officer who did it, which made an
        unchecked one worse than useless.
        """
        self._require_officer(actor, "suspending a member", admin=admin)
        return self.store.suspend_user(user_id, reason=reason, actor=actor or "")

    def restore_user(self, user_id: str, *, actor: str | None, admin: bool = False) -> User:
        """Put a suspended member back.

        Checked for the same reason as `suspend_user`, and more urgently: a
        suspension the suspended member can lift is not a suspension, and every
        held-job guarantee rests on this one.
        """
        self._require_officer(actor, "restoring a member", admin=admin)
        return self.store.restore_user(user_id, actor=actor or "")

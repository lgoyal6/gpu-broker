"""The only module that writes SQL.

Two rules hold this together:

1. A job's state changes only through `transition()`, which checks the state
   machine and writes the job row, the transition record, and any ledger
   consequence in a single committed transaction. If the process dies mid-call,
   none of it happened.

2. Balances are never stored. They are aggregated from the append-only ledger
   on read. A crash can therefore lose an in-flight write but can never leave a
   balance that disagrees with its own history.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal

from .clock import Clock, from_iso, to_iso
from .config import BrokerConfig
from .db.connection import transaction
from .environments import Build, Environment
from .errors import IllegalTransition, UnknownJob, UnknownUser
from .models import (
    Balance,
    Job,
    LedgerEntry,
    LedgerKind,
    Notification,
    Price,
    Sample,
    User,
)
from .money import ZERO, Currency, billing_period, money, quantize
from .states import ACTIVE, TERMINAL, JobState, can_transition


@dataclass(frozen=True)
class LedgerWrite:
    """A ledger row to be committed as part of a state transition."""

    kind: LedgerKind
    amount: Decimal
    currency: Currency
    note: str | None = None


def new_job_id() -> str:
    return uuid.uuid4().hex


class Store:
    def __init__(
        self, conn: sqlite3.Connection, clock: Clock, config: BrokerConfig
    ) -> None:
        self.conn = conn
        self.clock = clock
        self.config = config

    # ------------------------------------------------------------------ users

    def upsert_user(
        self,
        user_id: str,
        display_name: str | None = None,
        budget_usd: Decimal | None = None,
        budget_gpu_hours: Decimal | None = None,
        is_admin: bool | None = None,
    ) -> User:
        """Create the user, or update only the fields given."""
        existing = self.maybe_user(user_id)
        now = self.clock.now()
        if existing is None:
            user = User(
                user_id=user_id,
                display_name=display_name or user_id,
                budget_usd=budget_usd
                if budget_usd is not None
                else self.config.default_budget_usd,
                budget_gpu_hours=budget_gpu_hours
                if budget_gpu_hours is not None
                else self.config.default_budget_gpu_hours,
                is_admin=bool(is_admin),
                created_at=now,
            )
            with transaction(self.conn) as conn:
                conn.execute(
                    "INSERT INTO users (user_id, display_name, budget_usd, "
                    "budget_gpu_hours, is_admin, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        user.user_id,
                        user.display_name,
                        str(user.budget_usd),
                        str(user.budget_gpu_hours),
                        int(user.is_admin),
                        to_iso(user.created_at),
                    ),
                )
            return user

        updated = User(
            user_id=existing.user_id,
            display_name=display_name or existing.display_name,
            budget_usd=budget_usd if budget_usd is not None else existing.budget_usd,
            budget_gpu_hours=budget_gpu_hours
            if budget_gpu_hours is not None
            else existing.budget_gpu_hours,
            is_admin=existing.is_admin if is_admin is None else bool(is_admin),
            created_at=existing.created_at,
            # Carried, not rewritten. Raising somebody's budget must not quietly
            # let them back onto the pool, and the UPDATE below does not touch
            # these columns -- so dropping them here would only make the object
            # this returns disagree with the row it just wrote.
            suspended_at=existing.suspended_at,
            suspended_reason=existing.suspended_reason,
            suspended_by=existing.suspended_by,
        )
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE users SET display_name = ?, budget_usd = ?, "
                "budget_gpu_hours = ?, is_admin = ? WHERE user_id = ?",
                (
                    updated.display_name,
                    str(updated.budget_usd),
                    str(updated.budget_gpu_hours),
                    int(updated.is_admin),
                    updated.user_id,
                ),
            )
        return updated

    def maybe_user(self, user_id: str) -> User | None:
        row = self.conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        return _user_from_row(row) if row else None

    def get_user(self, user_id: str) -> User:
        user = self.maybe_user(user_id)
        if user is None:
            raise UnknownUser(user_id)
        return user

    def list_users(self) -> list[User]:
        rows = self.conn.execute("SELECT * FROM users ORDER BY user_id").fetchall()
        return [_user_from_row(row) for row in rows]

    def has_admins(self) -> bool:
        """Does the pool have any officer at all?

        Read by the bootstrap in `Broker._require_officer`: while the answer is
        no there is nobody who could pass an officer check, so the first officer
        has to be creatable. `LIMIT 1` because the count is never the question.
        """
        row = self.conn.execute(
            "SELECT 1 FROM users WHERE is_admin = 1 LIMIT 1"
        ).fetchone()
        return row is not None

    def suspend_user(self, user_id: str, *, reason: str, actor: str) -> User:
        """Take somebody off the club's capacity without deleting them.

        Deletion is not available: their past jobs reference this row, the
        ledger has to keep adding up, and somebody removed by mistake should get
        their queue position back rather than lose it.
        """
        self.get_user(user_id)  # raises UnknownUser before any write
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE users SET suspended_at = ?, suspended_reason = ?, "
                "suspended_by = ? WHERE user_id = ?",
                (to_iso(self.clock.now()), reason, actor, user_id),
            )
        return self.get_user(user_id)

    def restore_user(self, user_id: str, *, actor: str) -> User:
        self.get_user(user_id)
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE users SET suspended_at = NULL, suspended_reason = '', "
                "suspended_by = '' WHERE user_id = ?",
                (user_id,),
            )
        return self.get_user(user_id)

    def suspended_users(self) -> dict[str, str]:
        """Everybody currently off the pool, and why.

        One query rather than one per queued job: a tick scores every queued job
        and there are usually no suspensions at all, so the whole answer is
        smaller than the question asked repeatedly.
        """
        rows = self.conn.execute(
            "SELECT user_id, suspended_reason FROM users WHERE suspended_at IS NOT NULL"
        ).fetchall()
        return {row["user_id"]: row["suspended_reason"] for row in rows}

    # ------------------------------------------------------------------- jobs

    def create_job(
        self,
        *,
        user_id: str,
        command: str,
        gpu_type: str,
        requested_hours: float,
        currency: Currency,
        reserved: Decimal,
        job_id: str | None = None,
        environment: str | None = None,
        origin: str = "real",
    ) -> Job:
        """Admit a job: queue row, first transition, and the budget hold, atomically.

        Nothing about this is safe to split. A job row without its reservation
        is a job that spends money nobody accounted for.
        """
        self.get_user(user_id)  # raises if unknown, before we write anything
        now = self.clock.now()
        job = Job(
            job_id=job_id or new_job_id(),
            user_id=user_id,
            command=command,
            gpu_type=gpu_type,
            requested_hours=requested_hours,
            currency=currency,
            reserved=quantize(reserved, currency),
            state=JobState.QUEUED,
            submitted_at=now,
            updated_at=now,
            environment=environment,
            origin=origin,
        )
        with transaction(self.conn) as conn:
            self._insert_job(conn, job)
            self._insert_transition(conn, job.job_id, None, JobState.QUEUED, now, "submitted")
            if job.reserved > ZERO:
                self._insert_ledger(
                    conn,
                    job_id=job.job_id,
                    user_id=user_id,
                    currency=currency,
                    kind=LedgerKind.RESERVE,
                    amount=job.reserved,
                    at=now,
                    note="admission hold",
                )
        return job

    def record_refusal(
        self,
        *,
        user_id: str,
        command: str,
        gpu_type: str,
        requested_hours: float,
        currency: Currency,
        reserved: Decimal,
        reason: str,
        origin: str = "real",
    ) -> Job:
        """Persist a submission we would not accept.

        No ledger entry: a refused job never held anything. The record exists so
        that "why was I refused on Tuesday" has an answer on Friday.
        """
        now = self.clock.now()
        job = Job(
            job_id=new_job_id(),
            user_id=user_id,
            command=command,
            gpu_type=gpu_type,
            requested_hours=requested_hours,
            currency=currency,
            reserved=quantize(reserved, currency),
            state=JobState.REFUSED,
            submitted_at=now,
            updated_at=now,
            finished_at=now,
            refusal_reason=reason,
            origin=origin,
        )
        with transaction(self.conn) as conn:
            self._insert_job(conn, job)
            self._insert_transition(conn, job.job_id, None, JobState.REFUSED, now, reason)
        return job

    def transition(
        self,
        job: Job | str,
        to_state: JobState,
        *,
        reason: str | None = None,
        ledger: Sequence[LedgerWrite] = (),
        backend: str | None = None,
        backend_handle: str | None = None,
        exit_code: int | None = None,
        clear_handle: bool = False,
    ) -> Job:
        """Move a job to a new state. One transaction, or nothing.

        Entering a terminal state from a state that was holding budget releases
        whatever is left of the hold, here, in the same transaction. That is the
        invariant that makes "held" a number you can trust rather than a number
        that leaks every time a job dies in an unusual way.
        """
        current = job if isinstance(job, Job) else self.get_job(job)
        if not can_transition(current.state, to_state):
            raise IllegalTransition(current.job_id, current.state, to_state)

        now = self.clock.now()
        writes = list(ledger)

        started_at = current.started_at
        if to_state in ACTIVE and started_at is None:
            # Billing starts when the broker takes capacity, not when the user's
            # command begins. An EC2 instance charges for its boot time, and a
            # job that spends ten minutes booting has stopped waiting in the
            # queue. Both meanings want the same timestamp.
            started_at = now
        finished_at = current.finished_at
        if to_state in TERMINAL:
            finished_at = now
            writes.extend(self._release_remaining_hold(current, extra=writes))

        handle = None if clear_handle else (backend_handle or current.backend_handle)
        updated = Job(
            job_id=current.job_id,
            user_id=current.user_id,
            command=current.command,
            gpu_type=current.gpu_type,
            requested_hours=current.requested_hours,
            currency=current.currency,
            reserved=current.reserved,
            state=to_state,
            submitted_at=current.submitted_at,
            updated_at=now,
            backend=backend or current.backend,
            backend_handle=handle,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code if exit_code is not None else current.exit_code,
            refusal_reason=current.refusal_reason,
            logs_fetched=current.logs_fetched,
            attempts=current.attempts,
            preemptions=current.preemptions,
            pinned_tier=current.pinned_tier,
            checkpoint_step=current.checkpoint_step,
            checkpoint_key=current.checkpoint_key,
            checkpoint_at=current.checkpoint_at,
            cancel_requested=current.cancel_requested,
            environment=current.environment,
        )

        with transaction(self.conn) as conn:
            # Guard against a concurrent writer having moved the job since we
            # read it. Without this, two schedulers could both dispatch one job.
            changed = conn.execute(
                "UPDATE jobs SET state = ?, updated_at = ?, backend = ?, "
                "backend_handle = ?, started_at = ?, finished_at = ?, exit_code = ? "
                "WHERE job_id = ? AND state = ?",
                (
                    str(updated.state),
                    to_iso(now),
                    updated.backend,
                    updated.backend_handle,
                    to_iso(updated.started_at) if updated.started_at else None,
                    to_iso(updated.finished_at) if updated.finished_at else None,
                    updated.exit_code,
                    updated.job_id,
                    str(current.state),
                ),
            ).rowcount
            if changed != 1:
                raise IllegalTransition(current.job_id, current.state, to_state)
            self._insert_transition(
                conn, current.job_id, current.state, to_state, now, reason
            )
            for write in writes:
                self._insert_ledger(
                    conn,
                    job_id=current.job_id,
                    user_id=current.user_id,
                    currency=write.currency,
                    kind=write.kind,
                    amount=write.amount,
                    at=now,
                    note=write.note,
                )
        return updated

    def _release_remaining_hold(
        self, job: Job, extra: Iterable[LedgerWrite]
    ) -> list[LedgerWrite]:
        """Give back the unspent part of a finishing job's reservation.

        `extra` is whatever the caller is already writing in this same
        transaction (typically a final SETTLE), which has not hit the database
        yet and so is invisible to `job_holding`.
        """
        outstanding = self.job_holding(job.job_id, job.currency)
        for write in extra:
            if write.currency is not job.currency:
                continue
            if write.kind is LedgerKind.RESERVE:
                outstanding += write.amount
            elif write.kind in (LedgerKind.RELEASE, LedgerKind.SETTLE):
                outstanding -= write.amount
        if outstanding <= ZERO:
            return []
        return [
            LedgerWrite(
                kind=LedgerKind.RELEASE,
                amount=quantize(outstanding, job.currency),
                currency=job.currency,
                note=f"unused hold returned on {job.state.value.lower()} -> terminal",
            )
        ]

    def settle(self, job: Job, amount: Decimal, note: str | None = None) -> None:
        """Record capacity actually consumed by a still-running job.

        Kept separate from `transition` because settlement happens on every tick
        while the state does not change.
        """
        amount = quantize(amount, job.currency)
        if amount <= ZERO:
            return
        now = self.clock.now()
        with transaction(self.conn) as conn:
            self._insert_ledger(
                conn,
                job_id=job.job_id,
                user_id=job.user_id,
                currency=job.currency,
                kind=LedgerKind.SETTLE,
                amount=amount,
                at=now,
                note=note or "accrued",
            )
            self._insert_ledger(
                conn,
                job_id=job.job_id,
                user_id=job.user_id,
                currency=job.currency,
                kind=LedgerKind.RELEASE,
                amount=amount,
                at=now,
                note="hold converted to spend",
            )
            conn.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                (to_iso(now), job.job_id),
            )

    def get_job(self, job_id: str) -> Job:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise UnknownJob(job_id)
        return _job_from_row(row)

    def find_job(self, prefix: str) -> Job:
        """Resolve a short id. Nobody types 32 hex characters."""
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE job_id LIKE ? || '%' ORDER BY submitted_at DESC",
            (prefix,),
        ).fetchall()
        if not rows:
            raise UnknownJob(prefix)
        if len(rows) > 1:
            matches = ", ".join(row["job_id"][:12] for row in rows[:5])
            raise UnknownJob(f"{prefix} (ambiguous: {matches})")
        return _job_from_row(rows[0])

    def list_jobs(
        self,
        *,
        user_id: str | None = None,
        states: Iterable[JobState] | None = None,
        limit: int | None = None,
    ) -> list[Job]:
        clauses: list[str] = []
        params: list[object] = []
        if user_id:
            clauses.append("user_id = ?")
            params.append(user_id)
        if states is not None:
            states = list(states)
            if not states:
                return []
            clauses.append(f"state IN ({','.join('?' * len(states))})")
            params.extend(str(state) for state in states)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM jobs {where} ORDER BY submitted_at ASC, job_id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [_job_from_row(row) for row in self.conn.execute(sql, params)]

    def queued_jobs(self) -> list[Job]:
        return self.list_jobs(states=[JobState.QUEUED])

    def active_jobs(self) -> list[Job]:
        return self.list_jobs(states=ACTIVE)

    def queued_count(self, user_id: str) -> int:
        """How many jobs this member has waiting. Counted, not listed: the
        limit that uses it is checked on every submission."""
        return self._count_in_states(user_id, [JobState.QUEUED])

    def running_count(self, user_id: str) -> int:
        """How many machines this member is holding right now.

        ACTIVE rather than RUNNING: a job that is still booting has already
        taken the capacity and is already being billed for it.
        """
        return self._count_in_states(user_id, list(ACTIVE))

    def _count_in_states(self, user_id: str, states: list[JobState]) -> int:
        row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM jobs WHERE user_id = ? "
            f"AND state IN ({','.join('?' * len(states))})",
            [user_id, *(str(state) for state in states)],
        ).fetchone()
        return row["n"]

    def history(self, job_id: str) -> list[tuple[str | None, str, dt.datetime, str | None]]:
        rows = self.conn.execute(
            "SELECT from_state, to_state, at, reason FROM job_transitions "
            "WHERE job_id = ? ORDER BY id",
            (job_id,),
        ).fetchall()
        return [
            (row["from_state"], row["to_state"], from_iso(row["at"]), row["reason"])
            for row in rows
        ]

    # ----------------------------------------------------------------- ledger

    def period_now(self) -> str:
        return billing_period(self.clock.now(), self.config.timezone)

    def balance(
        self, user_id: str, currency: Currency, period: str | None = None
    ) -> Balance:
        period = period or self.period_now()
        user = self.get_user(user_id)
        held, spent = self._aggregate(
            "SELECT kind, amount FROM ledger WHERE user_id = ? AND currency = ? AND period = ?",
            (user_id, str(currency), period),
        )
        return Balance(
            user_id=user_id,
            currency=currency,
            budget=user.budget(currency),
            held=held,
            spent=spent,
        )

    def pool_balance(self, currency: Currency, period: str | None = None) -> Balance:
        period = period or self.period_now()
        held, spent = self._aggregate(
            "SELECT kind, amount FROM ledger WHERE currency = ? AND period = ?",
            (str(currency), period),
        )
        return Balance(
            user_id="<pool>",
            currency=currency,
            budget=self.config.pool_budget(currency),
            held=held,
            spent=spent,
        )

    def pool_committed(self, currency: Currency, period: str | None = None) -> Decimal:
        """What the pool cap actually governs: money already spent, plus money
        held by jobs that have real capacity right now.

        Deliberately different from `pool_balance().committed`, which also counts
        reservations held by jobs still sitting in the queue. Those two numbers
        answer different questions and conflating them breaks both:

          A queued job has consumed nothing. Counting its hold against the pool
          cap would mean a deep enough queue blocks its own dispatch forever,
          and the club would appear over budget while the GPUs sat idle.

          A user's *personal* budget does count queued holds, because that is
          what stops somebody queueing fifty jobs they cannot pay for and
          discovering it on the fiftieth.

        Refused up front by your own budget; made to wait by the pool's. That is
        the distinction, and this method is where it lives.
        """
        period = period or self.period_now()
        _, spent = self._aggregate(
            "SELECT kind, amount FROM ledger WHERE currency = ? AND period = ?",
            (str(currency), period),
        )
        active_holds = ZERO
        for job in self.active_jobs():
            if job.currency is currency:
                active_holds += self.job_holding(job.job_id, currency)
        return spent + active_holds

    def pool_headroom(self, currency: Currency) -> Decimal:
        """How much more the pool can commit before it hits its cap."""
        return self.config.pool_budget(currency) - self.pool_committed(currency)

    def job_holding(self, job_id: str, currency: Currency) -> Decimal:
        """How much of this job's reservation is still outstanding."""
        held, _ = self._aggregate(
            "SELECT kind, amount FROM ledger WHERE job_id = ? AND currency = ?",
            (job_id, str(currency)),
        )
        return held

    def job_spend(self, job_id: str) -> Decimal:
        _, spent = self._aggregate(
            "SELECT kind, amount FROM ledger WHERE job_id = ?", (job_id,)
        )
        return spent

    def _aggregate(self, sql: str, params: Sequence[object]) -> tuple[Decimal, Decimal]:
        held = ZERO
        spent = ZERO
        for row in self.conn.execute(sql, params):
            amount = money(row["amount"])
            kind = LedgerKind(row["kind"])
            if kind is LedgerKind.RESERVE:
                held += amount
            elif kind is LedgerKind.RELEASE:
                held -= amount
            else:
                spent += amount
        return held, spent

    def settlements_since(
        self, cutoff: dt.datetime, currency: Currency
    ) -> list[tuple[str, Decimal, dt.datetime]]:
        """Every SETTLE in the window, for fair-share decay. Newest last."""
        rows = self.conn.execute(
            "SELECT user_id, amount, at FROM ledger "
            "WHERE kind = 'SETTLE' AND currency = ? AND at >= ? ORDER BY at",
            (str(currency), to_iso(cutoff)),
        ).fetchall()
        return [
            (row["user_id"], money(row["amount"]), from_iso(row["at"])) for row in rows
        ]

    def ledger_for_job(self, job_id: str) -> list[LedgerEntry]:
        rows = self.conn.execute(
            "SELECT * FROM ledger WHERE job_id = ? ORDER BY id", (job_id,)
        ).fetchall()
        return [_ledger_from_row(row) for row in rows]

    def request_cancel(self, job: Job, actor: str) -> Job:
        """Ask for a job to be stopped. Does not stop it.

        The web app has no credentials and no SSH key by design, so it cannot
        terminate a machine. It records the request and the scheduler daemon,
        which does have both, acts on it within a tick.
        """
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE jobs SET cancel_requested = ?, updated_at = ? WHERE job_id = ?",
                (actor, to_iso(now), job.job_id),
            )
            conn.execute(
                "INSERT INTO job_logs (job_id, at, stream, line) VALUES (?, ?, 'broker', ?)",
                (job.job_id, to_iso(now), f"cancel requested by {actor}"),
            )
        return replace(job, cancel_requested=actor, updated_at=now)

    # -------------------------------------------------------------- baseline

    def save_baseline(self, period: str, instance_hours: float, dollars, source: str) -> None:
        """What the club spent before the broker existed.

        Kept because Cost Explorer is slow and because a club whose IAM user
        cannot read it should still be able to have a baseline, typed in.
        """
        with transaction(self.conn) as conn:
            conn.execute(
                "INSERT INTO baseline (period, instance_hours, dollars, source, recorded_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(period) DO UPDATE SET "
                "instance_hours = excluded.instance_hours, dollars = excluded.dollars, "
                "source = excluded.source, recorded_at = excluded.recorded_at",
                (period, float(instance_hours), str(dollars), source,
                 to_iso(self.clock.now())),
            )

    # ----------------------------------------------------------- environments

    def save_environment(self, environment: Environment) -> Environment:
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "INSERT INTO environments (name, digest, base_image, python_version, "
                "requirements, apt_packages, owner, description, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET digest = excluded.digest, "
                "base_image = excluded.base_image, python_version = excluded.python_version, "
                "requirements = excluded.requirements, apt_packages = excluded.apt_packages, "
                "description = excluded.description, updated_at = excluded.updated_at",
                (
                    environment.name,
                    environment.digest,
                    environment.base_image,
                    environment.python_version,
                    "\n".join(environment.requirements),
                    "\n".join(environment.apt_packages),
                    environment.owner,
                    environment.description,
                    to_iso(environment.created_at or now),
                    to_iso(now),
                ),
            )
        return replace(environment, created_at=environment.created_at or now, updated_at=now)

    def environment(self, name: str) -> Environment | None:
        row = self.conn.execute(
            "SELECT * FROM environments WHERE name = ?", (name,)
        ).fetchone()
        return _environment_from_row(row) if row else None

    def environments(self) -> list[Environment]:
        return [
            _environment_from_row(row)
            for row in self.conn.execute("SELECT * FROM environments ORDER BY name")
        ]

    def delete_environment(self, name: str) -> None:
        with transaction(self.conn) as conn:
            conn.execute("DELETE FROM environments WHERE name = ?", (name,))

    def record_build(
        self, digest: str, target: str, state: str, reference: str = "", detail: str = ""
    ) -> Build:
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "INSERT INTO environment_builds (digest, target, state, reference, detail, built_at) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(digest, target) DO UPDATE SET "
                "state = excluded.state, reference = excluded.reference, "
                "detail = excluded.detail, built_at = excluded.built_at",
                (digest, target, state, reference, detail, to_iso(now)),
            )
        return Build(digest=digest, target=target, state=state, reference=reference,
                     detail=detail, built_at=now)

    def build(self, digest: str, target: str) -> Build | None:
        row = self.conn.execute(
            "SELECT * FROM environment_builds WHERE digest = ? AND target = ?",
            (digest, target),
        ).fetchone()
        if row is None:
            return None
        return Build(
            digest=row["digest"], target=row["target"], state=row["state"],
            reference=row["reference"], detail=row["detail"],
            built_at=from_iso(row["built_at"]),
        )

    def builds_for(self, digest: str) -> list[Build]:
        return [
            Build(digest=row["digest"], target=row["target"], state=row["state"],
                  reference=row["reference"], detail=row["detail"],
                  built_at=from_iso(row["built_at"]))
            for row in self.conn.execute(
                "SELECT * FROM environment_builds WHERE digest = ? ORDER BY target", (digest,)
            )
        ]

    # ------------------------------------------------------------ resumption

    def record_attempt(self, job: Job) -> Job:
        """Count a start. Called as the job is dispatched, not when it succeeds:
        an attempt that dies during boot is still an attempt."""
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE jobs SET attempts = attempts + 1, updated_at = ? WHERE job_id = ?",
                (to_iso(now), job.job_id),
            )
        return replace(job, attempts=job.attempts + 1, updated_at=now)

    def record_preemption(self, job: Job) -> Job:
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE jobs SET preemptions = preemptions + 1, updated_at = ? WHERE job_id = ?",
                (to_iso(now), job.job_id),
            )
        return replace(job, preemptions=job.preemptions + 1, updated_at=now)

    def record_checkpoint(self, job: Job, step: int, key: str) -> Job:
        """Remember where a job got to.

        Duplicated from the checkpoint store on purpose: deciding whether a
        preempted job made progress happens on every preemption, and it must not
        need a round trip to S3.
        """
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE jobs SET checkpoint_step = ?, checkpoint_key = ?, "
                "checkpoint_at = ?, updated_at = ? WHERE job_id = ?",
                (int(step), key, to_iso(now), to_iso(now), job.job_id),
            )
        return replace(job, checkpoint_step=int(step), checkpoint_key=key, checkpoint_at=now)

    def pin_tier(self, job: Job, tier: str, reason: str) -> Job:
        now = self.clock.now()
        with transaction(self.conn) as conn:
            conn.execute(
                "UPDATE jobs SET pinned_tier = ?, updated_at = ? WHERE job_id = ?",
                (tier, to_iso(now), job.job_id),
            )
            conn.execute(
                "INSERT INTO job_logs (job_id, at, stream, line) VALUES (?, ?, 'broker', ?)",
                (job.job_id, to_iso(now), f"pinned to {tier}: {reason}"),
            )
        return replace(job, pinned_tier=tier, updated_at=now)

    # ---------------------------------------------------------------- samples

    def record_samples(self, job_id: str, samples: Sequence[Sample]) -> None:
        """Append utilization samples. One transaction for a whole poll."""
        if not samples:
            return
        with transaction(self.conn) as conn:
            conn.executemany(
                "INSERT INTO gpu_samples (job_id, at, gpu_percent, memory_mb, source) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (job_id, to_iso(s.at), s.gpu_percent, s.memory_mb, s.source)
                    for s in samples
                ],
            )

    def samples_for(
        self, job_id: str, since: dt.datetime | None = None, limit: int = 5_000
    ) -> list[Sample]:
        sql = "SELECT at, gpu_percent, memory_mb, source FROM gpu_samples WHERE job_id = ?"
        params: list[object] = [job_id]
        if since is not None:
            sql += " AND at >= ?"
            params.append(to_iso(since))
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [
            Sample(
                at=from_iso(row["at"]),
                gpu_percent=row["gpu_percent"],
                memory_mb=row["memory_mb"],
                source=row["source"],
            )
            for row in reversed(rows)
        ]

    def last_sample_at(self, job_id: str) -> dt.datetime | None:
        row = self.conn.execute(
            "SELECT MAX(at) AS at FROM gpu_samples WHERE job_id = ?", (job_id,)
        ).fetchone()
        return from_iso(row["at"]) if row and row["at"] else None

    # ---------------------------------------------------------- notifications

    def notify(
        self, *, user_id: str, kind: str, message: str, job_id: str | None = None
    ) -> Notification:
        """Record something the broker has told somebody.

        Recorded, not delivered: Phase 5 puts these in front of people. What
        matters now is that the record exists before anything acts on it.
        """
        now = self.clock.now()
        with transaction(self.conn) as conn:
            cursor = conn.execute(
                "INSERT INTO notifications (job_id, user_id, kind, message, at) "
                "VALUES (?, ?, ?, ?, ?)",
                (job_id, user_id, kind, message, to_iso(now)),
            )
            notification_id = cursor.lastrowid
        return Notification(
            notification_id=notification_id or 0,
            job_id=job_id,
            user_id=user_id,
            kind=kind,
            message=message,
            at=now,
        )

    def notifications_for_job(self, job_id: str, kind: str | None = None) -> list[Notification]:
        sql = "SELECT * FROM notifications WHERE job_id = ?"
        params: list[object] = [job_id]
        if kind is not None:
            sql += " AND kind = ?"
            params.append(kind)
        sql += " ORDER BY id"
        return [_notification_from_row(row) for row in self.conn.execute(sql, params)]

    def notifications_for_user(self, user_id: str, limit: int = 50) -> list[Notification]:
        rows = self.conn.execute(
            "SELECT * FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [_notification_from_row(row) for row in reversed(rows)]

    # ----------------------------------------------------------------- prices

    def save_prices(self, prices: Iterable[Price]) -> None:
        rows = [
            (
                price.gpu_type,
                price.instance_type,
                price.region,
                str(price.hourly),
                str(price.currency),
                price.source,
                to_iso(price.priced_at),
            )
            for price in prices
        ]
        if not rows:
            return
        with transaction(self.conn) as conn:
            conn.executemany(
                "INSERT INTO prices (gpu_type, instance_type, region, hourly, currency, "
                "source, priced_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(gpu_type) DO UPDATE SET instance_type = excluded.instance_type, "
                "region = excluded.region, hourly = excluded.hourly, "
                "currency = excluded.currency, source = excluded.source, "
                "priced_at = excluded.priced_at",
                rows,
            )

    def prices(self) -> dict[str, Price]:
        rows = self.conn.execute("SELECT * FROM prices").fetchall()
        return {row["gpu_type"]: _price_from_row(row) for row in rows}

    # ------------------------------------------------------------- host drains

    def drained_hosts(self) -> dict[str, str]:
        """Hostname to reason, for hosts an admin has taken out of service."""
        rows = self.conn.execute(
            "SELECT hostname, reason FROM host_drains ORDER BY hostname"
        ).fetchall()
        return {row["hostname"]: row["reason"] for row in rows}

    def drain_host(self, hostname: str, reason: str, actor: str = "") -> None:
        with transaction(self.conn) as conn:
            conn.execute(
                "INSERT INTO host_drains (hostname, reason, drained_at, drained_by) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(hostname) DO UPDATE SET "
                "reason = excluded.reason, drained_at = excluded.drained_at, "
                "drained_by = excluded.drained_by",
                (hostname, reason, to_iso(self.clock.now()), actor),
            )

    def undrain_host(self, hostname: str) -> None:
        with transaction(self.conn) as conn:
            conn.execute("DELETE FROM host_drains WHERE hostname = ?", (hostname,))

    # ------------------------------------------------------------------- logs

    TRUNCATION_NOTICE = (
        "[broker] output truncated: this job hit the {limit:,} line limit. "
        "Write anything longer to a file under /data."
    )

    def _within_log_budget(
        self, job_id: str, lines: Sequence[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        """As much of `lines` as this job is still allowed to store.

        Two bounds, both from `config`: how long one line may be, and how many
        lines one job may keep. Without them a job that prints is never told to
        stop -- 20,000 lines of 200 bytes grew the database by 10.7 MB, and one
        5 MB line was stored whole.

        The last line a job is allowed to store is the notice saying why there
        is no more, so somebody reading the page does not conclude their job
        stopped printing.
        """
        cap = self.config.max_log_lines_per_job
        stored = self.conn.execute(
            "SELECT COUNT(*) AS n FROM job_logs WHERE job_id = ?", (job_id,)
        ).fetchone()["n"]
        if stored >= cap:
            return []

        width = self.config.max_log_line_bytes
        clipped = [
            (stream, line if len(line) <= width else line[: width - 3] + "...")
            for stream, line in lines
        ]
        room = cap - stored
        if len(clipped) < room:
            return clipped
        return clipped[: room - 1] + [
            ("broker", self.TRUNCATION_NOTICE.format(limit=cap))
        ]

    def append_log(self, job_id: str, stream: str, line: str) -> None:
        keep = self._within_log_budget(job_id, [(stream, line)])
        if not keep:
            return
        with transaction(self.conn) as conn:
            conn.executemany(
                "INSERT INTO job_logs (job_id, at, stream, line) VALUES (?, ?, ?, ?)",
                [
                    (job_id, to_iso(self.clock.now()), kept_stream, kept_line)
                    for kept_stream, kept_line in keep
                ],
            )

    def append_backend_logs(self, job: Job, lines: Sequence[tuple[str, str]]) -> int:
        """Store a poll's worth of output and advance the job's read cursor.

        Both in one transaction. Storing the lines without moving the cursor
        would re-deliver them on the next tick; moving it without storing them
        would lose the user's output for good.
        """
        if not lines:
            return job.logs_fetched
        at = to_iso(self.clock.now())
        # The cursor counts what was polled, not what was stored. Advancing it
        # by the kept lines alone would re-read the dropped ones every tick.
        cursor = job.logs_fetched + len(lines)
        keep = self._within_log_budget(job.job_id, list(lines))
        with transaction(self.conn) as conn:
            conn.executemany(
                "INSERT INTO job_logs (job_id, at, stream, line) VALUES (?, ?, ?, ?)",
                [(job.job_id, at, stream, line) for stream, line in keep],
            )
            conn.execute(
                "UPDATE jobs SET logs_fetched = ?, updated_at = ? WHERE job_id = ?",
                (cursor, at, job.job_id),
            )
        return cursor

    def read_logs(
        self, job_id: str, after_id: int = 0, limit: int = 1000
    ) -> list[tuple[int, dt.datetime, str, str]]:
        rows = self.conn.execute(
            "SELECT id, at, stream, line FROM job_logs WHERE job_id = ? AND id > ? "
            "ORDER BY id LIMIT ?",
            (job_id, after_id, limit),
        ).fetchall()
        return [
            (row["id"], from_iso(row["at"]), row["stream"], row["line"]) for row in rows
        ]

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _insert_job(conn: sqlite3.Connection, job: Job) -> None:
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, command, gpu_type, requested_hours, "
            "currency, reserved, state, backend, backend_handle, submitted_at, "
            "started_at, finished_at, exit_code, refusal_reason, updated_at, environment, "
            "origin) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job.job_id,
                job.user_id,
                job.command,
                job.gpu_type,
                job.requested_hours,
                str(job.currency),
                str(job.reserved),
                str(job.state),
                job.backend,
                job.backend_handle,
                to_iso(job.submitted_at),
                to_iso(job.started_at) if job.started_at else None,
                to_iso(job.finished_at) if job.finished_at else None,
                job.exit_code,
                job.refusal_reason,
                to_iso(job.updated_at),
                job.environment,
                job.origin,
            ),
        )

    @staticmethod
    def _insert_transition(
        conn: sqlite3.Connection,
        job_id: str,
        from_state: JobState | None,
        to_state: JobState,
        at: dt.datetime,
        reason: str | None,
    ) -> None:
        conn.execute(
            "INSERT INTO job_transitions (job_id, from_state, to_state, at, reason) "
            "VALUES (?, ?, ?, ?, ?)",
            (job_id, str(from_state) if from_state else None, str(to_state), to_iso(at), reason),
        )

    def _insert_ledger(
        self,
        conn: sqlite3.Connection,
        *,
        job_id: str | None,
        user_id: str,
        currency: Currency,
        kind: LedgerKind,
        amount: Decimal,
        at: dt.datetime,
        note: str | None,
    ) -> None:
        conn.execute(
            "INSERT INTO ledger (job_id, user_id, currency, kind, amount, period, at, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job_id,
                user_id,
                str(currency),
                str(kind),
                str(quantize(amount, currency)),
                billing_period(at, self.config.timezone),
                to_iso(at),
                note,
            ),
        )


def _user_from_row(row: sqlite3.Row) -> User:
    return User(
        user_id=row["user_id"],
        display_name=row["display_name"],
        budget_usd=money(row["budget_usd"]),
        budget_gpu_hours=money(row["budget_gpu_hours"]),
        is_admin=bool(row["is_admin"]),
        created_at=from_iso(row["created_at"]),
        suspended_at=from_iso(row["suspended_at"]) if row["suspended_at"] else None,
        suspended_reason=row["suspended_reason"],
        suspended_by=row["suspended_by"],
    )


def _job_from_row(row: sqlite3.Row) -> Job:
    return Job(
        job_id=row["job_id"],
        user_id=row["user_id"],
        command=row["command"],
        gpu_type=row["gpu_type"],
        requested_hours=row["requested_hours"],
        currency=Currency(row["currency"]),
        reserved=money(row["reserved"]),
        state=JobState(row["state"]),
        submitted_at=from_iso(row["submitted_at"]),
        updated_at=from_iso(row["updated_at"]),
        backend=row["backend"],
        backend_handle=row["backend_handle"],
        started_at=from_iso(row["started_at"]) if row["started_at"] else None,
        finished_at=from_iso(row["finished_at"]) if row["finished_at"] else None,
        exit_code=row["exit_code"],
        refusal_reason=row["refusal_reason"],
        logs_fetched=row["logs_fetched"],
        attempts=row["attempts"],
        preemptions=row["preemptions"],
        pinned_tier=row["pinned_tier"],
        checkpoint_step=row["checkpoint_step"],
        checkpoint_key=row["checkpoint_key"],
        checkpoint_at=from_iso(row["checkpoint_at"]) if row["checkpoint_at"] else None,
        cancel_requested=row["cancel_requested"],
        environment=row["environment"],
        origin=row["origin"],
    )


def _environment_from_row(row: sqlite3.Row) -> Environment:
    return Environment(
        name=row["name"],
        base_image=row["base_image"],
        python_version=row["python_version"],
        requirements=tuple(line for line in row["requirements"].splitlines() if line.strip()),
        apt_packages=tuple(line for line in row["apt_packages"].splitlines() if line.strip()),
        owner=row["owner"],
        description=row["description"],
        created_at=from_iso(row["created_at"]),
        updated_at=from_iso(row["updated_at"]),
    )


def _notification_from_row(row: sqlite3.Row) -> Notification:
    return Notification(
        notification_id=row["id"],
        job_id=row["job_id"],
        user_id=row["user_id"],
        kind=row["kind"],
        message=row["message"],
        at=from_iso(row["at"]),
        seen_at=from_iso(row["seen_at"]) if row["seen_at"] else None,
    )


def _price_from_row(row: sqlite3.Row) -> Price:
    return Price(
        gpu_type=row["gpu_type"],
        hourly=money(row["hourly"]),
        currency=Currency(row["currency"]),
        source=row["source"],
        priced_at=from_iso(row["priced_at"]),
        instance_type=row["instance_type"],
        region=row["region"],
    )


def _ledger_from_row(row: sqlite3.Row) -> LedgerEntry:
    return LedgerEntry(
        entry_id=row["id"],
        job_id=row["job_id"],
        user_id=row["user_id"],
        currency=Currency(row["currency"]),
        kind=LedgerKind(row["kind"]),
        amount=money(row["amount"]),
        period=row["period"],
        at=from_iso(row["at"]),
        note=row["note"],
    )

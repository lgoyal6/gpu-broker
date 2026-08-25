"""`gpu` on the command line.

Written for a sophomore who has never used a scheduler. Two rules the rest of
this file follows:

  Every error says what to do next, not just what went wrong.
  Every number that came out of a policy decision is shown with the policy.

The identity model is deliberately crude for Phase 0: whoever you are on this
machine is who you are to the broker. Phase 5 replaces it with GitHub OAuth.
"""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from .broker import Broker
from .clock import SystemClock
from .config import load_config
from .errors import BrokerError
from .models import Job
from .money import Currency, fmt
from .states import ACTIVE, JobState

console = Console()
err_console = Console(stderr=True)

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Submit GPU jobs to the club's shared pool.",
    rich_markup_mode="rich",
)
admin_app = typer.Typer(no_args_is_help=True, help="Club admin: users and budgets.")
app.add_typer(admin_app, name="admin")
env_app = typer.Typer(no_args_is_help=True, help="Named environments jobs can run in.")
app.add_typer(env_app, name="env")
demo_app = typer.Typer(no_args_is_help=True, help="Seeded demo data, in its own database.")
app.add_typer(demo_app, name="demo")

STATE_DIR_ENV = "GPU_BROKER_HOME"
USER_ENV = "GPU_BROKER_USER"


def whoami(explicit: str | None = None) -> str:
    return explicit or os.environ.get(USER_ENV) or os.environ.get("USER") or "unknown"


def open_broker(state_dir: str | None = None) -> Broker:
    root = state_dir or os.environ.get(STATE_DIR_ENV)
    return Broker.open(Path(root) if root else None, clock=SystemClock())


def _default_state_dir(state_dir: str | None = None) -> Path:
    from .config import DEFAULT_STATE_DIR

    root = state_dir or os.environ.get(STATE_DIR_ENV)
    return Path(root) if root else DEFAULT_STATE_DIR


def die(message: str, hint: str | None = None) -> None:
    err_console.print(f"[red]error:[/red] {message}")
    if hint:
        err_console.print(f"[dim]hint: {hint}[/dim]")
    raise typer.Exit(code=1)


def ensure_user(broker: Broker, user_id: str) -> None:
    """Self-register on first use.

    Adoption is the deliverable. Making twenty people ask an admin to be added
    before they can run anything is exactly the friction that kills it. The
    allowlist arrives in Phase 5 with real identity behind it; until then, a
    default budget is the guardrail.
    """
    if broker.store.maybe_user(user_id) is None:
        broker.add_user(user_id)
        console.print(
            f"[dim]welcome, {user_id}. Starting budget: "
            f"{fmt(broker.config.default_budget_usd, Currency.USD)} and "
            f"{fmt(broker.config.default_budget_gpu_hours, Currency.GPU_HOUR)} this month.[/dim]"
        )


def state_style(state: JobState) -> str:
    if state is JobState.COMPLETED:
        return "green"
    if state in (JobState.PREEMPTED, JobState.RESUMING):
        return "magenta"
    if state in (JobState.FAILED, JobState.REFUSED):
        return "red"
    if state is JobState.CANCELLED:
        return "yellow"
    if state in ACTIVE:
        return "cyan"
    return "white"


# --------------------------------------------------------------------- submit


@app.command(
    context_settings={"allow_interspersed_args": False},
    help="Queue a job. Everything after `--` is the command to run.",
)
def submit(
    ctx: typer.Context,
    command: Annotated[
        Optional[list[str]],
        typer.Argument(help="The command to run, after a literal `--`."),
    ] = None,
    gpu: Annotated[str, typer.Option("--gpu", help="GPU type, e.g. a10g.")] = "a10g",
    hours: Annotated[float, typer.Option("--hours", help="How long you need it.")] = 1.0,
    budget: Annotated[
        Optional[str],
        typer.Option("--budget", help="Hard ceiling. Defaults to what --hours costs."),
    ] = None,
    env: Annotated[
        Optional[str],
        typer.Option("--env", help="A named environment. `gpu env list` shows them."),
    ] = None,
    user: Annotated[Optional[str], typer.Option("--user", hidden=True)] = None,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    if not command:
        die(
            "no command given",
            "gpu submit --gpu a10g --hours 4 --budget 12 -- python train.py",
        )

    parsed_budget: Decimal | None = None
    if budget is not None:
        try:
            parsed_budget = Decimal(budget.lstrip("$"))
        except InvalidOperation:
            die(f"--budget {budget!r} is not a number", "try --budget 12")

    broker = open_broker(state_dir)
    user_id = whoami(user)
    try:
        ensure_user(broker, user_id)
        result = broker.submit(
            user_id=user_id,
            command=" ".join(command),
            gpu_type=gpu,
            hours=hours,
            budget=parsed_budget,
            environment=env,
        )
    except BrokerError as exc:
        die(str(exc))

    if not result.accepted:
        err_console.print(f"[red]{result.refusal.reason}[/red]")
        err_console.print(
            f"[dim]recorded as {result.job.short_id}. "
            f"See `gpu budget` for where your month went.[/dim]"
        )
        raise typer.Exit(code=2)

    job = result.job
    console.print(
        f"[green]queued[/green] {job.short_id}  {job.gpu_type}  {hours:g}h  "
        f"ceiling {fmt(job.reserved, job.currency)}"
        + (f"  env {job.environment}" if job.environment else "")
    )
    if result.warning:
        console.print(f"[yellow]{result.warning}[/yellow]")
    position = _queue_position(broker, job.job_id)
    if position:
        console.print(f"[dim]position {position} in the queue. `gpu queue --why` for the ordering.[/dim]")


def _queue_position(broker: Broker, job_id: str) -> int | None:
    for index, (job, _) in enumerate(broker.queue(), start=1):
        if job.job_id == job_id:
            return index
    return None


# --------------------------------------------------------------------- status


@app.command(help="Show one job, or your recent jobs.")
def status(
    job_id: Annotated[Optional[str], typer.Argument(help="Job id (a prefix is fine).")] = None,
    user: Annotated[Optional[str], typer.Option("--user")] = None,
    all_users: Annotated[bool, typer.Option("--all", help="Everyone's jobs.")] = False,
    limit: Annotated[int, typer.Option("--limit")] = 20,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    if job_id:
        try:
            job = broker.status(job_id)
        except BrokerError as exc:
            die(str(exc))
        _print_job_detail(broker, job)
        return

    who = None if all_users else whoami(user)
    jobs = broker.history(user_id=who, limit=limit)
    if not jobs:
        console.print("[dim]no jobs yet. `gpu submit --gpu a10g --hours 1 -- nvidia-smi`[/dim]")
        return
    _print_job_table(broker, jobs, title="your jobs" if who else "all jobs")


def _print_job_detail(broker: Broker, job: Job) -> None:
    spent = broker.job_spend(job.job_id)
    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column(style="dim", width=12)
    table.add_column()
    table.add_row("job", job.job_id)
    table.add_row("user", job.user_id)
    table.add_row("command", job.command)
    table.add_row("gpu", f"{job.gpu_type} for {job.requested_hours:g}h")
    table.add_row("state", f"[{state_style(job.state)}]{job.state}[/]")
    table.add_row("cost", f"{fmt(spent, job.currency)} of {fmt(job.reserved, job.currency)} ceiling")
    if job.backend:
        table.add_row("running on", f"{job.backend}/{job.backend_handle or '-'}")
    if job.refusal_reason:
        table.add_row("refused", f"[red]{job.refusal_reason}[/red]")
    if job.exit_code is not None:
        table.add_row("exit code", str(job.exit_code))
    if job.attempts > 1:
        table.add_row("attempts", f"{job.attempts} ({job.preemptions} preempted)")
    if job.checkpoint_step is not None:
        table.add_row(
            "checkpoint",
            f"step {job.checkpoint_step}"
            + (f", saved {job.checkpoint_at:%H:%M:%S}" if job.checkpoint_at else ""),
        )
    elif job.preemptions:
        table.add_row("checkpoint", "[yellow]none saved[/yellow]")
    if job.pinned_tier:
        table.add_row("pinned to", f"[yellow]{job.pinned_tier}[/yellow]")
    console.print(table)

    samples = broker.store.samples_for(job.job_id, limit=200)
    if samples:
        recent = samples[-12:]
        peak = max(sample.gpu_percent for sample in samples)
        console.print(
            f"\n[dim]gpu utilization ({len(samples)} samples, peak {peak:.1f}%)[/dim]"
        )
        for sample in recent:
            bar = "#" * int(sample.gpu_percent / 5)
            console.print(
                f"  [dim]{sample.at:%H:%M:%S}[/dim] {sample.gpu_percent:5.1f}% "
                f"[dim]{bar}[/dim]"
            )

    warnings = broker.store.notifications_for_job(job.job_id)
    if warnings:
        console.print("\n[yellow]notices[/yellow]")
        for note in warnings:
            console.print(f"  [dim]{note.at:%Y-%m-%d %H:%M}[/dim] {note.message}")

    history = broker.store.history(job.job_id)
    if history:
        console.print("\n[dim]history[/dim]")
        for from_state, to_state, at, reason in history:
            arrow = f"{from_state} -> {to_state}" if from_state else to_state
            console.print(
                f"  [dim]{at:%Y-%m-%d %H:%M:%S}[/dim]  {arrow}"
                + (f"  [dim]{reason}[/dim]" if reason else "")
            )


def _print_job_table(broker: Broker, jobs: list[Job], title: str) -> None:
    table = Table(title=title, title_justify="left", header_style="dim")
    table.add_column("id")
    table.add_column("user")
    table.add_column("gpu")
    table.add_column("state")
    table.add_column("cost", justify="right")
    table.add_column("command", overflow="ellipsis", max_width=32)
    for job in jobs:
        spent = broker.job_spend(job.job_id)
        state = f"[{state_style(job.state)}]{job.state}[/]"
        if job.preemptions:
            state += f" [dim]x{job.preemptions}[/dim]"
        table.add_row(
            job.short_id,
            job.user_id,
            job.gpu_type,
            state,
            f"{fmt(spent, job.currency)}",
            job.command,
        )
    console.print(table)


# ---------------------------------------------------------------------- queue


@app.command(help="What is waiting, in the order it will run.")
def queue(
    why: Annotated[bool, typer.Option("--why", help="Show how each position was computed.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    entries = broker.queue()
    if not entries:
        console.print("[dim]queue is empty.[/dim]")
        return

    table = Table(header_style="dim")
    table.add_column("#", justify="right")
    table.add_column("id")
    table.add_column("user")
    table.add_column("gpu")
    table.add_column("need", justify="right")
    table.add_column("waited", justify="right")
    if why:
        table.add_column("priority", justify="right")
        table.add_column("= fair", justify="right")
        table.add_column("+ age", justify="right")
        table.add_column("used", justify="right")

    for position, (job, priority) in enumerate(entries, start=1):
        row = [
            str(position),
            job.short_id,
            job.user_id,
            job.gpu_type,
            fmt(job.reserved, job.currency),
            f"{priority.wait_hours:.1f}h",
        ]
        if why:
            row += [
                f"{priority.score:.3f}",
                f"{priority.fair_term:.3f}",
                f"{priority.age_term:.3f}",
                f"{priority.usage_share:.0%}",
            ]
        table.add_row(*row)
    console.print(table)

    if why:
        config = broker.config
        console.print(
            f"\n[dim]priority = {config.w_fair:g} x (1 - share of pool used) + "
            f"{config.w_age:g} x min(1, waited / {config.age_max_hours:g}h)\n"
            f"usage decays by half every {config.fairshare_half_life_days:g} days.[/dim]"
        )


# --------------------------------------------------------------------- cancel


@app.command(help="Stop a job and get the unspent budget back.")
def cancel(
    job_id: Annotated[str, typer.Argument(help="Job id (a prefix is fine).")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Say what would happen. Stop nothing.")
    ] = False,
    user: Annotated[Optional[str], typer.Option("--user")] = None,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    try:
        job = broker.status(job_id)
        actor = whoami(user)
        if job.user_id != actor and not _is_admin(broker, actor):
            die(
                f"job {job.short_id} belongs to {job.user_id}, not you",
                "ask them, or ask an admin to cancel it",
            )
        if dry_run:
            _print_cancel_plan(broker, job)
            return
        cancelled = broker.cancel(job_id, actor=actor)
    except BrokerError as exc:
        die(str(exc))

    refunded = cancelled.reserved - broker.job_spend(cancelled.job_id)
    console.print(
        f"[yellow]cancelled[/yellow] {cancelled.short_id}. "
        f"{fmt(refunded, cancelled.currency)} returned to your budget."
    )


def _print_cancel_plan(broker: Broker, job: Job) -> None:
    if job.is_terminal:
        console.print(f"[dim]{job.short_id} is already {job.state.lower()}. Nothing would happen.[/dim]")
        return
    refund = job.reserved - broker.job_spend(job.job_id)
    console.print(f"would cancel {job.short_id} ({job.user_id}, {job.gpu_type})")
    if job.backend and job.backend_handle:
        backend = broker.scheduler.backend(job.backend)
        if backend is None:
            console.print(f"  [yellow]backend {job.backend} is not configured; the machine would be left running[/yellow]")
        else:
            try:
                console.print(f"  {backend.validate_terminate(job.backend_handle)}")
            except BrokerError as exc:
                console.print(f"  [red]would fail: {exc}[/red]")
    else:
        console.print("  it is still queued, so no machine would be touched")
    console.print(f"  {fmt(refund, job.currency)} would go back to {job.user_id}'s budget")


def _is_admin(broker: Broker, user_id: str) -> bool:
    user = broker.store.maybe_user(user_id)
    return bool(user and user.is_admin)


# --------------------------------------------------------------------- budget


@app.command(help="Where your month went.")
def budget(
    user: Annotated[Optional[str], typer.Option("--user")] = None,
    pool: Annotated[bool, typer.Option("--pool", help="Show the club pool instead.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    if pool:
        _print_pool(broker)
        return

    user_id = whoami(user)
    balances = broker.budgets(user_id)
    table = Table(
        title=f"{user_id} - {broker.store.period_now()}",
        title_justify="left",
        header_style="dim",
    )
    table.add_column("currency")
    table.add_column("spent", justify="right")
    table.add_column("held", justify="right")
    table.add_column("left", justify="right")
    table.add_column("budget", justify="right")
    for currency, balance in balances.items():
        low = balance.available <= balance.budget / 10
        table.add_row(
            "dollars" if currency is Currency.USD else "free gpu-hours",
            fmt(balance.spent, currency),
            fmt(balance.held, currency),
            f"[{'red' if low else 'green'}]{fmt(balance.available, currency)}[/]",
            fmt(balance.budget, currency),
        )
    console.print(table)
    console.print("[dim]'held' is reserved by your jobs that have not finished yet.[/dim]")


def _print_pool(broker: Broker) -> None:
    """The club view.

    Split out because the pool has one number a user does not: how much of it is
    already committed to jobs that are actually running, versus how much has
    merely been asked for by jobs still in the queue. Only the first constrains
    dispatch, and showing them in one column would make a deep queue look like
    an overspent pool.
    """
    table = Table(
        title=f"club pool - {broker.store.period_now()}",
        title_justify="left",
        header_style="dim",
    )
    table.add_column("currency")
    table.add_column("spent", justify="right")
    table.add_column("running", justify="right")
    table.add_column("queued", justify="right")
    table.add_column("free to dispatch", justify="right")
    table.add_column("cap", justify="right")

    for currency, balance in broker.pool().items():
        committed = broker.store.pool_committed(currency)
        headroom = broker.store.pool_headroom(currency)
        queued = balance.committed - committed
        table.add_row(
            "dollars" if currency is Currency.USD else "free gpu-hours",
            fmt(balance.spent, currency),
            fmt(committed - balance.spent, currency),
            fmt(queued, currency),
            f"[{'red' if headroom <= 0 else 'green'}]{fmt(headroom, currency)}[/]",
            fmt(balance.budget, currency),
        )
    console.print(table)
    console.print(
        "[dim]'queued' is demand waiting for room. It does not count against the "
        "cap until it is dispatched.[/dim]"
    )


# ------------------------------------------------------------------------ who


@app.command(help="Who is holding capacity right now.")
def who(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    jobs = broker.who()
    if not jobs:
        console.print("[dim]nothing running. The pool is idle.[/dim]")
        return

    table = Table(header_style="dim")
    table.add_column("user")
    table.add_column("id")
    table.add_column("gpu")
    table.add_column("state")
    table.add_column("running", justify="right")
    table.add_column("spent", justify="right")
    table.add_column("of ceiling", justify="right")
    now = broker.clock.now()
    for job in jobs:
        table.add_row(
            job.user_id,
            job.short_id,
            job.gpu_type,
            f"[{state_style(job.state)}]{job.state}[/]",
            f"{job.elapsed_hours(now):.1f}h",
            fmt(broker.job_spend(job.job_id), job.currency),
            fmt(job.reserved, job.currency),
        )
    console.print(table)


# ----------------------------------------------------------------------- logs


@app.command(help="Show a job's output.")
def logs(
    job_id: Annotated[str, typer.Argument(help="Job id (a prefix is fine).")],
    follow: Annotated[bool, typer.Option("--follow", "-f", help="Keep printing as it runs.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    try:
        job = broker.status(job_id)
    except BrokerError as exc:
        die(str(exc))

    after = 0
    while True:
        lines = broker.logs(job.job_id, after=after)
        for entry_id, at, stream, line in lines:
            after = entry_id
            style = "red" if stream == "stderr" else ("dim" if stream == "broker" else "")
            console.print(f"[dim]{at:%H:%M:%S}[/dim] {f'[{style}]{line}[/]' if style else line}")
        if not follow:
            break
        job = broker.status(job.job_id)
        if job.is_terminal:
            console.print(f"[dim]-- job {job.state.lower()} --[/dim]")
            break
        broker.clock.sleep(2.0)


# ---------------------------------------------------------------- run / tick


@app.command(help="Run one scheduling pass. Normally a daemon does this.")
def tick(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Say what would happen. Launch nothing."),
    ] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    if dry_run:
        _print_plan(broker)
        return
    report = broker.tick()
    parts = []
    for label, items in (
        ("dispatched", report.dispatched),
        ("completed", report.completed),
        ("failed", report.failed),
        ("stopped at ceiling", report.stopped_at_ceiling),
        ("waiting on pool cap", report.blocked_on_pool_cap),
        ("waiting on capacity", report.blocked_on_capacity),
    ):
        if items:
            parts.append(f"{label}: {', '.join(item[:8] for item in items)}")
    console.print("\n".join(parts) if parts else "[dim]nothing to do.[/dim]")


def _print_plan(broker: Broker) -> None:
    """Render a dry run.

    The launch checks come from the provider, not from us: on EC2 each one is a
    real `run_instances(DryRun=True)`, so a missing IAM permission shows up here
    instead of halfway through the first real launch.
    """
    plan = broker.plan()
    if not plan.decisions:
        console.print("[dim]nothing queued. A real tick would do nothing.[/dim]")
        return

    checks = dict(plan.checks)
    problems = dict(plan.problems)

    table = Table(title="dry run - nothing was launched", title_justify="left", header_style="dim")
    table.add_column("id")
    table.add_column("user")
    table.add_column("gpu")
    table.add_column("would")
    table.add_column("detail", overflow="fold")
    for decision in plan.decisions:
        job = decision.job
        if decision.action == "DISPATCH":
            verb, style = "launch", "green"
            detail = problems.get(job.job_id) or checks.get(job.job_id, decision.detail)
            if job.job_id in problems:
                verb, style = "FAIL", "red"
        elif decision.action == "BLOCKED_POOL":
            verb, style, detail = "wait", "yellow", f"pool cap: {decision.detail}"
        else:
            verb, style, detail = "wait", "yellow", decision.detail
        table.add_row(job.short_id, job.user_id, job.gpu_type, f"[{style}]{verb}[/]", detail)
    console.print(table)

    if problems:
        err_console.print(
            f"\n[red]{len(problems)} of these would fail.[/red] "
            "Fix them before the next real tick."
        )
        raise typer.Exit(code=1)


@app.command(help="Machines that are alive with no live job behind them.")
def reap(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    """Reports. Never terminates, and there is no flag that makes it."""
    broker = open_broker(state_dir)
    report = broker.reap()

    if not report.orphans:
        console.print(
            f"[green]nothing to reap.[/green] Scanned {report.scanned} machine(s); "
            "every one has a live job behind it."
        )
        return

    table = Table(
        title=f"{len(report.orphans)} orphan(s) of {report.scanned} machine(s) scanned",
        title_justify="left",
        header_style="dim",
    )
    table.add_column("machine")
    table.add_column("gpu")
    table.add_column("launched by")
    table.add_column("age", justify="right")
    table.add_column("burned", justify="right")
    table.add_column("why", overflow="fold", max_width=52)
    for orphan in report.orphans:
        table.add_row(
            f"{orphan.backend}/{orphan.handle}",
            orphan.gpu_type,
            orphan.user_id or "[red]untagged[/red]",
            f"{orphan.age_hours:.1f}h" if orphan.launched_at else "?",
            f"[red]{fmt(orphan.burned, orphan.currency)}[/red]",
            orphan.reason,
        )
    console.print(table)

    totals = report.total_burned
    burned = ", ".join(
        fmt(amount, currency) for currency, amount in totals.items() if amount > 0
    )
    if burned:
        console.print(f"\ntotal burned by orphans: [red]{burned}[/red]")
    if report.by_user:
        console.print("[dim]who to ask, most expensive first: " +
                      ", ".join(f"{user} ({fmt(amount, Currency.USD)})"
                                for user, amount in report.by_user.items()) + "[/dim]")
    console.print(
        "\n[dim]Nothing was terminated. Check each one, then stop it yourself "
        "with `gpu cancel` or the AWS console.[/dim]"
    )
    raise typer.Exit(code=1)


@app.command(help="What each kind of capacity costs, and how fresh the number is.")
def prices(
    refresh: Annotated[bool, typer.Option("--refresh", help="Pull current prices from AWS.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    if refresh:
        try:
            report = broker.refresh_prices()
        except BrokerError as exc:
            die(str(exc))
        for gpu_type, why in report.failed:
            err_console.print(f"[yellow]{gpu_type}: {why}[/yellow]")
        console.print(f"[dim]refreshed {len(report.updated)} price(s) from AWS.[/dim]\n")

    now = broker.clock.now()
    table = Table(header_style="dim")
    table.add_column("gpu")
    table.add_column("instance")
    table.add_column("per hour", justify="right")
    table.add_column("source")
    table.add_column("age", justify="right")
    for price in sorted(broker.prices.all().values(), key=lambda p: p.gpu_type):
        builtin = price.source == "builtin"
        age = "-" if price.currency is not Currency.USD else f"{price.age_days(now):.0f}d"
        table.add_row(
            price.gpu_type,
            price.instance_type or "-",
            fmt(price.hourly, price.currency),
            f"[yellow]{price.source}[/yellow]" if builtin else f"[green]{price.source}[/green]",
            age,
        )
    console.print(table)
    if any(p.source == "builtin" and p.currency is Currency.USD for p in broker.prices.all().values()):
        console.print(
            "\n[dim]`builtin` means a number typed in by hand, not one AWS gave us. "
            "Run `gpu prices --refresh`.[/dim]"
        )


@app.command(help="At the current burn rate, when does the pool run dry.")
def forecast(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    worst = "OK"
    for currency in Currency:
        result = broker.forecast(currency)
        label = "dollars" if currency is Currency.USD else "free gpu-hours"
        colour = {"OK": "green", "WARNING": "yellow", "CRITICAL": "red", "EXHAUSTED": "red"}[
            result.level
        ]
        console.print(f"[{colour}]{result.level:9}[/] {label:15} {result.headline()}")
        if result.dry_on is not None and result.days_left:
            console.print(f"[dim]{'':9} {'':15} empty around {result.dry_on:%A %d %B}[/dim]")
        if result.level != "OK":
            worst = result.level
    console.print(
        f"\n[dim]Straight-line from the last {broker.config.forecast_window_days:g} days. "
        "Three people starting week-long runs tomorrow is not in this number.[/dim]"
    )
    if worst in ("CRITICAL", "EXHAUSTED"):
        raise typer.Exit(code=1)


@app.command(help="Jobs holding a GPU without using it.")
def reclaim(
    force: Annotated[
        bool, typer.Option("--force", help="Actually reclaim, even if reclaim_enabled is false.")
    ] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    report = broker.reclaim(force=force)
    now = broker.clock.now()

    if not report.idle:
        console.print("[green]nothing idle.[/green] Every running job is using its GPU.")
        return

    table = Table(header_style="dim")
    table.add_column("job")
    table.add_column("user")
    table.add_column("gpu")
    table.add_column("idle", justify="right")
    table.add_column("peak", justify="right")
    table.add_column("wasted", justify="right")
    table.add_column("status")
    held = dict(report.held)
    reclaimed = set(report.reclaimed)
    for verdict in report.idle:
        job_id = verdict.job.job_id
        if job_id in reclaimed:
            status = "[red]reclaimed[/red]"
        elif job_id in held:
            status = f"[dim]{held[job_id]}[/dim]"
        else:
            status = "[yellow]would reclaim[/yellow]"
        table.add_row(
            verdict.job.short_id,
            verdict.job.user_id,
            verdict.job.gpu_type,
            f"{verdict.idle_hours(now):.1f}h",
            f"{verdict.peak_percent:.1f}%",
            fmt(verdict.wasted, verdict.currency),
            status,
        )
    console.print(table)

    if report.would_reclaim:
        console.print(
            f"\n[yellow]{len(report.would_reclaim)} job(s) would have been reclaimed.[/yellow] "
            "Nothing was killed: reclaim_enabled is false in config.json. "
            "Watch this be right for a few weeks, then turn it on."
        )
        for verdict in report.would_reclaim:
            console.print(f"\n[dim]{verdict.job.short_id} -- the samples that justify it:[/dim]")
            console.print(f"[dim]{verdict.evidence()}[/dim]")
    console.print(
        "\n[dim]Every one of these was told before it appeared here. "
        "`gpu status <id>` shows a job's own samples.[/dim]"
    )


@app.command(help="The lab machines: what they have, and whether they can take work.")
def hosts(
    refresh: Annotated[bool, typer.Option("--refresh", help="Check now instead of using the cached result.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    health = broker.hosts(refresh=refresh)
    if not health:
        console.print(
            "[dim]no local pool configured. Add hosts under `local` in config.json "
            "and put \"local\" in `backends`.[/dim]"
        )
        return

    table = Table(header_style="dim")
    table.add_column("host")
    table.add_column("state")
    table.add_column("gpus")
    table.add_column("free", justify="right")
    table.add_column("mps")
    table.add_column("limits")
    backend = broker.local_backend()
    for entry in health:
        style = "green" if entry.state.accepts_jobs else "yellow"
        table.add_row(
            entry.hostname,
            f"[{style}]{entry.state}[/]",
            ", ".join(f"{gpu.name} {gpu.memory_total_mb}MB" for gpu in entry.gpus) or "-",
            str(backend._free_on(entry)) if backend and entry.state.accepts_jobs else "0",
            "up" if entry.mps_ready else "[dim]down[/dim]",
            "[green]yes[/green]"
            if entry.limits_enforced
            else ("[red]NO[/red]" if entry.limits_tested else "[dim]?[/dim]"),
        )
    console.print(table)

    # Reasons go on their own lines rather than in a column. In an 80-column
    # terminal Rich squeezes a wide cell down to nothing, and the sentence
    # explaining why a machine is unusable is the one thing on this page that
    # has to survive.
    for entry in health:
        if entry.state.accepts_jobs:
            continue
        if entry.limits_tested and not entry.limits_enforced:
            err_console.print(
                f"\n[red]{entry.hostname}: cgroup limits are NOT ENFORCED.[/red] "
                "One job can take the whole machine down, so nothing will be "
                f"placed here until it is fixed. {entry.reason}"
            )
        else:
            console.print(
                f"\n[yellow]{entry.hostname} is {entry.state.lower()}[/yellow]: {entry.reason}"
            )

    if any(not entry.state.accepts_jobs for entry in health):
        console.print(
            "\n[dim]A drained host keeps running whatever is already on it. "
            "It just stops taking new work.[/dim]"
        )


@app.command(help="Check credentials, quota, and connectivity, and say what is wrong.")
def doctor(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    from .doctor import FAIL, OK, WARN, diagnose

    broker = open_broker(state_dir)
    results = diagnose(broker)

    marks = {OK: "[green]ok  [/green]", WARN: "[yellow]warn[/yellow]", FAIL: "[red]FAIL[/red]"}
    for check in results:
        console.print(f"{marks[check.status]}  [bold]{check.name}[/bold]  {check.detail}")
        if check.fix:
            console.print(f"       [dim]{check.fix}[/dim]")

    broken = [check for check in results if check.bad]
    if broken:
        err_console.print(
            f"\n[red]{len(broken)} thing(s) need fixing[/red] before this will run "
            "against real hardware."
        )
        raise typer.Exit(code=1)
    console.print("\n[green]everything checks out.[/green]")


@app.command(help="What the pool has been doing, over time.")
def metrics(
    prune: Annotated[bool, typer.Option("--prune", help="Delete series past the retention window.")] = False,
    export: Annotated[bool, typer.Option("--prometheus", help="Print the /metrics body.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    import datetime as dt

    from .metrics import HEAD_WAIT_HOURS, QUEUE_DEPTH, RUNNING_JOBS, UTILIZATION, prometheus

    broker = open_broker(state_dir)
    if export:
        console.print(prometheus(broker.metrics), end="")
        return
    if prune:
        cutoff = broker.clock.now() - dt.timedelta(days=broker.config.metrics_retention_days)
        console.print(f"removed {broker.metrics.prune(cutoff)} point(s) older than {cutoff:%Y-%m-%d}")
        return

    since = broker.clock.now() - dt.timedelta(days=7)
    table = Table(title="last 7 days", title_justify="left", header_style="dim")
    table.add_column("metric")
    table.add_column("mean", justify="right")
    table.add_column("peak", justify="right")
    table.add_column("points", justify="right")
    for name, label in (
        (QUEUE_DEPTH, "jobs queued"),
        (RUNNING_JOBS, "jobs running"),
        (HEAD_WAIT_HOURS, "longest wait (h)"),
        (UTILIZATION, "gpu utilization %"),
    ):
        points = broker.metrics.series(name, since=since)
        if not points:
            continue
        mean = sum(p.value for p in points) / len(points)
        table.add_row(label, f"{mean:.1f}", f"{max(p.value for p in points):.1f}", str(len(points)))
    console.print(table)
    console.print(
        "[dim]Scrape `gpu metrics --prometheus`, or the web app's /metrics.[/dim]"
    )


@app.command(help="The weekly note to the club.")
def digest(
    days: Annotated[float, typer.Option("--days")] = 7.0,
    send: Annotated[bool, typer.Option("--send", help="Post it to the configured webhook.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    from .digest import render
    from .digest import send as post

    broker = open_broker(state_dir)
    body = render(broker.digest(days=days))
    console.print(body)
    if send:
        try:
            console.print(f"[green]{post(broker.config, body)}[/green]")
        except BrokerError as exc:
            die(str(exc))


@app.command(help="The measurement report: what actually happened, including where it loses.")
def report(
    days: Annotated[float, typer.Option("--days", help="How far back to look.")] = 90.0,
    baseline: Annotated[bool, typer.Option("--baseline", help="Pull pre-broker spend from Cost Explorer first.")] = False,
    out: Annotated[Optional[str], typer.Option("--out", help="Write it to a file too.")] = None,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    import datetime as dt

    from .report import markdown

    broker = open_broker(state_dir)
    if baseline:
        try:
            months = broker.refresh_baseline()
            console.print(f"[dim]baseline: {len(months)} month(s) from Cost Explorer[/dim]")
        except BrokerError as exc:
            err_console.print(f"[yellow]{exc}[/yellow]")

    body = markdown(broker.report(since=broker.clock.now() - dt.timedelta(days=days)))
    console.print(body)
    if out:
        Path(out).expanduser().write_text(body)
        console.print(f"\n[dim]written to {out}[/dim]")


@app.command(help="Serve the web app. Holds no credentials; run `gpu run` separately.")
def web(
    host: Annotated[str, typer.Option("--host")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port")] = 8000,
    public: Annotated[
        bool,
        typer.Option("--public", help="Serve the unauthenticated status page at /status."),
    ] = False,
    only_public: Annotated[
        bool,
        typer.Option(
            "--only-public",
            help="Serve ONLY the status page. No sign-in, no submit, no admin.",
        ),
    ] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    try:
        import uvicorn

        from .web.app import create_app
    except ImportError as exc:
        die(
            f"the web app needs its extras: {exc}",
            "pip install 'gpu-broker[web]'",
        )

    root = state_dir or os.environ.get(STATE_DIR_ENV)
    config = load_config(Path(root) if root else None)

    if only_public:
        # Nothing else is mounted. This is the process you put on a public
        # hostname: it cannot sign anyone in and has no route that writes.
        from .web.app import open_broker as open_web_broker
        from .web.publicapp import create_public_app

        application = create_public_app(lambda: open_web_broker(config))
        console.print(
            f"[dim]serving the status page on http://{host}:{port} -- "
            f"read-only, no sign-in, nothing here can change anything.[/dim]"
        )
        uvicorn.run(application, host=host, port=port, log_level="warning")
        return

    try:
        overrides = {"public_status": True} if public else None
        application = create_app(config=config, web_overrides=overrides)
    except BrokerError as exc:
        die(str(exc))

    console.print(
        f"[dim]serving on http://{host}:{port} -- this process holds no AWS "
        f"credentials and launches nothing. Run `gpu run` alongside it.[/dim]"
    )
    if public:
        console.print(f"[dim]status page, no sign-in: http://{host}:{port}/status[/dim]")
    uvicorn.run(application, host=host, port=port, log_level="warning")


@app.command(help="Run the scheduler until the queue drains.")
def run(
    interval: Annotated[float, typer.Option("--interval", help="Seconds between passes.")] = 10.0,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    console.print(f"[dim]scheduling every {interval:g}s. ctrl-c to stop.[/dim]")

    def narrate(report) -> None:
        """Say what happened, as it happens.

        Without this the command is silent until the queue drains, which for a
        first run means staring at nothing wondering whether it is working. The
        first thing somebody does with a new tool is run it and watch.
        """
        for job_id in report.dispatched:
            job = broker.status(job_id)
            console.print(
                f"  [green]start[/green]  {job.short_id}  {job.user_id}  "
                f"{job.gpu_type}  [dim]{job.command[:44]}[/dim]"
            )
        for job_id in report.completed:
            console.print(f"  [green]done [/green]  {broker.status(job_id).short_id}")
        for job_id in report.failed:
            job = broker.status(job_id)
            console.print(f"  [red]fail [/red]  {job.short_id}  [dim]{job.state.lower()}[/dim]")
        for job_id in report.stopped_at_ceiling:
            console.print(f"  [yellow]stop [/yellow]  {broker.status(job_id).short_id}  [dim]hit its budget ceiling[/dim]")

    try:
        reports = broker.run_until_idle(step_seconds=interval, on_tick=narrate)
    except KeyboardInterrupt:
        console.print("\n[dim]stopped. Jobs keep running; state is on disk.[/dim]")
        raise typer.Exit(code=0)
    dispatched = sum(len(report.dispatched) for report in reports)
    finished = sum(len(report.completed) + len(report.failed) for report in reports)
    console.print(f"queue drained. dispatched {dispatched}, finished {finished}.")


@app.command(help="Compare what the backends are holding against our records.")
def reconcile(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    drifts = broker.reconcile()
    if not drifts:
        console.print("[green]no drift.[/green] Every record matches every backend.")
        return
    console.print(f"[yellow]{len(drifts)} disagreement(s) found.[/yellow] Reporting only, nothing was touched.\n")
    for drift in drifts:
        console.print(f"  {drift}")
    console.print(
        "\n[dim]ORPHAN means a backend is holding capacity for a job we consider finished.[/dim]"
    )
    raise typer.Exit(code=1)


# ---------------------------------------------------------------------- admin


@admin_app.command("add-user", help="Add a club member.")
def admin_add_user(
    user_id: str,
    usd: Annotated[Optional[str], typer.Option("--usd", help="Monthly dollar budget.")] = None,
    gpu_hours: Annotated[Optional[str], typer.Option("--gpu-hours")] = None,
    admin: Annotated[bool, typer.Option("--admin")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    user = broker.add_user(
        user_id,
        budget_usd=Decimal(usd.lstrip("$")) if usd else None,
        budget_gpu_hours=Decimal(gpu_hours) if gpu_hours else None,
        is_admin=admin or None,
    )
    console.print(
        f"[green]{user.user_id}[/green]: {fmt(user.budget_usd, Currency.USD)} and "
        f"{fmt(user.budget_gpu_hours, Currency.GPU_HOUR)} per month"
        + (" [dim](admin)[/dim]" if user.is_admin else "")
    )


@env_app.command("list", help="Environments anyone can run a job in.")
def env_list(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    found = broker.store.environments()
    if not found:
        console.print(
            "[dim]no environments yet. "
            "`gpu env create mine --requirements requirements.txt`[/dim]"
        )
        return

    table = Table(header_style="dim")
    table.add_column("name")
    table.add_column("digest")
    table.add_column("packages", justify="right")
    table.add_column("built")
    table.add_column("owner")
    for environment in found:
        builds = broker.store.builds_for(environment.digest)
        ready = [b.target for b in builds if b.usable]
        table.add_row(
            environment.name,
            f"[dim]{environment.digest}[/dim]",
            str(len(environment.requirements)),
            ", ".join(ready) if ready else "[dim]on first use[/dim]",
            environment.owner or "-",
        )
    console.print(table)
    console.print(
        "[dim]An environment is built the first time a job asks for it, then reused. "
        "Changing a package changes the digest, so a cached build is never stale.[/dim]"
    )


@env_app.command("show", help="What is in an environment, and where it has been built.")
def env_show(
    name: str,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    environment = broker.store.environment(name)
    if environment is None:
        die(f"no environment called {name!r}", "`gpu env list` shows them")

    console.print(f"[bold]{environment.name}[/bold]  [dim]{environment.digest}[/dim]")
    if environment.description:
        console.print(f"  {environment.description}")
    console.print(f"  base    {environment.base_image}")
    for requirement in environment.requirements:
        console.print(f"  pip     {requirement}")
    for package in environment.apt_packages:
        console.print(f"  apt     {package}")
    builds = broker.store.builds_for(environment.digest)
    if builds:
        for build in builds:
            colour = "green" if build.usable else "yellow"
            console.print(f"  [{colour}]{build.target}[/]  {build.reference or build.detail}")
    else:
        console.print("  [dim]not built anywhere yet; the first job that asks builds it[/dim]")


@env_app.command("create", help="Create or update a named environment.")
def env_create(
    name: str,
    requirements: Annotated[
        Optional[str], typer.Option("--requirements", "-r", help="Path to a requirements.txt.")
    ] = None,
    pip: Annotated[Optional[list[str]], typer.Option("--pip", help="A package, repeatable.")] = None,
    apt: Annotated[Optional[list[str]], typer.Option("--apt", help="A system package, repeatable.")] = None,
    base: Annotated[Optional[str], typer.Option("--base", help="Base image.")] = None,
    description: Annotated[str, typer.Option("--description")] = "",
    user: Annotated[Optional[str], typer.Option("--user", hidden=True)] = None,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    from .environments import DEFAULT_BASE, Environment, from_requirements

    broker = open_broker(state_dir)
    packages = list(pip or [])
    if requirements:
        path = Path(requirements).expanduser()
        if not path.is_file():
            die(f"no such file: {path}")
        packages.extend(from_requirements(name, path.read_text()).requirements)

    environment = Environment(
        name=name,
        base_image=base or DEFAULT_BASE,
        requirements=tuple(packages),
        apt_packages=tuple(apt or []),
        owner=whoami(user),
        description=description,
    )
    try:
        environment.validate()
    except BrokerError as exc:
        die(str(exc))

    existing = broker.store.environment(name)
    saved = broker.store.save_environment(environment)
    if existing and existing.digest != saved.digest:
        console.print(
            f"[yellow]{name} changed[/yellow]: {existing.digest} -> {saved.digest}. "
            "Jobs already running keep the old build; the next one builds this."
        )
    else:
        console.print(f"[green]{name}[/green]  [dim]{saved.digest}[/dim]  {len(packages)} package(s)")
    console.print(f"[dim]use it with: gpu submit --env {name} -- python train.py[/dim]")


@env_app.command("delete", help="Remove an environment. Running jobs are unaffected.")
def env_delete(
    name: str,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    if broker.store.environment(name) is None:
        die(f"no environment called {name!r}")
    broker.store.delete_environment(name)
    console.print(f"[yellow]{name} removed.[/yellow] Jobs already using it keep running.")


@admin_app.command("drain", help="Stop placing new jobs on a lab host.")
def admin_drain(
    hostname: str,
    reason: Annotated[str, typer.Option("--reason", help="Why, so `gpu hosts` can say.")] = "maintenance",
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    try:
        broker.drain_host(hostname, reason, actor=whoami())
    except BrokerError as exc:
        die(str(exc))
    console.print(
        f"[yellow]{hostname} drained[/yellow]: {reason}. "
        "Jobs already running there are untouched."
    )


@admin_app.command("undrain", help="Let a lab host take jobs again.")
def admin_undrain(
    hostname: str,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    try:
        broker.undrain_host(hostname)
    except BrokerError as exc:
        die(str(exc))
    console.print(f"[green]{hostname}[/green] will be re-checked on the next pass.")


@admin_app.command("users", help="List club members and their budgets.")
def admin_users(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    broker = open_broker(state_dir)
    table = Table(header_style="dim")
    table.add_column("user")
    table.add_column("dollars left", justify="right")
    table.add_column("gpu-hours left", justify="right")
    table.add_column("admin")
    for user in broker.users():
        balances = broker.budgets(user.user_id)
        table.add_row(
            user.user_id,
            fmt(balances[Currency.USD].available, Currency.USD),
            fmt(balances[Currency.GPU_HOUR].available, Currency.GPU_HOUR),
            "yes" if user.is_admin else "",
        )
    console.print(table)


@demo_app.command("seed", help="Build a demo database so the status page has something on it.")
def demo_seed(
    directory: Annotated[
        Optional[str],
        typer.Option("--dir", help="Where to put it. Defaults to <state dir>/demo."),
    ] = None,
    days: Annotated[int, typer.Option("--days", help="How much history to generate.")] = 30,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Redo an existing one.")] = False,
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    from .demo import seed

    target = Path(directory) if directory else _default_state_dir(state_dir) / "demo"
    try:
        report = seed(target, days=days, overwrite=overwrite)
    except RuntimeError as exc:
        die(str(exc))

    console.print(
        f"[green]seeded[/green] {report.jobs} jobs from {report.users} people "
        f"over {report.days} days"
    )
    console.print(f"[dim]{report.directory}[/dim]")
    if report.missing_states:
        # Reported rather than swallowed: the scenario is meant to be an
        # integration fixture, so a state it never reaches is a fact about the
        # broker worth knowing, not a detail to hide behind a green tick.
        console.print(
            f"[yellow]never reached: {', '.join(sorted(report.missing_states))}[/yellow]"
        )
    console.print(
        "[dim]Serve it with: gpu web --state-dir "
        f"{report.directory} --public[/dim]"
    )


@demo_app.command("path", help="Where the demo database would go.")
def demo_path(
    state_dir: Annotated[Optional[str], typer.Option("--state-dir", hidden=True)] = None,
) -> None:
    console.print(str(_default_state_dir(state_dir) / "demo"))


def main() -> None:
    try:
        app()
    except BrokerError as exc:  # anything that escaped a command
        die(str(exc))


if __name__ == "__main__":
    main()

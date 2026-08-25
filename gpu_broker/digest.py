"""The weekly note to the club.

Who used what, what was reclaimed, and how long the credits last. Short enough
that somebody reads it in a channel without opening anything.

Generated always; sent only if a webhook is configured. That split is
deliberate: a digest nobody can produce without a Slack workspace is a digest
that does not exist for a club that uses Discord, or email, or a whiteboard.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .config import BrokerConfig
from .errors import BrokerError
from .money import ZERO, Currency, fmt, quantize
from .states import JobState


@dataclass(frozen=True)
class UserLine:
    user_id: str
    jobs: int
    dollars: Decimal
    gpu_hours: Decimal
    useful_fraction: float | None


@dataclass(frozen=True)
class Digest:
    since: dt.datetime
    until: dt.datetime
    users: tuple[UserLine, ...]
    jobs_run: int
    jobs_failed: int
    reclaimed_jobs: int
    reclaimed_dollars: Decimal
    forecasts: tuple[Any, ...]
    idle_now: int
    new_users: tuple[str, ...]

    @property
    def quiet(self) -> bool:
        return self.jobs_run == 0


def build(broker, days: float = 7.0) -> Digest:
    store = broker.store
    now = broker.clock.now()
    since = now - dt.timedelta(days=days)
    jobs = [job for job in store.list_jobs() if job.submitted_at >= since]

    per_user: dict[str, list] = {}
    for job in jobs:
        if job.state is not JobState.REFUSED:
            per_user.setdefault(job.user_id, []).append(job)

    lines: list[UserLine] = []
    for user_id, theirs in per_user.items():
        dollars = sum(
            (store.job_spend(j.job_id) for j in theirs if j.currency is Currency.USD), ZERO
        )
        hours = sum(
            (store.job_spend(j.job_id) for j in theirs if j.currency is Currency.GPU_HOUR), ZERO
        )
        fractions = []
        for job in theirs:
            samples = store.samples_for(job.job_id, limit=5_000)
            if samples:
                busy = sum(
                    1 for s in samples if s.gpu_percent > broker.config.idle_threshold_percent
                )
                fractions.append(busy / len(samples))
        lines.append(
            UserLine(
                user_id=user_id,
                jobs=len(theirs),
                dollars=quantize(dollars, Currency.USD),
                gpu_hours=quantize(hours, Currency.GPU_HOUR),
                useful_fraction=sum(fractions) / len(fractions) if fractions else None,
            )
        )
    lines.sort(key=lambda line: (-line.dollars, -line.jobs, line.user_id))

    reclaimed = [job for job in jobs if job.state is JobState.RECLAIMED]
    returned = sum(
        (max(ZERO, job.reserved - store.job_spend(job.job_id))
         for job in reclaimed if job.currency is Currency.USD),
        ZERO,
    )

    return Digest(
        since=since,
        until=now,
        users=tuple(lines),
        jobs_run=sum(1 for job in jobs if job.state is not JobState.REFUSED),
        jobs_failed=sum(1 for job in jobs if job.state is JobState.FAILED),
        reclaimed_jobs=len(reclaimed),
        reclaimed_dollars=quantize(returned, Currency.USD),
        forecasts=tuple(broker.forecast(currency) for currency in Currency),
        idle_now=len(broker.idle_jobs()),
        new_users=tuple(
            sorted(user.user_id for user in store.list_users() if user.created_at >= since)
        ),
    )


def render(digest: Digest) -> str:
    """Markdown, which Slack and Discord both render and a human can read raw."""
    out = [
        f"**GPU pool, {digest.since:%d %b} to {digest.until:%d %b}**",
        "",
    ]

    # What is left is one of the three things this note exists to carry, so it
    # goes in even on a week when nobody spent anything -- that is exactly the
    # week somebody wonders whether there is anything in the pool at all.
    for forecast in digest.forecasts:
        mark = {"OK": "", "WARNING": " :warning:", "CRITICAL": " :rotating_light:",
                "EXHAUSTED": " :rotating_light:"}[forecast.level]
        out.append(f"- {forecast.headline()}{mark}")
    out.append("")

    if digest.quiet:
        out += ["Nobody ran anything this week.", ""]
    else:
        out += [f"**{digest.jobs_run} job(s)**"
                + (f", {digest.jobs_failed} failed" if digest.jobs_failed else ""), ""]
        for line in digest.users:
            bits = [f"{line.jobs} job{'' if line.jobs == 1 else 's'}"]
            if line.dollars > ZERO:
                bits.append(fmt(line.dollars, Currency.USD))
            if line.gpu_hours > ZERO:
                bits.append(fmt(line.gpu_hours, Currency.GPU_HOUR))
            if line.useful_fraction is not None:
                bits.append(f"{line.useful_fraction * 100:.0f}% of it using the GPU")
            out.append(f"- **{line.user_id}**: {', '.join(bits)}")
        out.append("")

    if digest.reclaimed_jobs:
        out += [
            f"Reclaimed {digest.reclaimed_jobs} idle job(s), returning "
            f"{fmt(digest.reclaimed_dollars, Currency.USD)} to the pool.",
            "",
        ]
    if digest.idle_now:
        out += [f"{digest.idle_now} job(s) are holding a GPU without using it right now.", ""]
    if digest.new_users:
        out += [f"New this week: {', '.join(digest.new_users)}. Welcome.", ""]

    return "\n".join(out)


def send(config: BrokerConfig, body: str, client: Any | None = None) -> str:
    """Post to whatever webhook is configured. Slack and Discord both take this
    shape; anything else can read the same JSON."""
    url = config.digest_webhook
    if not url:
        raise BrokerError(
            "no digest_webhook in config.json. `gpu digest` prints it either way -- "
            "paste it wherever the club actually talks"
        )
    payload = json.dumps({"text": body, "content": body}).encode()

    if client is None:
        import httpx

        client = httpx.Client(timeout=15.0)
    response = client.post(
        url, content=payload, headers={"Content-Type": "application/json"}
    )
    status = getattr(response, "status_code", 0)
    if not 200 <= status < 300:
        raise BrokerError(f"the webhook rejected it: HTTP {status}")
    return f"posted to {url.split('/')[2] if '//' in url else url}"


# --- the "before" baseline --------------------------------------------------


def fetch_baseline(client: Any, start: str, end: str) -> list[tuple[str, float, Decimal]]:
    """Historical EC2 spend and usage, from Cost Explorer.

    The measurement report's "before" column. Filtered to EC2 compute so that
    S3, data transfer and everything else the account does are not counted as
    GPU spend, which would make the broker look better than it is.
    """
    from .money import money

    try:
        response = client.get_cost_and_usage(
            TimePeriod={"Start": start, "End": end},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost", "UsageQuantity"],
            Filter={
                "Dimensions": {
                    "Key": "SERVICE",
                    "Values": ["Amazon Elastic Compute Cloud - Compute"],
                }
            },
        )
    except Exception as exc:  # noqa: BLE001 - botocore, permissions, network
        raise BrokerError(
            f"could not read Cost Explorer: {exc}. The IAM user needs "
            "ce:GetCostAndUsage, or put the baseline in by hand"
        ) from exc

    out: list[tuple[str, float, Decimal]] = []
    for entry in response.get("ResultsByTime", []):
        period = entry.get("TimePeriod", {}).get("Start", "")[:7]
        totals = entry.get("Total", {})
        dollars = money(totals.get("UnblendedCost", {}).get("Amount", "0"))
        hours = float(totals.get("UsageQuantity", {}).get("Amount", "0") or 0)
        if period:
            out.append((period, hours, quantize(dollars, Currency.USD)))
    return out

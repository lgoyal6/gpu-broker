"""Time series: the things that are gone if nobody writes them down.

Most of what the club wants to know is derivable after the fact. How many people
used the broker, how many jobs ran, who came back a second time, what was spent
and what was reclaimed -- all of that is in `jobs` and `ledger`, and has been
since the first deploy.

A series is different. How deep the queue was at three o'clock last Tuesday
exists only if something recorded it at three o'clock last Tuesday. So the
scheduler writes a handful of gauges every tick, and that is the whole design.

Deliberately not a time-series database. Twenty people generate a few thousand
rows a week, SQLite handles that without noticing, and the alternative is
another daemon for the club to keep alive.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .clock import from_iso, to_iso
from .db.connection import transaction
from .money import Currency

# Gauge names. Constants because they appear in three places -- recording,
# querying, and the Prometheus export -- and a typo in one of them produces a
# metric that is silently always empty.
QUEUE_DEPTH = "queue_depth"
RUNNING_JOBS = "running_jobs"
HEAD_WAIT_HOURS = "head_wait_hours"
FREE_SLOTS = "free_slots"
UTILIZATION = "gpu_utilization_percent"
SPEND = "spend_total"
POOL_REMAINING = "pool_remaining"


@dataclass(frozen=True)
class Point:
    name: str
    at: dt.datetime
    value: float
    labels: dict[str, str]


def encode_labels(labels: dict[str, str] | None) -> str:
    """`a=1,b=2`, sorted. Sorted so the same label set is always the same string
    and a series does not split in two because a dict came out differently."""
    if not labels:
        return ""
    return ",".join(f"{key}={value}" for key, value in sorted(labels.items()))


def decode_labels(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in raw.split(","):
        if "=" in part:
            key, _, value = part.partition("=")
            out[key] = value
    return out


class Metrics:
    """Reads and writes the series. Owns no policy."""

    def __init__(self, store) -> None:
        self.store = store

    def record(self, points: list[tuple[str, float, dict[str, str] | None]]) -> None:
        """One transaction for a whole tick's worth of gauges."""
        if not points:
            return
        at = to_iso(self.store.clock.now())
        with transaction(self.store.conn) as conn:
            conn.executemany(
                "INSERT INTO metrics (name, at, value, labels) VALUES (?, ?, ?, ?)",
                [(name, at, float(value), encode_labels(labels)) for name, value, labels in points],
            )

    def series(
        self, name: str, since: dt.datetime | None = None, labels: dict[str, str] | None = None
    ) -> list[Point]:
        sql = "SELECT name, at, value, labels FROM metrics WHERE name = ?"
        params: list[object] = [name]
        if since is not None:
            sql += " AND at >= ?"
            params.append(to_iso(since))
        if labels is not None:
            sql += " AND labels = ?"
            params.append(encode_labels(labels))
        sql += " ORDER BY id"
        return [
            Point(row["name"], from_iso(row["at"]), row["value"], decode_labels(row["labels"]))
            for row in self.store.conn.execute(sql, params)
        ]

    def latest(self, name: str) -> list[Point]:
        """The newest value of each label set. What Prometheus scrapes."""
        rows = self.store.conn.execute(
            "SELECT name, at, value, labels FROM metrics m WHERE name = ? AND id = ("
            "  SELECT MAX(id) FROM metrics WHERE name = m.name AND labels = m.labels"
            ") ORDER BY labels",
            (name,),
        ).fetchall()
        return [
            Point(row["name"], from_iso(row["at"]), row["value"], decode_labels(row["labels"]))
            for row in rows
        ]

    def average(self, name: str, since: dt.datetime, labels: dict[str, str] | None = None) -> float | None:
        points = self.series(name, since=since, labels=labels)
        return sum(p.value for p in points) / len(points) if points else None

    def peak(self, name: str, since: dt.datetime) -> float | None:
        points = self.series(name, since=since)
        return max((p.value for p in points), default=None)

    def bucketed(
        self, name: str, since: dt.datetime, until: dt.datetime, buckets: int = 48
    ) -> list[float | None]:
        """The series averaged into equal time buckets, oldest first.

        `None` for a bucket with no samples, so a gap in the record renders as a
        gap rather than as zero. A pool that was switched off for six hours did
        not have a GPU sitting at 0%; nothing was running at all, and drawing
        that as a floor would be a different and wrong claim.
        """
        span = (until - since).total_seconds()
        if span <= 0 or buckets < 1:
            return []
        sums = [0.0] * buckets
        counts = [0] * buckets
        for point in self.series(name, since=since):
            index = int((point.at - since).total_seconds() / span * buckets)
            index = min(max(index, 0), buckets - 1)
            sums[index] += point.value
            counts[index] += 1
        return [
            (sums[i] / counts[i]) if counts[i] else None for i in range(buckets)
        ]

    def prune(self, older_than: dt.datetime) -> int:
        """Series older than the retention window. Nothing calls this on a
        schedule; `gpu metrics --prune` does, when somebody decides to."""
        with transaction(self.store.conn) as conn:
            cursor = conn.execute("DELETE FROM metrics WHERE at < ?", (to_iso(older_than),))
        return cursor.rowcount


def collect(store, config, clock, backends) -> list[tuple[str, float, dict[str, str] | None]]:
    """One tick's gauges.

    Explicit arguments rather than a broker, so this can be called from the
    scheduler -- which is where a tick actually is -- without the scheduler
    having to look like a broker.

    Read-only and cheap: everything comes from state the tick already loaded. A
    collector that costs a round trip per gauge becomes the reason ticks are slow.
    """
    now = clock.now()
    queued = store.queued_jobs()
    active = store.active_jobs()

    points: list[tuple[str, float, dict[str, str] | None]] = [
        (QUEUE_DEPTH, len(queued), None),
        (RUNNING_JOBS, len(active), None),
        (HEAD_WAIT_HOURS, max((job.wait_hours(now) for job in queued), default=0.0), None),
    ]

    for gpu in config.gpu_types:
        waiting = sum(1 for job in queued if job.gpu_type == gpu.name)
        points.append((QUEUE_DEPTH + "_by_gpu", waiting, {"gpu": gpu.name}))
        free = sum(backend.free_slots(gpu.name) for backend in backends)
        points.append((FREE_SLOTS, free, {"gpu": gpu.name}))

    for job in active:
        samples = store.samples_for(job.job_id, limit=3)
        if samples:
            recent = sum(s.gpu_percent for s in samples) / len(samples)
            points.append((UTILIZATION, recent, {"job": job.short_id, "user": job.user_id}))

    for currency in Currency:
        pool = store.pool_balance(currency)
        points.append((SPEND, float(pool.spent), {"currency": str(currency)}))
        points.append(
            (POOL_REMAINING, float(pool.budget - store.pool_committed(currency)),
             {"currency": str(currency)})
        )

    return points


# --- Prometheus ------------------------------------------------------------

_HELP = {
    QUEUE_DEPTH: ("gauge", "Jobs waiting for capacity"),
    QUEUE_DEPTH + "_by_gpu": ("gauge", "Jobs waiting, by GPU type"),
    RUNNING_JOBS: ("gauge", "Jobs holding capacity"),
    HEAD_WAIT_HOURS: ("gauge", "Hours the longest-waiting queued job has waited"),
    FREE_SLOTS: ("gauge", "Free slots, by GPU type"),
    UTILIZATION: ("gauge", "Recent GPU utilization of a running job, percent"),
    SPEND: ("counter", "Spent this billing period"),
    POOL_REMAINING: ("gauge", "Pool budget not yet committed"),
}


def prometheus(metrics: Metrics) -> str:
    """The `/metrics` body.

    Hand-written rather than via a client library. The exposition format is a
    dozen lines of text, and the alternative is a dependency that wants a
    process-wide registry the broker does not otherwise need.
    """
    lines: list[str] = []
    for name, (kind, help_text) in _HELP.items():
        points = metrics.latest(name)
        if not points:
            continue
        metric = f"gpu_broker_{name}"
        lines.append(f"# HELP {metric} {help_text}")
        lines.append(f"# TYPE {metric} {kind}")
        for point in points:
            labels = "".join(
                f'{key}="{_escape(value)}",' for key, value in sorted(point.labels.items())
            ).rstrip(",")
            suffix = f"{{{labels}}}" if labels else ""
            lines.append(f"{metric}{suffix} {point.value:g}")
    return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

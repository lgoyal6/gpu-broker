"""Time, behind an interface.

Nothing in the broker calls `datetime.now()` directly. Every phase has to be
runnable with no AWS account and no real waiting, which means the clock is
injected and the tests drive it by hand.
"""

from __future__ import annotations

import datetime as dt
from typing import Protocol


class Clock(Protocol):
    """The only source of 'now' in the system."""

    def now(self) -> dt.datetime:
        """Current time, always timezone-aware and always UTC."""
        ...

    def sleep(self, seconds: float) -> None:
        """Advance past `seconds`. Real clocks block; fake clocks jump."""
        ...


class SystemClock:
    """Wall time. Used in production, never in tests."""

    def now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    def sleep(self, seconds: float) -> None:
        import time

        time.sleep(seconds)


class ManualClock:
    """A clock that only moves when a test tells it to.

    This is what makes 'inject a spot interruption at a random point in 100
    simulated jobs' a test that finishes in under a second.
    """

    def __init__(self, start: dt.datetime | None = None) -> None:
        if start is None:
            # Mid-month and mid-day, so tests that backdate a few weeks do not
            # accidentally land on a billing-period or timezone boundary.
            start = dt.datetime(2026, 1, 15, 12, 0, tzinfo=dt.timezone.utc)
        if start.tzinfo is None:
            raise ValueError("ManualClock needs a timezone-aware start time")
        self._now = start.astimezone(dt.timezone.utc)

    def now(self) -> dt.datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds=seconds)

    def advance(
        self, *, seconds: float = 0, minutes: float = 0, hours: float = 0, days: float = 0
    ) -> dt.datetime:
        """Move forward. Returns the new time so tests can read it inline."""
        delta = dt.timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)
        if delta.total_seconds() < 0:
            raise ValueError("clocks do not run backwards")
        self._now += delta
        return self._now


def to_iso(moment: dt.datetime) -> str:
    """Serialize for SQLite. Always UTC, always the same width."""
    return moment.astimezone(dt.timezone.utc).isoformat(timespec="microseconds")


def from_iso(text: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)

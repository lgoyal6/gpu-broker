"""The clock is injected so that every later phase can be simulated."""

from __future__ import annotations

import datetime as dt

import pytest

from gpu_broker.clock import ManualClock, SystemClock, from_iso, to_iso


def test_manual_clock_only_moves_when_told():
    clock = ManualClock()
    start = clock.now()
    assert clock.now() == start
    clock.advance(hours=4)
    assert (clock.now() - start) == dt.timedelta(hours=4)


def test_sleep_advances_a_manual_clock_instead_of_blocking():
    """`run_until_idle` calls `clock.sleep`. That has to be free in tests and
    real in production, or the scheduler loop needs two versions."""
    clock = ManualClock()
    start = clock.now()
    clock.sleep(3600)
    assert clock.now() - start == dt.timedelta(hours=1)


def test_clocks_do_not_run_backwards():
    with pytest.raises(ValueError):
        ManualClock().advance(hours=-1)


def test_naive_start_times_are_rejected():
    with pytest.raises(ValueError):
        ManualClock(dt.datetime(2026, 1, 1))


def test_system_clock_is_utc_and_aware():
    now = SystemClock().now()
    assert now.tzinfo is not None
    assert now.utcoffset() == dt.timedelta(0)


def test_iso_round_trip_is_lossless_and_fixed_width():
    moment = dt.datetime(2026, 3, 4, 5, 6, 7, 89, tzinfo=dt.timezone.utc)
    text = to_iso(moment)
    assert from_iso(text) == moment
    assert len(text) == len(to_iso(moment + dt.timedelta(seconds=1)))


def test_iso_ordering_matches_time_ordering():
    """Timestamps are compared as strings in SQL, so this has to hold."""
    clock = ManualClock()
    stamps = []
    for _ in range(5):
        stamps.append(to_iso(clock.now()))
        clock.advance(minutes=7)
    assert stamps == sorted(stamps)

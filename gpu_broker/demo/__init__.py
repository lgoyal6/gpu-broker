"""Seeded demo data, kept in its own database.

Nothing in here writes to a real broker. `seed()` takes a directory, builds a
broker there, and drives it with a fake backend and a simulated clock -- the
same code paths a real job takes, which is why the result is usable as an
integration fixture and not only as something to screenshot.
"""

from .scenario import SEEDED, SeedReport, seed

__all__ = ["SEEDED", "SeedReport", "seed"]

"""SQLite, configured for the thing this project cares most about being correct.

State durability is the standing constraint: the broker survives its own crash
without losing the queue, the ledger, or the user-to-resource mapping. Three
settings do most of that work, and each costs something we can afford at twenty
users:

  journal_mode=WAL     readers never block the writer, and a half-written
                       transaction is rolled back from the log on next open.
  synchronous=FULL     fsync on every commit. Slower than NORMAL, and the
                       difference between "committed" and "committed unless the
                       machine loses power in the next 200ms".
  foreign_keys=ON      off by default in SQLite, which surprises everyone.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

# Wait this long for another process's write lock before giving up. The CLI and
# a running scheduler loop are separate processes hitting the same file.
BUSY_TIMEOUT_MS = 5_000


def connect(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        path,
        # We manage transactions by hand via `transaction()`. Python's implicit
        # BEGIN would open one for us at unpredictable moments.
        isolation_level=None,
        timeout=BUSY_TIMEOUT_MS / 1000,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """One unit of durability.

    IMMEDIATE takes the write lock at BEGIN rather than at first write, so two
    processes contend at a predictable point instead of one of them failing
    halfway through with SQLITE_BUSY.

    Nesting is not supported on purpose: if a caller needs two of these to be
    atomic, that is one transaction and the code should say so.
    """
    if conn.in_transaction:
        raise RuntimeError(
            "nested transaction: this call is already inside one. "
            "Widen the outer transaction instead of opening a second."
        )
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")

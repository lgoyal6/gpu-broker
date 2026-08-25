"""Forward-only schema migrations.

Numbered `.sql` files in `migrations/`. Each runs exactly once, inside one
transaction with its own version bump, so an interrupted migration either
happened or did not.
"""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
from pathlib import Path

from ..errors import MigrationError

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_FILENAME = re.compile(r"^(\d{3})_([a-z0-9_]+)\.sql$")

_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL
)
"""


def discover() -> list[tuple[int, str, Path]]:
    """All migration files on disk, in order. Rejects gaps and duplicates."""
    found: list[tuple[int, str, Path]] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = _FILENAME.match(path.name)
        if not match:
            raise MigrationError(
                f"{path.name} does not look like a migration. "
                "Expected NNN_lower_snake_case.sql"
            )
        found.append((int(match.group(1)), match.group(2), path))

    versions = [version for version, _, _ in found]
    if len(versions) != len(set(versions)):
        raise MigrationError(f"duplicate migration numbers: {versions}")
    if versions and versions != list(range(1, len(versions) + 1)):
        raise MigrationError(f"migration numbers must be 1..N with no gaps, got {versions}")
    return found


LATEST_VERSION = len(discover())


def current_version(conn: sqlite3.Connection) -> int:
    conn.execute(_VERSION_TABLE)
    row = conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
    return row["v"] or 0


def migrate(conn: sqlite3.Connection) -> list[int]:
    """Bring the database up to LATEST_VERSION. Returns versions applied."""
    have = current_version(conn)
    if have > LATEST_VERSION:
        raise MigrationError(
            f"database is at schema version {have}, but this build only knows about "
            f"{LATEST_VERSION}. You are running an old broker against a new database."
        )

    applied: list[int] = []
    for version, name, path in discover():
        if version <= have:
            continue

        # The schema change and its version bump have to land together, or a
        # crash between them leaves a database that lies about its own shape.
        #
        # `executescript` implicitly commits any transaction that is already
        # open before it runs, so wrapping it in an outer BEGIN does nothing.
        # The BEGIN has to be inside the script.
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds")
        script = "\n".join(
            [
                "BEGIN IMMEDIATE;",
                path.read_text(),
                "INSERT INTO schema_migrations (version, name, applied_at) "
                f"VALUES ({version}, {_quote(name)}, {_quote(now)});",
                "COMMIT;",
            ]
        )
        try:
            conn.executescript(script)
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        applied.append(version)
    return applied


def _quote(text: str) -> str:
    """SQL string literal. `executescript` takes no parameters, so migration
    metadata is inlined; these values are filenames and timestamps we generate,
    but escaping them is one line and removes the question."""
    escaped = text.replace("'", "''")
    return f"'{escaped}'"

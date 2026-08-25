"""Schema migrations, and the durability pragmas that back them."""

from __future__ import annotations

import pytest

from gpu_broker.db import LATEST_VERSION, connect, current_version, migrate
from gpu_broker.db.connection import transaction
from gpu_broker.db.migrations import _VERSION_TABLE
from gpu_broker.errors import MigrationError


def test_migrate_brings_a_fresh_database_up_to_date(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    assert current_version(conn) == 0
    assert migrate(conn) == list(range(1, LATEST_VERSION + 1))
    assert current_version(conn) == LATEST_VERSION


def test_migrate_is_idempotent(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    assert migrate(conn) == []


def test_migrate_refuses_a_database_from_a_newer_build(tmp_path):
    """Running an old broker against a new database would corrupt it quietly."""
    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, 'future', '2030')",
        (LATEST_VERSION + 5,),
    )
    with pytest.raises(MigrationError, match="old broker against a new database"):
        migrate(conn)


def test_durability_pragmas_are_actually_set(tmp_path):
    """These are the settings the whole 'survives its own crash' claim rests on."""
    conn = connect(tmp_path / "b.sqlite3")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_foreign_keys_are_enforced(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn) as active:
            active.execute(
                "INSERT INTO jobs (job_id, user_id, command, gpu_type, requested_hours,"
                " currency, reserved, state, submitted_at, updated_at) "
                "VALUES ('j','ghost','x','a10g',1,'USD','1','QUEUED','2026','2026')"
            )


def test_a_failed_transaction_leaves_nothing_behind(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    with pytest.raises(RuntimeError):
        with transaction(conn) as active:
            active.execute(
                "INSERT INTO users (user_id, display_name, budget_usd, budget_gpu_hours,"
                " is_admin, created_at) VALUES ('ana','ana','25','8',0,'2026')"
            )
            raise RuntimeError("boom")
    assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0


def test_nested_transactions_are_refused(tmp_path):
    """Silently joining an outer transaction would make 'one transition, one
    commit' false without anything looking wrong."""
    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    with pytest.raises(RuntimeError, match="nested transaction"):
        with transaction(conn):
            with transaction(conn):
                pass
    # The outer block's own rollback already ran, so the connection is clean.
    assert not conn.in_transaction


def test_a_backend_handle_belongs_to_exactly_one_job(tmp_path):
    """Reconciliation cannot decide what an orphan is if two jobs claim one
    instance."""
    import sqlite3

    conn = connect(tmp_path / "b.sqlite3")
    migrate(conn)
    with transaction(conn) as active:
        active.execute(
            "INSERT INTO users (user_id, display_name, budget_usd, budget_gpu_hours,"
            " is_admin, created_at) VALUES ('ana','ana','25','8',0,'2026')"
        )
        for job_id in ("j1", "j2"):
            active.execute(
                "INSERT INTO jobs (job_id, user_id, command, gpu_type, requested_hours,"
                " currency, reserved, state, submitted_at, updated_at) "
                f"VALUES ('{job_id}','ana','x','a10g',1,'USD','1','QUEUED','2026','2026')"
            )
    with transaction(conn) as active:
        active.execute("UPDATE jobs SET backend='cloud', backend_handle='i-1' WHERE job_id='j1'")
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn) as active:
            active.execute("UPDATE jobs SET backend='cloud', backend_handle='i-1' WHERE job_id='j2'")


def test_a_phase_0_database_upgrades_in_place(tmp_path):
    """Somebody running the broker already has a database with jobs in it. The
    upgrade has to keep them, not start over."""
    from gpu_broker.db.migrations import discover

    conn = connect(tmp_path / "b.sqlite3")
    first, name, path = discover()[0]

    # Bring it up to version 1 only, the way a Phase 0 install would be.
    conn.executescript(path.read_text())
    conn.execute(_VERSION_TABLE)
    conn.execute(
        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, '2026-01-01')",
        (first, name),
    )
    with transaction(conn) as active:
        active.execute(
            "INSERT INTO users (user_id, display_name, budget_usd, budget_gpu_hours,"
            " is_admin, created_at) VALUES ('ana','ana','25','8',0,'2026')"
        )
        active.execute(
            "INSERT INTO jobs (job_id, user_id, command, gpu_type, requested_hours,"
            " currency, reserved, state, submitted_at, updated_at) "
            "VALUES ('old','ana','python train.py','a10g',4,'USD','12','RUNNING','2026','2026')"
        )

    applied = migrate(conn)

    assert applied == list(range(first + 1, LATEST_VERSION + 1)), "nothing was upgraded"
    row = conn.execute("SELECT * FROM jobs WHERE job_id = 'old'").fetchone()
    assert row["command"] == "python train.py", "the existing job was lost"
    assert row["logs_fetched"] == 0, "the new column has no sane default"
    assert conn.execute("SELECT COUNT(*) FROM host_drains").fetchone()[0] == 0


def test_the_version_table_helper_is_importable():
    """`_VERSION_TABLE` is used by the upgrade test above; keep it real."""
    from gpu_broker.db.migrations import _VERSION_TABLE

    assert "schema_migrations" in _VERSION_TABLE

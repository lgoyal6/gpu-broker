"""`gpu doctor`: does it find the real problem, and say what to do about it.

Every assertion here is about the *message*. A diagnostic that reports
`AccessDenied` and stops has told you something is broken, which you already
knew.
"""

from __future__ import annotations

from dataclasses import replace


from gpu_broker.doctor import FAIL, OK, WARN, diagnose


def named(checks, name):
    for check in checks:
        if check.name == name:
            return check
    raise AssertionError(f"no check called {name!r}. Saw: {[c.name for c in checks]}")


def test_a_healthy_default_install_is_clean(broker):
    checks = diagnose(broker)
    assert not [c for c in checks if c.bad]
    assert named(checks, "database").status == OK
    assert named(checks, "config").status == OK


def test_it_says_the_simulator_needs_no_aws(broker):
    assert "no AWS account" in named(diagnose(broker), "backend: fake").detail


def test_it_reports_the_schema_version_and_durability_mode(broker):
    detail = named(diagnose(broker), "database").detail
    assert "WAL" in detail
    assert "schema v" in detail


def test_a_database_from_a_newer_build_is_a_failure(broker):
    from gpu_broker.db.migrations import LATEST_VERSION

    broker.store.conn.execute(
        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, 'future', '2030')",
        (LATEST_VERSION + 3,),
    )
    check = named(diagnose(broker), "database")
    assert check.status == FAIL
    assert "old binary" in check.fix


def test_an_unwritable_checkpoint_directory_is_found(broker, tmp_path):
    from gpu_broker.checkpoint import LocalCheckpointStore

    blocked = tmp_path / "blocked"
    blocked.write_text("this is a file, not a directory")
    broker.checkpoints = LocalCheckpointStore(blocked / "inside")

    check = named(diagnose(broker), "checkpoints")
    assert check.status == FAIL
    assert "cannot write" in check.detail


def test_spot_with_local_checkpoints_is_caught_before_it_bites(broker):
    """Rather than discovering it the first time somebody is preempted."""
    broker.config = replace(broker.config, backends=("fake", "ec2-spot"))
    check = named(diagnose(broker), "checkpoints")
    assert check.status == FAIL
    assert "checkpoints are on local disk" in check.detail
    assert "comes back on a different machine" in check.fix
    assert "checkpoint_store to 's3'" in check.fix


def test_no_oauth_app_is_a_warning_with_the_url(broker):
    check = named(diagnose(broker), "web")
    assert check.status == WARN
    assert "github.com/settings/developers" in check.fix


def test_dev_login_is_called_out_as_a_local_only_thing(broker):
    from gpu_broker.web.auth import WebConfig

    broker.config = replace(broker.config, web=WebConfig(dev_login="ana"))
    check = named(diagnose(broker), "web")
    assert check.status == WARN
    assert "never in production" in check.fix


def test_an_oauth_app_with_nobody_allowed_is_a_failure(broker):
    from gpu_broker.web.auth import WebConfig

    broker.config = replace(broker.config, web=WebConfig(github_client_id="cid"))
    check = named(diagnose(broker), "web")
    assert check.status == FAIL
    assert "nobody can sign in" in check.detail


def test_a_missing_client_secret_names_the_variable(broker, monkeypatch):
    from gpu_broker.web.auth import WebConfig

    monkeypatch.delenv("GPU_BROKER_GITHUB_CLIENT_SECRET", raising=False)
    broker.config = replace(
        broker.config, web=WebConfig(github_client_id="cid", allowlist=("ana",))
    )
    check = named(diagnose(broker), "web")
    assert check.status == FAIL
    assert "GPU_BROKER_GITHUB_CLIENT_SECRET" in check.fix


def test_a_broken_lab_host_is_reported_by_name(broker, gpu_host, clock, transport):
    from gpu_broker.backends.local import LocalBackend
    from gpu_broker.local.hosts import LocalConfig

    gpu_host.nvidia_smi_works = False
    backend = LocalBackend(
        clock=clock,
        config=LocalConfig(hosts=(gpu_host.server.spec(),), command_timeout_seconds=3.0),
        transport=transport,
    )
    broker.scheduler.backends = [backend]
    broker.config = replace(broker.config, backends=("local",))

    check = named(diagnose(broker), f"lab: {gpu_host.server.spec().hostname}")
    assert check.status == FAIL
    assert "nvidia-smi failed" in check.detail
    assert "gpu hosts" in check.fix


def test_a_check_that_explodes_is_reported_not_propagated(broker, monkeypatch):
    """A diagnostic that crashes is useless exactly when it is needed."""
    import gpu_broker.doctor as doctor

    def boom(config):
        raise RuntimeError("everything is on fire")

    monkeypatch.setattr(doctor, "_config", boom)
    check = named(diagnose(broker), "config")
    assert check.status == FAIL
    assert "everything is on fire" in check.detail


def test_the_cli_exits_nonzero_when_something_is_broken(tmp_path, monkeypatch):
    """So it can gate a deploy without parsing output."""
    import json

    from typer.testing import CliRunner

    from gpu_broker.cli import app

    state = tmp_path / "broker"
    state.mkdir(parents=True)
    (state / "config.json").write_text(
        json.dumps({"backends": ["fake"], "web": {"github_client_id": "cid"}})
    )
    monkeypatch.setenv("GPU_BROKER_HOME", str(state))

    result = CliRunner().invoke(app, ["doctor"], catch_exceptions=False)
    assert result.exit_code == 1
    assert "need fixing" in result.output


def test_the_cli_exits_zero_on_a_clean_install(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_BROKER_HOME", str(tmp_path / "broker"))
    from typer.testing import CliRunner

    from gpu_broker.cli import app

    result = CliRunner().invoke(app, ["doctor"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "everything checks out" in result.output

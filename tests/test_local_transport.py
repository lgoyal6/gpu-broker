"""The SSH layer, against a real in-process SSH server."""

from __future__ import annotations

import pytest

from gpu_broker.errors import BackendError
from gpu_broker.local.transport import HostSpec, SshTransport


def test_a_command_runs_and_returns_its_output(transport, gpu_host):
    result = transport.run(gpu_host.server.spec(), "echo ok")
    assert result.ok
    assert result.stdout.strip() == "ok"


def test_a_nonzero_exit_is_reported_not_raised(transport, gpu_host):
    """Plenty of commands are expected to fail -- `systemctl is-active` on a
    finished unit, for one. The caller decides what a failure means."""
    result = transport.run(gpu_host.server.spec(), "nonsense-binary")
    assert not result.ok
    assert result.exit_status == 127
    assert "not found" in result.stderr


def test_require_turns_a_failure_into_a_readable_error(transport, gpu_host):
    result = transport.run(gpu_host.server.spec(), "nonsense-binary")
    with pytest.raises(BackendError, match="starting the thing failed"):
        result.require("starting the thing")


def test_the_connection_is_reused_across_commands(transport, gpu_host):
    """Every tick polls every running job. A handshake per poll per job is most
    of what the broker would be doing."""
    spec = gpu_host.server.spec()
    for _ in range(20):
        assert transport.run(spec, "echo ok").ok
    assert len(transport._connections) == 1


def test_an_unreachable_host_raises_something_that_names_it(transport):
    unreachable = HostSpec(hostname="127.0.0.1", username="broker", port=1)
    with pytest.raises(BackendError, match="127.0.0.1"):
        transport.run(unreachable, "echo ok")


def test_a_dead_pooled_connection_is_retried_once(transport, gpu_host):
    """A connection that died between ticks looks like an arbitrary failure on
    first use. One reconnect is the difference between a transient blip and a
    host being marked unhealthy."""
    spec = gpu_host.server.spec()
    assert transport.run(spec, "echo ok").ok

    stale = next(iter(transport._connections.values()))
    stale.abort()

    assert transport.run(spec, "echo ok").ok, "the transport did not recover"


def test_closing_is_idempotent(gpu_host):
    made = SshTransport()
    made.run(gpu_host.server.spec(), "echo ok")
    made.close()
    made.close()
    with pytest.raises(BackendError, match="closed"):
        made.run(gpu_host.server.spec(), "echo ok")


def test_a_command_that_hangs_times_out_with_its_own_name(transport, gpu_host, monkeypatch):
    def never_returns(command):
        import time

        time.sleep(2)  # long enough to blow the timeout, short enough to tear down
        return 0, "", ""

    monkeypatch.setattr(gpu_host, "handle", never_returns)
    with pytest.raises(BackendError, match="timed out"):
        transport.run(gpu_host.server.spec(), "sleep forever", timeout=0.5)

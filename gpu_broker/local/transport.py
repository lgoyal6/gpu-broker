"""Running commands on a remote host, behind an interface.

`asyncssh` is asynchronous and the rest of the broker is not, so this module owns
the bridge: one event loop on a background thread, connections pooled per host,
and a plain blocking `run()` for callers.

Two things here are deliberate and easy to get wrong.

*Connections are reused.* Every tick polls every running job. Opening a fresh SSH
connection for each poll means a full handshake per job per tick, which on a busy
lab machine is most of what the broker would be doing.

*Environment goes in the command string, never through the SSH channel.*
`asyncssh` can send env vars, but real `sshd` refuses them unless the host's
`AcceptEnv` allows it, and the default allows only `LANG` and `LC_*`. A variable
that silently fails to arrive is how a job ends up with no GPU memory limit. So
callers build `VAR=value cmd` themselves and this layer never touches env.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ..errors import BackendError


@dataclass(frozen=True)
class HostSpec:
    """How to reach one machine."""

    hostname: str
    username: str = "broker"
    port: int = 22
    client_key: str | None = None
    """Path to a private key. None means whatever the SSH agent offers."""
    known_hosts: str | None = None
    """Path to a known_hosts file. None disables host key checking, which is
    only ever appropriate against a test server."""

    @property
    def name(self) -> str:
        return self.hostname


@dataclass(frozen=True)
class CommandResult:
    exit_status: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.exit_status == 0

    def require(self, what: str) -> "CommandResult":
        if not self.ok:
            detail = (self.stderr or self.stdout).strip().splitlines()
            first = detail[0] if detail else f"exit {self.exit_status}"
            raise BackendError(f"{what} failed: {first}")
        return self


@runtime_checkable
class Transport(Protocol):
    """Run a shell command on a host and wait for it to finish."""

    def run(self, host: HostSpec, command: str, timeout: float | None = None) -> CommandResult: ...

    def close(self) -> None: ...


class SshTransport:
    """Real SSH, with one background event loop and pooled connections."""

    def __init__(self, connect_timeout: float = 15.0, command_timeout: float = 60.0) -> None:
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="gpu-broker-ssh", daemon=True
        )
        self._thread.start()
        self._connections: dict[str, object] = {}
        self._lock = threading.Lock()
        self._closed = False

    # ------------------------------------------------------------ Transport

    def run(
        self, host: HostSpec, command: str, timeout: float | None = None
    ) -> CommandResult:
        if self._closed:
            raise BackendError("transport is closed")
        limit = timeout or self.command_timeout
        future = asyncio.run_coroutine_threadsafe(
            self._run(host, command, limit), self._loop
        )
        try:
            # A little longer than the command's own timeout, so the inner
            # timeout is what fires and the error names the command.
            return future.result(timeout=limit + self.connect_timeout + 5)
        except TimeoutError as exc:
            future.cancel()
            raise BackendError(
                f"{host.name}: no response within {limit:.0f}s running: {command[:80]}"
            ) from exc

    async def _run(self, host: HostSpec, command: str, timeout: float) -> CommandResult:
        for attempt in (1, 2):
            connection = await self._connect(host, reconnect=attempt == 2)
            try:
                result = await asyncio.wait_for(
                    connection.run(command, check=False), timeout=timeout
                )
            except asyncio.TimeoutError:
                raise BackendError(
                    f"{host.name}: command timed out after {timeout:.0f}s: {command[:80]}"
                ) from None
            except Exception as exc:  # noqa: BLE001
                # A pooled connection that died between ticks looks like an
                # arbitrary failure on first use. Drop it and try once more
                # before deciding the host is unhealthy.
                self._forget(host)
                if attempt == 2:
                    raise BackendError(f"{host.name}: {exc}") from exc
                continue
            return CommandResult(
                exit_status=result.exit_status if result.exit_status is not None else -1,
                stdout=_text(result.stdout),
                stderr=_text(result.stderr),
            )
        raise BackendError(f"{host.name}: unreachable")  # pragma: no cover

    async def _connect(self, host: HostSpec, reconnect: bool = False):
        key = _key(host)
        if reconnect:
            self._forget(host)
        existing = self._connections.get(key)
        if existing is not None:
            return existing

        import asyncssh

        options: dict[str, object] = {
            "username": host.username,
            "port": host.port,
            "known_hosts": host.known_hosts,
        }
        if host.client_key:
            options["client_keys"] = [host.client_key]
        try:
            connection = await asyncio.wait_for(
                asyncssh.connect(host.hostname, **options), timeout=self.connect_timeout
            )
        except asyncio.TimeoutError:
            raise BackendError(
                f"{host.name}: no SSH response within {self.connect_timeout:.0f}s"
            ) from None
        except Exception as exc:  # noqa: BLE001
            raise BackendError(f"{host.name}: cannot connect: {exc}") from exc

        self._connections[key] = connection
        return connection

    def _forget(self, host: HostSpec) -> None:
        connection = self._connections.pop(_key(host), None)
        if connection is not None:
            try:
                connection.close()
            except Exception:  # noqa: BLE001
                pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for connection in list(self._connections.values()):
            try:
                connection.close()
            except Exception:  # noqa: BLE001
                pass
        self._connections.clear()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()


def _key(host: HostSpec) -> str:
    return f"{host.username}@{host.hostname}:{host.port}"


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")

"""Thin wrappers around moto's clients.

moto is the right tool for this backend: it answers the real EC2 and SSM APIs,
so the tests check that the broker calls AWS correctly rather than checking that
it calls a fake written from the same assumptions as the code. It notably
honours `DryRun`, which is most of what Phase 1 needs to prove.

It has two gaps the broker depends on, and one thing it cannot know:

  `describe_instance_information` is not implemented. That call is how the
  broker decides an instance is usable, so `SsmDouble` supplies it and lets a
  test say when the agent registers.

  `cancel_command` is not implemented. It raises `NotImplementedError`, which is
  itself a useful test: a failing cancel must never stop the terminate under it.

  It does not run anything, so an invocation is `Success` the instant it is sent.
  `SsmDouble.force` makes a command sit in a chosen state, which is what a real
  training run does for four hours.

Everything else delegates straight through to moto.
"""

from __future__ import annotations

from typing import Any


class _Delegating:
    """Passes through to the wrapped client and records every call."""

    def __init__(self, real: Any) -> None:
        self._real = real
        self.calls: list[tuple[str, dict]] = []

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._real, name)
        if not callable(attr):
            return attr

        def wrapper(*args, **kwargs):
            self.calls.append((name, kwargs))
            return attr(*args, **kwargs)

        return wrapper

    def kwargs_for(self, operation: str) -> dict:
        """The parameters of the last call to `operation`."""
        for name, kwargs in reversed(self.calls):
            if name == operation:
                return kwargs
        raise AssertionError(f"{operation} was never called. Saw: {[n for n, _ in self.calls]}")

    def called(self, operation: str) -> int:
        return sum(1 for name, _ in self.calls if name == operation)


class Ec2Double(_Delegating):
    pass


class SsmDouble(_Delegating):
    def __init__(self, real: Any) -> None:
        super().__init__(real)
        self.online: set[str] = set()
        """Instances whose SSM agent has registered. Tests add to this to say
        'the machine finished booting'."""
        self.forced: dict[str, dict] = {}
        self.cancelled: list[str] = []
        self.describe_error: Exception | None = None

    def sends(self, sampler: bool = False) -> list[dict]:
        """The `send_command` calls, split by what they are for.

        Two commands go to each instance: the user's job, and a loop that
        watches the GPU. Counting them together would hide the property that
        actually matters, which is that the *job* is sent exactly once.
        """
        out = []
        for name, kwargs in self.calls:
            if name != "send_command":
                continue
            is_sampler = "sampler" in kwargs.get("Comment", "")
            if is_sampler == sampler:
                out.append(kwargs)
        return out

    def job_send(self) -> dict:
        sends = self.sends()
        assert sends, "the job command was never sent"
        return sends[-1]

    def register(self, instance_id: str) -> None:
        self.online.add(instance_id)

    def force(self, command_id: str, status: str, response_code: int | None = None, stderr: str = "") -> None:
        """Pin an invocation's status, the way a long-running job would."""
        self.forced[command_id] = {
            "Status": status,
            "ResponseCode": response_code,
            "StandardErrorContent": stderr,
        }

    def describe_instance_information(self, **kwargs) -> dict:
        self.calls.append(("describe_instance_information", kwargs))
        if self.describe_error is not None:
            raise self.describe_error
        wanted: list[str] = []
        for filt in kwargs.get("Filters", []):
            if filt.get("Key") == "InstanceIds":
                wanted.extend(filt.get("Values", []))
        return {
            "InstanceInformationList": [
                {"InstanceId": instance_id, "PingStatus": "Online"}
                for instance_id in wanted
                if instance_id in self.online
            ]
        }

    def cancel_command(self, **kwargs) -> dict:
        self.calls.append(("cancel_command", kwargs))
        self.cancelled.append(kwargs["CommandId"])
        return {}

    def get_command_invocation(self, **kwargs) -> dict:
        self.calls.append(("get_command_invocation", kwargs))
        result = self._real.get_command_invocation(**kwargs)
        override = self.forced.get(kwargs["CommandId"])
        if override:
            result = {**result, **{k: v for k, v in override.items() if v is not None}}
        return result


class LogsDouble(_Delegating):
    """CloudWatch, plus a way to put lines in a stream.

    SSM writes command output to CloudWatch on the instance's behalf, which moto
    has no way to simulate. `emit` does what the agent would have done.
    """

    def emit(self, log_group: str, stream: str, lines: list[str]) -> None:
        try:
            self._real.create_log_group(logGroupName=log_group)
        except Exception:  # noqa: BLE001 - already exists
            pass
        try:
            self._real.create_log_stream(logGroupName=log_group, logStreamName=stream)
        except Exception:  # noqa: BLE001 - already exists
            pass
        existing = len(
            self._real.get_log_events(
                logGroupName=log_group, logStreamName=stream, startFromHead=True
            )["events"]
        )
        # Wall clock, not the injected one: CloudWatch rejects any event older
        # than 14 days or more than 2 hours ahead, and moto enforces it. The
        # broker never reads these timestamps, only the order.
        import time

        base = int(time.time() * 1000)
        self._real.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": base + (existing + i), "message": line}
                for i, line in enumerate(lines)
            ],
        )

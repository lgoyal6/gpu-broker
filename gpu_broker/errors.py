"""Every error the broker raises on purpose.

Anything not in here is a bug, not a policy decision.
"""

from __future__ import annotations


class BrokerError(Exception):
    """Base class. Catch this at the CLI boundary and print `str(e)`."""


class ConfigError(BrokerError):
    """The config file is missing something or contradicts itself."""


class MigrationError(BrokerError):
    """The database is at a version this build does not understand."""


class IllegalTransition(BrokerError):
    """A job was asked to move between two states with no legal edge."""

    def __init__(self, job_id: str, from_state: str, to_state: str) -> None:
        super().__init__(
            f"job {job_id}: {from_state} -> {to_state} is not a legal transition"
        )
        self.job_id = job_id
        self.from_state = from_state
        self.to_state = to_state


class UnknownJob(BrokerError):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"no job with id {job_id!r}")
        self.job_id = job_id


class UnknownUser(BrokerError):
    def __init__(self, user_id: str) -> None:
        super().__init__(
            f"no user {user_id!r}. Add them with: gpu admin add-user {user_id}"
        )
        self.user_id = user_id


class UnknownGpuType(BrokerError):
    def __init__(self, gpu_type: str, known: list[str]) -> None:
        super().__init__(
            f"unknown gpu type {gpu_type!r}. Known types: {', '.join(sorted(known))}"
        )
        self.gpu_type = gpu_type


class BackendError(BrokerError):
    """A backend failed in a way the broker did not cause and cannot fix."""

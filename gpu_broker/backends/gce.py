"""Google Compute Engine capacity adapter.

This adapter deliberately supports on-demand VM lifecycle only. It uses the
official Compute Engine SDK for VM control and the existing SSH transport for
job execution. It does not provide SSM, CloudWatch, spot notices, or checkpoint
upload parity with EC2; those differences stay visible in configuration and
runtime behavior.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from typing import Any

from ..clock import Clock, to_iso
from ..errors import BackendError, ConfigError
from ..local.transport import HostSpec, Transport
from ..models import Job
from ..money import Currency
from .base import (
    Allocation,
    BackendStatus,
    Observation,
    Resource,
    UtilizationSample,
    tags_for,
)


@dataclass(frozen=True)
class GceConfig:
    """Explicit GCE inputs; credentials come from Google ADC, never config."""

    project: str = ""
    zone: str = "us-central1-a"
    boot_image: str = ""
    network: str = "global/networks/default"
    subnetwork: str = ""
    external_ip: bool = False
    """Opt in when the broker cannot SSH over a private VPC path."""
    service_account: str = ""
    service_account_scopes: tuple[str, ...] = (
        "https://www.googleapis.com/auth/cloud-platform",
    )
    ssh_username: str = "gpu-broker"
    ssh_private_key: str = ""
    known_hosts: str = ""
    ssh_timeout_seconds: float = 30.0
    machine_types: dict[str, str] = field(default_factory=dict)
    accelerator_types: dict[str, str] = field(default_factory=dict)
    accelerator_counts: dict[str, int] = field(default_factory=dict)
    max_instances: dict[str, int] = field(default_factory=dict)

    def require_launchable(self) -> None:
        missing = [
            name
            for name, value in (
                ("gce.project", self.project),
                ("gce.boot_image", self.boot_image),
                ("gce.ssh_private_key", self.ssh_private_key),
                ("gce.known_hosts", self.known_hosts),
            )
            if not value
        ]
        if missing:
            raise ConfigError(
                "cannot launch GCE: " + ", ".join(missing) + " must be set"
            )
        if not self.machine_types:
            raise ConfigError(
                "cannot launch GCE: gce.machine_types must map at least one GPU type"
            )
        missing_limits = set(self.machine_types) - set(self.max_instances)
        if missing_limits:
            raise ConfigError(
                "gce.max_instances is missing " + ", ".join(sorted(missing_limits))
            )
        missing_accelerators = set(self.machine_types) - set(self.accelerator_types)
        if missing_accelerators:
            raise ConfigError(
                "gce.accelerator_types is missing "
                + ", ".join(sorted(missing_accelerators))
            )
        if any(not machine_type for machine_type in self.machine_types.values()):
            raise ConfigError("gce.machine_types values cannot be empty")
        if any(not name for name in self.accelerator_types.values()):
            raise ConfigError("gce.accelerator_types values cannot be empty")
        if any(
            not isinstance(limit, int) or limit < 0
            for limit in self.max_instances.values()
        ):
            raise ConfigError("gce.max_instances values must be non-negative integers")
        if any(
            not isinstance(count, int) or count < 1
            for count in self.accelerator_counts.values()
        ):
            raise ConfigError("gce.accelerator_counts values must be positive integers")


class GceBackend:
    """On-demand GCE VMs with SSH-based execution and explicit capabilities."""

    name = "gce"
    currency = Currency.USD
    tier = "ondemand"

    def __init__(
        self,
        *,
        clock: Clock,
        config: GceConfig,
        instances: Any,
        transport: Transport,
    ) -> None:
        self.clock = clock
        self.config = config
        self.instances = instances
        self.transport = transport
        self._deleted_logs: dict[str, list[tuple[str, str]]] = {}

    def supports(self, gpu_type: str) -> bool:
        return gpu_type in self.config.machine_types

    def free_slots(self, gpu_type: str) -> int:
        if not self.supports(gpu_type):
            return 0
        return max(
            0,
            self.config.max_instances.get(gpu_type, 0)
            - sum(
                1
                for r in self.list_resources()
                if r.gpu_type == gpu_type
                and r.status
                not in (
                    BackendStatus.GONE,
                    BackendStatus.COMPLETED,
                    BackendStatus.FAILED,
                    BackendStatus.INTERRUPTED,
                )
            ),
        )

    def launch(self, job: Job) -> Allocation:
        self.config.require_launchable()
        if job.resumable or job.environment:
            raise BackendError(
                "GCE adapter does not support broker checkpoints or named container environments"
            )
        if not self.supports(job.gpu_type):
            raise BackendError(f"GCE has no {job.gpu_type} machine type configured")
        if self.free_slots(job.gpu_type) < 1:
            raise BackendError(f"GCE has no free {job.gpu_type} slots")
        name = _instance_name(job.job_id)
        labels = {
            "broker-job-id": _label(job.job_id),
            "broker-user": _label(job.user_id),
            "broker-gpu-type": _label(job.gpu_type),
            "broker-launched-at": "t-"
            + self.clock.now().strftime("%Y%m%d%H%M%S").lower(),
        }
        instance = {
            "name": name,
            "machine_type": f"zones/{self.config.zone}/machineTypes/{self.config.machine_types[job.gpu_type]}",
            "guest_accelerators": [
                {
                    "accelerator_type": (
                        f"zones/{self.config.zone}/acceleratorTypes/"
                        f"{self.config.accelerator_types[job.gpu_type]}"
                    ),
                    "accelerator_count": self.config.accelerator_counts.get(
                        job.gpu_type, 1
                    ),
                }
            ],
            "scheduling": {"on_host_maintenance": "TERMINATE", "automatic_restart": True},
            "labels": labels,
            "metadata": {
                "items": [
                    {"key": "broker-job-id", "value": job.job_id},
                    {"key": "broker-user", "value": job.user_id},
                ]
            },
            "disks": [
                {
                    "boot": True,
                    "auto_delete": True,
                    "initialize_params": {"source_image": self.config.boot_image},
                }
            ],
            "network_interfaces": [
                {
                    "network": self.config.network,
                    **(
                        {"subnetwork": self.config.subnetwork}
                        if self.config.subnetwork
                        else {}
                    ),
                    **(
                        {
                            "access_configs": [
                                {"name": "External NAT", "type_": "ONE_TO_ONE_NAT"}
                            ]
                        }
                        if self.config.external_ip
                        else {}
                    ),
                }
            ],
            "service_accounts": (
                [
                    {
                        "email": self.config.service_account,
                        "scopes": list(self.config.service_account_scopes),
                    }
                ]
                if self.config.service_account
                else []
            ),
        }
        try:
            from google.cloud import compute_v1

            # The official SDK accepts an Instance message, not an arbitrary
            # JSON-shaped dict. Constructing it here also catches bad fields
            # before any provider request can be sent.
            instance_resource = compute_v1.Instance(**instance)
            self.instances.insert(
                project=self.config.project,
                zone=self.config.zone,
                instance_resource=instance_resource,
            ).result(timeout=300)
        except Exception as exc:  # SDK exceptions vary by provider response.
            raise BackendError(f"could not create GCE instance: {exc}") from exc
        return Allocation(
            handle=name,
            gpu_type=job.gpu_type,
            tags=tags_for(job, to_iso(self.clock.now())),
        )

    def start(self, handle: str, job: Job) -> None:
        """Prepare and dispatch under an atomic remote claim."""
        host = self._host(handle)
        root = f"/tmp/gpu-broker/jobs/{_safe_id(job.job_id)}"
        command_file = f"{root}/command.sh"
        output_file = f"{root}/output.log"
        status_file = f"{root}/exit-code"
        runner = (
            f"bash {shlex.quote(command_file)}; rc=$?; "
            f"printf '%s\\n' \"$rc\" > {shlex.quote(status_file + '.tmp')} && "
            f"mv {shlex.quote(status_file + '.tmp')} {shlex.quote(status_file)}"
        )
        script = (
            f"mkdir -p {shlex.quote(root)} || exit; "
            f"if mkdir {shlex.quote(root + '/started')} 2>/dev/null; then "
            f"if ! printf '%s\\n' {shlex.quote(job.command)} > {shlex.quote(command_file)}; then "
            f"rmdir {shlex.quote(root + '/started')}; exit 1; fi; "
            f"nohup bash -lc {shlex.quote(runner)} >> {shlex.quote(output_file)} 2>&1 </dev/null & "
            f"else test -d {shlex.quote(root + '/started')}; fi"
        )
        self._run(host, script, "start GCE job")

    def poll(self, handle: str) -> Observation:
        instance = self._get(handle)
        status = _field(instance, "status", "")
        if status in ("DELETING", "TERMINATED", "STOPPING", "STOPPED"):
            return Observation(BackendStatus.GONE, detail=f"GCE instance is {status}")
        if status != "RUNNING":
            return Observation(
                BackendStatus.PENDING, detail=f"GCE instance is {status or 'unknown'}"
            )
        host = self._host_from_instance(instance)
        job_id = _safe_id(_metadata(instance).get("broker-job-id", "unknown"))
        root = f"/tmp/gpu-broker/jobs/{job_id}"
        try:
            result = self._run(
                host,
                f"if test -f {shlex.quote(root + '/exit-code')}; then cat {shlex.quote(root + '/exit-code')}; elif test -d {shlex.quote(root + '/started')}; then echo RUNNING; else echo READY; fi",
                "poll GCE job",
            )
        except BackendError as exc:
            return Observation(BackendStatus.PENDING, detail=f"waiting for SSH: {exc}")
        marker = result.stdout.strip()
        if marker == "READY":
            return Observation(BackendStatus.READY)
        if marker == "RUNNING":
            return Observation(BackendStatus.RUNNING)
        try:
            code = int(marker)
        except ValueError:
            raise BackendError(f"GCE job status is invalid: {marker[:80]}") from None
        return Observation(
            BackendStatus.COMPLETED if code == 0 else BackendStatus.FAILED,
            exit_code=code,
            detail=f"remote process exited {code}",
        )

    def validate_launch(self, job: Job) -> str:
        self.config.require_launchable()
        if job.resumable or job.environment:
            raise BackendError(
                "GCE adapter does not support broker checkpoints or named container environments"
            )
        if not self.supports(job.gpu_type):
            raise BackendError(f"GCE has no {job.gpu_type} machine type configured")
        if self.free_slots(job.gpu_type) < 1:
            raise BackendError(f"GCE has no free {job.gpu_type} slots")
        return (
            f"GCE request is locally configured for {self.config.machine_types[job.gpu_type]}; "
            "Compute Engine has no insert dry-run, so IAM/quota acceptance is unverified"
        )

    def validate_terminate(self, handle: str) -> str:
        instance = self._get(handle)
        if _field(instance, "status", "") == "TERMINATED":
            return "GCE instance is already terminated"
        return "GCE instance exists; delete permission is not provider-validated"

    def fetch_logs(self, handle: str, after: int = 0) -> list[tuple[str, str]]:
        cached = self._deleted_logs.get(handle)
        if cached is not None:
            return cached[after:]
        host = self._host(handle)
        output = f"/tmp/gpu-broker/jobs/{_safe_id(self._job_id(handle))}/output.log"
        result = self._run(
            host,
            f"if test -f {shlex.quote(output)}; then awk 'NR > {max(after, 0)}' {shlex.quote(output)}; fi",
            "read GCE job logs",
        )
        return [("combined", line) for line in result.stdout.splitlines()]

    def terminate(self, handle: str, reason: str) -> None:
        del reason
        log_error = None
        try:
            self._deleted_logs[handle] = self.fetch_logs(handle, 0)
        except BackendError as exc:
            log_error = str(exc)
            self._deleted_logs[handle] = [
                ("broker", f"final GCE log read failed: {exc}")
            ]
        try:
            self.instances.delete(
                project=self.config.project, zone=self.config.zone, instance=handle
            ).result(timeout=300)
        except Exception as exc:
            if "notFound" not in str(exc) and "404" not in str(exc):
                raise BackendError(
                    f"could not delete GCE instance {handle}: {exc}"
                ) from exc
        if log_error:
            return

    def list_resources(self) -> list[Resource]:
        try:
            response = self.instances.list(
                project=self.config.project,
                zone=self.config.zone,
                filter="labels.broker-job-id:*",
            )
            out = []
            for instance in response:
                labels = _labels(instance)
                if "broker-job-id" not in labels:
                    continue
                state = _field(instance, "status", "")
                status = (
                    BackendStatus.GONE
                    if state in ("DELETING", "TERMINATED", "STOPPING", "STOPPED")
                    else BackendStatus.RUNNING
                )
                out.append(
                    Resource(
                        _field(instance, "name", ""),
                        labels.get("broker-gpu-type", ""),
                        status,
                        {
                            "broker-job-id": _metadata(instance).get(
                                "broker-job-id", ""
                            ),
                            "broker-user": _metadata(instance).get("broker-user", ""),
                        },
                    )
                )
            return out
        except Exception as exc:
            raise BackendError(f"could not list GCE instances: {exc}") from exc

    def sample_utilization(self, handle: str) -> list[UtilizationSample]:
        host = self._host(handle)
        result = self._run(
            host,
            "date +%s; nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader,nounits",
            "sample GCE GPU utilization",
        )
        lines = result.stdout.splitlines()
        try:
            at = float(lines[0])
            utilization, memory = (
                int(value.strip()) for value in lines[1].split(",", 1)
            )
        except (IndexError, ValueError):
            raise BackendError(
                "GCE nvidia-smi returned an unsupported response"
            ) from None
        return [UtilizationSample(at, utilization, memory)]

    def close(self) -> None:
        self.transport.close()

    def _get(self, handle: str) -> Any:
        try:
            return self.instances.get(
                project=self.config.project, zone=self.config.zone, instance=handle
            )
        except Exception as exc:
            if "notFound" in str(exc) or "404" in str(exc):
                return {"status": "TERMINATED", "name": handle, "labels": {}}
            raise BackendError(
                f"could not inspect GCE instance {handle}: {exc}"
            ) from exc

    def _host(self, handle: str) -> HostSpec:
        return self._host_from_instance(self._get(handle))

    def _host_from_instance(self, instance: Any) -> HostSpec:
        try:
            iface = _field(instance, "network_interfaces", [])[0]
            access = _field(iface, "access_configs", [])
            address = (_field(access[0], "nat_i_p", "") if access else "") or _field(
                iface, "network_i_p", ""
            )
            if not address:
                raise ValueError("instance has no reachable network address")
            return HostSpec(
                address,
                username=self.config.ssh_username,
                client_key=self.config.ssh_private_key,
                known_hosts=self.config.known_hosts,
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise BackendError(f"cannot reach GCE instance: {exc}") from exc

    def _job_id(self, handle: str) -> str:
        return _metadata(self._get(handle)).get(
            "broker-job-id", handle.removeprefix("gpu-broker-")
        )

    def _run(self, host: HostSpec, command: str, action: str):
        try:
            result = self.transport.run(
                host, command, timeout=self.config.ssh_timeout_seconds
            )
        except Exception as exc:
            raise BackendError(f"{action} failed on {host.hostname}: {exc}") from exc
        if not result.ok:
            raise BackendError(
                f"{action} failed on {host.hostname}: {(result.stderr or result.stdout).strip()[:300]}"
            )
        return result


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _labels(instance: Any) -> dict[str, str]:
    labels = _field(instance, "labels", {}) or {}
    return dict(labels.items()) if hasattr(labels, "items") else {}


def _metadata(instance: Any) -> dict[str, str]:
    metadata = _field(instance, "metadata", {}) or {}
    items = _field(metadata, "items", []) or []
    return {_field(item, "key", ""): _field(item, "value", "") for item in items}


def _label(value: str) -> str:
    value = re.sub(r"[^a-z0-9_-]", "-", value.lower()).strip("-_")
    if not value or not value[0].isalpha():
        value = "x-" + value
    return value[:63]


def _safe_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9-]{1,63}", value):
        raise ConfigError(
            "job id must contain only letters, digits, and hyphens for GCE"
        )
    return value


def _instance_name(job_id: str) -> str:
    safe = _safe_id(job_id).lower()
    if len(safe) > 52:
        raise ConfigError("job id is too long for a GCE instance name")
    return "gpu-broker-" + safe

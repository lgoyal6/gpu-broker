"""Check the GCE adapter through typed SDK fixtures and local shell execution.

These controls cover backends/gce.py without cloud credentials or VM calls.
Shell probes execute real status branches instead of assuming their output.
"""
from __future__ import annotations

import subprocess
import shlex
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from google.cloud import compute_v1

from gpu_broker import broker as broker_module
from gpu_broker.backends.base import Backend, BackendStatus
from gpu_broker.backends.gce import GceBackend, GceConfig
from gpu_broker.broker import build_backends
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config, load_gce_config
from gpu_broker.errors import BackendError, ConfigError
from gpu_broker.local.transport import CommandResult
from gpu_broker.models import Job
from gpu_broker.money import Currency
from gpu_broker.states import JobState


class Operation:
    def __init__(self):
        self.waited = False

    def result(self, timeout=None):
        self.waited = True


class Instances:
    def __init__(self):
        self.items = {}
        self.inserted = []
        self.deleted = []
        self.insert_operation = Operation()
        self.insert_error = None

    def insert(self, **kwargs):
        self.inserted.append(kwargs)
        if self.insert_error is not None:
            raise self.insert_error
        resource = kwargs["instance_resource"]
        name = resource.name
        resource.status = "RUNNING"
        resource.network_interfaces = [
            compute_v1.NetworkInterface(network_i_p="10.0.0.8")
        ]
        self.items[name] = resource
        return self.insert_operation

    def get(self, **kwargs):
        return self.items[kwargs["instance"]]

    def list(self, **kwargs):
        return list(self.items.values())

    def delete(self, **kwargs):
        self.deleted.append(kwargs)
        self.items.pop(kwargs["instance"], None)
        return Operation()


class Transport:
    def __init__(self):
        self.commands = []
        self.responses = []

    def run(self, host, command, timeout=None):
        self.commands.append((host, command, timeout))
        response = self.responses.pop(0) if self.responses else ""
        return CommandResult(0, response, "")

    def close(self):
        pass


@pytest.fixture
def rig():
    config = GceConfig(
        project="test-project",
        zone="us-central1-a",
        boot_image="projects/test-project/global/images/gpu-image",
        network="global/networks/test",
        ssh_username="broker",
        ssh_private_key="/tmp/test-key",
        known_hosts="/tmp/known-hosts",
        machine_types={"t4": "n1-standard-4"},
        accelerator_types={"t4": "nvidia-tesla-t4"},
        max_instances={"t4": 2},
    )
    clock = ManualClock()
    instances = Instances()
    transport = Transport()
    return (
        GceBackend(
            clock=clock, config=config, instances=instances, transport=transport
        ),
        instances,
        transport,
    )


def job(*, resumable=False, environment=None):
    return Job(
        job_id="01234567-89ab-cdef-0123-456789abcdef",
        user_id="ana",
        command="python train.py",
        gpu_type="t4",
        requested_hours=1.0,
        currency=Currency.USD,
        reserved=Decimal("5.00"),
        state=JobState.ALLOCATING,
        submitted_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        checkpoint_key="existing" if resumable else None,
        environment=environment,
    )


def test_load_gce_config_checks_unknown_keys_and_sequence_fields():
    config = load_gce_config({"project": "p", "service_account_scopes": ["scope"]})
    assert config.project == "p"
    assert config.service_account_scopes == ("scope",)
    with pytest.raises(ConfigError, match="unknown keys under 'gce'"):
        load_gce_config({"credentials_file": "/secret/key.json"})


def test_broker_config_accepts_gce_backend_and_provider_config(tmp_path):
    config = load_config(
        tmp_path,
        {
            "backends": ["gce"],
            "gce": {
                "project": "test-project",
                "boot_image": "projects/p/global/images/i",
                "machine_types": {"t4": "n1-standard-4"},
                "accelerator_types": {"t4": "nvidia-tesla-t4"},
                "max_instances": {"t4": 1},
            },
        },
    )
    assert config.backends == ("gce",)
    assert config.gce.project == "test-project"
    with pytest.raises(ConfigError, match="unknown backend"):
        load_config(tmp_path / "unknown", {"backends": ["gcp"]})


def test_backend_builder_wires_official_gce_client_without_api_calls(
    tmp_path, monkeypatch
):
    client = Instances()
    transport = Transport()
    monkeypatch.setattr(compute_v1, "InstancesClient", lambda: client)
    monkeypatch.setattr(
        broker_module,
        "SshTransport",
        lambda **kwargs: transport,
    )
    config = load_config(
        tmp_path,
        {
            "backends": ["gce"],
            "gce": {
                "project": "test-project",
                "boot_image": "projects/p/global/images/i",
                "ssh_private_key": "/tmp/id_ed25519",
                "known_hosts": "/tmp/known_hosts",
                "machine_types": {"t4": "n1-standard-4"},
                "accelerator_types": {"t4": "nvidia-tesla-t4"},
                "max_instances": {"t4": 1},
            },
        },
    )
    backends = build_backends(config, ManualClock())
    assert len(backends) == 1
    assert isinstance(backends[0], Backend)
    assert backends[0].name == "gce"
    assert backends[0].instances is client
    assert backends[0].transport is transport
    assert client.inserted == []


def test_launch_uses_compute_instance_resource_and_waits_for_operation(rig):
    backend, instances, _ = rig
    allocation = backend.launch(job())
    assert allocation.handle == "gpu-broker-01234567-89ab-cdef-0123-456789abcdef"
    assert instances.insert_operation.waited
    request = instances.inserted[0]
    assert request["project"] == "test-project"
    resource = request["instance_resource"]
    assert resource.machine_type.endswith("/n1-standard-4")
    assert resource.guest_accelerators[0].accelerator_type.endswith("/nvidia-tesla-t4")
    assert resource.guest_accelerators[0].accelerator_count == 1
    assert resource.scheduling.on_host_maintenance == "TERMINATE"
    assert resource.scheduling.automatic_restart
    assert resource.disks[0].initialize_params.source_image.endswith("gpu-image")
    assert resource.labels["broker-job-id"].startswith("x-")
    assert resource.metadata.items[0].key == "broker-job-id"
    assert resource.metadata.items[0].value == job().job_id
    assert not resource.network_interfaces[0].access_configs


def test_launch_rejects_features_without_gce_parity(rig):
    backend, instances, _ = rig
    with pytest.raises(BackendError, match="does not support broker checkpoints"):
        backend.launch(job(resumable=True))
    with pytest.raises(BackendError, match="does not support broker checkpoints"):
        backend.launch(job(environment="pytorch"))
    assert instances.inserted == []


def test_gce_readiness_start_is_idempotent_and_status_is_observed(rig):
    backend, _, transport = rig
    allocation = backend.launch(job())
    transport.responses = ["READY", "", "RUNNING", "0"]
    assert backend.poll(allocation.handle).status is BackendStatus.READY
    backend.start(allocation.handle, job())
    assert (
        "mkdir /tmp/gpu-broker/jobs/01234567-89ab-cdef-0123-456789abcdef/started"
        in transport.commands[-1][1]
    )
    assert "nohup bash -lc" in transport.commands[-1][1]
    assert backend.poll(allocation.handle).status is BackendStatus.RUNNING
    completed = backend.poll(allocation.handle)
    assert completed.status is BackendStatus.COMPLETED
    assert completed.exit_code == 0


def test_gce_validation_is_explicit_about_missing_provider_dry_run(rig):
    backend, _, _ = rig
    result = backend.validate_launch(job())
    assert "no insert dry-run" in result
    assert "IAM/quota acceptance is unverified" in result


def test_compute_api_launch_failure_is_reported_without_fallback(rig):
    backend, instances, _ = rig
    instances.insert_error = RuntimeError("permission denied")
    with pytest.raises(
        BackendError, match="could not create GCE instance: permission denied"
    ):
        backend.launch(job())


def test_terminal_gce_instance_is_gone_without_ssh(rig):
    backend, instances, transport = rig
    allocation = backend.launch(job())
    instances.items[allocation.handle].status = "TERMINATED"
    assert backend.poll(allocation.handle).status is BackendStatus.GONE
    assert transport.commands == []


def test_gce_fetches_output_and_lists_tagged_resources(rig):
    backend, _, transport = rig
    allocation = backend.launch(job())
    resource = backend.list_resources()[0]
    assert resource.handle == allocation.handle
    assert resource.job_id == job().job_id
    transport.responses = ["epoch 2\n"]
    assert backend.fetch_logs(allocation.handle, after=1) == [("combined", "epoch 2")]


@pytest.mark.parametrize(
    ("marker", "expected"),
    [(None, BackendStatus.READY), ("started", BackendStatus.RUNNING),
     ("0", BackendStatus.COMPLETED), ("7", BackendStatus.FAILED)],
)
def test_poll_executes_status_shell(rig, tmp_path, marker, expected):
    backend, _, transport = rig
    allocation = backend.launch(job())
    root = tmp_path / job().job_id
    root.mkdir()
    if marker == "started":
        (root / "started").mkdir()
    elif marker is not None:
        (root / "exit-code").write_text(marker + "\n")

    def run_shell(host, command, timeout=None):
        # Substitute only the owned directory, then execute the actual shell.
        command = command.replace("/tmp/gpu-broker/jobs", str(tmp_path))
        result = subprocess.run(
            ["bash", "-c", command], check=False, capture_output=True, text=True, timeout=timeout
        )
        return CommandResult(result.returncode, result.stdout, result.stderr)

    transport.run = run_shell
    observed = backend.poll(allocation.handle)
    assert observed.status is expected, observed.detail


def test_gce_empty_status_is_not_running(rig):
    backend, _, transport = rig
    allocation = backend.launch(job())
    transport.responses = [""]
    with pytest.raises(BackendError, match="status is invalid"):
        backend.poll(allocation.handle)


@pytest.fixture
def shell_rig(rig, tmp_path):
    backend, instances, transport = rig

    def run_shell(host, command, timeout=None):
        command = command.replace("/tmp/gpu-broker/jobs", str(tmp_path))
        result = subprocess.run(
            ["bash", "-c", command], capture_output=True, text=True, timeout=timeout
        )
        return CommandResult(result.returncode, result.stdout, result.stderr)

    transport.run = run_shell
    effects = tmp_path / "effects"
    candidate = replace(job(), command=f"printf 'run\\n' >> {shlex.quote(str(effects))}")
    allocation = backend.launch(candidate)
    return backend, allocation, candidate, tmp_path / candidate.job_id, effects


def wait_for_shell_exit(root):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if (root / "exit-code").exists():
            return
        time.sleep(0.01)
    pytest.fail("local job shell did not record its exit")


def test_start_preparation_failure_releases_dispatch_claim(shell_rig):
    backend, allocation, candidate, root, effects = shell_rig
    root.mkdir()
    (root / "command.sh").mkdir()  # Real shell redirection failure.
    for _ in range(2):
        with pytest.raises(BackendError):
            backend.start(allocation.handle, candidate)
        assert not (root / "started").exists()
        assert backend.poll(allocation.handle).status is BackendStatus.READY
        assert not effects.exists()
    (root / "command.sh").rmdir()
    backend.start(allocation.handle, candidate)
    wait_for_shell_exit(root)
    assert effects.read_text().splitlines() == ["run"]
    assert backend.poll(allocation.handle).status is BackendStatus.COMPLETED


def test_parallel_start_shell_dispatches_command_once(shell_rig):
    backend, allocation, candidate, root, effects = shell_rig
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: backend.start(allocation.handle, candidate), range(8)))
    wait_for_shell_exit(root)
    backend.start(allocation.handle, candidate)  # Retry after completion.
    assert effects.read_text().splitlines() == ["run"]
    assert backend.poll(allocation.handle).status is BackendStatus.COMPLETED


@pytest.mark.parametrize("candidate_job", [job(resumable=True), job(environment="pytorch")])
def test_validate_launch_rejects_unsupported_capabilities(rig, candidate_job):
    backend, instances, _ = rig
    with pytest.raises(BackendError, match="does not support broker checkpoints"):
        backend.validate_launch(candidate_job)
    assert instances.inserted == []

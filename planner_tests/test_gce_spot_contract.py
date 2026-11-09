from __future__ import annotations

import base64

import pytest
from datetime import UTC, datetime
from decimal import Decimal
from google.cloud import compute_v1

from gpu_broker.backends.gce import GceConfig
from gpu_broker.backends.base import BackendStatus
from gpu_broker.backends.gce import GceBackend
from gpu_broker.clock import ManualClock
from gpu_broker.errors import ConfigError
from gpu_broker.gcp import GCE_SPOT_RUNNER, gce_spot_job_script
from gpu_broker.local.transport import CommandResult
from gpu_broker.models import Job
from gpu_broker.money import Currency
from gpu_broker.states import JobState


class Operation:
    def result(self, timeout=None):
        return None


class Instances:
    def __init__(self):
        self.items = {}
        self.inserted = []

    def list(self, **kwargs):
        return list(self.items.values())

    def insert(self, **kwargs):
        resource = kwargs["instance_resource"]
        resource.status = "PROVISIONING"
        resource.network_interfaces = [compute_v1.NetworkInterface(network_i_p="10.0.0.8")]
        self.items[resource.name] = resource
        self.inserted.append(resource)
        return Operation()

    def get(self, **kwargs):
        return self.items[kwargs["instance"]]


class Transport:
    def __init__(self):
        self.commands = []

    def run(self, host, command, timeout=None):
        self.commands.append(command)
        return CommandResult(0, "", "")

    def close(self):
        pass


def test_typed_gce_spot_vm_and_guest_start_are_offline_contracts() -> None:
    config = GceConfig(
        project="test-project", zone="us-central1-a", boot_image="projects/test/images/gpu",
        ssh_private_key="key", known_hosts="known", machine_types={"t4": "n1-standard-4"},
        accelerator_types={"t4": "nvidia-tesla-t4"}, max_instances={"t4": 1},
        spot=True, checkpoint_bucket="checkpoint-bucket", workload_revision="code@sha256:abc",
        model_revision="model@sha256:def", service_account="gce-worker@test-project.iam.gserviceaccount.com",
        configuration_sha256="a" * 64,
    )
    instances, transport = Instances(), Transport()
    backend = GceBackend(clock=ManualClock(), config=config, instances=instances, transport=transport)
    job = Job(job_id="01234567-89ab-cdef-0123-456789abcdef", user_id="user1",
              command="python train.py", gpu_type="t4", requested_hours=1.0,
              currency=Currency.USD, reserved=Decimal("2.00"), state=JobState.ALLOCATING,
              submitted_at=datetime(2026, 11, 4, tzinfo=UTC),
              updated_at=datetime(2026, 11, 4, tzinfo=UTC), checkpoint_key="checkpoints/job/step-1")
    allocation = backend.launch(job)
    request = instances.inserted[0]
    assert backend.tier == "spot"
    assert request.scheduling.provisioning_model == "SPOT"
    assert request.scheduling.instance_termination_action == "STOP"
    assert request.scheduling.automatic_restart is False
    assert request.labels["broker-spot"] == "true"
    backend.start(allocation.handle, job)
    assert len(transport.commands) == 1
    assert "instance/preempted" in transport.commands[0]
    assert "checkpoint-bucket" in transport.commands[0]
    request.status = "TERMINATED"
    assert backend.poll(allocation.handle).status is BackendStatus.INTERRUPTED


def test_spot_runner_is_valid_and_binds_checkpoint_to_workload() -> None:
    compile(GCE_SPOT_RUNNER, "gce-spot-runner", "exec")
    command = "python train.py --resume $GPU_BROKER_RESUME_DIR"
    script = gce_spot_job_script(
        command=command, bucket="private-checkpoints", prefix="jobs",
        job_id="job-123", attempt=2, resume_key="jobs/job-123/step-000000000042",
        checkpoint_dir="/tmp/ckpt", resume_dir="/tmp/resume",
        checkpoint_deadline_seconds=20, workload_revision="code@sha256:abc",
        model_revision="model@sha256:def", configuration_sha256="a" * 64,
    )
    encoded = base64.b64encode(command.encode()).decode()
    assert encoded in script
    assert "instance/preempted" in script
    assert "uploadType=resumable" in GCE_SPOT_RUNNER
    assert "MANIFEST.json" in GCE_SPOT_RUNNER
    assert "gpu-broker.gcs-checkpoint/v1" in GCE_SPOT_RUNNER
    assert "code@sha256:abc" in script
    assert "model@sha256:def" in script


def test_spot_requires_durable_bucket_and_pinned_revisions() -> None:
    required = dict(
        project="test-project", boot_image="projects/test/global/images/gpu",
        ssh_private_key="key", known_hosts="known", machine_types={"t4": "n1-standard-4"},
        accelerator_types={"t4": "nvidia-tesla-t4"}, max_instances={"t4": 1}, spot=True,
        service_account="gce-worker@test-project.iam.gserviceaccount.com",
    )
    with pytest.raises(ConfigError, match="checkpoint_bucket"):
        GceConfig(**required).require_launchable()
    with pytest.raises(ConfigError, match="pinned gce.workload_revision"):
        GceConfig(**required, checkpoint_bucket="checkpoints").require_launchable()
    launchable = GceConfig(**required, checkpoint_bucket="checkpoints",
                           workload_revision="code@sha256:abc", model_revision="model@sha256:def",
                           configuration_sha256="a" * 64)
    launchable.require_launchable()

"""A job's whole life on EC2, driven through the broker.

The unit tests in `test_ec2.py` check that each AWS call is right. This checks
that the scheduler drives them in the right order: that a job is not billed for a
machine it never got, that the command is sent exactly once, and that the
instance is gone when the job is.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from aws_doubles import Ec2Double, LogsDouble, SsmDouble
from test_ec2 import TEST_AMI
from gpu_broker.aws import AwsConfig
from gpu_broker.backends.ec2 import Ec2Backend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.states import JobState


# Entering and leaving moto's mock costs about 0.4s, and these tests keep a
# mock per test rather than sharing one, because a shared mock leaks state
# between tests and produces the kind of failure that gets rerun rather than
# read. That buys isolation at roughly 25 seconds across the file.
#
#   pytest -m "not aws"   the fast inner loop, about four seconds
#   pytest                everything, which is what CI runs
pytestmark = pytest.mark.aws


@pytest.fixture(autouse=True)
def fake_credentials(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def rig(aws, tmp_path: Path):
    """A broker whose only capacity is EC2."""
    clock = ManualClock()
    ec2_real = boto3.client("ec2", region_name="us-west-2")
    boto3.client("iam").create_instance_profile(InstanceProfileName="gpu-broker-node")
    ami = TEST_AMI

    clients = {
        "ec2": Ec2Double(ec2_real),
        "ssm": SsmDouble(boto3.client("ssm", region_name="us-west-2")),
        "logs": LogsDouble(boto3.client("logs", region_name="us-west-2")),
    }
    aws_config = AwsConfig(
        region="us-west-2",
        ami=ami,
        instance_profile="gpu-broker-node",
        log_group="/gpu-broker/jobs",
        instance_types={"a10g": "g5.xlarge", "t4": "g4dn.xlarge"},
        max_instances={"a10g": 2, "t4": 1},
    )
    state_dir = tmp_path / "broker"
    config = load_config(state_dir)

    def build() -> Ec2Backend:
        return Ec2Backend(
            clock=clock, aws=aws_config,
            ec2=clients["ec2"], ssm=clients["ssm"], logs=clients["logs"],
            cache_seconds=0.0,
        )

    backend = build()
    broker = Broker.open(state_dir, clock=clock, backends=[backend], config=config)
    for name in ("ana", "bo"):
        broker.add_user(name)
    return {
        "broker": broker, "clock": clock, "clients": clients,
        "backend": backend, "state_dir": state_dir, "config": config, "build": build,
    }


def handle_of(broker, job_id: str) -> str:
    handle = broker.status(job_id).backend_handle
    assert handle, "the job has no machine"
    return handle


# ------------------------------------------------------------------ the path


def test_a_job_runs_end_to_end_on_ec2(rig):
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="python train.py", gpu_type="a10g", hours=2).job

    # tick 1: placed on EC2. The machine is booting; nothing of the user's runs.
    broker.tick()
    assert broker.status(job.job_id).state is JobState.ALLOCATING
    handle = handle_of(broker, job.job_id)
    assert ssm.sends() == [], "sent a command before the agent registered"

    # tick 2: still booting.
    clock.advance(minutes=1)
    broker.tick()
    assert broker.status(job.job_id).state is JobState.ALLOCATING

    # The agent registers. Next tick starts the command.
    ssm.register(handle)
    clock.advance(minutes=1)
    broker.tick()
    assert len(ssm.sends()) == 1

    command_id = rig["backend"]._command_id(handle)
    ssm.force(command_id, "InProgress")
    clock.advance(minutes=5)
    broker.tick()
    assert broker.status(job.job_id).state is JobState.RUNNING

    ssm.force(command_id, "Success", response_code=0)
    clock.advance(hours=1)
    broker.tick()

    final = broker.status(job.job_id)
    assert final.state is JobState.COMPLETED
    assert final.exit_code == 0


def test_the_machine_is_gone_when_the_job_is(rig):
    """A completed job whose instance is still up is the failure this whole
    project exists to prevent."""
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    ssm.force(rig["backend"]._command_id(handle), "Success", response_code=0)
    clock.advance(minutes=10)
    broker.tick()

    assert broker.status(job.job_id).state is JobState.COMPLETED
    assert rig["backend"].list_resources() == [], "the instance outlived the job"
    assert broker.reap().orphans == ()


def test_billing_starts_at_launch_not_at_first_output(rig):
    """EC2 charges from the moment the instance starts, including the boot and
    the wait for the agent. So does the broker."""
    broker, clock = rig["broker"], rig["clock"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job
    broker.tick()

    clock.advance(minutes=10)  # still booting; under the 15-minute agent timeout
    broker.tick()
    assert broker.status(job.job_id).state is JobState.ALLOCATING
    assert broker.job_spend(job.job_id) > Decimal("0"), "ten minutes of boot was free"


def test_the_command_is_sent_exactly_once_across_many_ticks(rig):
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)

    for _ in range(10):
        clock.advance(minutes=1)
        broker.tick()
        ssm.force(rig["backend"]._command_id(handle), "InProgress")

    assert len(ssm.sends()) == 1


def test_a_restarted_broker_does_not_run_the_job_twice(rig):
    """The command id lives in a tag on the instance, so a broker that comes back
    up mid-job finds the command it already started."""
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    assert len(ssm.sends()) == 1
    command_id = rig["backend"]._command_id(handle)
    ssm.force(command_id, "InProgress")
    broker.close()

    restarted = Broker.open(
        rig["state_dir"], clock=clock, backends=[rig["build"]()], config=rig["config"]
    )
    clock.advance(minutes=5)
    restarted.tick()

    assert len(ssm.sends()) == 1, "the restart started the job a second time"
    assert restarted.status(job.job_id).state is JobState.RUNNING
    assert restarted.reconcile() == []
    restarted.close()


def test_cancelling_mid_run_stops_the_command_and_the_machine(rig):
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    command_id = rig["backend"]._command_id(handle)
    ssm.force(command_id, "InProgress")
    clock.advance(hours=1)
    broker.tick()

    broker.cancel(job.job_id, actor="ana")

    assert broker.status(job.job_id).state is JobState.CANCELLED
    assert ssm.cancelled == [command_id]
    assert rig["backend"].list_resources() == []
    # An hour of running was real and is still charged.
    assert broker.job_spend(job.job_id) > Decimal("0")
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


def test_a_ceiling_kill_stops_the_instance(rig):
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(
        user_id="ana", command="x", gpu_type="a10g", hours=4, budget="1.50"
    ).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    ssm.force(rig["backend"]._command_id(handle), "InProgress")

    for _ in range(12):
        clock.advance(minutes=15)
        broker.tick()
        if broker.status(job.job_id).is_terminal:
            break

    assert broker.status(job.job_id).state is JobState.FAILED
    assert broker.job_spend(job.job_id) == Decimal("1.50")
    assert rig["backend"].list_resources() == [], "the instance ran past the ceiling"


def test_logs_reach_the_broker(rig):
    broker, clock, ssm, logs = rig["broker"], rig["clock"], rig["clients"]["ssm"], rig["clients"]["logs"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    command_id = rig["backend"]._command_id(handle)
    ssm.force(command_id, "InProgress")

    logs.emit(
        "/gpu-broker/jobs",
        f"{command_id}/{handle}/aws-runShellScript/stdout",
        ["epoch 1 loss 2.31", "epoch 2 loss 1.88"],
    )
    clock.advance(minutes=1)
    broker.tick()

    stored = [line for _, _, _, line in broker.logs(job.job_id)]
    assert "epoch 1 loss 2.31" in stored
    assert "epoch 2 loss 1.88" in stored
    assert len(stored) == len(set(stored)), "log lines were delivered twice"


def test_an_agent_that_never_registers_fails_the_job_and_releases_the_machine(rig):
    broker, clock = rig["broker"], rig["clock"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()

    clock.advance(seconds=901)
    broker.tick()

    final = broker.status(job.job_id)
    assert final.state is JobState.FAILED
    assert rig["backend"].list_resources() == [], "a broken instance was left running"
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


def test_an_instance_killed_in_the_console_is_noticed(rig):
    broker, clock, ssm = rig["broker"], rig["clock"], rig["clients"]["ssm"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    handle = handle_of(broker, job.job_id)
    ssm.register(handle)
    clock.advance(minutes=2)
    broker.tick()
    ssm.force(rig["backend"]._command_id(handle), "InProgress")

    # Somebody terminates it by hand.
    rig["clients"]["ec2"]._real.terminate_instances(InstanceIds=[handle])
    clock.advance(minutes=5)
    broker.tick()

    assert broker.status(job.job_id).state is JobState.FAILED
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


# ------------------------------------------------------------------ dry run


def test_a_dry_run_asks_aws_and_launches_nothing(rig):
    broker, clients = rig["broker"], rig["clients"]
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2)

    plan = broker.plan()

    assert len(plan.would_dispatch) == 1
    assert plan.problems == ()
    assert "would launch a g5.xlarge" in dict(plan.checks)[plan.would_dispatch[0].job.job_id]
    assert clients["ec2"].kwargs_for("run_instances")["DryRun"] is True
    assert rig["backend"].list_resources() == [], "the dry run launched something"
    assert broker.store.queued_jobs(), "the dry run moved the job out of the queue"


def test_a_dry_run_reports_a_pool_cap_the_same_way_a_tick_would(rig):
    from dataclasses import replace

    broker = Broker.open(
        rig["state_dir"], clock=rig["clock"], backends=[rig["backend"]],
        config=replace(rig["config"], pool_budget_usd=Decimal("3.00")),
    )
    for name in ("ana", "bo"):
        broker.add_user(name)
    broker.submit(user_id="ana", command="a", gpu_type="a10g", hours=1, budget="2.00")
    rig["clock"].advance(minutes=1)
    broker.submit(user_id="bo", command="b", gpu_type="a10g", hours=1, budget="2.00")

    plan = broker.plan()
    actions = [decision.action for decision in plan.decisions]
    assert actions == ["DISPATCH", "BLOCKED_POOL"]
    assert "pool has" in plan.decisions[1].detail
    broker.close()


def test_a_dry_run_surfaces_a_missing_permission_before_the_first_real_launch(rig, monkeypatch):
    import botocore.exceptions

    broker = rig["broker"]
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2)

    def denied(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "no ec2:RunInstances"}},
            "RunInstances",
        )

    monkeypatch.setattr(rig["clients"]["ec2"], "run_instances", denied)
    plan = broker.plan()

    assert plan.problems, "a denied launch was reported as fine"
    assert "missing an EC2 permission" in plan.problems[0][1]


def test_a_quota_error_leaves_the_job_queued_with_an_explanation(rig, monkeypatch):
    """Our G and P vCPU quota may be zero. That has to be survivable and legible."""
    import botocore.exceptions

    broker = rig["broker"]
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job

    def over_quota(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "VcpuLimitExceeded", "Message": "limit is 0"}}, "RunInstances"
        )

    monkeypatch.setattr(rig["clients"]["ec2"], "run_instances", over_quota)
    report = broker.tick()

    assert job.job_id in report.blocked_on_capacity
    assert broker.status(job.job_id).state is JobState.QUEUED
    lines = [line for _, _, _, line in broker.logs(job.job_id)]
    assert any("Service Quotas" in line for line in lines)

"""The EC2 backend, against moto's implementation of the real AWS APIs.

No AWS account, no credentials, no quota. That is a standing constraint, not a
convenience: our G and P vCPU quota may be zero.
"""

from __future__ import annotations

import datetime as dt

import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from aws_doubles import Ec2Double, LogsDouble, SsmDouble
from gpu_broker.aws import TAG_COMMAND_ID, TAG_JOB_ID, TAG_LAUNCHED_AT, TAG_USER, AwsConfig
from gpu_broker.backends.base import BackendStatus
from gpu_broker.backends.ec2 import Ec2Backend
from gpu_broker.clock import ManualClock
from gpu_broker.errors import BackendError, ConfigError
from gpu_broker.models import Job
from gpu_broker.money import Currency, money
from gpu_broker.states import JobState


# A made-up id. moto does not validate ImageId, and looking a real one up costs
# 1.6 seconds per test because it loads AWS's entire public image catalogue --
# about 70 seconds across this file. The broker never inspects the AMI; it passes
# it through, and real EC2 is what rejects a bad one (`explain` has a hint for
# InvalidAMIID.NotFound). Nothing is lost by not asking moto for one.
TEST_AMI = "ami-0gpubroker00000"


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
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-west-2",
    }.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def clients(aws):
    ec2 = boto3.client("ec2", region_name="us-west-2")
    boto3.client("iam").create_instance_profile(InstanceProfileName="gpu-broker-node")
    ami = TEST_AMI
    return {
        "ec2": Ec2Double(ec2),
        "ssm": SsmDouble(boto3.client("ssm", region_name="us-west-2")),
        "logs": LogsDouble(boto3.client("logs", region_name="us-west-2")),
        "ami": ami,
    }


@pytest.fixture
def aws_config(clients) -> AwsConfig:
    return AwsConfig(
        region="us-west-2",
        ami=clients["ami"],
        instance_profile="gpu-broker-node",
        log_group="/gpu-broker/jobs",
        max_instances={"a10g": 2, "t4": 1},
        instance_types={"a10g": "g5.xlarge", "t4": "g4dn.xlarge"},
    )


@pytest.fixture
def backend(clients, aws_config, clock) -> Ec2Backend:
    return Ec2Backend(
        clock=clock,
        aws=aws_config,
        ec2=clients["ec2"],
        ssm=clients["ssm"],
        logs=clients["logs"],
        cache_seconds=0.0,  # tests change state constantly; never serve a stale read
    )


def make_job(job_id="j1", user_id="ana", gpu_type="a10g", hours=2.0) -> Job:
    now = dt.datetime(2026, 1, 15, 12, 0, tzinfo=dt.timezone.utc)
    return Job(
        job_id=job_id * 8 if len(job_id) < 8 else job_id,
        user_id=user_id,
        command="python train.py --epochs 10",
        gpu_type=gpu_type,
        requested_hours=hours,
        currency=Currency.USD,
        reserved=money("6.00"),
        state=JobState.QUEUED,
        submitted_at=now,
        updated_at=now,
    )


# --------------------------------------------------------------------- launch


def test_launch_returns_the_instance_id(backend):
    allocation = backend.launch(make_job())
    assert allocation.handle.startswith("i-")
    assert allocation.gpu_type == "a10g"


def test_every_instance_carries_the_three_tags(backend, clock):
    """Reconciliation and reap work off these and nothing else, so an orphan is
    always attributable to a person."""
    job = make_job(user_id="bo")
    allocation = backend.launch(job)

    assert allocation.tags[TAG_JOB_ID] == job.job_id
    assert allocation.tags[TAG_USER] == "bo"
    assert allocation.tags[TAG_LAUNCHED_AT].startswith("2026-01-15")


def test_tags_are_applied_at_creation_not_afterwards(backend, clients):
    """A separate create_tags can fail, or the broker can die between the two
    calls, and the result is an untagged GPU instance nobody can be asked about."""
    backend.launch(make_job())
    params = clients["ec2"].kwargs_for("run_instances")
    resources = {spec["ResourceType"] for spec in params["TagSpecifications"]}
    assert resources == {"instance", "volume"}
    assert clients["ec2"].called("create_tags") == 0


def test_the_volume_is_tagged_too(backend, clients):
    """An orphaned 200GB gp3 is a smaller bill than an orphaned g5, and just as
    invisible."""
    backend.launch(make_job())
    volumes = clients["ec2"]._real.describe_volumes(
        Filters=[{"Name": "tag-key", "Values": [TAG_JOB_ID]}]
    )["Volumes"]
    assert len(volumes) == 1


def test_instances_terminate_on_shutdown_and_require_imdsv2(backend, clients):
    """`terminate`, not `stop`. A stopped GPU instance still holds its EBS volume
    and is invisible in most "what is running" views."""
    backend.launch(make_job())
    params = clients["ec2"].kwargs_for("run_instances")
    assert params["InstanceInitiatedShutdownBehavior"] == "terminate"
    assert params["MetadataOptions"]["HttpTokens"] == "required"


def test_the_gpu_type_maps_to_a_real_instance_type(backend, clients):
    backend.launch(make_job(gpu_type="t4"))
    assert clients["ec2"].kwargs_for("run_instances")["InstanceType"] == "g4dn.xlarge"


def test_launching_without_an_ami_names_the_setting(clients, clock):
    backend = Ec2Backend(
        clock=clock,
        aws=AwsConfig(instance_profile="p"),
        ec2=clients["ec2"], ssm=clients["ssm"], logs=clients["logs"],
    )
    with pytest.raises(ConfigError, match="aws.ami"):
        backend.launch(make_job())


def test_free_slots_falls_as_instances_launch(backend):
    assert backend.free_slots("a10g") == 2
    backend.launch(make_job("a"))
    assert backend.free_slots("a10g") == 1
    backend.launch(make_job("b"))
    assert backend.free_slots("a10g") == 0


def test_launching_past_the_configured_limit_is_refused(backend):
    backend.launch(make_job("a"))
    backend.launch(make_job("b"))
    with pytest.raises(BackendError, match="limit of 2"):
        backend.launch(make_job("c"))


def test_an_unmapped_gpu_type_is_not_supported(backend):
    assert not backend.supports("a100")
    assert backend.free_slots("a100") == 0


# ------------------------------------------------------------------- dry run


def test_validate_launch_uses_the_real_dry_run_and_launches_nothing(backend, clients):
    """EC2 signals a successful dry run by raising DryRunOperation. That checks
    IAM and every parameter against the real account, which is the only way to
    find a missing permission before the first real launch."""
    description = backend.validate_launch(make_job())

    assert "would launch a g5.xlarge" in description
    assert "ana" in description
    assert clients["ec2"].kwargs_for("run_instances")["DryRun"] is True
    assert backend.free_slots("a10g") == 2, "the dry run actually launched something"


def test_the_dry_run_validates_the_same_parameters_a_real_launch_would_send(backend, clients):
    """If the two calls were built differently the dry run would be validating
    something adjacent to what actually happens, which is worse than useless."""
    job = make_job()
    backend.validate_launch(job)
    checked = dict(clients["ec2"].kwargs_for("run_instances"))
    checked.pop("DryRun")

    backend.launch(job)
    real = clients["ec2"].kwargs_for("run_instances")
    assert checked == real


def test_a_permission_failure_surfaces_with_a_hint(backend, clients, monkeypatch):
    def denied(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "not authorized"}},
            "RunInstances",
        )

    monkeypatch.setattr(clients["ec2"], "run_instances", denied)
    with pytest.raises(BackendError, match="missing an EC2 permission"):
        backend.validate_launch(make_job())


def test_a_dry_run_that_does_not_raise_is_not_trusted(backend, clients, monkeypatch):
    """If the flag were ever dropped, a 'dry run' would come within one call of
    launching for real. Refuse rather than report success."""
    monkeypatch.setattr(clients["ec2"], "run_instances", lambda **kwargs: {"Instances": []})
    with pytest.raises(BackendError, match="Refusing to trust"):
        backend.validate_launch(make_job())


def test_validate_terminate_touches_nothing(backend, clients):
    handle = backend.launch(make_job()).handle
    assert "would terminate" in backend.validate_terminate(handle)
    assert clients["ec2"].kwargs_for("terminate_instances")["DryRun"] is True
    assert backend.poll(handle).status is not BackendStatus.GONE


def test_validate_terminate_on_something_already_gone(backend):
    assert "already gone" in backend.validate_terminate("i-00000000000000000")


# ---------------------------------------------------------- ready then start


def test_a_running_instance_is_not_ready_until_the_agent_registers(backend, clients):
    handle = backend.launch(make_job()).handle
    observation = backend.poll(handle)
    assert observation.status is BackendStatus.PENDING
    assert "ssm agent" in observation.detail

    clients["ssm"].register(handle)
    assert backend.poll(handle).status is BackendStatus.READY


def test_polling_never_starts_anything(backend, clients):
    """`poll` runs on every tick. A mutation hidden inside a read is one that
    cannot be dry-run and will eventually happen twice."""
    handle = backend.launch(make_job()).handle
    clients["ssm"].register(handle)
    for _ in range(5):
        backend.poll(handle)
    assert clients["ssm"].sends() == []


def test_start_sends_the_command_with_cloudwatch_output(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    clients["ssm"].register(handle)
    backend.start(handle, job)

    params = clients["ssm"].job_send()
    assert params["InstanceIds"] == [handle]
    assert params["DocumentName"] == "AWS-RunShellScript"
    assert params["Parameters"]["commands"] == [job.command]
    # Untruncated logs. GetCommandInvocation caps output at 24KB.
    assert params["CloudWatchOutputConfig"] == {
        "CloudWatchLogGroupName": "/gpu-broker/jobs",
        "CloudWatchOutputEnabled": True,
    }


def test_the_command_gets_an_execution_timeout_from_the_job(backend, clients):
    job = make_job(hours=3)
    handle = backend.launch(job).handle
    backend.start(handle, job)
    timeout = int(clients["ssm"].job_send()["Parameters"]["executionTimeout"][0])
    assert timeout == 3 * 3600 + 3600


def test_the_command_id_is_recorded_on_the_instance(backend, clients):
    """In a tag, not in memory: the broker can restart between sending the
    command and noticing it succeeded."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)

    tags = {
        tag["Key"]: tag["Value"]
        for tag in clients["ec2"]._real.describe_instances(InstanceIds=[handle])[
            "Reservations"
        ][0]["Instances"][0]["Tags"]
    }
    assert tags[TAG_COMMAND_ID] == _command_id_from_calls(clients)


def test_starting_twice_does_not_run_the_job_twice(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    backend.start(handle, job)
    assert len(clients["ssm"].sends()) == 1


def test_a_fresh_backend_sees_a_command_already_started(clients, aws_config, clock):
    """The restart case. A second broker process must not send a second command."""
    job = make_job()
    first = Ec2Backend(clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
                       logs=clients["logs"], cache_seconds=0.0)
    handle = first.launch(job).handle
    first.start(handle, job)

    second = Ec2Backend(clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
                        logs=clients["logs"], cache_seconds=0.0)
    second.start(handle, job)
    assert len(clients["ssm"].sends()) == 1


# ---------------------------------------------------------------------- poll


def test_a_running_command_polls_as_running(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    clients["ssm"].force(_command_id(backend, handle), "InProgress")
    assert backend.poll(handle).status is BackendStatus.RUNNING


def test_a_finished_command_polls_as_completed_with_its_exit_code(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    clients["ssm"].force(_command_id(backend, handle), "Success", response_code=0)

    observation = backend.poll(handle)
    assert observation.status is BackendStatus.COMPLETED
    assert observation.exit_code == 0


def test_a_failed_command_carries_its_stderr(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    clients["ssm"].force(
        _command_id(backend, handle), "Failed", response_code=1, stderr="CUDA out of memory"
    )

    observation = backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert observation.exit_code == 1
    assert "CUDA out of memory" in observation.detail


def test_an_agent_that_never_registers_fails_with_something_actionable(backend, clock):
    handle = backend.launch(make_job()).handle
    clock.advance(seconds=901)

    observation = backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert "AmazonSSMManagedInstanceCore" in observation.detail
    assert "gpu-broker-node" in observation.detail


def test_the_agent_timeout_reports_why_the_check_was_failing(backend, clients, clock):
    clients["ssm"].describe_error = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no ssm:DescribeInstanceInformation"}},
        "DescribeInstanceInformation",
    )
    handle = backend.launch(make_job()).handle
    clock.advance(seconds=901)
    assert "AccessDeniedException" in backend.poll(handle).detail


def test_an_ondemand_instance_killed_by_hand_is_a_failure_not_a_preemption(backend, clients):
    """A preemption and somebody hitting Terminate in the console look identical
    in `describe-instances`, and they mean opposite things: one should requeue
    the job, the other should not. On-demand capacity is never reclaimed, so a
    terminated on-demand instance was terminated by a person."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    clients["ssm"].force(_command_id(backend, handle), "InProgress")
    clients["ec2"]._real.terminate_instances(InstanceIds=[handle])

    observation = backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert "somebody terminated it" in observation.detail


def test_a_command_that_finished_before_teardown_still_counts_as_completed(backend, clients):
    """Terminating an instance after its job succeeded is just teardown, and must
    not turn a successful run into somebody's failure."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    clients["ssm"].force(_command_id(backend, handle), "Success", response_code=0)
    clients["ec2"]._real.terminate_instances(InstanceIds=[handle])

    assert backend.poll(handle).status is BackendStatus.COMPLETED


def test_an_unknown_handle_is_gone(backend):
    assert backend.poll("i-00000000000000000").status is BackendStatus.GONE


# ----------------------------------------------------------------- terminate


def test_terminate_stops_the_instance(backend, clients):
    handle = backend.launch(make_job()).handle
    backend.terminate(handle, "cancelled by ana")

    state = clients["ec2"]._real.describe_instances(InstanceIds=[handle])["Reservations"][0][
        "Instances"
    ][0]["State"]["Name"]
    assert state in ("shutting-down", "terminated")


def test_terminate_cancels_the_command_first(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    backend.terminate(handle, "cancelled")
    assert clients["ssm"].cancelled == [_command_id_from_calls(clients)]


def test_a_failing_cancel_never_blocks_the_terminate(backend, clients, monkeypatch):
    """The call whose whole job is stopping the machine must not be stopped by a
    best-effort call that runs first. This is the failure the project exists to
    prevent, arriving through the back door."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)

    def explode(**kwargs):
        raise NotImplementedError("SSM had a bad day")

    monkeypatch.setattr(clients["ssm"], "cancel_command", explode)
    backend.terminate(handle, "cancelled")

    state = clients["ec2"]._real.describe_instances(InstanceIds=[handle])["Reservations"][0][
        "Instances"
    ][0]["State"]["Name"]
    assert state in ("shutting-down", "terminated")


def test_terminating_something_already_gone_is_not_an_error(backend):
    backend.terminate("i-00000000000000000", "already dead")


# ------------------------------------------------------------------ the logs


def test_logs_come_back_from_cloudwatch(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    command_id = _command_id(backend, handle)

    clients["logs"].emit(
        "/gpu-broker/jobs",
        f"{command_id}/{handle}/aws-runShellScript/stdout",
        ["epoch 1 loss 2.31", "epoch 2 loss 1.88"],
    )
    lines = backend.fetch_logs(handle)
    assert [line for _, line in lines] == ["epoch 1 loss 2.31", "epoch 2 loss 1.88"]
    assert all(stream == "stdout" for stream, _ in lines)


def test_stderr_is_labelled_separately(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    command_id = _command_id(backend, handle)
    clients["logs"].emit(
        "/gpu-broker/jobs", f"{command_id}/{handle}/aws-runShellScript/stderr", ["Traceback"]
    )
    assert ("stderr", "Traceback") in backend.fetch_logs(handle)


def test_logs_are_not_re_delivered(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    command_id = _command_id(backend, handle)
    stream = f"{command_id}/{handle}/aws-runShellScript/stdout"

    clients["logs"].emit("/gpu-broker/jobs", stream, ["first"])
    seen = len(backend.fetch_logs(handle))
    clients["logs"].emit("/gpu-broker/jobs", stream, ["second"])

    assert [line for _, line in backend.fetch_logs(handle, after=seen)] == ["second"]


def test_no_logs_before_the_command_starts(backend):
    handle = backend.launch(make_job()).handle
    assert backend.fetch_logs(handle) == []


def test_a_missing_log_stream_is_not_an_error(backend, clients):
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    assert backend.fetch_logs(handle) == []


# ------------------------------------------------------- reconciliation view


def test_list_resources_only_shows_instances_we_tagged(backend, clients):
    backend.launch(make_job("a"))
    # Somebody launched this by hand in the console.
    clients["ec2"]._real.run_instances(
        ImageId=clients["ami"], InstanceType="g5.xlarge", MinCount=1, MaxCount=1
    )
    resources = backend.list_resources()
    assert len(resources) == 1
    assert resources[0].job_id is not None


def test_list_resources_reports_the_gpu_type_not_the_instance_type(backend):
    backend.launch(make_job(gpu_type="t4"))
    assert backend.list_resources()[0].gpu_type == "t4"


def test_terminated_instances_drop_out_of_the_listing(backend):
    handle = backend.launch(make_job()).handle
    backend.terminate(handle, "done")
    assert backend.list_resources() == []


def test_instances_are_listed_with_who_launched_them(backend):
    backend.launch(make_job(user_id="cy"))
    assert backend.list_resources()[0].user_id == "cy"


# --------------------------------------------------------------------- cache


def test_repeated_slot_checks_do_not_hammer_the_api(clients, aws_config, clock):
    """Every queued job asks about free slots. Twenty people sharing one account
    reach RequestLimitExceeded faster than you would guess."""
    backend = Ec2Backend(
        clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
        logs=clients["logs"], cache_seconds=5.0,
    )
    backend.free_slots("a10g")
    before = clients["ec2"].called("get_paginator")
    for _ in range(10):
        backend.free_slots("a10g")
    assert clients["ec2"].called("get_paginator") == before


def test_the_cache_expires(clients, aws_config, clock):
    backend = Ec2Backend(
        clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
        logs=clients["logs"], cache_seconds=5.0,
    )
    backend.free_slots("a10g")
    before = clients["ec2"].called("get_paginator")
    clock.advance(seconds=6)
    backend.free_slots("a10g")
    assert clients["ec2"].called("get_paginator") > before


def test_launching_invalidates_the_cache_immediately(clients, aws_config, clock):
    """Otherwise the next placement decision in the same tick believes a slot is
    still free and the broker double-books it."""
    backend = Ec2Backend(
        clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
        logs=clients["logs"], cache_seconds=60.0,
    )
    assert backend.free_slots("a10g") == 2
    backend.launch(make_job("a"))
    assert backend.free_slots("a10g") == 1


# ------------------------------------------------------------------ helpers


def _command_id(backend: Ec2Backend, handle: str) -> str:
    command_id = backend._command_id(handle)
    assert command_id, "no command id tag on the instance"
    return command_id


def _command_id_from_calls(clients) -> str:
    for name, kwargs in reversed(clients["ec2"].calls):
        if name == "create_tags":
            for tag in kwargs["Tags"]:
                if tag["Key"] == TAG_COMMAND_ID:
                    return tag["Value"]
    raise AssertionError("no command id was ever tagged")


# ------------------------------------------------------------- gpu sampling


def test_a_gpu_watcher_starts_alongside_the_job(backend, clients):
    """Its own SSM command, so its output lands in its own CloudWatch stream and
    the broker reads samples with a call it already makes, instead of an SSM
    round trip per job per tick."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)

    samplers = clients["ssm"].sends(sampler=True)
    assert len(samplers) == 1
    assert "nvidia-smi" in samplers[0]["Parameters"]["commands"][0]
    assert samplers[0]["CloudWatchOutputConfig"]["CloudWatchOutputEnabled"] is True


def test_the_sampler_id_is_recorded_on_the_instance(backend, clients):
    from gpu_broker.aws import TAG_SAMPLER_ID

    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)

    tags = {
        tag["Key"]: tag["Value"]
        for tag in clients["ec2"]._real.describe_instances(InstanceIds=[handle])[
            "Reservations"
        ][0]["Instances"][0]["Tags"]
    }
    assert tags[TAG_SAMPLER_ID], "the watcher's command id was not recorded"
    # Recorded for the same reason as the job's command: a restarted broker has
    # to find the watcher it already started rather than starting a second one.
    assert backend.sample_utilization(handle) == []


def test_samples_come_back_from_the_watchers_stream(backend, clients):
    from gpu_broker.aws import TAG_SAMPLER_ID, command_log_stream, parse_tags

    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    sampler_id = parse_tags(
        clients["ec2"]._real.describe_instances(InstanceIds=[handle])["Reservations"][0][
            "Instances"
        ][0].get("Tags")
    )[TAG_SAMPLER_ID]

    clients["logs"].emit(
        "/gpu-broker/jobs",
        command_log_stream(sampler_id, handle, "stdout"),
        ["1700000000, 87, 12000", "1700000060, 3, 400"],
    )
    samples = backend.sample_utilization(handle)

    assert [s.gpu_percent for s in samples] == [87.0, 3.0]
    assert samples[0].memory_mb == 12_000
    assert samples[0].at == 1700000000.0, "the sample lost the host's own timestamp"


def test_a_job_with_no_watcher_yields_no_samples(backend):
    """Rather than zeroes, which are indistinguishable from an idle job."""
    handle = backend.launch(make_job()).handle
    assert backend.sample_utilization(handle) == []


def test_garbage_in_the_sample_stream_is_skipped(backend, clients):
    from gpu_broker.aws import TAG_SAMPLER_ID, command_log_stream, parse_tags

    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    sampler_id = parse_tags(
        clients["ec2"]._real.describe_instances(InstanceIds=[handle])["Reservations"][0][
            "Instances"
        ][0].get("Tags")
    )[TAG_SAMPLER_ID]
    clients["logs"].emit(
        "/gpu-broker/jobs",
        command_log_stream(sampler_id, handle, "stdout"),
        ["nvidia-smi: command not found", "1700000000, 87, 12000"],
    )
    assert [s.gpu_percent for s in backend.sample_utilization(handle)] == [87.0]


def test_a_failing_watcher_does_not_stop_the_job(backend, clients, monkeypatch):
    """Idle detection is a nice-to-have. The training run is the point."""
    job = make_job()
    handle = backend.launch(job).handle

    original = clients["ssm"].send_command
    calls = {"n": 0}

    def fail_the_second(**kwargs):
        calls["n"] += 1
        if "sampler" in kwargs.get("Comment", ""):
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "SendCommand"
            )
        return original(**kwargs)

    monkeypatch.setattr(clients["ssm"], "send_command", fail_the_second)
    backend.start(handle, job)

    assert len(clients["ssm"].sends()) == 1, "the job command did not go out"
    assert backend.sample_utilization(handle) == []


# ---------------------------------------------------------------------- spot


@pytest.fixture
def spot_backend(clients, aws_config, clock) -> Ec2Backend:
    return Ec2Backend(
        "ec2-spot", clock=clock, aws=aws_config,
        ec2=clients["ec2"], ssm=clients["ssm"], logs=clients["logs"],
        cache_seconds=0.0, spot=True,
        checkpoint_bucket="club-ckpt", checkpoint_prefix="checkpoints",
        max_launch_failures=2, cooldown_seconds=900.0,
    )


def test_a_spot_backend_lands_in_the_spot_tier(spot_backend, backend):
    assert spot_backend.tier == "spot"
    assert backend.tier == "ondemand"


def test_a_spot_launch_asks_for_spot(spot_backend, clients):
    spot_backend.launch(make_job())
    options = clients["ec2"].kwargs_for("run_instances")["InstanceMarketOptions"]
    assert options["MarketType"] == "spot"
    assert options["SpotOptions"]["InstanceInterruptionBehavior"] == "terminate"


def test_a_spot_instance_really_is_one(spot_backend, clients):
    """`terminate`, not `stop` or `hibernate`: the job's state lives in the
    checkpoint store, and a stopped instance is an EBS volume nobody watches."""
    handle = spot_backend.launch(make_job()).handle
    instance = clients["ec2"]._real.describe_instances(InstanceIds=[handle])[
        "Reservations"
    ][0]["Instances"][0]
    assert instance.get("InstanceLifecycle") == "spot"


def test_the_cheapest_candidate_is_chosen(spot_backend, clients, monkeypatch):
    """One instance type is one AZ's worth of luck."""
    def priced(**kwargs):
        return {"SpotPriceHistory": [
            {"InstanceType": "g5.xlarge", "SpotPrice": "0.90", "AvailabilityZone": "a"},
            {"InstanceType": "g5.2xlarge", "SpotPrice": "0.40", "AvailabilityZone": "b"},
            {"InstanceType": "g5.4xlarge", "SpotPrice": "1.80", "AvailabilityZone": "c"},
        ]}

    monkeypatch.setattr(clients["ec2"], "describe_spot_price_history", priced)
    spot_backend.launch(make_job())
    assert clients["ec2"].kwargs_for("run_instances")["InstanceType"] == "g5.2xlarge"


def test_a_pricing_hiccup_does_not_stop_a_launch(spot_backend, clients, monkeypatch):
    def broken(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "RequestLimitExceeded", "Message": "slow down"}},
            "DescribeSpotPriceHistory",
        )

    monkeypatch.setattr(clients["ec2"], "describe_spot_price_history", broken)
    assert spot_backend.launch(make_job()).handle.startswith("i-")


def test_repeated_spot_failures_stand_the_tier_down(spot_backend, clients, monkeypatch):
    """When a spot pool is out, it is out for everyone for a while. Retrying
    into the same wall every tick means the queue never falls through."""
    def no_capacity(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "InsufficientInstanceCapacity", "Message": "none left"}},
            "RunInstances",
        )

    monkeypatch.setattr(clients["ec2"], "run_instances", no_capacity)
    for _ in range(2):
        with pytest.raises(BackendError):
            spot_backend.launch(make_job())

    assert spot_backend.free_slots("a10g") == 0, "kept offering a pool that has nothing"


def test_the_cooldown_expires(spot_backend, clients, clock, monkeypatch):
    def no_capacity(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "InsufficientInstanceCapacity", "Message": "none left"}},
            "RunInstances",
        )

    monkeypatch.setattr(clients["ec2"], "run_instances", no_capacity)
    for _ in range(2):
        with pytest.raises(BackendError):
            spot_backend.launch(make_job())
    assert spot_backend.free_slots("a10g") == 0

    clock.advance(seconds=901)
    assert spot_backend.free_slots("a10g") > 0


def test_one_success_clears_the_failure_count(spot_backend, clients, monkeypatch):
    calls = {"n": 0}
    real = clients["ec2"].run_instances

    def flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "InsufficientInstanceCapacity", "Message": "none"}},
                "RunInstances",
            )
        return real(**kwargs)

    monkeypatch.setattr(clients["ec2"], "run_instances", flaky)
    with pytest.raises(BackendError):
        spot_backend.launch(make_job("a"))
    spot_backend.launch(make_job("b"))
    assert spot_backend._failures == 0


def test_an_ondemand_backend_never_stands_itself_down(backend, clients, monkeypatch):
    """On-demand running out is a quota problem, not a pool problem, and going
    quiet would leave the queue with nowhere at all to go."""
    def denied(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "InsufficientInstanceCapacity", "Message": "none"}},
            "RunInstances",
        )

    monkeypatch.setattr(clients["ec2"], "run_instances", denied)
    for _ in range(5):
        with pytest.raises(BackendError):
            backend.launch(make_job())
    assert backend.free_slots("a10g") > 0


# ----------------------------------------------------- the on-instance wrapper


def test_a_spot_job_is_wrapped_so_it_can_survive_the_notice(spot_backend, clients):
    job = make_job()
    handle = spot_backend.launch(job).handle
    spot_backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "spot/instance-action" in script, "nothing is watching for the notice"
    assert "kill -USR1" in script, "the job is never told to checkpoint"
    assert "CHECKPOINT_COMPLETE" in script, "uploads without waiting for the job to finish writing"
    assert "MANIFEST.json" in script, "the checkpoint would never become visible"
    assert job.command in script


def test_the_wrapper_waits_for_the_job_before_uploading(spot_backend, clients):
    """A directory of half-written tensors loads without complaint and trains
    garbage for another six hours."""
    job = make_job()
    handle = spot_backend.launch(job).handle
    spot_backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    wait_at = script.index('[ ! -f "$CKPT/CHECKPOINT_COMPLETE" ]')
    upload_at = script.index("aws s3 cp --recursive --quiet --exclude")
    assert wait_at < script.index("upload &&"), "uploads before waiting"
    assert upload_at > 0


def test_a_resumable_job_is_told_where_to_restore_from(spot_backend, clients):
    from dataclasses import replace

    job = replace(make_job(), checkpoint_key="checkpoints/j1/step-000000000500")
    handle = spot_backend.launch(job).handle
    spot_backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "s3://club-ckpt/checkpoints/j1/step-000000000500" in script


def test_a_first_run_has_nothing_to_restore(spot_backend, clients):
    job = make_job()
    handle = spot_backend.launch(job).handle
    spot_backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]
    assert 'if [ -n "" ]' in script, "tried to restore from an empty URI"


def test_without_a_bucket_the_command_is_sent_plain(backend, clients):
    """No checkpoint store configured means no wrapper. A job that cannot be
    resumed should not carry machinery that pretends otherwise."""
    job = make_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    assert clients["ssm"].job_send()["Parameters"]["commands"] == [job.command]


def test_a_terminated_spot_instance_reports_interrupted(spot_backend, clients):
    job = make_job()
    handle = spot_backend.launch(job).handle
    spot_backend.start(handle, job)
    clients["ssm"].force(spot_backend._command_id(handle), "InProgress")
    clients["ec2"]._real.terminate_instances(InstanceIds=[handle])

    assert spot_backend.poll(handle).status is BackendStatus.INTERRUPTED


# --------------------------------------------------- environments and volumes


@pytest.fixture
def env_backend(clients, aws_config, clock):
    """An EC2 backend that knows about one environment and an EFS volume."""
    from gpu_broker.environments import Environment
    from gpu_broker.volumes import VolumeConfig

    vision = Environment("vision", requirements=("torch==2.3.0",), apt_packages=("ffmpeg",))
    return Ec2Backend(
        clock=clock, aws=aws_config,
        ec2=clients["ec2"], ssm=clients["ssm"], logs=clients["logs"],
        cache_seconds=0.0,
        environments=lambda name: vision if name == "vision" else None,
        volumes=VolumeConfig(enabled=True, efs_id="fs-abc123"),
        ecr_repository="gpu-broker/environments",
        ecr=boto3.client("ecr", region_name="us-west-2"),
    ), vision


def env_job(environment="vision"):
    from dataclasses import replace

    return replace(make_job(), environment=environment)


def test_the_ecr_repository_is_created_if_it_is_missing(env_backend):
    backend, vision = env_backend
    uri = backend.image_for(vision)
    assert uri is not None
    assert uri.endswith(f"gpu-broker/environments:{vision.digest}")


def test_the_image_tag_is_the_digest_not_the_name(env_backend):
    """Two environments with the same packages share a build; changing a
    package produces a different tag rather than overwriting one."""
    backend, vision = env_backend
    assert backend.image_for(vision).endswith(vision.digest)
    assert "vision" not in backend.image_for(vision).rsplit(":", 1)[1]


def test_an_unbuilt_environment_is_a_cache_miss(env_backend):
    backend, vision = env_backend
    assert not backend.cached(vision)


def test_the_job_runs_inside_its_environments_image(env_backend, clients):
    backend, vision = env_backend
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "docker run" in script
    assert vision.digest in script
    assert job.command in script


def test_a_cache_miss_builds_once_and_pushes(env_backend, clients):
    """The first job that wants an environment pays for the build; everybody
    after it pulls. No separate build server has to exist."""
    backend, _ = env_backend
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "docker pull" in script
    assert script.index("docker pull") < script.index("docker build")
    assert "docker push" in script


def test_a_failed_push_does_not_fail_the_job(env_backend, clients):
    backend, _ = env_backend
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]
    assert "could not push; the next job rebuilds" in script


def test_the_checkpoint_directory_means_the_same_inside_the_container(env_backend, clients):
    """Otherwise the adapters would have to know they are in one."""
    backend, _ = env_backend
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "-v /tmp/gpu-broker/checkpoint:/tmp/gpu-broker/checkpoint" in script
    assert "-e GPU_BROKER_CHECKPOINT_DIR" in script


def test_the_data_volume_is_mounted_before_the_job_runs(env_backend, clients):
    backend, _ = env_backend
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "fs-abc123.efs.us-west-2.amazonaws.com" in script
    assert script.index("mount -t nfs4") < script.index("docker run")
    assert "-v /data:/data" in script


def test_a_job_with_no_environment_runs_on_the_machine_as_it_comes(env_backend, clients):
    backend, _ = env_backend
    job = env_job(environment=None)
    handle = backend.launch(job).handle
    backend.start(handle, job)
    script = clients["ssm"].job_send()["Parameters"]["commands"][0]

    assert "docker run" not in script
    assert job.command in script
    # The volume is still mounted; that is not part of the environment.
    assert "mount -t nfs4" in script


def test_without_an_ecr_repository_nothing_is_containerised(clients, aws_config, clock):
    from gpu_broker.environments import Environment

    vision = Environment("vision", requirements=("torch",))
    backend = Ec2Backend(
        clock=clock, aws=aws_config, ec2=clients["ec2"], ssm=clients["ssm"],
        logs=clients["logs"], cache_seconds=0.0,
        environments=lambda name: vision,
    )
    job = env_job()
    handle = backend.launch(job).handle
    backend.start(handle, job)
    assert "docker" not in clients["ssm"].job_send()["Parameters"]["commands"][0]

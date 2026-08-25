"""Real EC2 instances, behind the same interface as the fake.

The lifecycle is four steps and each is a separate, dry-runnable call:

    launch    run_instances, tagged at creation so nothing is ever untagged
    ready     wait for the instance to run *and* the SSM agent to register
    start     send_command, with output going to CloudWatch
    terminate cancel the command, then terminate the instance

`poll` is read-only. That is a rule, not an accident: the scheduler polls on
every tick, and a mutation hidden inside a read is a mutation that cannot be
dry-run and will eventually happen twice.
"""

from __future__ import annotations

import base64
import datetime as dt
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from ..aws import (
    LIVE_INSTANCE_STATES,
    containerised,
    TAG_COMMAND_ID,
    TAG_JOB_ID,
    TAG_SAMPLER_ID,
    AwsConfig,
    command_log_stream,
    error_code,
    explain,
    is_dry_run_success,
    job_wrapper_script,
    launched_at_of,
    parse_sample_line,
    parse_tags,
    sampler_script,
)
from ..clock import Clock, to_iso
from ..errors import BackendError
from ..environments import Environment
from ..models import Job
from ..money import Currency
from ..volumes import DATA_PATH, VolumeConfig, efs_mount_script
from .base import (
    Allocation,
    BackendStatus,
    Observation,
    Resource,
    UtilizationSample,
    tags_for,
)

# SSM invocation statuses, mapped to what the broker calls them.
_SSM_STATUS = {
    "Pending": BackendStatus.RUNNING,
    "InProgress": BackendStatus.RUNNING,
    "Delayed": BackendStatus.RUNNING,
    "Success": BackendStatus.COMPLETED,
    "Cancelled": BackendStatus.FAILED,
    "TimedOut": BackendStatus.FAILED,
    "Failed": BackendStatus.FAILED,
    "Cancelling": BackendStatus.RUNNING,
    "Undeliverable": BackendStatus.FAILED,
    "Terminated": BackendStatus.FAILED,
}

# AWS-RunShellScript caps executionTimeout at 48 hours.
_MAX_EXECUTION_TIMEOUT = 172_800


class Ec2Backend:
    name: str
    currency = Currency.USD

    def __init__(
        self,
        name: str = "ec2",
        *,
        clock: Clock,
        aws: AwsConfig,
        ec2: Any,
        ssm: Any,
        logs: Any,
        cache_seconds: float = 5.0,
        tier: str | None = None,
        sample_interval_seconds: int = 60,
        spot: bool = False,
        checkpoint_bucket: str = "",
        checkpoint_prefix: str = "checkpoints",
        checkpoint_deadline_seconds: int = 90,
        max_launch_failures: int = 3,
        cooldown_seconds: float = 900.0,
        environments: "Callable[[str], Environment | None] | None" = None,
        volumes: VolumeConfig | None = None,
        ecr_repository: str = "",
        ecr: Any | None = None,
    ) -> None:
        self.name = name
        self.clock = clock
        self.tier = tier if tier is not None else ("spot" if spot else "ondemand")
        self.aws = aws
        self.ec2 = ec2
        self.ssm = ssm
        self.logs = logs

        # One `describe_instances` per tick instead of one per queued job.
        # Twenty people sharing an account reach RequestLimitExceeded faster
        # than you would guess, and every queued job asks about free slots.
        self.cache_seconds = cache_seconds
        self.sample_interval_seconds = sample_interval_seconds
        self.spot = spot
        self.checkpoint_bucket = checkpoint_bucket
        self.checkpoint_prefix = checkpoint_prefix.strip("/")
        self.checkpoint_deadline_seconds = checkpoint_deadline_seconds

        # Spot requests fail in bursts: when a pool is out, it is out for
        # everyone for a while. Counting failures and standing down for a
        # cooldown means the queue falls through to on-demand instead of
        # retrying into the same wall on every tick.
        self.max_launch_failures = max_launch_failures
        self.cooldown_seconds = cooldown_seconds
        self._failures = 0
        self._cooling_until: dt.datetime | None = None

        # A resolver rather than the store: backends know nothing about
        # storage, and keeping it that way is what lets the fake exist.
        self.environments = environments
        self.volumes = volumes or VolumeConfig()
        self.ecr_repository = ecr_repository
        self.ecr = ecr
        self._registry: str | None = None
        self._cache: list[dict] | None = None
        self._cached_at: dt.datetime | None = None

        # Accumulated CloudWatch output per instance, plus the forward token we
        # last read to. Purely an optimisation: after a restart these are empty
        # and the streams are re-read from the head.
        self._log_lines: dict[str, list[tuple[str, str]]] = {}
        self._log_tokens: dict[str, str] = {}
        self._agent_check_error: str | None = None

    # ------------------------------------------------------------- capacity

    def supports(self, gpu_type: str) -> bool:
        if self.spot:
            return bool(self.aws.spot_instance_types.get(gpu_type))
        return gpu_type in self.aws.instance_types

    def free_slots(self, gpu_type: str) -> int:
        if not self.supports(gpu_type):
            return 0
        if self._cooling_until is not None:
            if self.clock.now() < self._cooling_until:
                return 0
            self._cooling_until = None
            self._failures = 0
        limit = self.aws.max_instances.get(gpu_type, 0)
        wanted = self.aws.instance_type_for(gpu_type)
        in_use = sum(
            1 for instance in self._instances() if instance.get("InstanceType") == wanted
        )
        return max(0, limit - in_use)

    # --------------------------------------------------------------- launch

    def launch(self, job: Job) -> Allocation:
        self._guard_capacity(job)
        params = self._run_params(job)
        try:
            response = self.ec2.run_instances(**params)
        except (ClientError, BotoCoreError) as exc:
            self._note_failure()
            raise BackendError(f"could not launch {job.gpu_type}: {explain(exc)}") from exc
        self._failures = 0

        instance = response["Instances"][0]
        self.invalidate()
        return Allocation(
            handle=instance["InstanceId"],
            gpu_type=job.gpu_type,
            tags=parse_tags(instance.get("Tags")),
        )

    def validate_launch(self, job: Job) -> str:
        """Ask EC2 whether this exact call would work, without making it.

        `DryRun` checks IAM and every parameter against the real account, which
        is the only way to find a missing permission before the first real
        launch rather than during it.
        """
        self._guard_capacity(job)
        params = self._run_params(job)
        instance_type = params["InstanceType"]
        try:
            self.ec2.run_instances(**params, DryRun=True)
        except (ClientError, BotoCoreError) as exc:
            if is_dry_run_success(exc):
                return (
                    f"would launch a {instance_type} ({job.gpu_type}) in "
                    f"{self.aws.region} from {self.aws.ami}, tagged to {job.user_id}"
                )
            raise BackendError(f"would fail to launch {job.gpu_type}: {explain(exc)}") from exc
        # Real EC2 always raises on a dry run. Anything else means the flag was
        # dropped somewhere and we came within one call of launching for real.
        raise BackendError(
            "run_instances(DryRun=True) returned without raising DryRunOperation. "
            "Refusing to trust this dry run."
        )

    def _note_failure(self) -> None:
        if not self.spot:
            return
        self._failures += 1
        if self._failures >= self.max_launch_failures:
            self._cooling_until = self.clock.now() + dt.timedelta(seconds=self.cooldown_seconds)

    def _cheapest_type(self, gpu_type: str) -> str:
        """Pick a spot instance type by recent price.

        Capacity-aware in the way that is actually available to us: AWS does not
        publish a capacity signal without `GetSpotPlacementScores`, which needs
        account history, so recent price stands in. A pool that is short is a
        pool whose price has moved.
        """
        candidates = self.aws.spot_instance_types.get(gpu_type) or []
        if not candidates:
            raise BackendError(f"no spot instance types configured for {gpu_type}")
        if len(candidates) == 1:
            return candidates[0]
        try:
            history = self.ec2.describe_spot_price_history(
                InstanceTypes=candidates,
                ProductDescriptions=["Linux/UNIX"],
                MaxResults=len(candidates) * 6,
            )["SpotPriceHistory"]
        except (ClientError, BotoCoreError):
            return candidates[0]  # a pricing hiccup is not a reason to refuse

        best: dict[str, Decimal] = {}
        for entry in history:
            try:
                price = Decimal(entry["SpotPrice"])
            except (KeyError, ArithmeticError, TypeError):
                continue
            name = entry.get("InstanceType", "")
            if name and (name not in best or price < best[name]):
                best[name] = price
        if not best:
            return candidates[0]
        return min(best, key=lambda name: (best[name], candidates.index(name)))

    def _guard_capacity(self, job: Job) -> None:
        self.aws.require_launchable()
        if not self.supports(job.gpu_type):
            raise BackendError(
                f"{self.name} has no instance type mapped for {job.gpu_type}"
            )
        if self.free_slots(job.gpu_type) <= 0:
            limit = self.aws.max_instances.get(job.gpu_type, 0)
            raise BackendError(
                f"{self.name} is at its {job.gpu_type} limit of {limit} instances"
            )

    def _run_params(self, job: Job) -> dict[str, Any]:
        """Every parameter of the launch, in one place.

        Built identically for the real call and the dry run, so the dry run
        genuinely validates what would happen rather than something adjacent.
        """
        tags = [
            {"Key": key, "Value": value}
            for key, value in tags_for(job, to_iso(self.clock.now())).items()
        ]
        instance_type = (
            self._cheapest_type(job.gpu_type)
            if self.spot
            else self.aws.instance_type_for(job.gpu_type)
        )
        params: dict[str, Any] = {
            "ImageId": self.aws.ami,
            "InstanceType": instance_type,
            "MinCount": 1,
            "MaxCount": 1,
            "IamInstanceProfile": {"Name": self.aws.instance_profile},
            "InstanceInitiatedShutdownBehavior": self.aws.shutdown_behavior,
            # Tag at creation, not afterwards. A separate create_tags call can
            # fail, or the broker can die between the two, and the result is an
            # untagged GPU instance that reconciliation cannot attribute to
            # anyone. That is the exact failure this project exists to stop.
            "TagSpecifications": [
                {"ResourceType": "instance", "Tags": tags},
                # Volumes too: an orphaned 200GB gp3 is a smaller bill than an
                # orphaned g5, and just as invisible.
                {"ResourceType": "volume", "Tags": tags},
            ],
            "MetadataOptions": {"HttpTokens": "required"},  # IMDSv2 only
        }
        if self.aws.subnet_id:
            params["SubnetId"] = self.aws.subnet_id
        if self.aws.security_group_ids:
            params["SecurityGroupIds"] = list(self.aws.security_group_ids)
        if self.spot:
            params["InstanceMarketOptions"] = {
                "MarketType": "spot",
                "SpotOptions": {
                    "SpotInstanceType": "one-time",
                    # `terminate`, not `stop` or `hibernate`. The job's state
                    # lives in the checkpoint store, and a stopped instance is
                    # an EBS volume nobody is watching.
                    "InstanceInterruptionBehavior": "terminate",
                },
            }
        return params

    # ---------------------------------------------------------------- start

    def start(self, handle: str, job: Job) -> None:
        """Send the job's command to an instance whose agent has registered."""
        if self._command_id(handle):
            return  # already started; a second send would run the job twice

        timeout = min(int(job.requested_hours * 3600) + 3600, _MAX_EXECUTION_TIMEOUT)
        try:
            response = self.ssm.send_command(
                InstanceIds=[handle],
                DocumentName="AWS-RunShellScript",
                Parameters={
                    "commands": [self._command_for(job)],
                    "workingDirectory": [self.aws.working_dir],
                    "executionTimeout": [str(timeout)],
                },
                # Untruncated output. GetCommandInvocation caps at 24KB, which a
                # training run blows through in seconds.
                CloudWatchOutputConfig={
                    "CloudWatchLogGroupName": self.aws.log_group,
                    "CloudWatchOutputEnabled": True,
                },
                Comment=f"gpu-broker {job.short_id} for {job.user_id}"[:100],
            )
        except (ClientError, BotoCoreError) as exc:
            raise BackendError(f"could not start job on {handle}: {explain(exc)}") from exc

        command_id = response["Command"]["CommandId"]
        sampler_id = self._start_sampler(handle, job)
        # Recorded on the instance, not in memory. The broker can restart between
        # sending this command and noticing it succeeded; without a durable
        # record it would send a second one and run the job twice.
        tags = {TAG_COMMAND_ID: command_id}
        if sampler_id:
            tags[TAG_SAMPLER_ID] = sampler_id
        self._tag(handle, tags)
        self.invalidate()

    def _start_sampler(self, handle: str, job: Job) -> str | None:
        """Start the GPU watcher alongside the job. Best effort.

        A failure here must not stop the job: idle detection is a nice-to-have
        and the training run is the point. It is logged by returning None, and
        `sample_utilization` simply finds nothing.
        """
        duration = int(job.requested_hours * 3600) + 3600
        try:
            response = self.ssm.send_command(
                InstanceIds=[handle],
                DocumentName="AWS-RunShellScript",
                Parameters={
                    "commands": [
                        sampler_script(duration, self.sample_interval_seconds)
                    ],
                    "executionTimeout": [str(min(duration, _MAX_EXECUTION_TIMEOUT))],
                },
                CloudWatchOutputConfig={
                    "CloudWatchLogGroupName": self.aws.log_group,
                    "CloudWatchOutputEnabled": True,
                },
                Comment=f"gpu-broker sampler {job.short_id}"[:100],
            )
        except (ClientError, BotoCoreError):
            return None
        return response["Command"]["CommandId"]

    def _environment_for(self, job: Job) -> "Environment | None":
        if not job.environment or self.environments is None:
            return None
        return self.environments(job.environment)

    def _repository_uri(self) -> str | None:
        """`<account>.dkr.ecr.<region>.amazonaws.com/<repo>`, created if absent.

        Looked up rather than assembled from an account id we would have to ask
        STS for, and cached: it does not change.
        """
        if not self.ecr_repository or self.ecr is None:
            return None
        if self._registry is not None:
            return self._registry or None
        try:
            found = self.ecr.describe_repositories(repositoryNames=[self.ecr_repository])
            uri = found["repositories"][0]["repositoryUri"]
        except (ClientError, BotoCoreError) as exc:
            if error_code(exc) != "RepositoryNotFoundException":
                self._registry = ""
                return None
            try:
                uri = self.ecr.create_repository(repositoryName=self.ecr_repository)[
                    "repository"
                ]["repositoryUri"]
            except (ClientError, BotoCoreError):
                self._registry = ""
                return None
        self._registry = uri
        return uri

    def image_for(self, environment: "Environment") -> str | None:
        uri = self._repository_uri()
        return f"{uri}:{environment.digest}" if uri else None

    def cached(self, environment: "Environment") -> bool:
        """Has this exact spec already been built and pushed?

        A miss is not an error -- the first job that wants it builds it on the
        instance and pushes, so the second one pulls.
        """
        if self.ecr is None or not self.ecr_repository:
            return False
        try:
            self.ecr.describe_images(
                repositoryName=self.ecr_repository,
                imageIds=[{"imageTag": environment.digest}],
            )
        except (ClientError, BotoCoreError):
            return False
        return True

    def _command_for(self, job: Job) -> str:
        """What actually runs on the instance.

        With no checkpoint bucket configured this is the user's command, plain.
        With one, it is wrapped: restore, run, watch for the interruption notice,
        and upload whatever the job saves. The watching has to happen here rather
        than in the broker -- two minutes is less than a tick.
        """
        environment = self._environment_for(job)
        image = self.image_for(environment) if environment else None
        checkpoint_dir = "/tmp/gpu-broker/checkpoint"
        resume_dir = "/tmp/gpu-broker/resume"

        inner = job.command
        if image:
            registry = image.rsplit("/", 1)[0].split(":")[0]
            inner = containerised(
                image=image,
                command=job.command,
                checkpoint_dir=checkpoint_dir,
                resume_dir=resume_dir,
                data_path=DATA_PATH,
                registry=registry,
                region=self.aws.region,
                dockerfile_b64=base64.b64encode(environment.dockerfile().encode()).decode(),
            )

        setup = (
            efs_mount_script(self.volumes, job.user_id, self.aws.region)
            if self.volumes.enabled
            else ""
        )

        if not self.checkpoint_bucket:
            if not setup and inner == job.command:
                return job.command
            return f"{setup}\n{inner}" if setup else inner

        base = f"s3://{self.checkpoint_bucket}/{self.checkpoint_prefix}/{job.job_id}"
        # `checkpoint_key` is the S3 key the store recorded, not a URI.
        resume_uri = (
            f"s3://{self.checkpoint_bucket}/{job.checkpoint_key}"
            if job.checkpoint_key
            else ""
        )
        return job_wrapper_script(
            command=inner,
            checkpoint_dir=checkpoint_dir,
            resume_dir=resume_dir,
            resume_uri=resume_uri,
            upload_uri=base,
            job_id=job.job_id,
            deadline_seconds=self.checkpoint_deadline_seconds,
            setup=setup,
        )

    # ----------------------------------------------------------------- poll

    def poll(self, handle: str) -> Observation:
        instance = self._instance(handle)
        if instance is None:
            return Observation(BackendStatus.GONE, detail="no such instance")

        state = instance["State"]["Name"]
        tags = parse_tags(instance.get("Tags"))
        command_id = tags.get(TAG_COMMAND_ID)

        if state in ("shutting-down", "terminated"):
            # If the command finished before the instance went away, the job
            # succeeded and the teardown is just teardown.
            if command_id:
                observation = self._invocation(handle, command_id)
                if observation.status in (BackendStatus.COMPLETED, BackendStatus.FAILED):
                    return observation
            # A preemption and somebody hitting Terminate in the console look
            # the same in `describe-instances`, and they mean opposite things:
            # one should requeue the job, the other should not. AWS does give a
            # signal for the real thing, so use it rather than guessing.
            reason = (instance.get("StateReason") or {}).get("Code", "")
            spot = instance.get("InstanceLifecycle") == "spot"
            if reason == "Server.SpotInstanceTermination" or spot:
                return Observation(
                    BackendStatus.INTERRUPTED,
                    detail=(
                        "spot capacity was reclaimed"
                        if reason == "Server.SpotInstanceTermination"
                        else f"spot instance {state} before the job finished"
                    ),
                )
            return Observation(
                BackendStatus.FAILED,
                detail=(
                    f"the instance was {state} outside the broker before the job "
                    "finished. On-demand capacity is not reclaimed, so somebody "
                    "terminated it"
                ),
            )

        if state in ("stopping", "stopped"):
            return Observation(
                BackendStatus.FAILED,
                detail=f"instance {state}; it was stopped outside the broker",
            )

        if state == "pending":
            return Observation(BackendStatus.PENDING, detail="instance booting")

        # running
        if command_id is None:
            if self._agent_online(handle):
                return Observation(BackendStatus.READY, detail="ssm agent registered")
            waited = self._seconds_since_launch(tags)
            if waited is not None and waited > self.aws.agent_timeout_seconds:
                because = (
                    f" Last error from SSM: {self._agent_check_error}."
                    if self._agent_check_error
                    else ""
                )
                return Observation(
                    BackendStatus.FAILED,
                    detail=(
                        f"the SSM agent never registered after {waited / 60:.0f} minutes. "
                        f"Check that {self.aws.ami} has the agent and that "
                        f"{self.aws.instance_profile} grants "
                        f"AmazonSSMManagedInstanceCore.{because}"
                    ),
                )
            return Observation(BackendStatus.PENDING, detail="waiting for the ssm agent")

        return self._invocation(handle, command_id)

    def _invocation(self, handle: str, command_id: str) -> Observation:
        try:
            result = self.ssm.get_command_invocation(
                CommandId=command_id, InstanceId=handle
            )
        except (ClientError, BotoCoreError) as exc:
            if error_code(exc) in ("InvocationDoesNotExist", "InvalidCommandId"):
                # Accepted by SSM but not yet visible against this instance.
                # Real SSM says InvocationDoesNotExist; the code is different
                # enough by path that both are worth accepting rather than
                # failing a job over a propagation delay.
                return Observation(BackendStatus.RUNNING, detail="command dispatching")
            raise BackendError(f"could not poll {handle}: {explain(exc)}") from exc

        status = result.get("Status", "InProgress")
        mapped = _SSM_STATUS.get(status, BackendStatus.RUNNING)
        exit_code = result.get("ResponseCode")
        detail = result.get("StatusDetails") or status
        if mapped is BackendStatus.FAILED:
            detail = f"{status}: {result.get('StandardErrorContent') or detail}".strip()
        return Observation(
            mapped,
            exit_code=exit_code if exit_code is not None and exit_code >= 0 else None,
            detail=detail[:500],
        )

    def _agent_online(self, handle: str) -> bool:
        """Has the SSM agent on this instance checked in yet?

        Any failure here means "not yet". This is polled every tick, so a
        transient error resolves itself, and a permanent one (a missing
        ssm:DescribeInstanceInformation permission, say) surfaces through the
        agent timeout with the reason attached rather than as a crash on the
        first poll.
        """
        try:
            info = self.ssm.describe_instance_information(
                Filters=[{"Key": "InstanceIds", "Values": [handle]}]
            )
        except Exception as exc:  # noqa: BLE001 - see docstring
            self._agent_check_error = explain(exc) if error_code(exc) else str(exc)
            return False
        self._agent_check_error = None
        return any(
            entry.get("PingStatus") == "Online"
            for entry in info.get("InstanceInformationList", [])
        )

    def _seconds_since_launch(self, tags: dict[str, str]) -> float | None:
        launched = launched_at_of(tags)
        if launched is None:
            return None
        return (self.clock.now() - launched).total_seconds()

    # ----------------------------------------------------------------- logs

    def sample_utilization(self, handle: str) -> list[UtilizationSample]:
        instance = self._instance(handle)
        if instance is None:
            return []
        sampler_id = parse_tags(instance.get("Tags")).get(TAG_SAMPLER_ID)
        if not sampler_id:
            return []

        samples: list[UtilizationSample] = []
        for line in self._read_stream(sampler_id, handle, "stdout"):
            parsed = parse_sample_line(line[1])
            if parsed is None:
                continue
            at, percent, memory = parsed
            samples.append(
                UtilizationSample(at=at, gpu_percent=percent, memory_mb=memory)
            )
        return samples

    def fetch_logs(self, handle: str, after: int = 0) -> list[tuple[str, str]]:
        command_id = self._command_id(handle)
        if not command_id:
            return []
        lines = self._log_lines.setdefault(handle, [])
        for stream in ("stdout", "stderr"):
            lines.extend(self._read_stream(command_id, handle, stream))
        # Both streams are appended per poll, so ordering is by poll then by
        # stream rather than strictly by wall clock. Interleaving stdout and
        # stderr exactly would need a merge across paginated reads of two
        # streams, which is not worth it while a tick is seconds wide.
        return lines[after:]

    def _read_stream(
        self, command_id: str, handle: str, stream: str
    ) -> list[tuple[str, str]]:
        name = command_log_stream(command_id, handle, stream)
        token = self._log_tokens.get(name)
        params: dict[str, Any] = {
            "logGroupName": self.aws.log_group,
            "logStreamName": name,
            "startFromHead": True,
        }
        if token:
            params["nextToken"] = token
        try:
            response = self.logs.get_log_events(**params)
        except (ClientError, BotoCoreError) as exc:
            if error_code(exc) == "ResourceNotFoundException":
                return []  # nothing has been written yet
            raise BackendError(f"could not read logs for {handle}: {explain(exc)}") from exc

        events = response.get("events", [])
        forward = response.get("nextForwardToken")
        if forward:
            self._log_tokens[name] = forward
        return [(stream, event["message"].rstrip("\n")) for event in events]

    # ------------------------------------------------------------ terminate

    def terminate(self, handle: str, reason: str) -> None:
        """Stop and release. Safe to call on something already gone."""
        command_id = self._command_id(handle)
        if command_id:
            try:
                self.ssm.cancel_command(CommandId=command_id, InstanceIds=[handle])
            except Exception:  # noqa: BLE001 - deliberately total
                # Nothing that happens here may stop the terminate below. The
                # command may already have finished, SSM may be throttling, the
                # instance may already be gone. Every one of those is fine; an
                # instance left running because a best-effort cancel raised is
                # not. This catch is broad on purpose.
                pass
        try:
            self.ec2.terminate_instances(InstanceIds=[handle])
        except (ClientError, BotoCoreError) as exc:
            if error_code(exc) in ("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"):
                return
            raise BackendError(f"could not terminate {handle}: {explain(exc)}") from exc
        finally:
            self.invalidate()

    def validate_terminate(self, handle: str) -> str:
        # Checked before the dry run, because EC2 evaluates DryRun before it
        # looks the instance up: a dry run against a machine that no longer
        # exists still reports DryRunOperation, which would have this cheerfully
        # promise to terminate something that is already gone.
        if self._instance(handle) is None:
            return f"{handle} is already gone"
        try:
            self.ec2.terminate_instances(InstanceIds=[handle], DryRun=True)
        except (ClientError, BotoCoreError) as exc:
            if is_dry_run_success(exc):
                return f"would terminate {handle}"
            if error_code(exc) in ("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"):
                return f"{handle} is already gone"
            raise BackendError(f"would fail to terminate {handle}: {explain(exc)}") from exc
        raise BackendError(
            "terminate_instances(DryRun=True) returned without raising DryRunOperation. "
            "Refusing to trust this dry run."
        )

    # ------------------------------------------------------- reconciliation

    def list_resources(self) -> list[Resource]:
        return [
            Resource(
                handle=instance["InstanceId"],
                gpu_type=self._gpu_type_of(instance.get("InstanceType", "")),
                status=self._coarse_status(instance),
                tags=parse_tags(instance.get("Tags")),
            )
            for instance in self._instances()
        ]

    def _coarse_status(self, instance: dict) -> BackendStatus:
        state = instance["State"]["Name"]
        if state == "pending":
            return BackendStatus.PENDING
        if state in ("stopping", "stopped"):
            return BackendStatus.FAILED
        tags = parse_tags(instance.get("Tags"))
        return BackendStatus.RUNNING if tags.get(TAG_COMMAND_ID) else BackendStatus.READY

    def _gpu_type_of(self, instance_type: str) -> str:
        for gpu_type, mapped in self.aws.instance_types.items():
            if mapped == instance_type:
                return gpu_type
        return instance_type

    # -------------------------------------------------------------- helpers

    def invalidate(self) -> None:
        self._cache = None
        self._cached_at = None

    def _instances(self) -> list[dict]:
        """Every live instance the broker has tagged, cached for a few seconds.

        Filtered on our tag key, so instances launched by hand or by another
        tool are invisible here. `gpu reap` is what looks at the whole account.
        """
        now = self.clock.now()
        if (
            self._cache is not None
            and self._cached_at is not None
            and (now - self._cached_at).total_seconds() < self.cache_seconds
        ):
            return self._cache

        found: list[dict] = []
        try:
            paginator = self.ec2.get_paginator("describe_instances")
            pages = paginator.paginate(
                Filters=[
                    {"Name": "tag-key", "Values": [TAG_JOB_ID]},
                    {"Name": "instance-state-name", "Values": list(LIVE_INSTANCE_STATES)},
                ]
            )
            for page in pages:
                for reservation in page.get("Reservations", []):
                    found.extend(reservation.get("Instances", []))
        except (ClientError, BotoCoreError) as exc:
            raise BackendError(f"could not list instances: {explain(exc)}") from exc

        self._cache = found
        self._cached_at = now
        return found

    def _instance(self, handle: str) -> dict | None:
        """Find one instance, including ones that are no longer live.

        `_instances` filters to running states because that is what capacity and
        reconciliation care about. `poll` cares about the opposite: an instance
        that has just been terminated underneath a running job is the single most
        important thing it can observe, and the filtered listing hides exactly
        that. So the cache is checked first, and anything not in it gets a direct
        lookup -- one extra call, only for instances that have left the live set.
        """
        for instance in self._instances():
            if instance["InstanceId"] == handle:
                return instance
        return self._describe_one(handle)

    def _describe_one(self, handle: str) -> dict | None:
        try:
            response = self.ec2.describe_instances(InstanceIds=[handle])
        except (ClientError, BotoCoreError) as exc:
            if error_code(exc) in (
                "InvalidInstanceID.NotFound",
                "InvalidInstanceID.Malformed",
            ):
                return None
            raise BackendError(f"could not describe {handle}: {explain(exc)}") from exc
        for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                return instance
        return None

    def _command_id(self, handle: str) -> str | None:
        instance = self._instance(handle)
        if instance is None:
            return None
        return parse_tags(instance.get("Tags")).get(TAG_COMMAND_ID)

    def _tag(self, handle: str, tags: dict[str, str]) -> None:
        try:
            self.ec2.create_tags(
                Resources=[handle],
                Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
            )
        except (ClientError, BotoCoreError) as exc:
            raise BackendError(f"could not tag {handle}: {explain(exc)}") from exc

"""Everything that knows AWS exists.

Kept in one module so the rest of the broker stays testable without boto3, and
so that "what does this need from our account" is one file rather than a hunt.

The execution path is SSM Run Command with output shipped to CloudWatch Logs.
That choice buys three things worth naming, because they are the reasons it beats
SSH for a club sharing one account:

  No inbound ports and no key pairs. Nothing to leak, nothing to rotate, and a
  compromised broker host does not hand over every instance.

  Untruncated logs. `GetCommandInvocation` caps output at 24KB, which is nothing
  for a training run. `CloudWatchOutputConfig` sends the full stream to
  CloudWatch instead, and it survives the instance.

  A live channel to a running job. Phase 4 has to tell a job to checkpoint inside
  a two-minute spot notice. A second `SendCommand` can do that; a user-data
  script baked in at launch cannot.
"""

from __future__ import annotations

import datetime as dt
import shlex
from dataclasses import dataclass, field, replace
from typing import Any

from .errors import BackendError, ConfigError

# --- tags ------------------------------------------------------------------
#
# Reconciliation and `gpu reap` work off these and nothing else, so an orphaned
# instance is always attributable to a person. Defined here, used everywhere.

TAG_JOB_ID = "broker-job-id"
TAG_USER = "broker-user"
TAG_LAUNCHED_AT = "broker-launched-at"
TAG_COMMAND_ID = "broker-command-id"
"""Written after SSM accepts the command.

Deliberately stored on the instance rather than in the broker's memory: the
broker can be restarted, and on the way back up it has to be able to find the
command it already started without starting a second one.
"""

PRICING_REGION = "us-east-1"
"""Where the Price List API is served from. Not where instances run."""

TAG_SAMPLER_ID = "broker-sampler-id"
"""The SSM command that is watching this instance's GPU. Tagged for the same
reason as the job's command: the broker can restart, and on the way back up it
has to find the sampler it already started rather than starting a second one."""

BROKER_TAG_PREFIX = "broker-"

# States in which EC2 is still charging us for an instance.
LIVE_INSTANCE_STATES = ("pending", "running", "stopping", "stopped")

SSM_PLUGIN = "aws-runShellScript"
"""The plugin name AWS-RunShellScript reports under. It appears in the
CloudWatch stream name, so it is not cosmetic."""


def containerised(
    *,
    image: str,
    command: str,
    checkpoint_dir: str,
    resume_dir: str,
    data_path: str,
    registry: str,
    region: str,
    dockerfile_b64: str,
) -> str:
    """Run the user's command inside their environment's image.

    Pull first; build only on a miss, and push what was built so the next job
    that wants this environment waits for a pull instead. The first job pays for
    the build, everybody after it does not, and no separate build server has to
    exist.

    Paths are bound through unchanged rather than remapped, so
    `GPU_BROKER_CHECKPOINT_DIR` means the same thing inside the container as
    outside and the adapters do not need to know they are in one.

    `docker run` in the foreground forwards signals to PID 1 by default, which
    is what lets the interruption watcher still reach the job with SIGUSR1.
    """
    return f"""IMAGE={shlex.quote(image)}
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  aws ecr get-login-password --region {region} 2>/dev/null \
    | docker login --username AWS --password-stdin {registry} >/dev/null 2>&1 || true
  if ! docker pull "$IMAGE" >/dev/null 2>&1; then
    echo "gpu-broker: no cached image for this environment; building it once" >&2
    mkdir -p /tmp/gpu-broker-build
    printf '%s' {shlex.quote(dockerfile_b64)} | base64 -d > /tmp/gpu-broker-build/Dockerfile
    docker build -t "$IMAGE" /tmp/gpu-broker-build || exit 1
    docker push "$IMAGE" >/dev/null 2>&1 \
      || echo "gpu-broker: built it, but could not push; the next job rebuilds" >&2
  fi
fi
docker run --rm --gpus all --ipc=host \
  --name gpu-broker-job \
  -v {shlex.quote(checkpoint_dir)}:{shlex.quote(checkpoint_dir)} \
  -v {shlex.quote(resume_dir)}:{shlex.quote(resume_dir)} \
  -v {shlex.quote(data_path)}:{shlex.quote(data_path)} \
  -e GPU_BROKER_CHECKPOINT_DIR -e GPU_BROKER_RESUME_DIR \
  -e GPU_BROKER_JOB_ID -e GPU_BROKER_CHECKPOINT_DEADLINE \
  -w {shlex.quote(data_path)} \
  "$IMAGE" bash -lc {shlex.quote(command)}"""


def job_wrapper_script(
    *,
    command: str,
    checkpoint_dir: str,
    resume_dir: str,
    resume_uri: str,
    upload_uri: str,
    job_id: str,
    deadline_seconds: int,
    poll_seconds: int = 5,
    setup: str = "",
) -> str:
    """Restore, run, and survive the two-minute notice.

    Everything in here has to happen *on the instance*, because two minutes is
    not enough time to go through the broker. The broker's tick can be a minute
    apart; by the time it noticed, the machine would be gone.

    The order matters:

      Restore first, so the job starts from where it stopped.
      Run the user's command as a child, so we still have a process to signal.
      Poll IMDS every few seconds for `spot/instance-action`.
      On a notice: SIGUSR1, then wait for the job's own "I have finished writing"
      marker before uploading. Uploading a directory mid-write produces a
      checkpoint that loads without complaint and trains garbage.
      Upload the files, then the manifest, so a cut-off upload leaves the
      previous checkpoint as the newest complete one.
    """
    return f"""set -u
CKPT={checkpoint_dir}
RESUME={resume_dir}
mkdir -p "$CKPT" "$RESUME"

if [ -n "{resume_uri}" ]; then
  aws s3 cp --recursive --quiet "{resume_uri}" "$RESUME" ||     echo "gpu-broker: could not restore a checkpoint, starting from the beginning" >&2
fi

export GPU_BROKER_CHECKPOINT_DIR="$CKPT"
export GPU_BROKER_RESUME_DIR="$RESUME"
export GPU_BROKER_JOB_ID={job_id}
export GPU_BROKER_CHECKPOINT_DEADLINE={deadline_seconds}

{setup}

bash -lc {_sh_quote(command)} &
JOB=$!

upload() {{
  STEP=$(cat "$CKPT/CHECKPOINT_COMPLETE" 2>/dev/null || echo 0)
  DEST="{upload_uri}/step-$(printf '%012d' "$STEP")"
  aws s3 cp --recursive --quiet --exclude CHECKPOINT_COMPLETE "$CKPT" "$DEST" || return 1
  python3 - "$CKPT" "$STEP" > /tmp/MANIFEST.json <<'PYEOF'
import json, os, sys, datetime
root, step = sys.argv[1], int(sys.argv[2])
files, total = [], 0
for base, _, names in os.walk(root):
    for name in names:
        if name == "CHECKPOINT_COMPLETE":
            continue
        path = os.path.join(base, name)
        files.append(os.path.relpath(path, root))
        total += os.path.getsize(path)
print(json.dumps({{
    "step": step,
    "written_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds"),
    "files": sorted(files),
    "bytes": total,
}}))
PYEOF
  aws s3 cp --quiet /tmp/MANIFEST.json "$DEST/MANIFEST.json"
}}

while kill -0 $JOB 2>/dev/null; do
  TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token"     -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
  CODE=$(curl -sf -o /dev/null -w '%{{http_code}}'     -H "X-aws-ec2-metadata-token: $TOKEN"     http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null || echo 404)
  if [ "$CODE" = "200" ]; then
    echo "gpu-broker: spot interruption notice; asking the job to checkpoint" >&2
    kill -USR1 $JOB 2>/dev/null || true
    WAITED=0
    while [ ! -f "$CKPT/CHECKPOINT_COMPLETE" ] && [ "$WAITED" -lt {deadline_seconds} ]; do
      sleep 1
      WAITED=$((WAITED + 1))
    done
    if [ -f "$CKPT/CHECKPOINT_COMPLETE" ]; then
      upload && echo "gpu-broker: checkpoint uploaded" >&2
    else
      echo "gpu-broker: the job did not checkpoint in {deadline_seconds}s; nothing to save" >&2
    fi
    kill -TERM $JOB 2>/dev/null || true
    exit 0
  fi
  sleep {poll_seconds}
done

wait $JOB
exit $?
"""


def _sh_quote(text: str) -> str:
    import shlex

    return shlex.quote(text)


def sampler_script(duration_seconds: int, interval_seconds: int) -> str:
    """A loop that prints what the GPU is doing, forever-ish.

    Sent as its own SSM command so its output lands in its own CloudWatch
    stream. That means the broker reads samples with `logs:GetLogEvents` -- a
    call it already makes -- rather than sending an SSM command per job per
    tick, which has agent-poll latency measured in seconds and would make a
    tick as slow as the number of jobs running.

    Each line carries its own timestamp because the sample happened on the
    machine, possibly minutes before anything read it.
    """
    return (
        f"end=$(( $(date +%s) + {duration_seconds} )); "
        "while [ $(date +%s) -lt $end ]; do "
        'printf "%s,%s\n" "$(date +%s)" '
        '"$(nvidia-smi --query-gpu=utilization.gpu,memory.used '
        '--format=csv,noheader,nounits 2>/dev/null | head -1)"; '
        f"sleep {interval_seconds}; done"
    )


def parse_sample_line(line: str) -> tuple[float, float, int] | None:
    """`<unix seconds>, <gpu percent>, <memory MB>`, or None if unusable."""
    parts = [part.strip() for part in line.split(",")]
    if len(parts) < 3:
        return None
    try:
        return float(parts[0]), float(parts[1]), int(float(parts[2]))
    except ValueError:
        return None


def command_log_stream(command_id: str, instance_id: str, stream: str) -> str:
    """Where SSM puts a command's output when CloudWatch output is enabled.

    The shape is fixed by SSM: `<command>/<instance>/<plugin>/<stdout|stderr>`.
    """
    return f"{command_id}/{instance_id}/{SSM_PLUGIN}/{stream}"


@dataclass(frozen=True)
class AwsConfig:
    """What the broker needs from the club's AWS account.

    Everything with a `None` default is something a human has to supply before
    a real instance can be launched. They are checked at launch with a message
    naming the setting, rather than surfacing as a boto3 ParamValidationError.
    """

    region: str = "us-west-2"

    ami: str | None = None
    """A GPU AMI with the SSM agent. The Deep Learning AMI and Amazon Linux 2
    both ship it; a bare Ubuntu image does not."""

    instance_profile: str | None = None
    """IAM instance profile name. Needs AmazonSSMManagedInstanceCore so the agent
    can register, and logs:PutLogEvents so command output reaches CloudWatch."""

    log_group: str = "/gpu-broker/jobs"

    subnet_id: str | None = None
    security_group_ids: tuple[str, ...] = ()

    instance_types: dict[str, str] = field(
        default_factory=lambda: {
            "t4": "g4dn.xlarge",
            "a10g": "g5.xlarge",
            "l4": "g6.xlarge",
            "a100": "p4d.24xlarge",
        }
    )
    spot_instance_types: dict[str, list[str]] = field(
        default_factory=lambda: {
            "t4": ["g4dn.xlarge", "g4dn.2xlarge"],
            "a10g": ["g5.xlarge", "g5.2xlarge", "g5.4xlarge"],
            "l4": ["g6.xlarge", "g6.2xlarge"],
        }
    )
    """Candidates for a spot request, in no particular order -- the cheapest
    with recent capacity is picked at launch. One instance type is one AZ's
    worth of luck; several is the difference between a spot pool that usually
    has room and one that usually does not."""

    max_instances: dict[str, int] = field(
        default_factory=lambda: {"t4": 4, "a10g": 4, "l4": 2, "a100": 1}
    )
    """Per-type concurrency the broker will not exceed, independent of quota.
    A quota error is a bad way to discover a limit; this is the polite one."""

    agent_timeout_seconds: float = 900.0
    """How long to wait for the SSM agent to register before calling the
    instance broken. A g5 with a Deep Learning AMI is usually under three
    minutes; fifteen is the point at which something is actually wrong."""

    working_dir: str = "/home/ubuntu"
    shutdown_behavior: str = "terminate"
    """`terminate`, not `stop`. A stopped GPU instance still holds its EBS volume
    and is invisible in most "what is running" views, which is exactly how a
    forgotten instance becomes a forgotten bill."""

    def instance_type_for(self, gpu_type: str) -> str:
        try:
            return self.instance_types[gpu_type]
        except KeyError:
            raise ConfigError(
                f"no EC2 instance type mapped for gpu type {gpu_type!r}. "
                f"Add it under aws.instance_types in config.json. "
                f"Known: {', '.join(sorted(self.instance_types))}"
            ) from None

    def require_launchable(self) -> None:
        """Fail before calling AWS, with the setting named."""
        missing = [
            name
            for name, value in (("aws.ami", self.ami), ("aws.instance_profile", self.instance_profile))
            if not value
        ]
        if missing:
            raise ConfigError(
                f"cannot launch: {' and '.join(missing)} is not set in config.json. "
                "The AMI needs the SSM agent; the instance profile needs "
                "AmazonSSMManagedInstanceCore and logs:PutLogEvents."
            )


def load_aws_config(raw: dict[str, Any] | None) -> AwsConfig:
    if not raw:
        return AwsConfig()
    known = {f for f in AwsConfig.__dataclass_fields__}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"unknown keys under 'aws' in config.json: {', '.join(sorted(unknown))}. "
            f"Known: {', '.join(sorted(known))}"
        )
    settings = dict(raw)
    if "security_group_ids" in settings:
        settings["security_group_ids"] = tuple(settings["security_group_ids"])
    return replace(AwsConfig(), **settings)


# --- clients ---------------------------------------------------------------


def make_clients(config: AwsConfig) -> dict[str, Any]:
    """Build the three boto3 clients the EC2 backend needs.

    Imported lazily so that a machine with no boto3 -- or no interest in AWS --
    can still run the whole broker against the fake backend.
    """
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise BackendError(
            "the EC2 backend needs boto3. Install it with: pip install 'gpu-broker[aws]'"
        ) from exc

    # Adaptive retries back off on throttling instead of hammering. Twenty people
    # sharing one account hit RequestLimitExceeded sooner than you would think.
    botocore_config = Config(
        region_name=config.region,
        retries={"max_attempts": 6, "mode": "adaptive"},
    )
    clients = {
        name: boto3.client(name, config=botocore_config)
        for name in ("ec2", "ssm", "logs")
    }
    # The Price List API lives in a handful of regions and not necessarily the
    # one we launch instances in. Prices are global anyway; the filter carries
    # the region we care about.
    clients["pricing"] = boto3.client(
        "pricing",
        config=Config(region_name=PRICING_REGION, retries={"max_attempts": 4, "mode": "adaptive"}),
    )
    return clients


# --- error translation -----------------------------------------------------


class DryRunSucceeded(Exception):
    """AWS confirmed the call would have worked.

    EC2 signals a successful dry run by *raising* `DryRunOperation`, which is a
    strange shape to propagate upward. Caught at the boundary and turned into
    this, so callers can write `except DryRunSucceeded` and mean it.
    """


def error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code")
    return None


def is_dry_run_success(exc: Exception) -> bool:
    return error_code(exc) == "DryRunOperation"


def explain(exc: Exception) -> str:
    """Turn a botocore error into something a club member can act on."""
    code = error_code(exc)
    message = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        message = response.get("Error", {}).get("Message", "")

    hints = {
        "UnauthorizedOperation": (
            "the broker's IAM user is missing an EC2 permission for this call"
        ),
        "AccessDeniedException": "the broker's IAM user is missing a permission for this call",
        "VcpuLimitExceeded": (
            "your G/P vCPU quota is exhausted. Request an increase in Service Quotas, "
            "or use the local pool"
        ),
        "InsufficientInstanceCapacity": (
            "AWS has no capacity for this instance type in this AZ right now. "
            "The job stays queued and will be retried"
        ),
        "InvalidAMIID.NotFound": "aws.ami does not exist in this region",
        "InvalidParameterValue": "one of the aws.* settings in config.json is wrong",
        "RequestLimitExceeded": "AWS is throttling us; the job stays queued",
        "InvalidInstanceId": "the instance is gone, or the SSM agent never registered",
    }
    hint = hints.get(code or "")
    text = f"{code}: {message}" if code else str(exc)
    return f"{text} ({hint})" if hint else text


def parse_tags(raw: list[dict[str, str]] | None) -> dict[str, str]:
    return {tag["Key"]: tag["Value"] for tag in (raw or [])}


def launched_at_of(tags: dict[str, str]) -> dt.datetime | None:
    stamp = tags.get(TAG_LAUNCHED_AT)
    if not stamp:
        return None
    try:
        parsed = dt.datetime.fromisoformat(stamp)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)

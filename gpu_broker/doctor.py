"""`gpu doctor`: what is wrong, in words somebody can act on.

Every check answers one question and, when the answer is bad, says what to do
about it. A diagnostic that reports `AccessDenied` and stops has told you that
something is broken, which you already knew.

Nothing here changes anything. It is safe to run at any time, including from a
member's laptop while jobs are running.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

OK = "ok"
WARN = "warn"
FAIL = "fail"


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str
    fix: str = ""

    @property
    def bad(self) -> bool:
        return self.status == FAIL


def _run(name: str, fn: Callable[[], Check]) -> Check:
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - a diagnostic that crashes is useless
        return Check(name, FAIL, f"the check itself failed: {exc}")


def diagnose(broker: Any) -> list[Check]:
    """Everything, in the order a problem would bite."""
    config = broker.config
    checks: list[Check] = [
        _run("config", lambda: _config(config)),
        _run("database", lambda: _database(broker)),
        _run("checkpoints", lambda: _checkpoints(broker)),
    ]

    for name in config.backends:
        if name == "fake":
            checks.append(
                Check(
                    "backend: fake",
                    OK,
                    "the simulator is available, so the broker works with no AWS account",
                )
            )
        elif name.startswith("ec2"):
            checks.extend(_aws(broker, name))
        elif name == "local":
            checks.extend(_local(broker))

    checks.append(_run("web", lambda: _web(config)))
    return checks


def _config(config) -> Check:
    missing = [name for name in config.backends if name not in ("fake", "ec2", "ec2-spot", "local")]
    if missing:
        return Check("config", FAIL, f"unknown backends: {missing}")
    return Check(
        "config",
        OK,
        f"backends {list(config.backends)}, placement {list(config.placement_order)}",
    )


def _database(broker) -> Check:
    from .db.migrations import LATEST_VERSION, current_version

    version = current_version(broker.store.conn)
    users = len(broker.users())
    if version != LATEST_VERSION:
        return Check(
            "database",
            FAIL,
            f"schema is at version {version}, this build expects {LATEST_VERSION}",
            "run any `gpu` command to migrate, or check you are not running an old binary",
        )
    journal = broker.store.conn.execute("PRAGMA journal_mode").fetchone()[0]
    if journal.lower() != "wal":
        return Check(
            "database",
            WARN,
            f"journal mode is {journal}, not WAL. Crash durability is weaker than intended",
        )
    return Check("database", OK, f"schema v{version}, WAL, {users} member(s)")


def _checkpoints(broker) -> Check:
    store = getattr(broker, "checkpoints", None)
    if store is None:
        return Check("checkpoints", WARN, "no checkpoint store, so preempted jobs start over")

    kind = type(store).__name__
    if kind == "LocalCheckpointStore":
        root = Path(store.root)
        try:
            root.mkdir(parents=True, exist_ok=True)
            probe = root / ".doctor"
            probe.write_text("ok")
            probe.unlink()
        except OSError as exc:
            return Check("checkpoints", FAIL, f"cannot write to {root}: {exc}",
                         "check the directory exists and is writable")
        if "ec2-spot" in broker.config.backends:
            return Check(
                "checkpoints", FAIL,
                "spot is configured but checkpoints are on local disk",
                "a preempted job comes back on a different machine, so set "
                "checkpoint_store to 's3'",
            )
        return Check("checkpoints", OK, f"local, at {root}")

    try:
        store.client.head_bucket(Bucket=store.bucket)
    except Exception as exc:  # noqa: BLE001
        return Check(
            "checkpoints", FAIL, f"cannot reach s3://{store.bucket}: {_short(exc)}",
            "create the bucket, or grant s3:ListBucket and s3:GetObject on it",
        )
    return Check("checkpoints", OK, f"s3://{store.bucket}/{store.prefix}")


def _aws(broker, backend_name: str) -> list[Check]:
    config = broker.config
    out: list[Check] = []
    try:
        from .aws import make_clients

        clients = make_clients(config.aws)
    except Exception as exc:  # noqa: BLE001
        return [Check(f"backend: {backend_name}", FAIL, f"boto3 is not usable: {_short(exc)}",
                      "pip install 'gpu-broker'")]

    import boto3

    try:
        who = boto3.client("sts", region_name=config.aws.region).get_caller_identity()
        out.append(
            Check("aws credentials", OK, f"{who.get('Arn', 'unknown')} in {config.aws.region}")
        )
    except Exception as exc:  # noqa: BLE001
        return out + [
            Check("aws credentials", FAIL, _short(exc),
                  "run `aws configure`, or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        ]

    if not config.aws.ami:
        out.append(Check("aws ami", FAIL, "aws.ami is not set in config.json",
                         "pick a Deep Learning AMI that has the SSM agent"))
    else:
        try:
            found = clients["ec2"].describe_images(ImageIds=[config.aws.ami])["Images"]
            out.append(
                Check("aws ami", OK if found else FAIL,
                      f"{config.aws.ami} {'exists' if found else 'does not exist'} in {config.aws.region}",
                      "" if found else "AMI ids are per-region; check you copied the right one")
            )
        except Exception as exc:  # noqa: BLE001
            out.append(Check("aws ami", FAIL, _short(exc),
                             "check aws.ami and that the region is right"))

    if not config.aws.instance_profile:
        out.append(Check("aws instance profile", FAIL,
                         "aws.instance_profile is not set", "jobs cannot register with SSM without it"))
    else:
        try:
            boto3.client("iam").get_instance_profile(
                InstanceProfileName=config.aws.instance_profile
            )
            out.append(Check("aws instance profile", OK, config.aws.instance_profile))
        except Exception as exc:  # noqa: BLE001
            out.append(Check(
                "aws instance profile", FAIL, _short(exc),
                "create the profile and give it AmazonSSMManagedInstanceCore "
                "plus logs:PutLogEvents. The broker also needs iam:PassRole on it, "
                "which is the permission everybody forgets",
            ))

    # Quota. The one that is actually zero on a fresh account.
    try:
        quota = boto3.client("service-quotas", region_name=config.aws.region).get_service_quota(
            ServiceCode="ec2", QuotaCode="L-DB2E81BA"
        )["Quota"]["Value"]
        if quota <= 0:
            out.append(Check(
                "aws gpu quota", FAIL, "your G and VT on-demand vCPU quota is 0",
                "request an increase in Service Quotas (it is zero on new accounts "
                "and takes a day or two), or run on the local pool meanwhile",
            ))
        else:
            out.append(Check("aws gpu quota", OK, f"{quota:.0f} G/VT on-demand vCPUs"))
    except Exception as exc:  # noqa: BLE001
        out.append(Check("aws gpu quota", WARN, f"could not read it: {_short(exc)}",
                         "grant servicequotas:GetServiceQuota to see this"))

    return out


def _local(broker) -> list[Check]:
    backend = broker.local_backend()
    if backend is None:
        return [Check("backend: local", WARN, "configured but not built in this process")]
    if not backend.hosts():
        return [Check("backend: local", FAIL, "no hosts under local.hosts")]

    out: list[Check] = []
    for health in backend.health(refresh=True):
        if health.state.accepts_jobs:
            gpus = ", ".join(gpu.name for gpu in health.gpus)
            out.append(Check(f"lab: {health.hostname}", OK, gpus or "reachable"))
        else:
            out.append(Check(
                f"lab: {health.hostname}", FAIL, f"{health.state}: {health.reason}",
                "`gpu hosts` has the same detail",
            ))
    return out


def _web(config) -> Check:
    web = config.web
    if not web.github_client_id and not web.dev_login:
        return Check("web", WARN, "no OAuth app configured, so the web UI cannot sign anyone in",
                     "register one at https://github.com/settings/developers")
    if web.dev_login:
        return Check("web", WARN, f"dev_login is on: everybody signs in as {web.dev_login}",
                     "fine locally, never in production")
    if not web.github_org and not web.allowlist and not web.allowlist_file:
        return Check("web", FAIL, "no org and no allowlist, so nobody can sign in",
                     "set web.github_org or web.allowlist")
    if not web.resolved_secret():
        return Check("web", FAIL, "the GitHub client secret is not set",
                     "export GPU_BROKER_GITHUB_CLIENT_SECRET")
    who = web.github_org or f"{len(web.allowlist)} listed login(s)"
    return Check("web", OK, f"GitHub OAuth, membership by {who}")


def _short(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error", {})
        return f"{error.get('Code', '?')}: {error.get('Message', '')}"[:160]
    return str(exc)[:160]

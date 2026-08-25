"""Somewhere for a member's data to live between jobs.

Without this, every run re-downloads the dataset. That is slow, it is bandwidth
somebody pays for, and on a spot instance that gets preempted twice it happens
three times.

Two backings, because the two kinds of capacity are different in the way that
matters. EC2 instances are fresh every time, so the data has to be somewhere
else: EFS, which any instance in the VPC can mount. The lab machine is the same
machine every time, so a directory on it is already persistent and free.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

from .errors import ConfigError

MOUNT_POINT = "/mnt/gpu-broker"
DATA_PATH = "/data"
"""Where a job sees its own data, on either backing. A member's script should
not have to know which kind of machine it landed on."""


@dataclass(frozen=True)
class VolumeConfig:
    enabled: bool = False
    efs_id: str = ""
    """`fs-0123...`. Required for the cloud side; the lab side needs nothing."""
    region: str = ""
    local_root: str = "~/.gpu-broker/data"
    quota_gb: float = 0.0
    """Advisory. EFS has no per-directory quota, so this is reported rather than
    enforced -- saying so is better than implying a limit that does not exist."""

    def validate(self) -> None:
        if self.enabled and not self.efs_id and not self.local_root:
            raise ConfigError(
                "volumes.enabled is set but there is nowhere to put anything: "
                "set volumes.efs_id for cloud jobs, or volumes.local_root for the lab"
            )
        if self.efs_id and not self.efs_id.startswith("fs-"):
            raise ConfigError(f"volumes.efs_id should look like fs-0123abc, got {self.efs_id!r}")


def load_volume_config(raw: dict | None) -> VolumeConfig:
    if not raw:
        return VolumeConfig()
    known = set(VolumeConfig.__dataclass_fields__)
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"unknown keys under 'volumes' in config.json: {', '.join(sorted(unknown))}. "
            f"Known: {', '.join(sorted(known))}"
        )
    from dataclasses import replace

    config = replace(VolumeConfig(), **raw)
    config.validate()
    return config


def efs_mount_script(config: VolumeConfig, user_id: str, region: str) -> str:
    """Mount the club's EFS and give this job its own corner of it.

    Mount the root, make the user's directory, then bind that one directory onto
    `/data`. The job therefore sees only its owner's data even though the whole
    filesystem is mounted: mounting the subdirectory directly would be tidier,
    but the directory has to exist before it can be mounted, and it cannot be
    created without mounting first.

    Plain NFS rather than `amazon-efs-utils`, which is not on every GPU AMI. The
    cost is no TLS on the wire inside the VPC, which is the same trade every
    NFS mount in a private subnet makes.
    """
    if not config.efs_id:
        return "true"
    host = f"{config.efs_id}.efs.{region}.amazonaws.com"
    user_dir = f"{MOUNT_POINT}/{_safe(user_id)}"
    return (
        f"mkdir -p {MOUNT_POINT} {DATA_PATH} && "
        f"mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 "
        f"{host}:/ {MOUNT_POINT} && "
        f"mkdir -p {shlex.quote(user_dir)} && "
        f"mount --bind {shlex.quote(user_dir)} {DATA_PATH}"
    )


def local_data_dir(config: VolumeConfig, user_id: str, home: str) -> str:
    root = config.local_root
    if root.startswith("~/"):
        root = f"{home}/{root[2:]}"
    elif root == "~":
        root = home
    return f"{root}/{_safe(user_id)}"


def local_mount_script(config: VolumeConfig, user_id: str, home: str) -> str:
    """On the lab machine the directory *is* the volume. Just make sure it exists."""
    if not config.enabled:
        return "true"
    return f"mkdir -p {shlex.quote(local_data_dir(config, user_id, home))}"


def _safe(user_id: str) -> str:
    """A GitHub login is already restricted, but this path is interpolated into
    a shell command and a mount point, so it is checked rather than trusted."""
    cleaned = "".join(c for c in user_id if c.isalnum() or c in "-_.")
    return cleaned or "unknown"

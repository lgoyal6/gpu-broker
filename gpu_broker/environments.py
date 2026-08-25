"""Named environments: the spec, and what its content hash is.

The commonest reason a member's job fails is that their environment differs from
the box. An environment here is a small declarative thing -- a base image, some
pip packages, maybe some apt packages -- and everything else is derived from it.

The derived thing that matters is the **digest**: a hash of the canonical spec.
It is the cache key everywhere. Two people who ask for the same packages get the
same digest, so the second one waits for a pull rather than a build. Changing a
requirement changes the digest, which is what makes "reuse across jobs" safe: a
cached build can never be stale, because a changed spec is a different key.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import shlex
from dataclasses import dataclass

from .errors import BrokerError

NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,38}[a-z0-9]$")
DEFAULT_BASE = "public.ecr.aws/deep-learning-containers/pytorch-training:2.3.0-gpu-py311"


class EnvironmentError_(BrokerError):
    """Bad spec. Named with a trailing underscore so it does not shadow the
    builtin, which would be a genuinely confusing thing to do in a codebase
    other people read."""


@dataclass(frozen=True)
class Environment:
    """One named, reproducible place for a job to run."""

    name: str
    base_image: str = DEFAULT_BASE
    python_version: str = ""
    requirements: tuple[str, ...] = ()
    apt_packages: tuple[str, ...] = ()
    owner: str = ""
    description: str = ""
    created_at: dt.datetime | None = None
    updated_at: dt.datetime | None = None

    @property
    def digest(self) -> str:
        """Content hash of the spec. The cache key, everywhere.

        Requirements are sorted before hashing so that the same set written in a
        different order is the same environment. A member who reorders their
        `requirements.txt` should not trigger a twenty-minute rebuild.
        """
        canonical = json.dumps(
            {
                "base": self.base_image,
                "python": self.python_version,
                "pip": sorted(_normalise(line) for line in self.requirements),
                "apt": sorted(self.apt_packages),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    @property
    def empty(self) -> bool:
        """Nothing to install: the base image is already the environment."""
        return not self.requirements and not self.apt_packages

    def validate(self) -> None:
        if not NAME.match(self.name):
            raise EnvironmentError_(
                f"{self.name!r} is not a usable name. Use lowercase letters, digits, "
                "dots, dashes and underscores, 2 to 40 characters"
            )
        if not self.base_image:
            raise EnvironmentError_(f"{self.name}: no base image")
        for line in self.requirements:
            if line.strip().startswith("-"):
                # `-e .`, `-r other.txt`, `--index-url ...` all refer to things
                # that will not exist inside the build, and the failure happens
                # twenty minutes in rather than here.
                raise EnvironmentError_(
                    f"{self.name}: requirement {line.strip()!r} is a pip flag, not a "
                    "package. Flags refer to files the build cannot see"
                )
        for package in self.apt_packages:
            if not re.match(r"^[a-z0-9][a-z0-9+.:-]*$", package):
                raise EnvironmentError_(f"{self.name}: {package!r} is not an apt package name")

    def describe(self) -> str:
        bits = [f"{len(self.requirements)} pip"]
        if self.apt_packages:
            bits.append(f"{len(self.apt_packages)} apt")
        return f"{self.name} ({self.digest}) on {self.base_image}, {', '.join(bits)}"

    # ------------------------------------------------------- materialisation

    def dockerfile(self) -> str:
        """For EC2, where every machine is fresh and an image is the honest unit."""
        lines = [f"FROM {self.base_image}"]
        if self.apt_packages:
            packages = " ".join(shlex.quote(p) for p in sorted(self.apt_packages))
            lines.append(
                "RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y "
                f"--no-install-recommends {packages} && rm -rf /var/lib/apt/lists/*"
            )
        if self.requirements:
            joined = " ".join(shlex.quote(r) for r in sorted(self.requirements))
            # No cache directory in the layer: it doubles the image size and
            # nothing ever reads it again.
            lines.append(f"RUN pip install --no-cache-dir {joined}")
        lines.append(f'LABEL gpu-broker.environment="{self.name}" gpu-broker.digest="{self.digest}"')
        return "\n".join(lines) + "\n"

    def venv_script(self, root: str) -> str:
        """For the lab machine, where Docker would mean redoing Phase 2.

        The A6000 shares one card between users via MPS with per-client memory
        limits, and host RAM and CPU are capped by systemd cgroups. Putting jobs
        in containers there would move both of those into Docker's flags and
        bind-mount the MPS pipe directory through -- a rework of isolation that
        already works. A cached virtualenv keyed on the same digest gets the
        thing we actually wanted, which is that everyone's packages match.

        Built beside the target and moved into place, so two jobs starting at
        once cannot see a half-installed environment.
        """
        target = f"{root}/{self.digest}"
        staging = f"{root}/.building-{self.digest}"
        pip = " ".join(shlex.quote(r) for r in sorted(self.requirements))
        install = f"{staging}/bin/pip install --disable-pip-version-check -q {pip}" if pip else "true"
        python = f"python{self.python_version}" if self.python_version else "python3"
        return (
            f"set -e; "
            f"if [ -x {shlex.quote(target)}/bin/python ]; then exit 0; fi; "
            f"mkdir -p {shlex.quote(root)}; "
            f"rm -rf {shlex.quote(staging)}; "
            f"{python} -m venv {shlex.quote(staging)}; "
            f"{staging}/bin/pip install --disable-pip-version-check -q --upgrade pip; "
            f"{install}; "
            f"mv {shlex.quote(staging)} {shlex.quote(target)}"
        )

    def venv_path(self, root: str) -> str:
        return f"{root}/{self.digest}"


def _normalise(requirement: str) -> str:
    """`Torch==2.3.0` and `torch == 2.3.0` are the same requirement.

    Only whitespace and case, deliberately. Anything cleverer -- resolving
    extras, comparing version ranges -- would sometimes decide two different
    specs are the same, and a wrong cache hit is a job that runs with the wrong
    packages and no error.
    """
    return re.sub(r"\s+", "", requirement).lower()


def from_requirements(name: str, text: str, **kwargs) -> Environment:
    """The path for somebody who just has a `requirements.txt`.

    Comments and blank lines go; everything else is passed through untouched.
    """
    requirements = []
    for line in text.splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            requirements.append(entry)
    environment = Environment(name=name, requirements=tuple(requirements), **kwargs)
    environment.validate()
    return environment


@dataclass(frozen=True)
class Build:
    """What is known about one environment on one kind of capacity."""

    digest: str
    target: str
    state: str
    reference: str = ""
    """An image URI, or a path to a virtualenv."""
    detail: str = ""
    built_at: dt.datetime | None = None

    READY = "ready"
    BUILDING = "building"
    FAILED = "failed"

    @property
    def usable(self) -> bool:
        return self.state == "ready" and bool(self.reference)

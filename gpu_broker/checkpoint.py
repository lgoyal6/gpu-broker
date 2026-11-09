"""Where a job's progress goes when its machine is taken away.

The point of a checkpoint here is narrow: a spot instance gets two minutes'
notice, and something has to survive that. So checkpoints live *off* the machine
that wrote them. A preempted job resumes somewhere else entirely, and anything
left on the old instance is gone with it.

Two rules that are easy to get wrong and expensive to get wrong:

*A checkpoint is only visible once it is complete.* Files are uploaded first and
a manifest is written last. A job that resumes from a half-uploaded checkpoint
does not crash cleanly -- it loads a truncated tensor and produces garbage for
another six hours.

*Checkpoints are never overwritten.* Each is written under its own step, and
resuming picks the highest one with a manifest. Writing to a single "latest"
key means an interruption during the upload destroys the checkpoint you already
had in exchange for one you cannot use.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import shutil
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from .clock import from_iso, to_iso
from .errors import BrokerError

MANIFEST = "MANIFEST.json"


class CheckpointError(BrokerError):
    pass


@dataclass(frozen=True)
class CheckpointRef:
    """One complete checkpoint."""

    job_id: str
    step: int
    key: str
    written_at: dt.datetime
    files: tuple[str, ...] = ()
    bytes: int = 0

    def describe(self) -> str:
        return f"step {self.step} ({self.bytes / 1_048_576:.1f}MB, {len(self.files)} files)"


@runtime_checkable
class CheckpointStore(Protocol):
    """Durable storage that outlives any one machine."""

    def put(self, job_id: str, step: int, directory: Path, at: dt.datetime) -> CheckpointRef:
        """Upload everything in `directory` as the checkpoint for `step`."""
        ...

    def latest(self, job_id: str) -> CheckpointRef | None:
        """The newest *complete* checkpoint, or None."""
        ...

    def fetch(self, ref: CheckpointRef, directory: Path) -> None:
        """Restore a checkpoint's files into `directory`."""
        ...

    def prune(self, job_id: str, keep: int = 2) -> int:
        """Delete all but the newest `keep` checkpoints. Returns how many went."""
        ...


def _manifest_body(ref_files: list[str], step: int, at: dt.datetime, total: int) -> str:
    return json.dumps(
        {"step": step, "written_at": to_iso(at), "files": sorted(ref_files), "bytes": total},
        indent=2,
    )


def _read_manifest(job_id: str, key: str, raw: str) -> CheckpointRef | None:
    try:
        data = json.loads(raw)
        return CheckpointRef(
            job_id=job_id,
            step=int(data["step"]),
            key=key,
            written_at=from_iso(data["written_at"]),
            files=tuple(data.get("files", ())),
            bytes=int(data.get("bytes", 0)),
        )
    except (ValueError, KeyError, TypeError):
        return None


class LocalCheckpointStore:
    """Checkpoints on a filesystem.

    Used by the lab pool, where jobs are not preempted by a provider and the
    machine is the same one every time, and by every test. On a shared
    filesystem it works across hosts too.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def _dir(self, job_id: str, step: int) -> Path:
        return self.root / job_id / f"step-{step:012d}"

    def put(self, job_id: str, step: int, directory: Path, at: dt.datetime) -> CheckpointRef:
        source = Path(directory)
        if not source.is_dir():
            raise CheckpointError(f"nothing to checkpoint: {source} is not a directory")

        target = self._dir(job_id, step)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)

        files: list[str] = []
        total = 0
        for path in sorted(source.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(source)
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            files.append(str(relative))
            total += path.stat().st_size

        # Last, and only now is this checkpoint visible to `latest`.
        (target / MANIFEST).write_text(_manifest_body(files, step, at, total))
        return CheckpointRef(
            job_id=job_id, step=step, key=str(target), written_at=at,
            files=tuple(sorted(files)), bytes=total,
        )

    def latest(self, job_id: str) -> CheckpointRef | None:
        job_root = self.root / job_id
        if not job_root.is_dir():
            return None
        best: CheckpointRef | None = None
        for candidate in sorted(job_root.iterdir()):
            manifest = candidate / MANIFEST
            if not manifest.is_file():
                continue  # incomplete: skipped, not repaired
            ref = _read_manifest(job_id, str(candidate), manifest.read_text())
            if ref is not None and (best is None or ref.step > best.step):
                best = ref
        return best

    def fetch(self, ref: CheckpointRef, directory: Path) -> None:
        source = Path(ref.key)
        if not (source / MANIFEST).is_file():
            raise CheckpointError(f"checkpoint {ref.key} has no manifest; refusing to restore it")
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        for name in ref.files:
            origin = source / name
            if not origin.is_file():
                raise CheckpointError(f"checkpoint {ref.key} is missing {name}")
            destination = target / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(origin, destination)

    def prune(self, job_id: str, keep: int = 2) -> int:
        job_root = self.root / job_id
        if not job_root.is_dir():
            return 0
        complete = sorted(
            (path for path in job_root.iterdir() if (path / MANIFEST).is_file()),
            key=lambda path: path.name,
        )
        removed = 0
        for path in complete[:-keep] if keep > 0 else complete:
            shutil.rmtree(path)
            removed += 1
        return removed


class S3CheckpointStore:
    """Checkpoints in S3, which is what makes resuming on another instance work.

    A local directory cannot do this job: the whole point is that the machine
    which wrote the checkpoint is gone.
    """

    def __init__(self, client: Any, bucket: str, prefix: str = "checkpoints") -> None:
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _key(self, job_id: str, step: int, name: str = "") -> str:
        base = f"{self.prefix}/{job_id}/step-{step:012d}"
        return f"{base}/{name}" if name else base

    def put(self, job_id: str, step: int, directory: Path, at: dt.datetime) -> CheckpointRef:
        source = Path(directory)
        if not source.is_dir():
            raise CheckpointError(f"nothing to checkpoint: {source} is not a directory")

        files: list[str] = []
        total = 0
        try:
            for path in sorted(source.rglob("*")):
                if not path.is_file():
                    continue
                relative = str(path.relative_to(source))
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=self._key(job_id, step, relative),
                    Body=path.read_bytes(),
                )
                files.append(relative)
                total += path.stat().st_size

            # The manifest goes last. Until it lands, `latest` cannot see this
            # checkpoint at all, so an upload cut off halfway leaves the
            # previous checkpoint as the newest complete one.
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(job_id, step, MANIFEST),
                Body=_manifest_body(files, step, at, total).encode(),
            )
        except Exception as exc:  # noqa: BLE001 - botocore, network, disk
            raise CheckpointError(f"could not upload checkpoint {step}: {exc}") from exc

        return CheckpointRef(
            job_id=job_id, step=step, key=self._key(job_id, step), written_at=at,
            files=tuple(sorted(files)), bytes=total,
        )

    def latest(self, job_id: str) -> CheckpointRef | None:
        best: CheckpointRef | None = None
        for key in self._manifests(job_id):
            try:
                body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            except Exception:  # noqa: BLE001 - deleted between list and get
                continue
            ref = _read_manifest(job_id, key.rsplit("/", 1)[0], body.decode())
            if ref is not None and (best is None or ref.step > best.step):
                best = ref
        return best

    def _manifests(self, job_id: str) -> list[str]:
        prefix = f"{self.prefix}/{job_id}/"
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            try:
                response = self.client.list_objects_v2(**kwargs)
            except Exception as exc:  # noqa: BLE001
                raise CheckpointError(f"could not list checkpoints for {job_id}: {exc}") from exc
            keys.extend(
                item["Key"] for item in response.get("Contents", [])
                if item["Key"].endswith(MANIFEST)
            )
            if not response.get("IsTruncated"):
                return keys
            token = response.get("NextContinuationToken")

    def fetch(self, ref: CheckpointRef, directory: Path) -> None:
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        for name in ref.files:
            try:
                body = self.client.get_object(
                    Bucket=self.bucket, Key=f"{ref.key}/{name}"
                )["Body"].read()
            except Exception as exc:  # noqa: BLE001
                raise CheckpointError(
                    f"checkpoint {ref.key} is missing {name}: {exc}"
                ) from exc
            destination = target / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(body)

    def prune(self, job_id: str, keep: int = 2) -> int:
        steps = sorted({key.rsplit("/", 2)[-2] for key in self._manifests(job_id)})
        doomed = steps[:-keep] if keep > 0 else steps
        removed = 0
        for step_name in doomed:
            prefix = f"{self.prefix}/{job_id}/{step_name}/"
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            for item in response.get("Contents", []):
                self.client.delete_object(Bucket=self.bucket, Key=item["Key"])
            removed += 1
        return removed


class GCSCheckpointStore:
    """GCS checkpoints with checksum-bound artifacts and manifest-last commit."""

    def __init__(self, client: Any, bucket: str, prefix: str = "checkpoints",
                 expected_context: dict[str, str] | None = None) -> None:
        self.client = client
        self.bucket_name = bucket
        self.bucket = client.bucket(bucket)
        self.prefix = prefix.strip("/")
        self.expected_context = dict(expected_context or {})

    def _base(self, job_id: str, step: int) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", job_id):
            raise CheckpointError("job id contains unsupported GCS object-key characters")
        return "/".join(part for part in
                         (self.prefix, job_id, f"step-{step:012d}") if part)

    def _blob(self, key: str):
        return self.bucket.blob(key)

    def put(self, job_id: str, step: int, directory: Path, at: dt.datetime) -> CheckpointRef:
        source = Path(directory)
        if not source.is_dir():
            raise CheckpointError(f"nothing to checkpoint: {source} is not a directory")
        base = self._base(job_id, step)
        artifacts = []
        total = 0
        try:
            for path in sorted(source.rglob("*")):
                if not path.is_file():
                    continue
                relative = path.relative_to(source).as_posix()
                if relative.startswith("/") or ".." in Path(relative).parts or relative == MANIFEST:
                    raise CheckpointError(f"unsafe checkpoint artifact path: {relative}")
                body = path.read_bytes()
                digest = hashlib.sha256(body).hexdigest()
                self._blob(f"{base}/{relative}").upload_from_string(body)
                artifacts.append({"name": relative, "bytes": len(body), "sha256": digest})
                total += len(body)
            if not artifacts:
                raise CheckpointError("checkpoint directory contains no artifact files")
            manifest = {
                "schema": "gpu-broker.gcs-checkpoint/v1", "job_id": job_id,
                "step": step, "written_at": to_iso(at), "files": artifacts,
                "bytes": total, "context": self.expected_context,
            }
            # Complete manifest is the commit record and is written only after
            # every immutable, content-addressed artifact is present.
            self._blob(f"{base}/{MANIFEST}").upload_from_string(
                json.dumps(manifest, sort_keys=True).encode(), content_type="application/json")
        except Exception as exc:
            if isinstance(exc, CheckpointError):
                raise
            raise CheckpointError(f"could not upload GCS checkpoint {step}: {exc}") from exc
        return CheckpointRef(job_id, step, base, at,
                             tuple(sorted(item["name"] for item in artifacts)), total)

    def latest(self, job_id: str) -> CheckpointRef | None:
        self._base(job_id, 0)
        prefix = self._base(job_id, 0).rsplit("/", 1)[0] + "/"
        best = None
        try:
            for blob in self.bucket.list_blobs(prefix=prefix):
                name = getattr(blob, "name", "")
                if not name.endswith(f"/{MANIFEST}"):
                    continue
                try:
                    data = json.loads(blob.download_as_bytes())
                    if data.get("schema") != "gpu-broker.gcs-checkpoint/v1" or data.get("job_id") != job_id:
                        continue
                    context = data.get("context", {})
                    if any(context.get(key) != value for key, value in self.expected_context.items()):
                        continue
                    ref = CheckpointRef(job_id, int(data["step"]), name.rsplit("/", 1)[0],
                                        from_iso(data["written_at"]),
                                        tuple(sorted(item["name"] for item in data["files"])),
                                        int(data["bytes"]))
                except (ValueError, KeyError, TypeError):
                    continue
                if best is None or ref.step > best.step:
                    best = ref
        except Exception as exc:
            raise CheckpointError(f"could not list GCS checkpoints: {exc}") from exc
        return best

    def fetch(self, ref: CheckpointRef, directory: Path) -> None:
        target = Path(directory)
        manifest_key = f"{ref.key}/{MANIFEST}"
        try:
            manifest = json.loads(self._blob(manifest_key).download_as_bytes())
            if (manifest.get("schema") != "gpu-broker.gcs-checkpoint/v1" or
                    manifest.get("job_id") != ref.job_id or int(manifest.get("step", -1)) != ref.step):
                raise CheckpointError("GCS checkpoint manifest identity mismatch")
            context = manifest.get("context", {})
            if any(context.get(key) != value for key, value in self.expected_context.items()):
                raise CheckpointError("GCS checkpoint context is incompatible with this workload")
            manifest_files = tuple(sorted(item["name"] for item in manifest["files"]))
            if manifest_files != tuple(sorted(ref.files)):
                raise CheckpointError("GCS checkpoint manifest file list differs from reference")
            staged: list[tuple[Path, bytes]] = []
            for item in manifest["files"]:
                name = str(item["name"])
                if name.startswith("/") or ".." in Path(name).parts:
                    raise CheckpointError("GCS checkpoint contains an unsafe artifact path")
                body = self._blob(f"{ref.key}/{name}").download_as_bytes()
                if len(body) != int(item["bytes"]) or hashlib.sha256(body).hexdigest() != item["sha256"]:
                    raise CheckpointError(f"GCS checkpoint artifact integrity mismatch: {name}")
                staged.append((target / name, body))
            target.mkdir(parents=True, exist_ok=True)
            for path, body in staged:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
        except CheckpointError:
            raise
        except Exception as exc:
            raise CheckpointError(f"could not restore GCS checkpoint {ref.step}: {exc}") from exc

    def prune(self, job_id: str, keep: int = 2) -> int:
        self._base(job_id, 0)
        prefix = self._base(job_id, 0).rsplit("/", 1)[0] + "/"
        complete = {}
        step_prefixes = {}
        parent_prefix = prefix.rstrip("/")
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            name = getattr(blob, "name", "")
            match = re.match(r"^" + re.escape(prefix) + r"step-(\d{12})/", name)
            if match:
                base = f"{parent_prefix}/" if parent_prefix else ""
                step_prefixes[int(match.group(1))] = base + f"step-{match.group(1)}"
            if not name.endswith(f"/{MANIFEST}"):
                continue
            try:
                data = json.loads(blob.download_as_bytes())
                if data.get("schema") == "gpu-broker.gcs-checkpoint/v1" and data.get("job_id") == job_id:
                    complete[int(data["step"])] = name.rsplit("/", 1)[0]
            except (ValueError, KeyError, TypeError):
                continue
        protected = set(sorted(complete)[-max(0, keep):]) if keep else set()
        removed = 0
        for step, base in step_prefixes.items():
            if step in protected:
                continue
            for blob in self.bucket.list_blobs(prefix=f"{base}/"):
                blob.delete()
            if step in complete:
                removed += 1
        return removed

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from gpu_broker.checkpoint import CheckpointError, GCSCheckpointStore, MANIFEST


class Blob:
    def __init__(self, bucket, name):
        self.bucket, self.name = bucket, name

    def upload_from_string(self, body, **kwargs):
        if self.bucket.fail_manifest and self.name.endswith("/" + MANIFEST):
            raise OSError("injected manifest publish interruption")
        self.bucket.objects[self.name] = bytes(body)

    def download_as_bytes(self):
        return self.bucket.objects[self.name]

    def delete(self):
        del self.bucket.objects[self.name]


class Bucket:
    def __init__(self):
        self.objects = {}
        self.fail_manifest = False

    def blob(self, name):
        return Blob(self, name)

    def list_blobs(self, prefix):
        return [Blob(self, name) for name in sorted(self.objects) if name.startswith(prefix)]


class Client:
    def __init__(self):
        self.test_bucket = Bucket()

    def bucket(self, name):
        assert name == "test-bucket"
        return self.test_bucket


def test_gcs_checkpoint_manifest_last_integrity_and_prune(tmp_path: Path) -> None:
    client = Client()
    store = GCSCheckpointStore(client, "test-bucket", "job-state",
                               expected_context={"model_revision": "model@sha256:abc"})
    source = tmp_path / "source"
    source.mkdir()
    (source / "state.bin").write_bytes(b"artifact-state")
    at = dt.datetime(2026, 11, 4, tzinfo=dt.timezone.utc)
    first = store.put("job-1", 10, source, at)
    assert store.latest("job-1") == first
    manifest = json.loads(client.test_bucket.objects[f"{first.key}/{MANIFEST}"])
    assert manifest["files"][0]["sha256"]
    assert manifest["context"] == {"model_revision": "model@sha256:abc"}

    restore = tmp_path / "restore"
    store.fetch(first, restore)
    assert (restore / "state.bin").read_bytes() == b"artifact-state"

    client.test_bucket.objects[f"{first.key}/state.bin"] = b"corrupted"
    with pytest.raises(CheckpointError, match="integrity mismatch"):
        store.fetch(first, tmp_path / "bad-restore")

    client.test_bucket.fail_manifest = True
    with pytest.raises(CheckpointError, match="could not upload"):
        store.put("job-1", 20, source, at)
    assert store.latest("job-1").step == 10
    client.test_bucket.fail_manifest = False
    # Restore valid first checkpoint before testing retention.
    client.test_bucket.objects[f"{first.key}/state.bin"] = b"artifact-state"
    second = store.put("job-1", 30, source, at)
    assert store.latest("job-1") == second
    assert store.prune("job-1", keep=1) == 1
    assert store.latest("job-1").step == 30

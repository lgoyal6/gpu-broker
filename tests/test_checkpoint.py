"""The checkpoint store: what survives a machine going away.

Two properties carry everything else, and both are about partial writes. A
checkpoint that is visible before it is complete does not fail loudly -- it
loads a truncated tensor and trains garbage for another six hours.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from gpu_broker.checkpoint import (
    MANIFEST,
    CheckpointError,
    CheckpointStore,
    LocalCheckpointStore,
    S3CheckpointStore,
)

NOW = dt.datetime(2026, 1, 15, 12, tzinfo=dt.timezone.utc)


@pytest.fixture
def payload(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    (work / "nested").mkdir(parents=True)
    (work / "model.pt").write_bytes(b"weights" * 100)
    (work / "nested" / "optimizer.pt").write_bytes(b"opt")
    return work


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path: Path):
    """Both stores, same tests. They have to behave identically or a job that
    resumes fine on the lab machine breaks on EC2."""
    if request.param == "local":
        yield LocalCheckpointStore(tmp_path / "store")
        return
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        client.create_bucket(
            Bucket="club", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        yield S3CheckpointStore(client, "club")


def test_both_stores_satisfy_the_protocol(store):
    assert isinstance(store, CheckpointStore)


def test_nothing_saved_yet(store):
    assert store.latest("j1") is None


def test_a_checkpoint_round_trips(store, payload, tmp_path):
    ref = store.put("j1", 100, payload, NOW)
    assert ref.step == 100
    assert set(ref.files) == {"model.pt", "nested/optimizer.pt"}
    assert ref.bytes == 703

    restored = tmp_path / "restored"
    store.fetch(ref, restored)
    assert (restored / "model.pt").read_bytes() == b"weights" * 100
    assert (restored / "nested" / "optimizer.pt").read_bytes() == b"opt"


def test_the_newest_complete_checkpoint_wins(store, payload):
    store.put("j1", 100, payload, NOW)
    store.put("j1", 300, payload, NOW)
    store.put("j1", 200, payload, NOW)
    assert store.latest("j1").step == 300


def test_steps_sort_numerically_not_lexically(store, payload):
    """`step-9` before `step-10` is how a resume goes backwards."""
    store.put("j1", 9, payload, NOW)
    store.put("j1", 10, payload, NOW)
    assert store.latest("j1").step == 10


def test_jobs_do_not_see_each_others_checkpoints(store, payload):
    store.put("j1", 100, payload, NOW)
    assert store.latest("j2") is None


def test_checkpoints_are_never_overwritten(store, payload):
    """Writing to a single 'latest' key means an interruption during the upload
    destroys the checkpoint you already had in exchange for one you cannot use."""
    first = store.put("j1", 100, payload, NOW)
    store.put("j1", 200, payload, NOW)
    assert store.latest("j1").step == 200

    # The older one is still there and still restorable, which is the point:
    # the new checkpoint did not replace the one that was already safe.
    import tempfile

    restored = Path(tempfile.mkdtemp())
    store.fetch(first, restored)
    assert (restored / "model.pt").exists()


def test_pruning_keeps_the_newest(store, payload):
    for step in (100, 200, 300, 400):
        store.put("j1", step, payload, NOW)
    assert store.prune("j1", keep=2) == 2
    assert store.latest("j1").step == 400


def test_pruning_a_job_with_nothing_saved_is_fine(store):
    assert store.prune("nobody", keep=2) == 0


# ------------------------------------------------- the half-written cases


def test_a_half_written_local_checkpoint_is_invisible(tmp_path, payload):
    store = LocalCheckpointStore(tmp_path / "store")
    store.put("j1", 100, payload, NOW)

    # A process killed mid-write: files, no manifest.
    half = tmp_path / "store" / "j1" / "step-000000000200"
    half.mkdir(parents=True)
    (half / "model.pt").write_bytes(b"trunc")

    assert store.latest("j1").step == 100, "resumed from a half-written checkpoint"


def test_a_half_uploaded_s3_checkpoint_is_invisible(payload):
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        client.create_bucket(
            Bucket="club", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        store = S3CheckpointStore(client, "club")
        store.put("j1", 100, payload, NOW)

        # The instance went away halfway through the upload.
        client.put_object(
            Bucket="club", Key="checkpoints/j1/step-000000000200/model.pt", Body=b"trunc"
        )
        assert store.latest("j1").step == 100


def test_a_corrupt_manifest_is_skipped_not_trusted(tmp_path, payload):
    store = LocalCheckpointStore(tmp_path / "store")
    store.put("j1", 100, payload, NOW)
    store.put("j1", 200, payload, NOW)
    (tmp_path / "store" / "j1" / "step-000000000200" / MANIFEST).write_text("{not json")

    assert store.latest("j1").step == 100


def test_restoring_a_checkpoint_with_a_missing_file_is_an_error(tmp_path, payload):
    """Better to fail loudly than to resume with half a model."""
    store = LocalCheckpointStore(tmp_path / "store")
    ref = store.put("j1", 100, payload, NOW)
    (Path(ref.key) / "model.pt").unlink()

    with pytest.raises(CheckpointError, match="missing model.pt"):
        store.fetch(ref, tmp_path / "restored")


def test_checkpointing_a_directory_that_is_not_there(store, tmp_path):
    with pytest.raises(CheckpointError, match="not a directory"):
        store.put("j1", 100, tmp_path / "nope", NOW)


def test_an_s3_failure_is_a_checkpoint_error_not_a_crash(payload):
    class Broken:
        def put_object(self, **kwargs):
            raise RuntimeError("connection reset")

    with pytest.raises(CheckpointError, match="could not upload"):
        S3CheckpointStore(Broken(), "club").put("j1", 100, payload, NOW)

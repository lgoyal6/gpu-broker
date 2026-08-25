"""What a training script imports so its work survives a preemption.

This is the half of the checkpoint contract that runs inside *your* job, not
inside the broker. Nothing here imports the broker, talks to a database, or
needs the CLI; it reads three environment variables and handles one signal.

The plain version, which works for anything:

    from gpu_broker.adapters import preempted, checkpoint_dir, saved

    for step, batch in enumerate(loader):
        train(batch)
        if preempted():
            torch.save(state, checkpoint_dir() / "state.pt")
            saved(step)
            break

`preempted()` is polled rather than acting from inside the signal handler. A
handler interrupts whatever was running, which on a training loop means the
middle of a CUDA call or a dataloader worker's fork, and saving from there is
how you get a corrupt checkpoint instead of no checkpoint.

There are also `Checkpointer` for a normal PyTorch loop and `BrokerCallback` for
HuggingFace `Trainer`. Both are wrappers over exactly this.
"""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path

__all__ = [
    "install",
    "preempted",
    "wait_for_preemption",
    "checkpoint_dir",
    "resume_dir",
    "job_id",
    "saved",
    "deadline_seconds",
    "Checkpointer",
    "BrokerCallback",
]

ENV_CHECKPOINT_DIR = "GPU_BROKER_CHECKPOINT_DIR"
ENV_RESUME_DIR = "GPU_BROKER_RESUME_DIR"
ENV_JOB_ID = "GPU_BROKER_JOB_ID"
ENV_DEADLINE = "GPU_BROKER_CHECKPOINT_DEADLINE"

SAVED_MARKER = "CHECKPOINT_COMPLETE"
"""Written by the job, read by the watcher. The watcher does not upload until
this exists, because a directory of half-written tensors is worse than nothing:
resuming from it does not crash, it produces garbage for another six hours."""

_notice = {"requested": False, "at": 0.0}
_installed = False


def install(sig: int = signal.SIGUSR1) -> None:
    """Start listening for the broker's checkpoint request.

    Called automatically the first time `preempted()` is used, so most scripts
    never call it. Idempotent, and a no-op off the main thread, where Python
    cannot install handlers at all.
    """
    global _installed
    if _installed:
        return

    def handler(signum, frame):  # noqa: ANN001, ARG001
        _notice["requested"] = True
        _notice["at"] = time.time()

    try:
        signal.signal(sig, handler)
    except (ValueError, OSError):
        # Not the main thread, or a platform without this signal. Polling
        # `preempted()` will simply never return True, which degrades to "this
        # job cannot checkpoint" rather than to a crash.
        return
    _installed = True


def preempted() -> bool:
    """Has the broker asked this job to checkpoint and stop?"""
    install()
    return bool(_notice["requested"])


def deadline_seconds() -> float:
    """Roughly how long is left. Spot gives two minutes total, and some of it
    is already gone by the time the signal arrives."""
    budget = float(os.environ.get(ENV_DEADLINE, "90"))
    if not _notice["requested"]:
        return budget
    return max(0.0, budget - (time.time() - _notice["at"]))


def wait_for_preemption(timeout: float | None = None) -> bool:
    """Block until a notice arrives. For a job whose work is not a loop."""
    install()
    started = time.time()
    while not _notice["requested"]:
        if timeout is not None and time.time() - started >= timeout:
            return False
        time.sleep(0.5)
    return True


def checkpoint_dir() -> Path:
    """Where to write. The broker preserves whatever ends up here."""
    directory = Path(os.environ.get(ENV_CHECKPOINT_DIR, "./checkpoints"))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def resume_dir() -> Path | None:
    """Where the last checkpoint was restored to, or None on a first run."""
    raw = os.environ.get(ENV_RESUME_DIR)
    if not raw:
        return None
    directory = Path(raw)
    return directory if directory.is_dir() and any(directory.iterdir()) else None


def job_id() -> str:
    return os.environ.get(ENV_JOB_ID, "")


def saved(step: int) -> None:
    """Say the checkpoint is complete and safe to upload.

    Call this *after* every file is closed. Until it is called the broker
    assumes the directory is still being written and will not take it.
    """
    (checkpoint_dir() / SAVED_MARKER).write_text(str(int(step)))


def _clear_saved() -> None:
    marker = checkpoint_dir() / SAVED_MARKER
    if marker.exists():
        marker.unlink()


from .torch_adapter import Checkpointer  # noqa: E402
from .hf_adapter import BrokerCallback  # noqa: E402

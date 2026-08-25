"""A `Checkpointer` for an ordinary PyTorch training loop.

    from gpu_broker.adapters import Checkpointer

    ckpt = Checkpointer(model, optimizer, scheduler=scheduler, every=500)
    start = ckpt.resume()                  # 0 on a first run

    for step in range(start, total_steps):
        train_one_step()
        if ckpt.step(step):                # saved because it was time, or asked to
            break                          # only True when the broker wants us gone

torch is imported lazily, so importing `gpu_broker.adapters` on a machine
without it still works -- which matters because the CLI imports the package and
the club's laptops do not have CUDA.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

STATE_FILE = "checkpoint.pt"


class Checkpointer:
    """Saves and restores model, optimizer, scheduler, RNG state, and position.

    RNG state is in there deliberately. Without it a resumed run sees a
    different data order and different dropout masks from the run it is
    continuing, and "resumed jobs produce the same final state as uninterrupted
    ones" quietly stops being true.
    """

    def __init__(
        self,
        model: Any,
        optimizer: Any = None,
        scheduler: Any = None,
        *,
        every: int = 0,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.optimizer = optimizer
        self.scheduler = scheduler
        self.every = every
        self.extra = extra or {}
        self.last_saved_step = -1

    # ------------------------------------------------------------------ save

    def save(self, step: int, epoch: int = 0) -> Path:
        import torch

        from . import checkpoint_dir, saved

        target = checkpoint_dir() / STATE_FILE
        payload = {
            "step": int(step),
            "epoch": int(epoch),
            "model": self.model.state_dict(),
            "optimizer": self.optimizer.state_dict() if self.optimizer is not None else None,
            "scheduler": self.scheduler.state_dict() if self.scheduler is not None else None,
            "rng": {
                "cpu": torch.get_rng_state(),
                "cuda": torch.cuda.get_rng_state_all() if torch.cuda.is_available() else None,
            },
            "extra": self.extra,
        }
        # Written beside the target and moved into place, so a process killed
        # mid-write leaves the previous file intact rather than a truncated one.
        staging = target.with_suffix(".partial")
        torch.save(payload, staging)
        staging.replace(target)

        saved(step)
        self.last_saved_step = step
        return target

    def step(self, step: int, epoch: int = 0) -> bool:
        """Call once per training step.

        Returns True only when the broker has asked this job to stop, which is
        the caller's signal to break out of the loop.
        """
        from . import _clear_saved, preempted

        if preempted():
            self.save(step, epoch)
            return True
        if self.every and step > 0 and step % self.every == 0 and step != self.last_saved_step:
            self.save(step, epoch)
            # Periodic saves are progress, not a stop request, and the marker
            # must not make the watcher think this job is finished.
            _clear_saved()
        return False

    # --------------------------------------------------------------- restore

    def resume(self) -> int:
        """Load the last checkpoint if there is one. Returns the step to start at."""
        import torch

        from . import resume_dir

        directory = resume_dir()
        if directory is None:
            return 0
        state_path = directory / STATE_FILE
        if not state_path.is_file():
            return 0

        payload = torch.load(state_path, map_location="cpu", weights_only=False)
        self.model.load_state_dict(payload["model"])
        if self.optimizer is not None and payload.get("optimizer") is not None:
            self.optimizer.load_state_dict(payload["optimizer"])
        if self.scheduler is not None and payload.get("scheduler") is not None:
            self.scheduler.load_state_dict(payload["scheduler"])

        rng = payload.get("rng") or {}
        if rng.get("cpu") is not None:
            torch.set_rng_state(rng["cpu"])
        if rng.get("cuda") is not None and torch.cuda.is_available():
            torch.cuda.set_rng_state_all(rng["cuda"])

        self.extra = payload.get("extra", {})
        # Resume *after* the saved step: that one is already done, and redoing
        # it is how a resumed run drifts from an uninterrupted one.
        return int(payload.get("step", -1)) + 1

    @property
    def epoch(self) -> int:
        return int(self.extra.get("epoch", 0))

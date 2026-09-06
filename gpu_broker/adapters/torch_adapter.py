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
    """Saves and restores model, optimizer, scheduler, RNG state, and step.

    RNG state is in there deliberately. Without it a resumed run sees different
    dropout masks from the run it is continuing.

    It is *not* enough for the data order, and that is a limit rather than a
    bug here: a `DataLoader` with `shuffle=True` draws its permutation once, at
    the top of the epoch, and the RNG state this restores is the one the
    training steps had advanced to by the time the notice arrived. So a resumed
    run redraws the permutation and starts it from the beginning. Measured on
    this repo's own recovery harness: 200 steps, interrupted at 110, the
    resumed run matched the uninterrupted control on the first 111 batches and
    on none of the rest, and its final parameters landed 4.8% away.

    A loop that needs the data order back gives the loader its own
    `torch.Generator` and puts that generator's epoch-start state and its
    position in the epoch into `extra`. A separate generator is the point:
    rewinding the global RNG far enough to redraw the permutation would rewind
    the dropout masks with it. With that done the same run came back
    bit-identical. `tests/test_recovery.py` pins both halves.
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

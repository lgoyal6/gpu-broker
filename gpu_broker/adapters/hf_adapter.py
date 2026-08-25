"""A callback for HuggingFace `Trainer`.

    from gpu_broker.adapters import BrokerCallback

    trainer = Trainer(..., callbacks=[BrokerCallback()])
    trainer.train(resume_from_checkpoint=BrokerCallback.resume_from())

`Trainer` already knows how to write and reload a checkpoint. This does not
replace any of that; it asks Trainer to save at the moment the broker needs it
to, and then to stop.
"""

from __future__ import annotations

from typing import Any


def _base() -> Any:
    """`TrainerCallback` if transformers is installed, otherwise a stand-in.

    Importing this module must not require transformers -- the broker imports
    the adapters package, and the broker does not train anything.
    """
    try:
        from transformers import TrainerCallback

        return TrainerCallback
    except ImportError:

        class _Stub:  # pragma: no cover - only on machines without transformers
            pass

        return _Stub


class BrokerCallback(_base()):  # type: ignore[misc]
    """Turns a preemption notice into a Trainer save-and-stop."""

    def __init__(self, check_every: int = 1) -> None:
        self.check_every = max(1, check_every)
        self.triggered = False

    def on_step_end(self, args: Any, state: Any, control: Any, **kwargs: Any) -> Any:
        from . import preempted

        if self.triggered or state.global_step % self.check_every:
            return control
        if not preempted():
            return control

        self.triggered = True
        control.should_save = True
        control.should_training_stop = True
        return control

    def on_save(self, args: Any, state: Any, control: Any, **kwargs: Any) -> Any:
        """Trainer has finished writing. Only now is it safe to upload."""
        if not self.triggered:
            return control
        from . import saved

        saved(int(getattr(state, "global_step", 0)))
        return control

    @staticmethod
    def resume_from() -> str | None:
        """Pass to `trainer.train(resume_from_checkpoint=...)`.

        Trainer writes `checkpoint-<step>` directories, so the restored copy is
        handed back the same way rather than flattened.
        """
        from . import resume_dir

        directory = resume_dir()
        if directory is None:
            return None
        candidates = sorted(
            (path for path in directory.iterdir() if path.is_dir() and path.name.startswith("checkpoint-")),
            key=lambda path: int(path.name.rsplit("-", 1)[-1] or 0),
        )
        if candidates:
            return str(candidates[-1])
        return str(directory) if any(directory.iterdir()) else None

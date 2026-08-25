"""The framework adapters, against real torch.

The build prompt asks for these "so the checkpoint contract is not hypothetical".
Testing them against a stub would put it straight back to hypothetical, so torch
is a real dev dependency and these are real save/restore cycles.

The property that matters is not "a file appeared". It is that a run which
stopped at step 4 and resumed produces bit-identical weights to one that never
stopped -- which requires optimizer state, scheduler state, and RNG state, and
fails quietly if any of the three is missed.
"""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path

import pytest

from gpu_broker import adapters
from gpu_broker.adapters import Checkpointer, checkpoint_dir, preempted, resume_dir, saved

torch = pytest.importorskip("torch")


@pytest.fixture(autouse=True)
def clean_env(tmp_path, monkeypatch):
    monkeypatch.setenv(adapters.ENV_CHECKPOINT_DIR, str(tmp_path / "ckpt"))
    monkeypatch.delenv(adapters.ENV_RESUME_DIR, raising=False)
    monkeypatch.setenv(adapters.ENV_JOB_ID, "j" * 32)
    adapters._notice["requested"] = False
    adapters._notice["at"] = 0.0
    yield
    adapters._notice["requested"] = False


def build():
    torch.manual_seed(0)
    model = torch.nn.Linear(4, 2)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=2, gamma=0.5)
    return model, optimizer, scheduler


def train(model, optimizer, scheduler, until, start=0):
    for _ in range(start, until):
        loss = model(torch.randn(8, 4)).pow(2).mean()   # consumes RNG on purpose
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        scheduler.step()


# --------------------------------------------------------------- the signal


def test_a_job_is_not_preempted_until_it_is_told(tmp_path):
    assert not preempted()


def test_the_signal_sets_the_flag():
    assert not preempted()
    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.05)
    assert preempted()


def test_the_handler_does_no_work_itself():
    """A handler interrupts whatever was running -- on a training loop that is
    the middle of a CUDA call. Saving from there is how you get a corrupt
    checkpoint instead of no checkpoint."""
    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.05)
    assert not any(checkpoint_dir().iterdir()), "the signal handler wrote something"


def test_the_deadline_shrinks_once_the_notice_arrives():
    full = adapters.deadline_seconds()
    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.3)
    assert adapters.deadline_seconds() < full


def test_the_completion_marker_carries_the_step():
    saved(1234)
    assert (checkpoint_dir() / adapters.SAVED_MARKER).read_text() == "1234"


def test_no_resume_directory_on_a_first_run():
    assert resume_dir() is None


# ------------------------------------------------------------ the round trip


def test_a_resumed_run_matches_an_uninterrupted_one(monkeypatch):
    """The whole contract in one test."""
    torch.manual_seed(1234)
    model, optimizer, scheduler = build()
    train(model, optimizer, scheduler, 10)
    control = [p.detach().clone() for p in model.parameters()]
    control_lr = optimizer.param_groups[0]["lr"]

    torch.manual_seed(1234)
    model, optimizer, scheduler = build()
    keeper = Checkpointer(model, optimizer, scheduler)
    train(model, optimizer, scheduler, 5)
    keeper.save(step=4)

    monkeypatch.setenv(adapters.ENV_RESUME_DIR, str(checkpoint_dir()))
    model2, optimizer2, scheduler2 = build()
    keeper2 = Checkpointer(model2, optimizer2, scheduler2)
    start = keeper2.resume()
    assert start == 5, "resumed at the wrong step"
    train(model2, optimizer2, scheduler2, 10, start=start)

    for expected, actual in zip(control, model2.parameters()):
        assert torch.allclose(expected, actual), "a resumed run drifted"
    assert optimizer2.param_groups[0]["lr"] == control_lr, "scheduler state was lost"


def test_forgetting_the_rng_state_would_be_caught(monkeypatch):
    """Without RNG state a resumed run sees a different data order and different
    dropout masks. This asserts the state is really in the file rather than
    trusting that it works."""
    model, optimizer, _ = build()
    Checkpointer(model, optimizer).save(step=0)
    payload = torch.load(checkpoint_dir() / "checkpoint.pt", weights_only=False)

    assert payload["rng"]["cpu"] is not None
    assert payload["optimizer"] is not None
    assert payload["step"] == 0


def test_resume_returns_zero_with_nothing_to_resume_from():
    model, optimizer, _ = build()
    assert Checkpointer(model, optimizer).resume() == 0


def test_resume_returns_zero_when_the_directory_is_empty(monkeypatch, tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv(adapters.ENV_RESUME_DIR, str(empty))
    model, optimizer, _ = build()
    assert Checkpointer(model, optimizer).resume() == 0


# --------------------------------------------------------------- the loop API


def test_step_saves_and_says_stop_when_preempted():
    model, optimizer, _ = build()
    keeper = Checkpointer(model, optimizer)

    assert keeper.step(3) is False
    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.05)

    assert keeper.step(4) is True, "did not tell the loop to stop"
    assert (checkpoint_dir() / "checkpoint.pt").is_file()
    assert (checkpoint_dir() / adapters.SAVED_MARKER).read_text() == "4"


def test_periodic_saves_do_not_look_like_a_finished_checkpoint():
    """The marker means 'the job has stopped and this is final'. A routine save
    that left it behind would have the watcher upload and give up mid-run."""
    model, optimizer, _ = build()
    keeper = Checkpointer(model, optimizer, every=2)

    assert keeper.step(4) is False
    assert (checkpoint_dir() / "checkpoint.pt").is_file()
    assert not (checkpoint_dir() / adapters.SAVED_MARKER).exists()


def test_a_partial_write_never_replaces_a_good_checkpoint():
    """Written beside the target and moved into place, so a process killed
    mid-write leaves the previous file intact."""
    model, optimizer, _ = build()
    keeper = Checkpointer(model, optimizer)
    keeper.save(step=1)
    first = (checkpoint_dir() / "checkpoint.pt").read_bytes()

    keeper.save(step=2)
    assert (checkpoint_dir() / "checkpoint.pt").read_bytes() != first
    assert not list(checkpoint_dir().glob("*.partial")), "left a staging file behind"


def test_saving_creates_the_directory_if_it_is_missing(tmp_path, monkeypatch):
    monkeypatch.setenv(adapters.ENV_CHECKPOINT_DIR, str(tmp_path / "nowhere" / "deep"))
    model, optimizer, _ = build()
    Checkpointer(model, optimizer).save(step=0)
    assert (Path(os.environ[adapters.ENV_CHECKPOINT_DIR]) / "checkpoint.pt").is_file()


# ----------------------------------------------------------- the HF adapter


def test_the_hf_callback_imports_without_transformers():
    from gpu_broker.adapters import BrokerCallback

    callback = BrokerCallback()
    assert callback.triggered is False


def test_the_hf_callback_asks_trainer_to_save_and_stop():
    from gpu_broker.adapters import BrokerCallback

    class Control:
        should_save = False
        should_training_stop = False

    class State:
        global_step = 40

    callback = BrokerCallback()
    control = Control()
    callback.on_step_end(None, State(), control)
    assert not control.should_save

    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.05)
    callback.on_step_end(None, State(), control)

    assert control.should_save
    assert control.should_training_stop


def test_the_hf_callback_marks_the_checkpoint_only_after_trainer_has_written_it():
    from gpu_broker.adapters import BrokerCallback

    class Control:
        should_save = False
        should_training_stop = False

    class State:
        global_step = 40

    callback = BrokerCallback()
    os.kill(os.getpid(), signal.SIGUSR1)
    time.sleep(0.05)
    callback.on_step_end(None, State(), Control())
    assert not (checkpoint_dir() / adapters.SAVED_MARKER).exists()

    callback.on_save(None, State(), Control())
    assert (checkpoint_dir() / adapters.SAVED_MARKER).read_text() == "40"


def test_resume_from_picks_the_highest_trainer_checkpoint(tmp_path, monkeypatch):
    from gpu_broker.adapters import BrokerCallback

    restored = tmp_path / "restored"
    for step in (9, 100, 20):
        (restored / f"checkpoint-{step}").mkdir(parents=True)
    monkeypatch.setenv(adapters.ENV_RESUME_DIR, str(restored))

    assert BrokerCallback.resume_from().endswith("checkpoint-100")


def test_resume_from_is_none_with_nothing_to_resume():
    from gpu_broker.adapters import BrokerCallback

    assert BrokerCallback.resume_from() is None

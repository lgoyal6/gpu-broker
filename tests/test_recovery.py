"""What actually survives an interruption, and what does not.

`test_adapters.py` proves the `Checkpointer` round-trips model, optimizer,
scheduler and RNG state, with a training loop that draws its batches straight
from `torch.randn`. Real jobs use a `DataLoader`, and that is where the contract
stops holding: `RandomSampler` draws its permutation once, at the top of the
epoch, from a generator the checkpoint does not carry. Restoring the global RNG
puts the dropout masks back and does nothing for the batch order.

Both halves are pinned here, because the first one is a limit somebody has to
know about and the second one is the pattern that fixes it. Measured end to end
against an uninterrupted control run: with the loader position carried, a
resumed run matched the control on all 200 steps and finished bit-identical;
without it, the batches diverged at the step of the resume and the final
parameters landed 4.8% away.
"""

from __future__ import annotations

import pytest

torch = pytest.importorskip("torch")

from gpu_broker import adapters  # noqa: E402
from gpu_broker.adapters import Checkpointer  # noqa: E402

STEPS = 24
BATCH = 4
SAMPLES = 32
STOP_AFTER = 9  # the step the interruption lands on


@pytest.fixture(autouse=True)
def clean_env(tmp_path, monkeypatch):
    monkeypatch.setenv(adapters.ENV_CHECKPOINT_DIR, str(tmp_path / "ckpt"))
    monkeypatch.delenv(adapters.ENV_RESUME_DIR, raising=False)
    monkeypatch.setenv(adapters.ENV_JOB_ID, "j" * 32)
    adapters._notice["requested"] = False
    yield
    adapters._notice["requested"] = False


def data():
    gen = torch.Generator().manual_seed(99)
    x = torch.randn(SAMPLES, 4, generator=gen)
    y = torch.randn(SAMPLES, 1, generator=gen)
    return torch.utils.data.TensorDataset(x, y, torch.arange(SAMPLES))


def build():
    torch.manual_seed(1234)
    model = torch.nn.Sequential(
        torch.nn.Linear(4, 8), torch.nn.ReLU(), torch.nn.Dropout(0.2), torch.nn.Linear(8, 1)
    )
    return model, torch.optim.AdamW(model.parameters(), lr=0.05)


def run(*, carry: bool, stop_at: int | None = None, resume: bool = False):
    """One attempt. Returns (batches seen, final parameters, the Checkpointer)."""
    dataset = data()
    model, opt = build()
    keeper = Checkpointer(model, opt, every=0)
    loader_gen = torch.Generator().manual_seed(4321) if carry else None

    start = keeper.resume() if resume else 0
    skip = int(keeper.extra.get("batch_in_epoch", 0)) if (resume and carry and start) else 0
    if resume and carry and start:
        loader_gen.set_state(keeper.extra["loader_state"])

    seen, step = [], start
    while step < STEPS:
        epoch_state = loader_gen.get_state() if carry else None
        loader = torch.utils.data.DataLoader(
            dataset, batch_size=BATCH, shuffle=True, drop_last=True, generator=loader_gen
        )
        position = 0
        for x, y, idx in loader:
            if skip:
                skip -= 1
                position += 1
                continue
            if step >= STEPS:
                break
            model.train()
            loss = torch.nn.functional.mse_loss(model(x), y)
            opt.zero_grad()
            loss.backward()
            opt.step()
            seen.append(idx.tolist())

            if carry:
                keeper.extra = {"loader_state": epoch_state, "batch_in_epoch": position + 1}
            if stop_at is not None and step == stop_at:
                keeper.save(step)
                return seen, [p.detach().clone() for p in model.parameters()], keeper
            step += 1
            position += 1

    return seen, [p.detach().clone() for p in model.parameters()], keeper


def resumed(monkeypatch, carry: bool):
    """Interrupt at STOP_AFTER, then resume from what was saved."""
    first, _, _ = run(carry=carry, stop_at=STOP_AFTER)
    monkeypatch.setenv(adapters.ENV_RESUME_DIR, str(adapters.checkpoint_dir()))
    second, params, _ = run(carry=carry, resume=True)
    return first + second, params


def test_the_checkpointer_alone_does_not_carry_the_data_loader_position(monkeypatch):
    """The limit, stated as a test rather than left to be discovered.

    Model, optimizer and RNG all come back. The batch order does not: the epoch
    permutation was drawn from the global RNG at the top of the epoch, and the
    state the checkpoint restores is the one the training steps had advanced to
    by the time the notice arrived. So the resumed run redraws, and from the
    step it resumed on it is training on a different stream of data.
    """
    control, control_params, _ = run(carry=False)
    batches, params = resumed(monkeypatch, carry=False)

    assert len(batches) == len(control) == STEPS
    assert batches[:STOP_AFTER] == control[:STOP_AFTER], "the first attempt already differed"
    assert batches != control, (
        "the data order survived, so this test is no longer describing the code"
    )
    drift = max(float((a - b).abs().max()) for a, b in zip(control_params, params))
    assert drift > 0, "the parameters matched despite a different data order"


def test_a_resumed_run_matches_batch_for_batch_when_the_loader_state_is_carried(monkeypatch):
    """And the pattern that closes it, which is three lines in the training loop.

    The loader gets its own generator, and the checkpoint's `extra` carries that
    generator's epoch-start state plus how far into the epoch we got. A separate
    generator matters: rewinding the global RNG far enough to redraw the
    permutation would rewind the dropout masks with it.
    """
    control, control_params, _ = run(carry=True)
    batches, params = resumed(monkeypatch, carry=True)

    assert batches == control, "the resumed run saw different batches"
    for expected, actual in zip(control_params, params):
        assert torch.equal(expected, actual), "a resumed run drifted"


def test_extra_survives_the_round_trip(monkeypatch):
    """`extra` is the only channel a training loop has for state the
    `Checkpointer` knows nothing about. If `resume()` stopped restoring it, the
    test above would still pass on the control half and quietly stop testing
    anything, so this asserts the channel directly."""
    model, opt = build()
    keeper = Checkpointer(model, opt)
    keeper.extra = {"batch_in_epoch": 7, "loader_state": torch.tensor([1, 2, 3], dtype=torch.uint8)}
    keeper.save(step=3)

    monkeypatch.setenv(adapters.ENV_RESUME_DIR, str(adapters.checkpoint_dir()))
    model2, opt2 = build()
    keeper2 = Checkpointer(model2, opt2)
    assert keeper2.resume() == 4
    assert keeper2.extra["batch_in_epoch"] == 7
    assert torch.equal(keeper2.extra["loader_state"], torch.tensor([1, 2, 3], dtype=torch.uint8))

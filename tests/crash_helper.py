"""A broker that does real work and then waits to be killed.

Run by `test_durability.py` as a separate process so the kill is a real SIGKILL
against a real SQLite file, not a simulated one. Prints READY once there is
state worth losing, then keeps writing so the kill lands at an arbitrary point
inside the write path rather than at a convenient boundary.

    python tests/crash_helper.py <state-dir>
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gpu_broker.backends import FakeBackend  # noqa: E402
from gpu_broker.broker import Broker  # noqa: E402
from gpu_broker.clock import ManualClock  # noqa: E402
from gpu_broker.config import load_config  # noqa: E402
from gpu_broker.money import Currency  # noqa: E402

USERS = ["ana", "bo", "cy", "di", "eo"]


def main() -> None:
    state_dir = Path(sys.argv[1])
    clock = ManualClock()
    config = load_config(state_dir)
    cloud = FakeBackend(
        "cloud",
        clock=clock,
        currency=Currency.USD,
        capacity={"a10g": 3, "t4": 2},
        state_path=state_dir / "cloud.json",
    )
    broker = Broker.open(state_dir, clock=clock, backends=[cloud], config=config)

    for name in USERS:
        broker.add_user(name)

    # Long jobs on purpose. The point of the crash test is to lose work that is
    # still in flight, so nothing dispatched may finish before the kill arrives.
    # Four hours at a $5 ceiling also leaves each user inside their $25 month.
    for index in range(20):
        broker.submit(
            user_id=USERS[index % len(USERS)],
            command=f"python train.py --seed {index}",
            gpu_type="a10g" if index % 2 else "t4",
            hours=4,
            budget="5.00",
        )
        clock.advance(minutes=1)

    broker.tick()  # fills every slot, so there are running jobs to lose
    assert broker.store.active_jobs(), "nothing was dispatched; the crash test would be vacuous"

    print("READY", flush=True)

    # Keep writing until killed. Every iteration opens several transactions, so
    # SIGKILL is overwhelmingly likely to arrive while one is in flight. The
    # clock moves in seconds, not minutes, so the running jobs stay running.
    counter = 0
    while True:
        counter += 1
        broker.submit(
            user_id=USERS[counter % len(USERS)],
            command=f"python churn.py --n {counter}",
            gpu_type="t4",
            hours=0.5,
            budget="1.00",
        )
        broker.tick()
        clock.advance(seconds=5)


if __name__ == "__main__":
    main()

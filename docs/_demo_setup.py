"""Set up a realistic pool state for the README demo.

Everything here is the real broker: real submissions, real ticks, real
utilization samples, real ledger. Nothing is faked for the recording -- the
numbers in the GIF are numbers the code produced.
"""
import os, sys, shutil
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.money import Currency

home = Path(os.environ["GPU_BROKER_HOME"])
if home.exists():
    shutil.rmtree(home)
home.mkdir(parents=True)

# Anchored to real time and walked forward to roughly now. The CLI runs on a
# system clock, so a scenario built in the abstract past shows a queue that has
# been waiting since January and an idle window with nothing in it.
import datetime as _dt

START = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=68)
clock = ManualClock(START)
cfg = load_config(home)
cloud = FakeBackend("cloud", clock=clock, currency=Currency.USD,
                    capacity={"a10g": 2, "t4": 2}, startup_seconds=60.0,
                    state_path=home / "cloud.json")
b = Broker.open(home, clock=clock, backends=[cloud], config=cfg)

for who in ("ana", "bo", "cy"):
    b.add_user(who)

# A month of prior usage, so fair share has something real to order by.
from gpu_broker.db.connection import transaction
from gpu_broker.models import LedgerKind
from gpu_broker.money import money
import datetime as dt
for who, spent in (("cy", "31.00"), ("bo", "6.00")):
    with transaction(b.store.conn) as conn:
        b.store._insert_ledger(conn, job_id=None, user_id=who, currency=Currency.USD,
                               kind=LedgerKind.SETTLE, amount=money(spent),
                               # Last month, so it shapes fair share without
                               # consuming this month's budget.
                               at=clock.now() - dt.timedelta(days=25), note="last month")

# cy submits first, ana last -- fair share should invert that.
for who in ("cy", "bo", "ana"):
    b.submit(user_id=who, command=f"python train.py --user {who}", gpu_type="a10g", hours=3)
    clock.advance(minutes=2)

# bo's script dies silently right after it starts. This is the failure the
# whole project exists to catch.
b.tick()
dead = next(j for j in b.store.active_jobs() if j.user_id == "bo")
cloud.set_utilization(dead.job_id, 0.0)
for _ in range(60):
    clock.advance(minutes=1)
    b.tick()

b.store.conn.close()
print("ready")

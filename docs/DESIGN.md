# Design notes

Why the broker does things the way it does. The [README](../README.md) has the
three rules; this is the reasoning underneath them, and the bits that are easy to
get subtly wrong.

- [Queue ordering](#queue-ordering)
- [Two currencies](#two-currencies)
- [Budgets](#budgets)
- [Durability](#durability)
- [Placement](#placement)
- [Idle detection and reclaim](#idle-detection-and-reclaim)
- [Spot and preemption](#spot-and-preemption)
- [Making your job resumable](#making-your-job-resumable)
- [Environments](#environments)
- [The lab pool](#the-lab-pool)
- [Measurement](#measurement)

---

## Queue ordering

```
priority = w_fair × (1 − share of pool used) + w_age × min(1, waited / age_max)
```

Defaults: `w_fair = 0.5`, `w_age = 0.5`, `age_max = 12h`. Usage decays by half
every 14 days over a 30-day window, so one heavy month fades rather than becoming
a sentence.

The first term is fair share. The second exists because the first alone starves
people: without it a heavy user loses to every newly-arriving light user forever,
and in a twenty-person club that is one person who stops using the broker.

**`w_age` must be at least `w_fair`.** If the age term cannot outweigh the
fairness term, the starvation guard is decorative. The config refuses to start
otherwise, with that explanation.

The ordering is a total order - ties break on submit time, then job id - because
without one, two ticks could dispatch same-priority jobs in different orders and
`gpu queue` would appear to shuffle while nothing changed.

`gpu queue --why` prints both terms for every job. If you are behind, the answer
is a row of numbers rather than a shrug.

**Where this loses.** Fair share is a cost paid by whoever would otherwise have
had the pool to themselves. With one active user it is pure overhead: queue
latency and a poll interval bought in exchange for a fairness nobody needed.
`gpu report` prints that user's cumulative wait by name, so the trade shows up
next to the numbers that flatter the design rather than only in the ones that do.

## Two currencies

Cloud capacity costs dollars. The lab A6000 costs nothing and is scheduled in
GPU-hours. These are not convertible, so:

- every ledger row carries a currency
- every member has two budgets
- fair share computes a share per currency and blends them (70% dollars, 30%
  GPU-hours), rather than inventing an exchange rate

The alternative - shadow-pricing free hours into dollars - reports "you spent
$3.20" to somebody who ran on free hardware, and silently rewrites everyone's
history each time the rate is retuned.

Somebody who lives on the free lab machine has still consumed real club capacity,
which is why free hours count toward their share at all.

## Budgets

Reserved at admission, settled as the job runs, remainder released however it
ends:

```
committed = held + spent          available = budget − committed
```

That gives two behaviours that look similar and are not:

- **Your budget refuses you.** If your monthly budget cannot cover the job, it is
  refused at submit with the shortfall as a number. Waiting would not help.
- **The pool cap makes you wait.** It frees up as other jobs settle, so the job
  queues rather than being thrown away.

Queued reservations count against *your* budget - so you cannot queue fifty jobs
you cannot pay for - but **not** against the pool cap, because a queued job has
consumed nothing. Conflating those means a deep enough queue blocks its own
dispatch and the club looks over budget while the GPUs sit idle.

`--budget` is a hard ceiling, not an estimate. Billing starts when the broker
takes capacity, not when your command starts, because that is when a cloud
instance starts charging - so the default reservation includes a 15-minute
startup allowance. Without it every job with a default budget dies one boot short
of finishing.

## Durability

The thing this project cares most about being correct.

- SQLite in WAL mode with `synchronous = FULL`. Every commit is fsynced.
- **One state transition is one committed transaction**, together with its ledger
  consequence. There is no state where a job has moved but its money has not.
- The ledger is **append-only**. Balances are aggregated from it on read, never
  stored, so a crash can lose an in-flight write but cannot leave a balance that
  disagrees with its own history.
- The `UPDATE` that moves a job is guarded on the state that was read, so two
  schedulers cannot both dispatch one job.
- On restart, `gpu reconcile` compares every backend's actual resources against
  the records and **reports** drift. It does not terminate anything.

`tests/test_gate.py` proves this by `SIGKILL`-ing a real process that is actively
opening transactions, then checking the queue, the ledger, and the job→machine
mapping all survived.

## Placement

An ordered list of tiers; a job takes the first tier with a free GPU **right
now**:

```json
{ "placement_order": ["local", "spot", "ondemand"] }
```

It never waits for cheaper capacity. If the lab GPU is busy and the pool has
credits, the job takes the credits. That trades money for latency deliberately -
making a sophomore wait six hours for the free card while four hundred dollars of
unspent credit sits there is how a broker stops getting used. Reverse the list if
you would rather queue than spend.

A backend that has not declared a tier defaults to **on-demand**, the expensive
one. A misconfigured backend that silently landed in the free tier would be
picked first and quietly spend money the policy meant to defer.

Every decision writes its reasoning onto the job, so "why did my job cost money"
has an answer in `gpu logs`.

## Idle detection and reclaim

The failure this exists for: somebody launches an instance, the training script
dies at 2am, and the machine bills until Thursday. Nobody is being careless;
nobody is looking.

Every running job's GPU is sampled on an interval. Under 5% for ten minutes marks
it idle, the owner is told, and after a grace period it is listed for reclaim.

Three rules, two of which are restrictions:

- **Nothing is reclaimed without a recorded notification.** The check is a lookup
  in the notifications table, not a promise in a comment.
- **The samples that justified a reclaim outlive the job.** Deleting them with
  the job would make the decision unauditable exactly when it matters.
- **It reports before it acts**, and is off until you set `reclaim_enabled`.

Idle time is measured from when the job actually stopped working, not from the
start of the detection window. Otherwise a job that has burned an afternoon
reports "idle 0.2h, $0.15 wasted", which is exactly the number nobody acts on.

On the lab machine, utilization is attributed **per process** against the job's
cgroup. Two users share one A6000 under MPS, so the card's own utilization says
nothing about whose job is working - reclaiming on that number would kill an idle
job's busy neighbour.

If usage cannot be attributed, **no sample is recorded**. A recorded 0% from a
failed lookup is indistinguishable from an idle job.

## Spot and preemption

Spot is how a fixed pool stretches, and it is why checkpointing has to exist: AWS
can take the machine back with two minutes' notice, and two minutes is less than
a scheduler tick. So everything that has to happen in that window happens **on
the instance**.

A watcher running beside the job polls IMDS for `spot/instance-action`. On a
notice it sends `SIGUSR1`, waits for the job to say it has finished writing,
uploads the files, and *then* uploads a manifest. Nothing is visible as a
checkpoint until the manifest lands, so an upload cut off halfway leaves the
previous checkpoint as the newest complete one. Resuming from a half-uploaded
checkpoint does not crash - it loads a truncated tensor and trains garbage for
another six hours.

Checkpoints are **never overwritten**. Each is written under its own step.

A preempted job is not a failed job: it goes back in the queue, keeps its budget
reservation, and resumes from its last checkpoint. What it already burned stays
billed, because that time was really spent.

**A job that never checkpoints gets moved off spot.** After two preemptions with
nothing saved it is pinned to on-demand and its owner is told why - paying twice
to redo the same hour costs more than on-demand would have.

A preemption and somebody hitting Terminate in the console look identical in
`describe-instances` and mean opposite things, so the broker discriminates on
`StateReason` / `InstanceLifecycle`: on-demand capacity is never reclaimed, so a
terminated on-demand instance was terminated by a person.

## Making your job resumable

Three ways in, one contract underneath. The broker sends a signal and preserves a
directory; what goes in that directory is your script's job.

**Anything at all:**

```python
from gpu_broker.adapters import preempted, checkpoint_dir, saved

for step, batch in enumerate(loader):
    train(batch)
    if preempted():
        torch.save(state, checkpoint_dir() / "state.pt")
        saved(step)          # nothing is uploaded until you say this
        break
```

`preempted()` is *polled* rather than acted on inside the signal handler. A
handler interrupts whatever was running - on a training loop that is the middle
of a CUDA call - and saving from there gives you a corrupt checkpoint instead of
no checkpoint.

**PyTorch:**

```python
from gpu_broker.adapters import Checkpointer

ckpt = Checkpointer(model, optimizer, scheduler=scheduler, every=500)
start = ckpt.resume()                    # 0 on a first run

for step in range(start, total_steps):
    train_one_step()
    if ckpt.step(step):
        break                            # True only when the broker wants us gone
```

Model, optimizer, scheduler, RNG state, and position. RNG state is in there
deliberately: without it a resumed run sees a different data order and different
dropout masks from the run it is continuing.

**HuggingFace `Trainer`:**

```python
from gpu_broker.adapters import BrokerCallback

trainer = Trainer(..., callbacks=[BrokerCallback()])
trainer.train(resume_from_checkpoint=BrokerCallback.resume_from())
```

### What checkpointing cannot do

There is no magic. Out of scope, explicitly:

- **Arbitrary process state.** No CRIU, no memory snapshots. If your state is
  only in RAM, it is gone.
- **Non-deterministic work.** Resuming reproduces an uninterrupted run only if
  your job is deterministic given its saved state.
- **External side effects.** Rows already written, files already pushed, an API
  already called - resume replays from the checkpoint and those happen twice.
- **Anything on the old machine.** Only the checkpoint directory survives.

## Environments

An environment is a **spec** - a base image plus packages - and everything keys
off the hash of that spec:

```bash
gpu env create vision -r requirements.txt --apt ffmpeg
gpu submit --gpu a10g --hours 4 --env vision -- python train.py
```

The digest is what makes reuse *safe*. Two people asking for the same packages
share one build; changing a requirement changes the key, so a cached build can
never be stale. Reordering a `requirements.txt` does **not** change it.

Nothing is built ahead of time. The first job that asks for an environment builds
it, on the machine it was going to run on anyway, and pushes so the next one
pulls. No build server exists.

Two materialisations, on purpose:

- **EC2**: a container image in ECR. Machines are fresh, so an image is the
  honest unit.
- **The lab machine**: a cached virtualenv keyed on the same digest. Not a
  container - the A6000 shares one card via MPS with per-client memory limits and
  cgroup limits from systemd. Docker there would move both into `docker run`
  flags and bind-mount the MPS pipe through: a rework of isolation that already
  works, in exchange for nothing the club can see.

The venv build runs *inside the job's own unit*, not before dispatch, because a
cache miss takes minutes and would hold the whole tick open.

## The lab pool

One physical A6000 shared by twenty people, which is where multi-tenancy stops
being a scheduling abstraction.

**GPU memory is capped by CUDA MPS.** The A6000 has no MIG, so the choices are
MPS or time-slicing, and MPS lets kernels from different processes run
concurrently rather than taking turns. Each job is an MPS client with its own
`CUDA_MPS_PINNED_DEVICE_MEM_LIMIT`, set per client rather than as a daemon-wide
default precisely so two jobs can have different limits.

**Host RAM and CPU are capped by cgroup v2** through a transient systemd service.
MPS caps GPU memory and nothing else; a dataloader can still OOM the machine and
take every other job with it.

**Limits come from whichever systemd manager is reachable.** With passwordless
sudo the broker creates a system unit and passes `--uid`; without it, it uses the
user manager, where recent systemd delegates `memory` and `cpu` to the user slice
anyway. A shared research machine will not hand out sudo, so the user path is not
a fallback, it is the normal one.

**A host that cannot enforce limits is drained.** `systemd-run` exits zero on a
machine where the memory controller is not delegated, and applies nothing - so
the health check runs a throwaway scope with a known cap and reads its own cgroup
back. If the number does not match, no jobs land there and it says why.

A drained host keeps running whatever is already on it. Pulling the rug out from
under somebody's training run is worse than whatever caused the drain.

## Measurement

`gpu report` is generated from the ledger, the job history and the samples. It
can be wrong; it cannot be flattering.

The headline is **dollars per useful GPU-hour** - useful meaning above the idle
threshold, not merely billed - because a pool that is half idle costs twice what
the invoice suggests.

Three things are counted separately because they are different complaints:

- **Work lost**: a job interrupted and never finished. Somebody's afternoon.
- **Time paid for twice**: a job with no checkpoint that still finished. Money,
  not work.
- **Reclaimed hours**: billed hours in which the GPU provably did nothing, with
  the samples still on disk.

The report has a **where it loses** section: startup overhead in minutes and
dollars, how many jobs waited longer than launching an instance themselves would
have taken, and the heaviest user's total fair-share penalty by name. That is not
modesty - the only way those numbers get looked at is if they print next to the
good ones.

What it does not measure, and says so: MPS overhead against exclusive access
(needs the same job timed both ways on the real card), and whether a member would
actually have got an instance (comparing a queue wait to a self-launch assumes
capacity and quota were there).

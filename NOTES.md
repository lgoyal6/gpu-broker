# NOTES

Ideas that came up while building but were **not** asked for. Nothing here is implemented.

## Phase 0

- **Hard starvation cutoff.** The chosen priority is `w_fair * (1 - usage_share) + w_age * min(1, wait/age_max)`.
  With the default weights (0.5 / 0.5) a fully-aged job outranks a fresh zero-usage job, so the heavy
  user is not starved. If real club traffic proves that insufficient, the blunt fix is a
  `starvation_hours` cutoff that floats any job waiting longer than N hours to the front. Deliberately
  not built: it is a second mechanism doing the first mechanism's job, and it should only exist if
  measurement says the weights failed.
- **Priority preemption.** Right now a running job is never bumped for a higher-priority queued job.
  Correct for Phase 0 (nothing can checkpoint yet). Revisit after Phase 4.
- **Backend price refresh.** Prices are a hardcoded table with a `priced_at` date. Phase 3 replaces
  this with the AWS pricing API, as specified. Leaving the shape ready, not the fetch.

## Phase 1

- **Terminating orphans.** `gpu reap` reports and stops there, as asked. When it
  has been watched being right for a few weeks, the obvious next step is
  `gpu reap --terminate` behind a confirmation and an age threshold. Deliberately
  absent: a test asserts there is no such flag, so adding one is a deliberate act.
- **AMI resolution from SSM public parameters.** `aws.ami` is a hardcoded id per
  region, which goes stale. AWS publishes Deep Learning AMI ids at
  `/aws/service/deeplearning/ami/...` in Parameter Store. Worth doing when
  somebody first hits a stale AMI, not before.
- **Interleaving stdout and stderr by timestamp.** Right now a poll appends all
  new stdout then all new stderr, so ordering is by poll rather than strictly by
  wall clock. A true merge means paginating two streams together. Not worth it
  while a tick is seconds wide; revisit if Phase 5's live streaming makes it
  visible.
- **Spot instances.** Phase 4. On-demand only until there is something to
  checkpoint, since a spot interruption without checkpointing is just a lost job.

## Phase 2

- **Combining poll and log-tail into one round trip.** `poll` and `fetch_logs`
  are two SSH commands per job per tick. One script could return both, halving
  the round trips. Not done because it couples two things the `Backend` protocol
  keeps separate, and on a LAN with a pooled connection each round trip is
  milliseconds. Revisit if the lab pool ever spans more than a couple of hosts.
- **Keeping stdout and stderr apart on local jobs.** They are merged, which is
  what you see running the same command in a terminal. Splitting them means two
  cursors that have to stay consistent across a broker restart, and the ordering
  between the two streams would still be a guess. EC2 keeps them separate
  because SSM gives us two named CloudWatch streams for free.
- **Automatic drain recovery.** A host drained by a failed health check comes
  back on its own when the check passes. A host drained by hand stays drained
  until somebody says otherwise. That asymmetry is deliberate, but it means a
  manual drain can be forgotten; a reminder in the weekly digest (Phase 7) would
  fix it.
- **Measuring the MPS overhead.** The gate proves the broker sets per-client
  memory limits and that one client cannot disturb another *in simulation*. What
  MPS actually costs against exclusive access on a real A6000 is unmeasured, and
  `max_jobs_per_gpu = 2` is a starting point rather than a finding. Phase 7
  publishes it either way.

## Phase 3

- **Spot as a real tier.** `placement_order` already has a `spot` rung and no
  backend claims it, so it is skipped. Phase 4 fills it in; until then, adding
  it now would mean a tier that quietly does nothing.
- **Reclaim on a schedule.** `gpu reclaim` is a command somebody runs. Once
  `reclaim_enabled` is on, it wants to be part of the tick rather than something
  you remember. Deliberately not wired in yet: a thing that kills jobs should
  need a person to run it until it has a track record.
- **Notifications that reach people.** They are recorded and shown in `gpu status`
  and nowhere else. Phase 5 puts them in front of somebody; Phase 7's digest is
  the other half. Email or Slack now would be a delivery mechanism with no
  audience.
- **Sampling cost on EC2.** One extra SSM command per job, which then writes to
  CloudWatch and is read with a call the broker already makes. The alternative
  -- an SSM round trip per job per tick -- has agent-poll latency measured in
  seconds and would make a tick as slow as the number of running jobs.
- **Per-process attribution on EC2.** Unnecessary today, since one instance runs
  one job, so whole-GPU utilization *is* the job's. If Phase 6 ever packs two
  containers onto one instance, this needs the cgroup matching the local backend
  already does.
- **Idle detection has a hole while the broker is down.** Samples stop, so a job
  that goes idle during an outage is not noticed until sampling resumes and the
  window refills. Correct but slow; a real fix would date the idle period from
  the last sample rather than from the window.

## Phase 4

- **The on-instance wrapper is asserted, not executed.** Its contents are
  checked -- that it polls `spot/instance-action`, sends SIGUSR1, waits for the
  job's completion marker before uploading, and writes the manifest last -- but
  no test runs it on a real instance, because no test has one. The same caveat
  as MPS in Phase 2: the shape is verified, the behaviour on real hardware is
  not, and only a real preemption will confirm it.
- **Capacity-aware selection uses price, not capacity.** `GetSpotPlacementScores`
  is the real signal and needs account history that a new club account does not
  have; moto does not implement it either. Recent spot price stands in, on the
  theory that a pool that is short is a pool whose price has moved. Worth
  revisiting once the account has run enough spot to get scores.
- **Checkpoints are never pruned automatically.** `CheckpointStore.prune` exists
  and nothing calls it. A month of checkpoints for a hundred jobs is real S3
  money; wiring it into the tick is a small change, but deleting somebody's only
  copy of six hours of work is exactly the kind of automation that should wait
  for a person to ask for it.
- **A resumed job is billed from scratch each attempt.** The ledger settles per
  attempt, so a job preempted four times shows four runs' worth of spend. That is
  honest -- the time really was bought -- but it makes "dollars per useful
  GPU-hour" in Phase 7 need care: the useful hours are the ones after the last
  resume, not the sum.
- **Nothing checks that a job is actually deterministic.** The gate proves the
  broker resumes correctly given a job that checkpoints correctly. A job with
  unseeded randomness will resume to a different answer and nothing will notice.

## Phase 5

- **The web app never runs the scheduler.** It could -- a background task would
  make it one process to deploy -- and it deliberately does not, because the box
  that has to be reachable for OAuth would then hold the AWS keys and the lab SSH
  key. Two things to run is the price.
- **No htmx after all.** The build prompt offered it; plain forms plus native
  `EventSource` turned out to need nothing at all, and a CDN dependency in a club
  tool is a thing that breaks on a Sunday. Revisit if a page ever wants partial
  updates that a form cannot express.
- **Sessions do not survive a restart unless the secret is set.** With no
  `GPU_BROKER_SESSION_SECRET` a fresh one is generated per process, so everybody
  is signed out on deploy. A generated default is a papercut; a hardcoded one
  would let anybody forge a session.
- **The dashboard has no history.** Utilization over time is per-job, and the
  pool view shows the present moment. Phase 7 stores time series, and that is
  where a chart belongs.
- **`gpu doctor` cannot check outbound network to AWS without credentials.**
  It checks what it can reach; a firewall that blocks the EC2 endpoint shows up
  as a credentials failure rather than as a firewall. Worth a dedicated probe if
  it ever bites.
- **No rate limiting on submit.** A member could queue a thousand jobs from the
  browser. Their own budget stops them from spending anything, so the damage is a
  cluttered queue rather than money, but it is not nothing.

## Phase 6

- **Two materialisations rather than one.** Containers everywhere would be one
  code path and stronger reproducibility, and it would mean redoing Phase 2's
  isolation: the lab box's memory and CPU limits move from systemd to `docker
  run`, and MPS needs its pipe directory bind-mounted. The digest is shared, so
  an environment means the same thing on both; only how it is materialised
  differs.
- **No build server.** The first job that wants an environment builds it, on the
  machine it was going to run on anyway, and pushes so the next one pulls. A
  dedicated builder would make the first job faster and would be one more thing
  to run, pay for, and notice when it breaks. Revisit if first-job latency turns
  out to be what stops people using environments.
- **Nothing prunes old images or virtualenvs.** Every distinct spec anybody has
  ever submitted stays in ECR and on the lab disk. That is real money and real
  disk. `gpu env delete` removes the *name* and deliberately leaves the build,
  because jobs already running against that digest still need it; reaping unused
  digests wants an age threshold and a look at what is running.
- **`volumes.quota_gb` is advisory.** EFS has no per-directory quota. Reporting
  usage is honest; enforcing it would mean a filesystem per user, which is a
  mount target per user and is not worth it at this size.
- **Environments are global, not per-user.** Anybody can create one and anybody
  can use it, and `gpu env create` on an existing name overwrites it. Fine for
  twenty people who know each other; wrong the moment it is not.
- **The image build has no cache between instances beyond ECR.** A miss rebuilds
  every layer from the base. Docker layer caching across ephemeral instances
  would need a registry cache or BuildKit's remote cache, which is more moving
  parts than the first-job-pays approach earns back.

## Phase 7

- **Metrics live in SQLite, not a TSDB.** A few thousand rows a week from twenty
  people. Prometheus can scrape `/metrics`, so anybody who wants real dashboards
  already can, without the club running another database.
- **Nothing prunes the series on a schedule.** `gpu metrics --prune` exists and
  a cron would be one line. Left manual for the same reason as everything else
  that deletes: it should be a person's decision the first few times.
- **The digest has no scheduler.** `gpu digest --send` from a weekly cron is the
  intended use, and the broker does not run cron for you. Building a scheduler
  in-process would be a second thing that has to survive a restart.
- **The "before" baseline needs `ce:GetCostAndUsage`.** Without it the report has
  no before-column and says so. Cost Explorer also lags by up to a day, so a
  baseline pulled today omits yesterday.
- **`dollars_per_useful_hour` is sampled, not integrated.** Useful hours are the
  fraction of samples above the idle threshold multiplied by billed time. At a
  60-second sampling interval a job that alternates fast between busy and idle is
  approximated, not measured. Stated in the report's own caveats.
- **The self-launch comparison is a guess.** Three minutes to launch an instance
  yourself, hardcoded and labelled as a guess wherever it is printed. It also
  assumes capacity and quota were there, which on a fresh account they are not --
  also stated.
- **MPS overhead is still unmeasured.** It needs the same job run both ways on
  the real A6000 and timed. The report names it as not measured rather than
  leaving a gap somebody assumes was covered.

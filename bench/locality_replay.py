#!/usr/bin/env python3
"""Replay one scripted week of lab placements twice, and count what changed.

Two arms over the same script:

* **baseline** -- the ordering the pool has always used: most free slots, then
  hostname.
* **candidate** -- the same ordering with one step inserted between them. Among
  hosts tied for emptiest, prefer one that already has the job's environment
  digest built.

Both arms call the shipped `prefer_warm` and the shipped `EnvironmentFacts`, so
this measures the rule that is actually in the placement path rather than a
second copy of it written to agree with the first.

**What this is not.** It is a deterministic local replay of a synthetic
workload. The materialisation cost is a number in this file, not a measured pip
install; the hosts are dictionaries; no SSH, no GPU, no account, no network. It
can show that the tie-break does not break admission or capacity ordering, and
it can show the direction and rough size of the environment-preparation saving
on *this* script. It cannot tell you what the lab pool would do, and a number
from here is not evidence that anything in production got faster.

Run:
    python3 bench/locality_replay.py --output bench/results/locality_replay.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from dataclasses import dataclass, field

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from gpu_broker.local.locality import FACT_TTL_SECONDS, EnvironmentFacts, prefer_warm

# --------------------------------------------------------------- the script
#
# Everything below is fixed. No clock, no randomness, no environment lookup:
# two runs a month apart have to produce the same file, or a difference in the
# numbers cannot be attributed to a change in the code.

START = dt.datetime(2026, 3, 2, 9, 0, tzinfo=dt.timezone.utc)

HOSTS: tuple[tuple[str, int], ...] = (
    # hostname, concurrent job slots
    ("lab1", 4),
    ("lab2", 4),
    ("lab3", 4),
)

DIGESTS: tuple[str, ...] = ("d0a1b2c3", "d1b2c3d4", "d2c3d4e5", "d3d4e5f6")
"""Four environments in circulation. Stand-ins for a content hash; the replay
only ever compares them for equality, which is all the placement rule does."""

SUPERSEDED_AT = 90
"""Part way through, one environment's spec changes and its digest changes with
it. Every host that was warm for the old digest is cold for the new one, which
is the case a tie-break keyed on a name rather than on content would get wrong."""

SUPERSEDES = {DIGESTS[0]: "d4e5f6a7"}

JOB_COUNT = 240
ARRIVAL_SECONDS = 200
"""Chosen so the pool runs busy but not saturated: mean occupancy is about eight
of the twelve slots, so hosts are frequently tied for emptiest -- which is the
only situation the tie-break can act in -- and every scripted job is admitted in
both arms.

That last part is the requirement, not a preference. A saturated script makes the
two arms place *different sets of jobs*, and once that happens the cold and warm
counts describe two different workloads and cannot be subtracted. The first run
of this replay was saturated and was rejected for exactly that reason; see the
admission check in `compare`, which still fails the run loudly if it recurs."""

MATERIALIZE_SECONDS = 240.0
"""What a cold environment costs the job that hits it. A stand-in for a pip
install of a few large wheels, chosen once and used by both arms identically."""

PRELOADED: dict[str, tuple[str, ...]] = {
    "lab1": (DIGESTS[0],),
    "lab2": (DIGESTS[1],),
    "lab3": (),
}
"""Where the script starts: two hosts already have one environment each."""

EVICTIONS: tuple[tuple[int, str, str], ...] = (
    # (job index at which it happens, hostname, digest removed from that host)
    # Somebody clears a virtualenv by hand. Any fact recorded before this and
    # still inside its lifetime is now wrong, which is the cost of caching and
    # is counted below rather than hidden.
    (60, "lab1", DIGESTS[0]),
    (150, "lab2", DIGESTS[1]),
)

def probe_fails(index: int, hostname: str) -> bool:
    """When the locality probe does not answer.

    Stated as a rule over (job, host) rather than as a list of job indices. A
    list only bites if the candidate happens to probe that exact host on that
    exact job, and `prefer_warm` stops probing as soon as a host says warm -- so
    a list quietly produced zero failures and left the case untested.

    A failure only ever makes the tie-break do less. It cannot move the result in
    the candidate's favour.
    """
    if hostname == "lab3" and 40 <= index <= 70:
        return True
    if hostname == "lab2" and index % 23 == 0:
        return True
    return False

UNREACHABLE: tuple[tuple[int, int, str], ...] = (
    # (first job index, last job index inclusive, hostname) the host is down.
    (75, 95, "lab3"),
)


def scripted_jobs() -> list[dict]:
    """The workload. Every field is a closed-form function of the index."""
    jobs = []
    for index in range(JOB_COUNT):
        # Every seventh job runs on the bare machine and needs no environment.
        digest = None if index % 7 == 6 else DIGESTS[(index * 3) % len(DIGESTS)]
        if digest is not None and index >= SUPERSEDED_AT:
            digest = SUPERSEDES.get(digest, digest)
        jobs.append(
            {
                "index": index,
                "job_id": f"job-{index:04d}",
                "arrives_at": index * ARRIVAL_SECONDS,
                "digest": digest,
                "run_seconds": 600 + (index % 5) * 300,
            }
        )
    return jobs


# ------------------------------------------------------------------ the arms


@dataclass
class Arm:
    """One pool, one policy, and the tally of what it did."""

    name: str
    uses_locality: bool
    disks: dict[str, set[str]] = field(default_factory=dict)
    running: list[dict] = field(default_factory=list)
    facts: EnvironmentFacts = field(default_factory=EnvironmentFacts)

    placed: int = 0
    not_admitted: int = 0
    not_admitted_jobs: list[str] = field(default_factory=list)
    cold_materializations: int = 0
    warm_reuses: int = 0
    no_environment: int = 0
    environment_seconds: float = 0.0
    by_host: dict[str, int] = field(default_factory=dict)
    tiers: dict[str, int] = field(default_factory=dict)
    probes: int = 0
    probe_failures: int = 0
    facts_reused: int = 0
    stale_fact_misses: int = 0
    """Times a believed fact said "warm" and the disk disagreed. The honest cost
    of caching an observation instead of asking every time."""

    def __post_init__(self) -> None:
        for hostname, _ in HOSTS:
            self.disks[hostname] = set(PRELOADED.get(hostname, ()))
            self.by_host[hostname] = 0

    # -- the world ---------------------------------------------------------

    def release_finished(self, now: int) -> None:
        self.running = [job for job in self.running if job["frees_at"] > now]

    def free_slots(self, hostname: str, capacity: int) -> int:
        live = sum(1 for job in self.running if job["hostname"] == hostname)
        return max(0, capacity - live)

    def reachable(self, hostname: str, index: int) -> bool:
        for first, last, down in UNREACHABLE:
            if down == hostname and first <= index <= last:
                return False
        return True

    def evict(self, index: int) -> None:
        for at, hostname, digest in EVICTIONS:
            if at == index:
                self.disks[hostname].discard(digest)

    # -- the decision ------------------------------------------------------

    def place(self, job: dict, now: dt.datetime) -> None:
        index = job["index"]
        self.evict(index)
        self.release_finished(job["arrives_at"])

        ranked = [
            (self.free_slots(hostname, capacity), hostname)
            for hostname, capacity in HOSTS
            if self.reachable(hostname, index)
        ]
        ranked = [pair for pair in ranked if pair[0] > 0]
        if not ranked:
            self.not_admitted += 1
            self.not_admitted_jobs.append(job["job_id"])
            return
        ranked.sort(key=lambda pair: (-pair[0], pair[1]))

        digest = job["digest"]
        is_warm = None
        if self.uses_locality and digest is not None:

            def is_warm(hostname: str) -> bool:
                return self.observe(hostname, digest, index, now)

        hostname = prefer_warm(ranked, is_warm)

        startup = 0.0
        if digest is None:
            self.no_environment += 1
        elif digest in self.disks[hostname]:
            self.warm_reuses += 1
        else:
            self.cold_materializations += 1
            startup = MATERIALIZE_SECONDS
            self.disks[hostname].add(digest)
            if self.uses_locality and self._believed_warm(hostname, digest, now):
                self.stale_fact_misses += 1

        self.environment_seconds += startup
        self.placed += 1
        self.by_host[hostname] += 1
        self.tiers["local"] = self.tiers.get("local", 0) + 1
        self.running.append(
            {
                "hostname": hostname,
                "frees_at": job["arrives_at"] + startup + job["run_seconds"],
            }
        )

    def _believed_warm(self, hostname: str, digest: str, now: dt.datetime) -> bool:
        fact = self.facts.get(hostname, digest, now)
        return fact is not None and fact.materialized

    def observe(self, hostname: str, digest: str, index: int, now: dt.datetime) -> bool:
        """What the broker does: believe a fresh fact, otherwise probe once."""
        fact = self.facts.get(hostname, digest, now)
        if fact is not None:
            self.facts_reused += 1
            return fact.materialized

        self.probes += 1
        if probe_fails(index, hostname):
            # Unreadable answer. Not recorded, not treated as a hit: the host
            # simply does not win the tie.
            self.probe_failures += 1
            return False

        present = digest in self.disks[hostname]
        self.facts.record(hostname, digest, present, now)
        return present

    # -- the report --------------------------------------------------------

    def report(self) -> dict:
        return {
            "arm": self.name,
            "locality_tie_break": self.uses_locality,
            "jobs_placed": self.placed,
            "jobs_not_admitted": self.not_admitted,
            "not_admitted_job_ids": self.not_admitted_jobs,
            "outcomes": {"placed": self.placed, "not_admitted": self.not_admitted},
            "jobs_with_no_environment": self.no_environment,
            "cold_environment_materializations": self.cold_materializations,
            "warm_environment_reuses": self.warm_reuses,
            "simulated_environment_startup_seconds": round(self.environment_seconds, 1),
            "placements_by_host": dict(sorted(self.by_host.items())),
            "placements_by_cost_tier": dict(sorted(self.tiers.items())),
            "locality_probes_issued": self.probes,
            "locality_probe_failures": self.probe_failures,
            "locality_facts_reused": self.facts_reused,
            "stale_fact_misses": self.stale_fact_misses,
        }


def run(arm: Arm) -> dict:
    for job in scripted_jobs():
        arm.place(job, START + dt.timedelta(seconds=job["arrives_at"]))
    return arm.report()


def compare(baseline: dict, candidate: dict) -> dict:
    """The differences that decide whether this may go in the placement path.

    An admission or capacity regression is disqualifying regardless of what the
    startup number says: the tie-break may only choose between hosts the old
    rule had already declared equally good.
    """
    saved = (
        baseline["simulated_environment_startup_seconds"]
        - candidate["simulated_environment_startup_seconds"]
    )
    return {
        # Guard, not a statistic. Everything below it is only meaningful when
        # both arms placed the same jobs; see ARRIVAL_SECONDS.
        "comparable": (
            baseline["jobs_placed"] == candidate["jobs_placed"] == JOB_COUNT
            and baseline["jobs_with_no_environment"]
            == candidate["jobs_with_no_environment"]
        ),
        "cold_materializations_delta": (
            candidate["cold_environment_materializations"]
            - baseline["cold_environment_materializations"]
        ),
        "warm_reuses_delta": (
            candidate["warm_environment_reuses"] - baseline["warm_environment_reuses"]
        ),
        "simulated_environment_startup_seconds_saved": round(saved, 1),
        "simulated_environment_startup_percent_saved": (
            round(100.0 * saved / baseline["simulated_environment_startup_seconds"], 1)
            if baseline["simulated_environment_startup_seconds"]
            else 0.0
        ),
        "runnable_job_admission_delta": (
            candidate["jobs_placed"] - baseline["jobs_placed"]
        ),
        "cost_tier_changes": _tier_changes(baseline, candidate),
        "cost_tier_note": (
            "Structurally zero. The tie-break runs inside the local backend's host "
            "selection, which is reached only after placement has already chosen a "
            "backend and therefore a tier. It cannot move a job between tiers."
        ),
    }


def _tier_changes(baseline: dict, candidate: dict) -> int:
    """Whether the candidate used a tier the baseline did not, or dropped one.

    Counts distinct tiers, not placements. Subtracting per-tier placement counts
    would report a difference whenever the arms placed different numbers of jobs
    for any reason at all, which says nothing about tiers.
    """
    return len(
        set(baseline["placements_by_cost_tier"]).symmetric_difference(
            candidate["placements_by_cost_tier"]
        )
    )


def verdict(differences: dict) -> str:
    """What the numbers permit as a claim, and nothing beyond it."""
    if differences["comparable"] is not True:
        return (
            "invalid comparison: the two arms did not place the same set of jobs, "
            "so their environment counts describe different workloads"
        )
    if differences["runnable_job_admission_delta"] < 0:
        return (
            "rejected: the tie-break admitted fewer runnable jobs than the "
            "existing ordering"
        )
    if differences["cost_tier_changes"] != 0:
        return "rejected: the tie-break moved jobs between cost tiers"
    if differences["cold_materializations_delta"] > 0:
        return "loss: the tie-break caused more cold environment materializations"
    if differences["cold_materializations_delta"] == 0:
        return (
            "tie: the tie-break changed no environment materialization on this "
            "script. It also cost nothing -- same jobs admitted, same cost tier, "
            "same total environment preparation. No performance claim is available "
            "from this run; what it supports is that the mechanism is inert when it "
            "has nothing to act on"
        )
    return (
        "measured on this synthetic replay: fewer cold environment materializations "
        "with no change to admission or cost tier. Not evidence of a production effect"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="bench/results/locality_replay.json",
        help="where to write the machine-readable result",
    )
    args = parser.parse_args()

    baseline = run(Arm("baseline", uses_locality=False))
    candidate = run(Arm("candidate", uses_locality=True))
    differences = compare(baseline, candidate)

    result = {
        "replay": "gpu-broker environment locality tie-break",
        "deterministic": True,
        "credential_free": True,
        "network_calls": 0,
        "script": {
            "hosts": [{"hostname": name, "slots": slots} for name, slots in HOSTS],
            "jobs": JOB_COUNT,
            "arrival_interval_seconds": ARRIVAL_SECONDS,
            "digests_in_circulation": list(DIGESTS),
            "digest_superseded_at_job": SUPERSEDED_AT,
            "digest_superseded": SUPERSEDES,
            "materialization_cost_seconds": MATERIALIZE_SECONDS,
            "preloaded_environments": {k: list(v) for k, v in PRELOADED.items()},
            "hand_evictions": [
                {"at_job": at, "hostname": host, "digest": digest}
                for at, host, digest in EVICTIONS
            ],
            "probe_failure_rule": (
                "lab3 for jobs 40-70 inclusive; lab2 on every 23rd job"
            ),
            "unreachable_windows": [
                {"from_job": a, "to_job": b, "hostname": h} for a, b, h in UNREACHABLE
            ],
            "fact_ttl_seconds": FACT_TTL_SECONDS,
        },
        "baseline": baseline,
        "candidate": candidate,
        "differences": differences,
        "verdict": verdict(differences),
        "limitations": [
            "Synthetic replay. Hosts are dictionaries and the materialisation cost "
            "is a constant in this file, not a measured build.",
            "No SSH, no GPU, no cloud account, no network, no money.",
            "Shows the direction and size of the effect on this script only. It is "
            "not a measurement of the lab pool and not evidence of production adoption.",
            "Both arms share one script, but occupancy diverges after the first "
            "differing placement, which is the effect being measured rather than a "
            "difference in inputs. The run is only reported when both arms still "
            "placed every job; see `differences.comparable`.",
            "Little room to act: three hosts and five digests means the hosts hold "
            "most environments most of the time, so only 15 of 206 environment jobs "
            "ever meet a cold environment at all. A script with more environments "
            "than host-digest slots would give the tie-break more to do, and was not "
            "substituted for this one after the result was known.",
            "The fact cache produced no hits here (`locality_facts_reused` is 0): at "
            "200s between arrivals a digest recurs roughly every 800s, which is past "
            "the 300s fact lifetime. Expiry is exercised heavily; reuse is covered by "
            "tests/test_locality.py rather than by this replay.",
        ],
    }

    out = pathlib.Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")

    print(f"baseline : {json.dumps(baseline)}")
    print(f"candidate: {json.dumps(candidate)}")
    print(f"deltas   : {json.dumps(differences)}")
    print(f"verdict  : {result['verdict']}")
    print(f"written  : {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""What a lab host already has on disk, and how long that stays believable.

The first job to ask for an environment pays minutes to build its virtualenv.
Every later job with the same digest pays nothing, because `venv_script` moves
the finished tree into `<root>/<digest>` and exits early when it finds it there.
So "which host already has this digest" is worth knowing when two hosts are
otherwise indistinguishable.

**Only when they are otherwise indistinguishable.** Sending a job to a busier
host to save one build trades a cost somebody pays once against a cost every job
sharing that card pays for its whole run. Free capacity decides first and this
decides last, which is why the thing recorded here is a fact and not a weight.

A fact is deliberately thin: this host, this digest, materialised or not,
observed then, believable until then. No commands, no usernames, no job
payloads, no scores. Anything richer would be a policy wearing an observation's
clothes, and the policy already exists -- it is the emptiest-host rule above.

Nothing here is ever a reason to *reject* a host. A probe that errors, times out
or answers something unreadable leaves placement exactly as it was before this
module existed.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Sequence
from dataclasses import dataclass

FACT_TTL_SECONDS = 300.0
"""How long an observation is worth acting on.

Short, because the thing observed can change without telling us: somebody clears
a stale virtualenv by hand, a disk fills, a host is reimaged. Acting on an old
"it is there" costs a job the build it thought it was skipping, which is the
same cost it would have paid anyway -- so the penalty for being wrong is small
and the window can stay small too.
"""


@dataclass(frozen=True)
class EnvironmentFact:
    """One observation: this digest, on this host, at this moment."""

    hostname: str
    digest: str
    materialized: bool
    """Whether the *final* environment path was usable. A `.building-<digest>`
    staging tree is not this, and neither is a directory with no interpreter in
    it."""
    observed_at: dt.datetime
    expires_at: dt.datetime

    def fresh(self, now: dt.datetime) -> bool:
        return now < self.expires_at


class EnvironmentFacts:
    """The facts we currently believe, one per (host, digest).

    In memory and nowhere else on purpose. This is a cache in front of a cheap
    probe, not a record anybody should be able to consult later: a persisted
    claim about a machine's disk outlives the machine's agreement with it, and
    the recovery from a wrong entry would be a cache invalidation problem the
    broker does not otherwise have.
    """

    def __init__(self, ttl_seconds: float = FACT_TTL_SECONDS) -> None:
        self.ttl_seconds = ttl_seconds
        self._facts: dict[tuple[str, str], EnvironmentFact] = {}

    def get(self, hostname: str, digest: str, now: dt.datetime) -> EnvironmentFact | None:
        """The fact we still believe, or None. An expired fact is not a fact."""
        fact = self._facts.get((hostname, digest))
        if fact is None:
            return None
        if not fact.fresh(now):
            del self._facts[(hostname, digest)]
            return None
        return fact

    def record(
        self, hostname: str, digest: str, materialized: bool, now: dt.datetime
    ) -> EnvironmentFact:
        self._drop_expired(now)
        fact = EnvironmentFact(
            hostname=hostname,
            digest=digest,
            materialized=materialized,
            observed_at=now,
            expires_at=now + dt.timedelta(seconds=self.ttl_seconds),
        )
        self._facts[(hostname, digest)] = fact
        return fact

    def _drop_expired(self, now: dt.datetime) -> None:
        """Bounds the cache without a size limit to tune. What keeps it small is
        that entries stop being worth anything on their own."""
        for key in [key for key, fact in self._facts.items() if not fact.fresh(now)]:
            del self._facts[key]

    def __len__(self) -> int:
        return len(self._facts)


def prefer_warm(
    ranked: Sequence[tuple[int, str]], is_warm: Callable[[str], bool] | None
) -> str:
    """Choose from hosts already ordered by (most free slots, then hostname).

    `ranked` is the existing ordering, unchanged. This only ever picks a
    different element of the *first* group -- the hosts tied for emptiest -- and
    only when one of them already has the environment. Everything else, including
    a single candidate, a job with no environment (`is_warm` is None) and two
    equally warm hosts, falls through to `ranked[0]`, which is what the broker
    chose before.
    """
    emptiest = ranked[0][0]
    tied = [hostname for free, hostname in ranked if free == emptiest]
    if is_warm is None or len(tied) < 2:
        return ranked[0][1]
    for hostname in tied:  # already in hostname order, so ties stay deterministic
        if is_warm(hostname):
            return hostname
    return ranked[0][1]

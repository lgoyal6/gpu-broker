"""Which capacity a job lands on, and why.

Now that there are three kinds of capacity, something has to choose. The policy
is an ordered list of tiers -- free local first, then spot, then on-demand -- and
the job goes to the first tier that has room *right now*.

The "right now" is the part worth being explicit about. This never makes a job
wait for cheaper capacity to free up. If the lab GPU is busy and the pool has
credits, the job takes the credits. That trades money for latency deliberately:
a broker that makes a sophomore wait six hours for a free card, while four
hundred dollars of unspent credit sits there, is a broker they stop using.

Every decision records the tiers it skipped and why. When somebody asks why
their job cost money, the answer is a list.
"""

from __future__ import annotations

from dataclasses import dataclass

from .backends.base import Backend
from .config import BrokerConfig
from .errors import BrokerError
from .models import Job

DEFAULT_ORDER: tuple[str, ...] = ("local", "spot", "ondemand")


@dataclass(frozen=True)
class Placement:
    """Where a job goes, and the reasoning that got it there."""

    backend: Backend | None
    tier: str | None
    reason: str
    skipped: tuple[tuple[str, str], ...] = ()
    """(tier or backend name, why it was not used), in the order considered."""

    @property
    def placed(self) -> bool:
        return self.backend is not None

    def explain(self) -> str:
        lines = [f"{self.reason}"]
        lines.extend(f"  skipped {name}: {why}" for name, why in self.skipped)
        return "\n".join(lines)

    def one_line(self) -> str:
        if self.placed:
            skipped = "; ".join(f"{name} ({why})" for name, why in self.skipped)
            return (
                f"placed on {self.backend.name} [{self.tier}]: {self.reason}"
                + (f" -- after {skipped}" if skipped else "")
            )
        return f"not placed: {self.reason}"


def choose(
    job: Job, backends: list[Backend], config: BrokerConfig
) -> Placement:
    """Pick a backend for this job, or explain why none fits."""
    gpu = config.gpu(job.gpu_type)
    skipped: list[tuple[str, str]] = []

    order = config.placement_order or DEFAULT_ORDER
    if job.pinned_tier:
        # This job has already shown that the cheap tier costs it more than it
        # saves. Everything above the pinned tier is off the table.
        if job.pinned_tier not in order:
            order = (job.pinned_tier,)
        else:
            order = order[order.index(job.pinned_tier) :]
    unknown = [tier for tier in order if not any(_tier_of(b) == tier for b in backends)]

    for tier in order:
        in_tier = [backend for backend in backends if _tier_of(backend) == tier]
        if not in_tier:
            continue
        for backend in in_tier:
            if backend.currency is not gpu.currency:
                skipped.append(
                    (
                        backend.name,
                        f"bills in {backend.currency}, {job.gpu_type} is priced in {gpu.currency}",
                    )
                )
                continue
            if not backend.supports(job.gpu_type):
                skipped.append((backend.name, f"has no {job.gpu_type}"))
                continue
            free = backend.free_slots(job.gpu_type)
            if free <= 0:
                skipped.append((backend.name, f"no free {job.gpu_type} right now"))
                continue
            return Placement(
                backend=backend,
                tier=tier,
                reason=(
                    f"first tier in the policy with a free {job.gpu_type} "
                    f"({free} slot{'s' if free != 1 else ''} available)"
                ),
                skipped=tuple(skipped),
            )

    if unknown and not skipped:
        return Placement(
            backend=None,
            tier=None,
            reason=(
                f"no backend is in any configured tier. placement_order is "
                f"{list(order)} and the tiers present are "
                f"{sorted({_tier_of(b) for b in backends})}"
            ),
        )

    return Placement(
        backend=None,
        tier=None,
        reason=f"nothing has a free {job.gpu_type} right now",
        skipped=tuple(skipped),
    )


def _tier_of(backend: Backend) -> str:
    """A backend that has not declared a tier is on-demand.

    Defaulting to the *expensive* tier is deliberate. A misconfigured backend
    that silently landed in the free tier would be picked first and quietly
    spend money the policy meant to defer.
    """
    return getattr(backend, "tier", "ondemand")


def validate_order(order: tuple[str, ...], backends: list[Backend]) -> None:
    tiers = {_tier_of(backend) for backend in backends}
    missing = tiers - set(order)
    if missing:
        raise BrokerError(
            f"backends exist in tiers {sorted(missing)} but placement_order is "
            f"{list(order)}, so they would never be used"
        )

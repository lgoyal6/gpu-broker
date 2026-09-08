"""Every tunable in one place, with the reasoning next to the number."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from .aws import AwsConfig, load_aws_config
from .local.hosts import LocalConfig, load_local_config
from .volumes import VolumeConfig, load_volume_config


def _web_default():
    """Imported lazily: `gpu_broker.web` pulls in FastAPI, and the CLI must work
    on a machine that never installed it."""
    from .web.auth import WebConfig

    return WebConfig()


def _load_web(raw):
    from .web.auth import load_web_config

    return load_web_config(raw)
from .errors import ConfigError, UnknownGpuType
from .money import Currency, money


@dataclass(frozen=True)
class GpuType:
    """One kind of capacity the club can ask for."""

    name: str
    currency: Currency
    hourly_price: Decimal
    """Dollars per hour, or 1.0 for GPU_HOUR types (an hour costs an hour)."""
    description: str = ""
    memory_mb: int = 0


# Placeholder on-demand prices, us-west-2, captured by hand on 2026-08-24.
# Phase 3 replaces this table with a refresh from the AWS pricing API. Do not
# use these for accounting until it does.
DEFAULT_GPU_TYPES: tuple[GpuType, ...] = (
    GpuType("t4", Currency.USD, money("0.526"), "g4dn.xlarge, 16GB", 16_384),
    GpuType("a10g", Currency.USD, money("1.006"), "g5.xlarge, 24GB", 24_576),
    GpuType("l4", Currency.USD, money("0.8048"), "g6.xlarge, 24GB", 24_576),
    GpuType("a100", Currency.USD, money("4.10"), "p4d slice, 40GB", 40_960),
    GpuType("a6000", Currency.GPU_HOUR, money("1.0"), "lab machine, 48GB, free", 49_152),
)


@dataclass(frozen=True)
class BrokerConfig:
    """Loaded once at startup. Nothing mutates it afterwards."""

    db_path: Path
    timezone: str = "America/Los_Angeles"

    # --- fair share ---------------------------------------------------------
    # priority = w_fair * (1 - usage_share) + w_age * min(1, wait / age_max)
    #
    # w_age is deliberately >= w_fair. If the age term cannot outweigh the
    # fair-share term, a heavy user loses to every newly-arriving light user
    # forever, which is the whole failure mode the age term exists to prevent.
    w_fair: float = 0.5
    w_age: float = 0.5
    age_max_hours: float = 12.0
    """Wait time at which the age term saturates. Waiting 30 hours is not
    three times more urgent than waiting 10; it is just 'too long'."""

    fairshare_window_days: float = 30.0
    """How far back usage counts at all."""
    fairshare_half_life_days: float = 14.0
    """Usage decays by half every this many days, so one heavy month does not
    exile somebody for the rest of the quarter."""

    # How the two currencies blend into a single share. Dollars weigh more
    # because they are the scarce thing; free GPU-hours still count so that
    # somebody who lives on the lab machine does not also get first pick of
    # the paid pool.
    w_usd_share: float = 0.7
    w_gpu_hour_share: float = 0.3

    # --- budgets ------------------------------------------------------------
    default_budget_usd: Decimal = money("25.00")
    default_budget_gpu_hours: Decimal = money("8.00")

    pool_budget_usd: Decimal = money("500.00")
    pool_budget_gpu_hours: Decimal = money("400.00")

    max_job_usd: Decimal = money("50.00")
    max_job_gpu_hours: Decimal = money("24.00")
    max_job_hours: float = 24.0

    # --- limits, so one member cannot take the broker down for everybody -----
    #
    # A reservation is already backpressure: on the default $25 a member gets 33
    # queued a10g jobs before their own budget refuses them. The hole is that
    # the reservation is whatever `--budget` says, and `--budget 0.01` turns
    # that 33 into 2500. Measured, on a laptop, with the fake backend.
    max_command_bytes: int = 4096
    """A command is stored verbatim and then run in a shell on a GPU host.
    Without this, a 10 MB `--command` was accepted and stored. Nothing anybody
    types is close to this; a job that needs more wants a script in /data."""

    max_queued_jobs_per_user: int = 32
    """One under what the default budget already allows, so a member using
    default budgets never meets this limit and a member using `--budget 0.01`
    meets the same ceiling as everybody else. Every queued job costs about
    20 kB of database (measured: 20,613 bytes/job over 200 submissions) and is
    re-scored on every tick, so the cost lands on everybody. Measured on this
    laptop: a tick against an empty queue took 0.8 ms and against 4,000 queued
    jobs took 140 ms. Wall-clock figures move with what else the machine is
    doing; the shape -- superlinear in queue depth, paid by every member -- is
    what the limit is for."""

    max_running_jobs_per_user: int = 4
    """How many machines one member may hold at once. Measured without it: one
    member took 63 of 64 free slots in a single tick. Over the limit a job
    *waits* rather than being refused -- it keeps its queue place and goes as
    soon as one of that member's jobs finishes."""

    max_log_lines_per_job: int = 10_000
    max_log_line_bytes: int = 2_000
    """A job that prints is otherwise never told to stop: 20,000 lines of 200
    bytes grew the database by 10.7 MB, and a single 5 MB line was stored as
    5 MB. Together these cap one job at about 20 MB. That is a loose bound --
    a real byte budget wants a counter on the job row rather than a COUNT(*)
    per poll -- but it is finite, which is the property that was missing."""

    startup_allowance_hours: float = 0.25
    """Billing starts when the broker takes capacity, not when the user's command
    does, because that is when a cloud instance starts charging. So the default
    reservation has to cover boot as well as run time, or every job with a default
    budget dies one boot short of finishing. Fifteen minutes is generous for a GPU
    AMI; explicit `--budget` overrides it."""

    gpu_types: tuple[GpuType, ...] = DEFAULT_GPU_TYPES

    # --- placement -----------------------------------------------------------
    placement_order: tuple[str, ...] = ("local", "spot", "ondemand")
    """Tiers in preference order. A job takes the first tier with a free GPU
    *right now*; it never waits for a cheaper tier to free up. That trades money
    for latency on purpose -- making somebody wait six hours for the free card
    while credits sit unspent is how a broker stops getting used."""

    # --- forecasting ---------------------------------------------------------
    forecast_window_days: float = 30.0
    forecast_min_days: float = 0.25
    """Floor on the period a burn rate is divided by. Without it, a broker
    installed an hour ago divides an hour of spending by a fraction of a day and
    reports a runway of minutes."""
    forecast_warning_days: float = 14.0
    forecast_critical_days: float = 7.0

    # --- idle detection ------------------------------------------------------
    idle_threshold_percent: float = 5.0
    idle_window_minutes: float = 10.0
    idle_min_samples: int = 5
    """Below this many samples nothing is decided. One reading through a
    checkpoint save is not an idle job."""
    idle_grace_minutes: float = 15.0
    idle_sample_interval_seconds: float = 60.0

    reclaim_enabled: bool = False
    """Off until somebody has watched it be right. `gpu reclaim` lists what it
    would have killed either way, with the samples that justified each one."""

    price_refresh_days: float = 7.0

    # --- spot and resumption --------------------------------------------------
    spot_max_preemptions_without_progress: int = 2
    """After this many preemptions with nothing saved, a job is pinned to
    on-demand. Paying twice to redo the same hour costs more than on-demand
    would have."""
    spot_max_launch_failures: int = 3
    """Consecutive spot request failures before the spot tier is skipped."""
    spot_cooldown_seconds: float = 900.0
    checkpoint_deadline_seconds: float = 90.0

    checkpoint_store: str = "local"
    """`local` or `s3`. Spot resumption needs `s3`: a preempted job comes back
    on a different machine, and a directory on the old one is gone with it."""
    checkpoint_root: str = ""
    """Where a local store keeps things. Empty means alongside the database."""
    checkpoint_bucket: str = ""
    checkpoint_prefix: str = "checkpoints"
    """How long a job gets to write a checkpoint after the notice arrives. Spot
    gives two minutes total; the rest goes on detecting it and uploading."""

    backends: tuple[str, ...] = ("fake",)
    """Which backends to bring up. Defaults to the simulator alone, so a fresh
    clone works on a laptop with no AWS account -- which every phase has to keep
    true. Set to ["ec2"] or ["fake", "ec2"] in config.json once the account is
    ready."""

    aws: AwsConfig = field(default_factory=AwsConfig)
    local: LocalConfig = field(default_factory=LocalConfig)
    volumes: VolumeConfig = field(default_factory=VolumeConfig)
    environment_root: str = "~/.gpu-broker/envs"
    """Where cached virtualenvs live on a lab host."""
    ecr_repository: str = ""

    digest_webhook: str = ""
    """Slack or Discord. Empty means `gpu digest` prints and nothing is sent,
    which is the right default: a digest that needs a Slack workspace does not
    exist for a club that uses Discord."""
    metrics_retention_days: float = 90.0
    """`gpu-broker/environments`. Empty means environments are not built as
    images, so cloud jobs run on the base AMI as it comes."""

    web: Any = field(default_factory=lambda: _web_default())
    """A `gpu_broker.web.auth.WebConfig`. Typed loosely on purpose: importing it
    here would pull FastAPI into every CLI invocation, and the CLI has to work
    on a machine that never installed it."""

    def gpu(self, name: str) -> GpuType:
        for gpu_type in self.gpu_types:
            if gpu_type.name == name:
                return gpu_type
        raise UnknownGpuType(name, [g.name for g in self.gpu_types])

    def pool_budget(self, currency: Currency) -> Decimal:
        return (
            self.pool_budget_usd
            if currency is Currency.USD
            else self.pool_budget_gpu_hours
        )

    def max_job(self, currency: Currency) -> Decimal:
        return self.max_job_usd if currency is Currency.USD else self.max_job_gpu_hours

    def share_weight(self, currency: Currency) -> float:
        return (
            self.w_usd_share if currency is Currency.USD else self.w_gpu_hour_share
        )

    def validate(self) -> None:
        for name in self.backends:
            if name not in ("fake", "ec2", "ec2-spot", "local"):
                raise ConfigError(
                    f"unknown backend {name!r} in config.json. "
                    "Known: fake, ec2, ec2-spot, local"
                )
        if not self.backends:
            raise ConfigError("no backends configured: nothing could ever run")
        if not self.placement_order:
            raise ConfigError("placement_order is empty: nothing could ever be placed")
        if self.checkpoint_store not in ("local", "s3"):
            raise ConfigError(
                f"unknown checkpoint_store {self.checkpoint_store!r}. Known: local, s3"
            )
        if self.checkpoint_store == "s3" and not self.checkpoint_bucket:
            raise ConfigError("checkpoint_store is 's3' but checkpoint_bucket is not set")
        # Keyed on a spot backend actually existing, not on the tier appearing
        # in the order. The default order lists `spot` so that turning it on is
        # one line, and a tier with no backend behind it is simply skipped.
        if "ec2-spot" in self.backends and self.checkpoint_store != "s3":
            raise ConfigError(
                "the ec2-spot backend is configured but checkpoints are stored "
                "locally. A preempted job comes back on a different machine, so "
                "its checkpoint has to be somewhere both can reach. Set "
                "checkpoint_store to 's3', or drop 'ec2-spot' from backends"
            )
        if self.idle_min_samples < 1:
            raise ConfigError("idle_min_samples must be at least 1")
        if not 0 <= self.idle_threshold_percent <= 100:
            raise ConfigError("idle_threshold_percent must be between 0 and 100")
        if self.idle_grace_minutes < 0:
            raise ConfigError("idle_grace_minutes cannot be negative")
        if self.forecast_critical_days > self.forecast_warning_days:
            raise ConfigError(
                f"forecast_critical_days ({self.forecast_critical_days}) is later than "
                f"forecast_warning_days ({self.forecast_warning_days}), so the critical "
                "alert would fire before the warning"
            )
        if self.w_fair < 0 or self.w_age < 0:
            raise ConfigError("fair-share weights cannot be negative")
        if self.w_fair == 0 and self.w_age == 0:
            raise ConfigError("w_fair and w_age cannot both be zero: nothing would order the queue")
        if self.w_age < self.w_fair:
            raise ConfigError(
                f"w_age ({self.w_age}) < w_fair ({self.w_fair}): with these weights a "
                "heavy user can never age past a light user, and will be starved. "
                "Set w_age >= w_fair, or accept starvation explicitly by editing this check."
            )
        if self.startup_allowance_hours < 0:
            raise ConfigError("startup_allowance_hours cannot be negative")
        for name in (
            "max_command_bytes",
            "max_queued_jobs_per_user",
            "max_running_jobs_per_user",
            "max_log_lines_per_job",
            "max_log_line_bytes",
        ):
            # Zero is not "no limit" here, it is "nothing may ever run". A
            # config that means to lift a limit should say a large number, so
            # that a typo cannot silently stop the club.
            if getattr(self, name) < 1:
                raise ConfigError(f"{name} must be at least 1")
        if self.age_max_hours <= 0:
            raise ConfigError("age_max_hours must be positive")
        if self.fairshare_half_life_days <= 0:
            raise ConfigError("fairshare_half_life_days must be positive")
        if not self.gpu_types:
            raise ConfigError("no gpu types configured: nothing could ever be submitted")
        names = [g.name for g in self.gpu_types]
        if len(names) != len(set(names)):
            raise ConfigError(f"duplicate gpu type names: {names}")


DEFAULT_STATE_DIR = Path(os.environ.get("GPU_BROKER_HOME", Path.home() / ".gpu-broker"))

_OVERRIDABLE = {
    "timezone": str,
    "w_fair": float,
    "w_age": float,
    "age_max_hours": float,
    "fairshare_window_days": float,
    "fairshare_half_life_days": float,
    "w_usd_share": float,
    "w_gpu_hour_share": float,
    "default_budget_usd": money,
    "default_budget_gpu_hours": money,
    "pool_budget_usd": money,
    "pool_budget_gpu_hours": money,
    "max_job_usd": money,
    "max_job_gpu_hours": money,
    "max_job_hours": float,
    "max_command_bytes": int,
    "max_queued_jobs_per_user": int,
    "max_running_jobs_per_user": int,
    "max_log_lines_per_job": int,
    "max_log_line_bytes": int,
    "startup_allowance_hours": float,
    "backends": list,
    "placement_order": list,
    "forecast_window_days": float,
    "forecast_min_days": float,
    "forecast_warning_days": float,
    "forecast_critical_days": float,
    "idle_threshold_percent": float,
    "idle_window_minutes": float,
    "idle_min_samples": int,
    "idle_grace_minutes": float,
    "idle_sample_interval_seconds": float,
    "reclaim_enabled": bool,
    "price_refresh_days": float,
    "spot_max_preemptions_without_progress": int,
    "spot_max_launch_failures": int,
    "spot_cooldown_seconds": float,
    "checkpoint_deadline_seconds": float,
    "checkpoint_store": str,
    "checkpoint_root": str,
    "checkpoint_bucket": str,
    "checkpoint_prefix": str,
    "environment_root": str,
    "ecr_repository": str,
    "digest_webhook": str,
    "metrics_retention_days": float,
}


def load_config(
    state_dir: Path | None = None, overrides: dict | None = None
) -> BrokerConfig:
    """Config comes from defaults, then `config.json` in the state dir, then
    explicit overrides. Missing file is fine; malformed file is not."""
    root = Path(state_dir) if state_dir else DEFAULT_STATE_DIR
    root.mkdir(parents=True, exist_ok=True)

    settings: dict = {}
    config_file = root / "config.json"
    if config_file.exists():
        try:
            raw = json.loads(config_file.read_text())
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{config_file} is not valid JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigError(f"{config_file} must contain a JSON object")
        settings.update(raw)
    if overrides:
        settings.update(overrides)

    # `aws` is a nested object, not a scalar override, so it is pulled out
    # before the flat key check.
    aws_settings = settings.pop("aws", None)
    if aws_settings is not None and not isinstance(aws_settings, dict):
        raise ConfigError("'aws' in config.json must be an object")
    local_settings = settings.pop("local", None)
    if local_settings is not None and not isinstance(local_settings, dict):
        raise ConfigError("'local' in config.json must be an object")
    volume_settings = settings.pop("volumes", None)
    if volume_settings is not None and not isinstance(volume_settings, dict):
        raise ConfigError("'volumes' in config.json must be an object")
    web_settings = settings.pop("web", None)
    if web_settings is not None and not isinstance(web_settings, dict):
        raise ConfigError("'web' in config.json must be an object")

    unknown = set(settings) - set(_OVERRIDABLE)
    if unknown:
        raise ConfigError(
            f"unknown config keys in {config_file}: {', '.join(sorted(unknown))}. "
            f"Known keys: {', '.join(sorted(_OVERRIDABLE))}"
        )

    coerced = {key: _OVERRIDABLE[key](value) for key, value in settings.items()}
    for key in ("backends", "placement_order"):
        if key in coerced:
            coerced[key] = tuple(coerced[key])
    coerced["aws"] = load_aws_config(aws_settings)
    coerced["local"] = load_local_config(local_settings)
    coerced["web"] = _load_web(web_settings)
    coerced["volumes"] = load_volume_config(volume_settings)
    config = replace(BrokerConfig(db_path=root / "broker.sqlite3"), **coerced)
    config.validate()
    return config

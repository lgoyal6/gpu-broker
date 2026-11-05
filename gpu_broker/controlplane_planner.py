"""Bounded CP-SAT placement proposals for the Go control plane.

This process is intentionally stateless: it reads a versioned JSON snapshot
and emits a proposal only. The caller must independently validate and commit
reservations transactionally. It never owns credentials or launches workers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ortools.sat.python import cp_model

MAX_SAFE_JSON_INTEGER = 2**53 - 1


def _safe_int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if type(value) is not int or abs(value) > MAX_SAFE_JSON_INTEGER:
        raise ValueError(f"{name} must be an exact JSON-safe integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    return tuple(value)


@dataclass(frozen=True)
class Job:
    id: str
    tenant: str
    project: str
    gpu_model: str
    gpus: int
    runtime_s: int
    setup_s: int = 0
    # Release/deadline are offsets in seconds from the snapshot's now_epoch_s.
    release_s: int = 0
    deadline_s: int | None = None
    budget_micro_usd: int = 2**62
    memory_gb: int = 0
    required_labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class Worker:
    id: str
    gpu_model: str
    free_gpus: int
    price_micro_usd_per_gpu_hour: int
    max_runtime_s: int
    memory_gb: int = 0
    labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class Snapshot:
    version: str
    now_epoch_s: int
    horizon_s: int
    slot_s: int
    jobs: tuple[Job, ...]
    workers: tuple[Worker, ...]
    project_budget_micro_usd: dict[str, int]
    tenant_max_gpus: dict[str, int]
    tenant_active_gpus: dict[str, int]


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def snapshot_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _coerce(payload: dict[str, Any]) -> Snapshot:
    if not isinstance(payload, dict):
        raise ValueError("snapshot must be an object")
    slot = _safe_int(payload["slot_s"], "slot_s", minimum=1)
    horizon_s = _safe_int(payload["horizon_s"], "horizon_s", minimum=slot)
    now_epoch_s = _safe_int(payload.get("now_epoch_s", 0), "now_epoch_s", minimum=0)
    if horizon_s < slot:
        raise ValueError("invalid planning horizon or slot size")
    if not isinstance(payload.get("jobs"), list) or not isinstance(payload.get("workers"), list):
        raise ValueError("snapshot jobs and workers must be lists")
    jobs = []
    for row in payload["jobs"]:
        if not isinstance(row, dict):
            raise ValueError("snapshot jobs must be objects")
        values = dict(row)
        for name, default, minimum in (
            ("gpus", None, 1), ("runtime_s", None, 1), ("setup_s", 0, 0),
            ("release_s", 0, 0), ("deadline_s", None, 0),
            ("budget_micro_usd", 2**52, 0), ("memory_gb", 0, 0),
        ):
            value = values.get(name, default)
            if value is not None:
                _safe_int(value, f"job.{name}", minimum=minimum)
            if name in ("budget_micro_usd", "memory_gb", "setup_s", "release_s"):
                values.setdefault(name, default)
        values["required_labels"] = _string_tuple(values.get("required_labels", []),
                                                   "job.required_labels")
        jobs.append(Job(**values))
    workers = []
    for row in payload["workers"]:
        if not isinstance(row, dict):
            raise ValueError("snapshot workers must be objects")
        values = dict(row)
        for name, default, minimum in (
            ("free_gpus", None, 0), ("price_micro_usd_per_gpu_hour", None, 0),
            ("max_runtime_s", None, 0), ("memory_gb", 0, 0),
        ):
            value = values.get(name, default)
            if value is not None:
                _safe_int(value, f"worker.{name}", minimum=minimum)
            if name == "memory_gb":
                values.setdefault(name, default)
        values["labels"] = _string_tuple(values.get("labels", []), "worker.labels")
        workers.append(Worker(**values))
    if len({job.id for job in jobs}) != len(jobs) or len({worker.id for worker in workers}) != len(workers):
        raise ValueError("job and worker IDs must be unique")
    if any(job.gpus < 1 or job.runtime_s < 1 or job.setup_s < 0 or job.memory_gb < 0 or
           job.release_s < 0 or (job.deadline_s is not None and job.deadline_s < 0)
           for job in jobs):
        raise ValueError("job resource and duration values must be positive")
    if any(worker.free_gpus < 0 or worker.price_micro_usd_per_gpu_hour < 0 or worker.memory_gb < 0
           for worker in workers):
        raise ValueError("worker capacity and price cannot be negative")
    def int_map(name: str, default_minimum: int = 0) -> dict[str, int]:
        values = payload.get(name, {})
        if not isinstance(values, dict):
            raise ValueError(f"snapshot {name} must be an object")
        return {str(key): _safe_int(value, f"{name}.{key}", minimum=default_minimum)
                for key, value in values.items()}

    version = payload.get("version")
    if not isinstance(version, str) or not version:
        raise ValueError("snapshot version must be a non-empty string")
    return Snapshot(version, now_epoch_s, horizon_s, slot, tuple(jobs), tuple(workers),
                    int_map("project_budget_micro_usd"),
                    int_map("tenant_max_gpus"), int_map("tenant_active_gpus"))


def _cost(job: Job, worker: Worker) -> int:
    seconds = job.setup_s + job.runtime_s
    cost = math.ceil(job.gpus * seconds * worker.price_micro_usd_per_gpu_hour / 3600)
    if cost > MAX_SAFE_JSON_INTEGER:
        raise ValueError("estimated reservation cost exceeds the exact JSON integer range")
    return cost


def propose(payload: dict[str, Any], timeout_s: float = 0.25) -> dict[str, Any]:
    """Solve a bounded discrete placement model; return a version-bound proposal."""
    s = _coerce(payload)
    model = cp_model.CpModel()
    horizon = s.horizon_s // s.slot_s
    slots: dict[tuple[str, str], tuple[Any, Any, Any, int]] = {}
    worker_intervals: dict[str, list[Any]] = {worker.id: [] for worker in s.workers}
    worker_demands: dict[str, list[int]] = {worker.id: [] for worker in s.workers}
    tenant_intervals: dict[str, list[Any]] = {}
    tenant_demands: dict[str, list[int]] = {}
    tenant_capacity: dict[str, int] = {}
    project_terms: dict[str, list[Any]] = {}
    objective = []

    for job in s.jobs:
        assigned = []
        for worker in s.workers:
            if (job.gpu_model not in ("", "any") and worker.gpu_model != job.gpu_model or job.gpus > worker.free_gpus or
                    job.setup_s + job.runtime_s > worker.max_runtime_s or
                    job.memory_gb > worker.memory_gb or
                    not set(job.required_labels).issubset(worker.labels)):
                continue
            duration_s = job.setup_s + job.runtime_s
            duration = math.ceil(duration_s / s.slot_s)
            earliest = max(0, math.ceil(job.release_s / s.slot_s))
            latest = horizon - duration
            if latest < earliest or _cost(job, worker) > job.budget_micro_usd:
                continue
            if (job.deadline_s is not None and
                    earliest * s.slot_s + duration * s.slot_s > job.deadline_s):
                continue
            presence = model.new_bool_var(f"assign_{job.id}_{worker.id}")
            start = model.new_int_var(earliest, latest, f"start_{job.id}_{worker.id}")
            end = model.new_int_var(earliest + duration, latest + duration, f"end_{job.id}_{worker.id}")
            interval = model.new_optional_interval_var(start, duration, end, presence,
                                                        f"interval_{job.id}_{worker.id}")
            assigned.append(presence)
            slots[(job.id, worker.id)] = (presence, start, end, duration)
            worker_intervals[worker.id].append(interval)
            worker_demands[worker.id].append(job.gpus)
            tenant_intervals.setdefault(job.tenant, []).append(interval)
            tenant_demands.setdefault(job.tenant, []).append(job.gpus)
            tenant_capacity[job.tenant] = max(0, s.tenant_max_gpus.get(job.tenant, 2**30) -
                                              s.tenant_active_gpus.get(job.tenant, 0))
            project_terms.setdefault(job.project, []).append(_cost(job, worker) * presence)

            if job.deadline_s is not None:
                model.add((start + duration) * s.slot_s <= job.deadline_s).only_enforce_if(presence)
            service_reward = 1_000_000
            # Earlier starts and lower cost break ties after useful completions.
            objective.append(service_reward * presence - start - (_cost(job, worker) // 1_000_000) * presence)
        if assigned:
            model.add(sum(assigned) <= 1)

    for worker in s.workers:
        if worker_intervals[worker.id]:
            model.add_cumulative(worker_intervals[worker.id], worker_demands[worker.id], worker.free_gpus)
    for tenant, intervals in tenant_intervals.items():
        model.add_cumulative(intervals, tenant_demands[tenant], tenant_capacity[tenant])
    for project, terms in project_terms.items():
        if project in s.project_budget_micro_usd:
            model.add(sum(terms) <= s.project_budget_micro_usd[project])
    if objective:
        model.maximize(sum(objective))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max(0.001, timeout_s)
    solver.parameters.num_search_workers = 1
    status = solver.solve(model)
    status_name = solver.status_name(status)
    placements = []
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        jobs = {job.id: job for job in s.jobs}
        workers = {worker.id: worker for worker in s.workers}
        for (job_id, worker_id), (presence, start, _, duration) in slots.items():
            if not solver.boolean_value(presence):
                continue
            start_s = solver.value(start) * s.slot_s
            job, worker = jobs[job_id], workers[worker_id]
            placements.append({"job_id": job_id, "worker_id": worker_id,
                               "start_offset_s": start_s,
                               "finish_offset_s": start_s + duration * s.slot_s,
                               "reserved_micro_usd": _cost(job, worker)})
    usable = status in (cp_model.OPTIMAL, cp_model.FEASIBLE) and (bool(placements) or not s.jobs)
    return {"schema": "gpu-broker.placement-proposal/v1",
            "snapshot_version": s.version,
            "snapshot_digest": snapshot_digest(payload),
            "solver_status": status_name,
            "placements": sorted(placements, key=lambda row: row["job_id"]),
            "objective": solver.objective_value if status in (cp_model.OPTIMAL, cp_model.FEASIBLE) else None,
            "wall_time_s": solver.wall_time,
            "fallback_required": not usable}


def validate_proposal(payload: dict[str, Any], proposal: dict[str, Any]) -> list[str]:
    """Check a v1 proposal against the current snapshot; time fields are snapshot-relative seconds."""
    s = _coerce(payload)
    errors = []
    if not isinstance(proposal, dict) or proposal.get("schema") != "gpu-broker.placement-proposal/v1":
        return ["unsupported proposal schema"]
    if proposal.get("snapshot_version") != s.version or proposal.get("snapshot_digest") != snapshot_digest(payload):
        return ["stale snapshot"]
    if proposal.get("solver_status") not in {"OPTIMAL", "FEASIBLE"} or proposal.get("fallback_required") is not False:
        errors.append("proposal is not usable")
    placements = proposal.get("placements")
    if not isinstance(placements, list):
        return errors + ["placements must be a list"]
    jobs = {job.id: job for job in s.jobs}
    workers = {worker.id: worker for worker in s.workers}
    seen = set()
    allocation: dict[str, list[tuple[int, int, int]]] = {}
    tenant_allocation: dict[str, list[tuple[int, int, int]]] = {}
    project_cost: dict[str, int] = {}
    for place in placements:
        if not isinstance(place, dict):
            errors.append("placement must be an object")
            continue
        job_id, worker_id = place.get("job_id"), place.get("worker_id")
        if not isinstance(job_id, str) or not isinstance(worker_id, str):
            errors.append("placement job and worker IDs must be strings")
            continue
        if job_id not in jobs or worker_id not in workers:
            errors.append("unknown job or worker")
            continue
        if job_id in seen:
            errors.append("job assigned multiple times")
            continue
        seen.add(job_id)
        job, worker = jobs[job_id], workers[worker_id]
        numeric_fields = ("start_offset_s", "finish_offset_s", "reserved_micro_usd")
        if any(type(place.get(field)) is not int for field in numeric_fields):
            errors.append("placement time and cost fields must be integers")
            continue
        try:
            start = _safe_int(place["start_offset_s"], "start_offset_s")
            finish = _safe_int(place["finish_offset_s"], "finish_offset_s")
            reserved_cost = _safe_int(place["reserved_micro_usd"], "reserved_micro_usd", minimum=0)
        except ValueError:
            errors.append("placement numeric fields exceed the exact JSON integer range")
            continue
        duration = job.setup_s + job.runtime_s
        if (job.gpu_model not in ("", "any") and worker.gpu_model != job.gpu_model or job.gpus > worker.free_gpus or
                duration > worker.max_runtime_s or job.memory_gb > worker.memory_gb or
                not set(job.required_labels).issubset(worker.labels)):
            errors.append("ineligible worker")
        if (start < job.release_s or finish - start < duration or finish > s.horizon_s or
                start % s.slot_s != 0 or finish % s.slot_s != 0):
            errors.append("invalid interval")
        if job.deadline_s is not None and finish > job.deadline_s:
            errors.append("missed deadline")
        cost = _cost(job, worker)
        if cost > job.budget_micro_usd or reserved_cost != cost:
            errors.append("job budget or reservation cost mismatch")
        project_cost[job.project] = project_cost.get(job.project, 0) + cost
        allocation.setdefault(worker_id, []).append((start, finish, job.gpus))
        tenant_allocation.setdefault(job.tenant, []).append((start, finish, job.gpus))
    for project, cost in project_cost.items():
        if cost > s.project_budget_micro_usd.get(project, 2**62):
            errors.append("project budget exceeded")
    for worker_id, items in allocation.items():
        worker = workers[worker_id]
        if _peak_overlap(items) > worker.free_gpus:
            errors.append("worker capacity exceeded")
    for tenant, items in tenant_allocation.items():
        capacity = max(0, s.tenant_max_gpus.get(tenant, 2**30) - s.tenant_active_gpus.get(tenant, 0))
        if _peak_overlap(items) > capacity:
            errors.append("tenant concurrency exceeded")
    return errors


def _peak_overlap(items: list[tuple[int, int, int]]) -> int:
    points = []
    for start, end, demand in items:
        points.extend(((start, demand), (end, -demand)))
    active = peak = 0
    for _, delta in sorted(points, key=lambda point: (point[0], point[1])):
        active += delta
        peak = max(peak, active)
    return peak


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--timeout-s", type=float, default=0.25)
    parser.add_argument("--output", type=Path, default=Path("-"))
    args = parser.parse_args()
    proposal = propose(json.loads(args.input.read_text()), args.timeout_s)
    errors = validate_proposal(json.loads(args.input.read_text()), proposal)
    if errors:
        raise SystemExit("invalid CP-SAT proposal: " + "; ".join(errors))
    rendered = json.dumps(proposal, sort_keys=True, indent=2) + "\n"
    if str(args.output) == "-":
        print(rendered, end="")
    else:
        args.output.write_text(rendered)


if __name__ == "__main__":
    main()

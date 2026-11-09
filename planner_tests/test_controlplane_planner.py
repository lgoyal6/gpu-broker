from __future__ import annotations

from copy import deepcopy

from gpu_broker.controlplane_planner import propose, snapshot_digest, validate_proposal


def snapshot() -> dict:
    return {
        "version": "epoch-17",
        "now_epoch_s": 1000,
        "horizon_s": 600,
        "slot_s": 10,
        "jobs": [
            {"id": "a", "tenant": "t1", "project": "p1", "gpu_model": "T4",
             "gpus": 1, "runtime_s": 100, "deadline_s": 200},
            {"id": "b", "tenant": "t2", "project": "p2", "gpu_model": "T4",
             "gpus": 1, "runtime_s": 120, "deadline_s": 300},
            {"id": "c", "tenant": "t1", "project": "p1", "gpu_model": "A100",
             "gpus": 1, "runtime_s": 50, "deadline_s": 200},
        ],
        "workers": [
            {"id": "w-t4", "gpu_model": "T4", "free_gpus": 1,
             "price_micro_usd_per_gpu_hour": 3600000, "max_runtime_s": 300},
            {"id": "w-a100", "gpu_model": "A100", "free_gpus": 1,
             "price_micro_usd_per_gpu_hour": 7200000, "max_runtime_s": 300},
        ],
        "project_budget_micro_usd": {"p1": 300000, "p2": 300000},
        "tenant_max_gpus": {"t1": 1, "t2": 1},
        "tenant_active_gpus": {"t1": 0, "t2": 0},
    }


def test_plan_is_snapshot_bound_and_independently_feasible() -> None:
    data = snapshot()
    plan = propose(data, timeout_s=1)
    assert plan["solver_status"] in {"OPTIMAL", "FEASIBLE"}
    assert len(plan["placements"]) == 3
    assert validate_proposal(data, plan) == []
    stale = deepcopy(data)
    stale["version"] = "epoch-18"
    assert validate_proposal(stale, plan) == ["stale snapshot"]


def test_independent_validator_rejects_capacity_budget_and_duplicate_mutants() -> None:
    data = snapshot()
    plan = propose(data, timeout_s=1)
    placements = plan["placements"]
    forged = deepcopy(plan)
    forged["placements"].append(deepcopy(placements[0]))
    assert "job assigned multiple times" in validate_proposal(data, forged)
    forged = deepcopy(plan)
    forged["placements"][0]["reserved_micro_usd"] += 1
    assert "job budget or reservation cost mismatch" in validate_proposal(data, forged)
    forged = deepcopy(plan)
    for place in forged["placements"]:
        if place["worker_id"] == "w-t4":
            place["start_offset_s"] = 0
    assert "worker capacity exceeded" in validate_proposal(data, forged)
    forged = deepcopy(plan)
    forged["snapshot_digest"] = "forged"
    assert validate_proposal(data, forged) == ["stale snapshot"]


def test_infeasible_or_overloaded_work_is_refused_under_project_budget() -> None:
    data = snapshot()
    data["project_budget_micro_usd"]["p1"] = 0
    plan = propose(data, timeout_s=1)
    assert all(place["job_id"] != "a" and place["job_id"] != "c"
               for place in plan["placements"])
    assert validate_proposal(data, plan) == []


def test_resource_compatibility_includes_memory_and_required_labels() -> None:
    data = snapshot()
    data["jobs"][0]["memory_gb"] = 48
    data["jobs"][0]["required_labels"] = ["nvlink"]
    plan = propose(data, timeout_s=1)
    assert all(place["job_id"] != "a" for place in plan["placements"])
    assert validate_proposal(data, plan) == []

    forged = deepcopy(plan)
    forged["placements"].append({"job_id": "a", "worker_id": "w-t4",
                                  "start_offset_s": 0, "finish_offset_s": 100,
                                  "reserved_micro_usd": 100000})
    assert "ineligible worker" in validate_proposal(data, forged)


def test_validator_rejects_malformed_schema_fields_offsets_and_deadlines() -> None:
    data = snapshot()
    plan = propose(data, timeout_s=1)
    malformed = deepcopy(plan)
    malformed["schema"] = "gpu-broker.placement-proposal/v2"
    assert validate_proposal(data, malformed) == ["unsupported proposal schema"]

    malformed = deepcopy(plan)
    malformed["placements"][0]["start_offset_s"] = 1.5
    assert "placement time and cost fields must be integers" in validate_proposal(data, malformed)

    malformed = deepcopy(plan)
    malformed["placements"][0]["start_offset_s"] = 1
    assert "invalid interval" in validate_proposal(data, malformed)

    deadline_data = snapshot()
    deadline_data["jobs"][0]["deadline_s"] = 50
    deadline_plan = propose(deadline_data, timeout_s=1)
    assert all(place["job_id"] != "a" for place in deadline_plan["placements"])
    forged = deepcopy(plan)
    place = next(place for place in forged["placements"] if place["job_id"] == "a")
    forged["snapshot_digest"] = snapshot_digest(deadline_data)
    forged["snapshot_version"] = deadline_data["version"]
    assert "missed deadline" in validate_proposal(deadline_data, forged)


def test_snapshot_and_proposal_reject_float_ambiguous_or_unsafe_json_integers() -> None:
    data = snapshot()
    data["jobs"][0]["runtime_s"] = 99.5
    try:
        propose(data, timeout_s=1)
    except ValueError as exc:
        assert "exact JSON-safe integer" in str(exc)
    else:
        raise AssertionError("fractional runtime was rounded into the planner model")

    data = snapshot()
    data["horizon_s"] = 2**53
    try:
        propose(data, timeout_s=1)
    except ValueError as exc:
        assert "exact JSON-safe integer" in str(exc)
    else:
        raise AssertionError("unsafe JSON integer horizon was accepted")

    data = snapshot()
    plan = propose(data, timeout_s=1)
    unsafe = deepcopy(plan)
    unsafe["placements"][0]["start_offset_s"] = 2**53
    assert "placement numeric fields exceed the exact JSON integer range" in validate_proposal(data, unsafe)

    data["jobs"][0]["release_s"] = 1.25
    try:
        propose(data, timeout_s=1)
    except ValueError as exc:
        assert "exact JSON-safe integer" in str(exc)
    else:
        raise AssertionError("fractional release offset was rounded into the planner model")

    data = snapshot()
    data["jobs"][0]["required_labels"] = "nvlink"
    try:
        propose(data, timeout_s=1)
    except ValueError as exc:
        assert "list of non-empty strings" in str(exc)
    else:
        raise AssertionError("a string was treated as a list of required worker labels")


def test_unusable_snapshot_returns_explicit_fallback() -> None:
    data = snapshot()
    data["workers"] = []
    plan = propose(data, timeout_s=0.01)
    assert plan["fallback_required"] is True
    assert plan["placements"] == []

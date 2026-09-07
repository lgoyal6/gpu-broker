from __future__ import annotations

import json

import pytest

from gpu_broker.demo import seed
from gpu_broker.schedule_trace import TraceError, export_trace, replay_trace


def test_seeded_trace_is_anonymized_deterministic_and_replayable(tmp_path):
    state = tmp_path / "demo"
    seed(state, days=8, rng_seed=3)

    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(state, clock=SystemClock())
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    export_trace(broker.store, first)
    export_trace(broker.store, second)

    assert first.read_bytes() == second.read_bytes()
    raw = first.read_text()
    assert "python train.py" not in raw
    assert "demo-1" not in raw
    assert "backend_handle" not in raw
    bundle = json.loads(raw)
    assert bundle["evaluation_limits"]["optimizer_comparison"].startswith("blocked")

    summary = replay_trace(first)
    assert summary.jobs == len(bundle["jobs"])
    assert summary.users == 6
    assert summary.completed > 0
    assert summary.max_active > 0
    assert summary.p95_wait_seconds is not None


def test_small_operational_history_is_refused(broker):
    broker.add_user("only-person")
    broker.submit(
        user_id="only-person", command="private training command",
        gpu_type="a10g", hours=1,
    )
    with pytest.raises(TraceError, match="too small to anonymize safely"):
        export_trace(broker.store, broker.config.db_path.parent / "trace.json")


def test_replay_rejects_modified_trace(tmp_path):
    state = tmp_path / "demo"
    seed(state, days=2, rng_seed=1)

    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    broker = Broker.open(state, clock=SystemClock())
    trace = tmp_path / "trace.json"
    export_trace(broker.store, trace)
    bundle = json.loads(trace.read_text())
    bundle["jobs"][0]["requested_hours"] = 999
    trace.write_text(json.dumps(bundle))
    with pytest.raises(TraceError, match="digest"):
        replay_trace(trace)

"""Write the golden cases that pin the Go fair-share port to the Python one.

The Go control plane (controlplane/internal/domain/policy) reimplements
gpu_broker.fairshare.priority_of. This script calls the real Python function
over a grid of shares and waits and writes its outputs, so a change to either
implementation that is not mirrored in the other fails a test.

    python scripts/fairshare_golden.py > controlplane/internal/domain/policy/testdata/fairshare_golden.json
"""

import datetime as dt
import json
from types import SimpleNamespace

from gpu_broker.fairshare import priority_of


def main() -> None:
    now = dt.datetime(2026, 10, 1, 12, tzinfo=dt.timezone.utc)
    config = SimpleNamespace(w_fair=0.5, w_age=0.5, age_max_hours=12.0)
    cases = []
    for share in (0.0, 0.05, 0.25, 0.5, 0.75, 1.0):
        for wait_minutes in (0, 1, 30, 90, 360, 719, 720, 721, 2880):
            job = SimpleNamespace(
                job_id="j", user_id="u",
                wait_hours=lambda now, m=wait_minutes: m / 60.0,
            )
            p = priority_of(job, {"u": share}, now, config)
            cases.append({
                "share": share, "wait_minutes": wait_minutes,
                "score": round(p.score, 9), "fair_term": round(p.fair_term, 9),
                "age_term": round(p.age_term, 9),
            })
    print(json.dumps({"source": "gpu_broker.fairshare.priority_of",
                      "config": {"w_fair": 0.5, "w_age": 0.5, "age_max_hours": 12.0},
                      "cases": cases}, indent=1))


if __name__ == "__main__":
    main()

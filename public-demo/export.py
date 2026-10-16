"""Export native Go simulator recordings; never starts workers, jobs, a DB or cloud calls."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).parent
DATASET = "controlplane/data/carbon/gb-2026-08-03_2026-09-28.json"
FILES = [
    "controlplane/cmd/gpubroker/offline.go",
    *sorted(str(path.relative_to(ROOT)) for folder in [
        "controlplane/internal/sim", "controlplane/internal/domain",
        "controlplane/internal/adapters/carbon"] for path in (ROOT / folder).rglob("*.go")
        if not path.name.endswith("_test.go")),
    DATASET, "controlplane/go.mod", "controlplane/go.sum",
]
COMMAND = ["go", "run", "./cmd/gpubroker", "sim", "--seed", "42",
           "--workload", "all", "--policy", "all"]


def extract(raw):
    # Runtime timing fields vary between runs and are not part of this recording.
    assert raw["evidence_class"] == "simulator" and raw["seed"] == 42
    rows = raw["results"]
    assert len(rows) == 32
    assert all(row["evidence_class"] == "simulator" for row in rows)
    return {key: raw[key] for key in ["evidence_class", "notice", "carbon_dataset",
            "carbon_dataset_sha256", "seed", "results"]}


def export(recording):
    commit = subprocess.check_output(
        ["git", "log", "-1", "--format=%H", "--", *FILES], cwd=ROOT, text=True).strip()
    return {
        "schema_version": 1, "source_commit": commit,
        "generation_argv": COMMAND, "generation_cwd": "controlplane",
        "sources": [{"path": path, "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest()}
                    for path in FILES],
        **recording,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="Rerun the bounded CPU simulator")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    record_path = HERE / "recorded-simulation.json"
    if args.refresh:
        with tempfile.TemporaryDirectory(prefix="gpu-broker-public-demo-") as tmp:
            raw_path = Path(tmp) / "simulation.json"
            subprocess.run([*COMMAND, "--out", str(raw_path)], cwd=ROOT / "controlplane", check=True)
            recording = extract(json.loads(raw_path.read_text()))
        record_path.write_text(json.dumps(recording, indent=2, allow_nan=False) + "\n")
    recording = extract(json.loads(record_path.read_text()))
    result = json.dumps(export(recording), indent=2, allow_nan=False) + "\n"
    target = HERE / "site/data.json"
    if args.check:
        if not target.exists() or target.read_text() != result:
            raise SystemExit("Recorded demo export is stale; run python3 public-demo/export.py")
        print("Simulator export and all native source hashes match.")
    else:
        target.parent.mkdir(exist_ok=True)
        target.write_text(result)
        print(target)

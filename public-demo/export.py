"""Export native Go simulator recordings; never starts workers, jobs, a DB or cloud calls."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).parent
DATASET = "controlplane/data/carbon/gb-2026-08-03_2026-09-28.json"
DATASET_ASSET = HERE / "site" / "carbon-fixture.json"
FILES = [
    "controlplane/cmd/gpubroker/offline.go",
    "controlplane/cmd/gpubroker-wasm/main_js.go",
    *sorted(str(path.relative_to(ROOT)) for folder in [
        "controlplane/internal/sim", "controlplane/internal/domain",
        "controlplane/internal/adapters/carbon"] for path in (ROOT / folder).rglob("*.go")
        if not path.name.endswith("_test.go")),
    DATASET, "controlplane/go.mod", "controlplane/go.sum",
    "public-demo/build_wasm.sh", "public-demo/export.py",
    "public-demo/site/app.js", "public-demo/site/sim-worker.js",
    "public-demo/site/index.html", "public-demo/site/style.css",
    "public-demo/site/wasm_exec.js",
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


def export(recording, source_commit=None):
    commit = source_commit or subprocess.check_output(
        ["git", "log", "-1", "--format=%H", "--", *FILES], cwd=ROOT, text=True).strip()
    return {
        "schema_version": 1, "source_commit": commit,
        "generation_argv": COMMAND, "generation_cwd": "controlplane",
        "sources": [{"path": path, "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest()}
                    for path in FILES],
        **recording,
    }


def verify_export(actual, recording):
    # Rebase changes commit IDs; byte hashes bind the retained source snapshot.
    commit = actual.get("source_commit", "")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid source snapshot commit")
    if actual != export(recording, source_commit=commit):
        raise ValueError("recorded results or current source hashes differ")
    available = subprocess.run(["git", "cat-file", "-e", commit + "^{commit}"],
                               cwd=ROOT, capture_output=True).returncode == 0
    if not available:
        return "Historical commit object unavailable; current source hashes and results verified."
    for source in actual["sources"]:
        raw = subprocess.check_output(["git", "show", commit + ":" + source["path"]], cwd=ROOT)
        if hashlib.sha256(raw).hexdigest() != source["sha256"]:
            raise ValueError("historical source snapshot differs: " + source["path"])
    return "Historical source snapshot, current source hashes and results verified."


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
        try:
            message = verify_export(json.loads(target.read_text()), recording)
        except (OSError, ValueError) as error:
            raise SystemExit("Recorded demo export is stale: " + str(error)) from error
        if not DATASET_ASSET.exists() or DATASET_ASSET.read_bytes() != (ROOT / DATASET).read_bytes():
            raise SystemExit("Browser carbon fixture is stale; run python3 public-demo/export.py")
        print(message)
    else:
        target.parent.mkdir(exist_ok=True)
        target.write_text(result)
        DATASET_ASSET.write_bytes((ROOT / DATASET).read_bytes())
        print(target)

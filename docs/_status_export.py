"""Export the status page as a self-contained static snapshot.

For hosting the page somewhere that cannot run the broker. It serves the real
app, fetches what it renders, and inlines the stylesheet so the result is one
file with no external requests.

    python docs/_status_export.py --out site/ [--dir <state dir>]

A snapshot, and labelled as one on the page: it shows the pool as it was when
the export ran and does not change until the next one. The live page is the
`--only-public` process next to the scheduler, which is what DEPLOY.md sets up.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gpu_broker.broker import Broker  # noqa: E402
from gpu_broker.clock import SystemClock  # noqa: E402
from gpu_broker.config import load_config  # noqa: E402
from gpu_broker.demo import seed  # noqa: E402
from gpu_broker.web.publicapp import create_public_app  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--out", default="site")
parser.add_argument("--dir", default="")
parser.add_argument("--days", type=int, default=30)
parser.add_argument("--port", type=int, default=8732)
parser.add_argument("--title", default="GPU broker")
args = parser.parse_args()

root = Path(__file__).resolve().parents[1]
if args.dir:
    state_dir = Path(args.dir)
else:
    state_dir = root / ".status-shot"
    shutil.rmtree(state_dir, ignore_errors=True)
    report = seed(state_dir, days=args.days, overwrite=True)
    print(f"seeded {report.jobs} jobs from {report.users} people")

config = load_config(state_dir)
app = create_public_app(
    lambda: Broker.open(state_dir, clock=SystemClock(), backends=[], config=config),
    title=args.title,
    cache_seconds=0.0,
)

import uvicorn  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="error"))
threading.Thread(target=server.run, daemon=True).start()
for _ in range(100):
    if server.started:
        break
    time.sleep(0.1)

client = TestClient(app)
html = client.get("/").text
payload = client.get("/status.json").json()
server.should_exit = True

css = (root / "gpu_broker" / "web" / "static" / "app.css").read_text()
# One file, no external requests: whatever hosts this may not serve /static.
html = html.replace(
    '<link rel="stylesheet" href="/static/app.css">',
    f"<style>\n{css}\n</style>",
)

out = root / args.out
out.mkdir(parents=True, exist_ok=True)
(out / "index.html").write_text(html)
(out / "status.json").write_text(json.dumps(payload, indent=2))
print(f"wrote {out}/index.html ({len(html):,} bytes) and status.json")

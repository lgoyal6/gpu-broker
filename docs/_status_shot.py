"""Screenshot the public status page.

Serves the real app over a real port and photographs it in a real browser, so
what lands in the README is the page, not a mock of it.

    python docs/_status_shot.py [--out docs/status-page.png]

Seeds a fresh demo database unless one is passed with --dir, which means the
image carries the demo banner. That is deliberate: a screenshot of generated
activity presented as a real club would be a claim about people who do not
exist. Re-shoot it against the pilot or the club once there is real data.
"""

from __future__ import annotations

import argparse
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
parser.add_argument("--out", default="docs/status-page.png")
parser.add_argument("--dir", default="")
parser.add_argument("--days", type=int, default=30)
parser.add_argument("--port", type=int, default=8731)
parser.add_argument("--width", type=int, default=1180)
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
    cache_seconds=0.0,
)

import uvicorn  # noqa: E402

server = uvicorn.Server(
    uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="error")
)
thread = threading.Thread(target=server.run, daemon=True)
thread.start()
for _ in range(100):
    if server.started:
        break
    time.sleep(0.1)

from playwright.sync_api import sync_playwright  # noqa: E402

out = root / args.out
with sync_playwright() as play:
    browser = play.chromium.launch()
    page = browser.new_page(viewport={"width": args.width, "height": 1000}, device_scale_factor=2)
    page.goto(f"http://127.0.0.1:{args.port}/", wait_until="networkidle")
    page.screenshot(path=str(out), full_page=True)
    browser.close()

server.should_exit = True
print(f"wrote {out}")

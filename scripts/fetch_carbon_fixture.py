"""Fetch the GB carbon-intensity dataset EcoShift is tested and evaluated on.

Source: National Grid ESO Carbon Intensity API (https://api.carbonintensity.org.uk),
a public, keyless API. Two series, half-hourly, over a fixed window:

- national: ESO's day-ahead forecast and the *actual* (observed) intensity.
  Forecast evaluation uses `actual` only.
- regional: ESO's regional intensity, which ESO publishes as a modelled
  estimate (the `forecast` field), not a metered observation. Placement across
  regions uses it, labelled as an estimate.

The output is versioned by its window and SHA-256; the manifest records both.
Re-running for the same window should reproduce the same values unless ESO
revises history, which the checksum would reveal.

    python scripts/fetch_carbon_fixture.py 2026-08-03 2026-09-28 \
        > controlplane/data/carbon/gb-2026-08-03_2026-09-28.json
"""

import datetime as dt
import hashlib
import json
import sys
import time
import urllib.request

API = "https://api.carbonintensity.org.uk"
REGIONS = {1: "gb-north-scotland", 13: "gb-london", 8: "gb-west-midlands"}
STEP = dt.timedelta(minutes=30)


def get(url: str) -> dict:
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return json.load(resp)
        except Exception:  # transient network errors: back off and retry
            time.sleep(2 ** attempt)
    raise RuntimeError(f"failed: {url}")


def iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%MZ")


def main(start: str, end: str) -> None:
    t0 = dt.datetime.fromisoformat(start).replace(tzinfo=dt.timezone.utc)
    t1 = dt.datetime.fromisoformat(end).replace(tzinfo=dt.timezone.utc)
    n = int((t1 - t0) / STEP)
    national_actual = [None] * n
    national_forecast = [None] * n
    regional = {name: [None] * n for name in REGIONS.values()}

    def index(frm: str) -> int | None:
        t = dt.datetime.strptime(frm, "%Y-%m-%dT%H:%MZ").replace(tzinfo=dt.timezone.utc)
        i = int((t - t0) / STEP)
        return i if 0 <= i < n else None

    cur = t0
    while cur < t1:
        nxt = min(cur + dt.timedelta(days=13), t1)
        for row in get(f"{API}/intensity/{iso(cur)}/{iso(nxt)}")["data"]:
            i = index(row["from"])
            if i is not None:
                national_actual[i] = row["intensity"].get("actual")
                national_forecast[i] = row["intensity"].get("forecast")
        for row in get(f"{API}/regional/intensity/{iso(cur)}/{iso(nxt)}")["data"]:
            i = index(row["from"])
            if i is None:
                continue
            for reg in row["regions"]:
                name = REGIONS.get(reg["regionid"])
                if name:
                    regional[name][i] = reg["intensity"]["forecast"]
        cur = nxt

    data = {"start": iso(t0), "step_minutes": 30, "points": n,
            "national": {"actual": national_actual, "eso_forecast": national_forecast},
            "regional_estimate": regional}
    digest = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    out = {
        "manifest": {
            "dataset": f"gb-carbon-intensity/{start}_{end}",
            "source": "National Grid ESO Carbon Intensity API",
            "source_url": API,
            "license_note": "Public API; see https://carbonintensity.org.uk for terms.",
            "retrieved_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "national_basis": "observed (actual) and ESO day-ahead forecast",
            "regional_basis": "ESO regional modelled estimate, not metered",
            "regions": REGIONS,
            "unit": "gCO2/kWh",
            "missing": "null; never interpolated",
            "sha256": digest,
        },
        "data": data,
    }
    json.dump(out, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

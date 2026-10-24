"""Daily weather for one city per state from the free Open-Meteo historical API.

The simulated dates are in 2015-2016, so this uses the archive endpoint and the
simulated date, not today. Responses are cached locally per city and month.

Standalone use:  python sim/weather_pull.py --date 2015-09-25
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date

import requests

from common import CFG

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
CITIES = {  # state_id -> (city, lat, lon, timezone)
    "CA": ("Los Angeles", 34.05, -118.24, "America/Los_Angeles"),
    "TX": ("Dallas", 32.78, -96.80, "America/Chicago"),
    "WI": ("Milwaukee", 43.04, -87.91, "America/Chicago"),
}


def _month_cache(state: str, d: date, offline: bool) -> dict:
    cache_dir = CFG.state_dir / "weather_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    f = cache_dir / f"{state}_{d:%Y-%m}.json"
    if f.exists():
        return json.loads(f.read_text())
    city, lat, lon, tz = CITIES[state]
    start = d.replace(day=1)
    end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    if offline:  # --dry-run without internet: plausible fake numbers
        rnd = random.Random(f"{state}{start}")
        days = [(start.toordinal() + i) for i in range((end - start).days)]
        out = {date.fromordinal(o).isoformat(): {"temp_max_c": round(rnd.uniform(5, 35), 1),
                                                 "temp_min_c": round(rnd.uniform(-5, 20), 1),
                                                 "precip_mm": round(max(0, rnd.gauss(1, 3)), 1)}
               for o in days}
    else:
        r = requests.get(ARCHIVE_URL, timeout=30, params={
            "latitude": lat, "longitude": lon, "timezone": tz,
            "start_date": start.isoformat(),
            "end_date": date.fromordinal(end.toordinal() - 1).isoformat(),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        })
        r.raise_for_status()
        dl = r.json()["daily"]
        out = {t: {"temp_max_c": mx, "temp_min_c": mn, "precip_mm": p}
               for t, mx, mn, p in zip(dl["time"], dl["temperature_2m_max"],
                                       dl["temperature_2m_min"], dl["precipitation_sum"])}
    f.write_text(json.dumps(out))
    return out


def weather_rows(d: date, offline: bool = False) -> list[dict]:
    rows = []
    for state, (city, *_rest) in CITIES.items():
        w = _month_cache(state, d, offline).get(d.isoformat(), {})
        rows.append({"date": d.isoformat(), "state_id": state, "city": city, **w,
                     "source": "open-meteo-archive"})
    return rows


def main():
    from common import Landing
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="simulated date, YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    d = date.fromisoformat(a.date)
    Landing(a.dry_run).write_jsonl(f"weather/{d.isoformat()}.json", weather_rows(d))
    print(f"wrote weather/{d.isoformat()}.json")


if __name__ == "__main__":
    main()

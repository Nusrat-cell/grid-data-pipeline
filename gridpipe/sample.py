"""Generates a week of hourly sample data for the two Danish price areas.

The records use the same field names and shape as two Energi Data Service
datasets (Elspotprices and ProductionConsumptionSettlement), so the pipeline
can read a real export with no changes. The values are synthetic but follow
realistic patterns: a daily demand curve, wind that comes in weather fronts,
solar only in daylight, and prices that fall when wind is high.

On purpose, the sample also contains the kinds of faults real feeds have, so
the pipeline's checks have something to catch:
  * one hour delivered twice (a re-sent message)
  * one hour missing entirely
  * one price that is null
  * one consumption value sent in kWh instead of MWh (1000x too big)
The week includes 29 March 2026, when Denmark moves to summer time and the
local day has only 23 hours.
"""

from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

DK = ZoneInfo("Europe/Copenhagen")
START_UTC = datetime(2026, 3, 25, 23, 0, tzinfo=timezone.utc)  # 26 March 00:00 local
HOURS = 7 * 24 - 1  # seven local days, 26 March to 1 April; 29 March has 23 hours
EUR_DKK = 7.46

AREAS = {
    # area: (base demand MWh, offshore capacity, onshore capacity, solar capacity)
    "DK1": (2300, 1300, 3600, 2200),
    "DK2": (1500, 1100, 700, 900),
}


def _fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def generate(seed: int = 7) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    prices, production = [], []
    wind_state = 0.35
    for h in range(HOURS):
        utc = START_UTC + timedelta(hours=h)
        local = utc.astimezone(DK)
        # Wind moves slowly, like a weather front passing.
        wind_state = min(1.0, max(0.05, wind_state + rng.gauss(0, 0.07)))
        hour = local.hour
        daylight = max(0.0, math.sin(math.pi * (hour - 6) / 13)) if 6 <= hour <= 19 else 0.0
        for area, (base, off_cap, on_cap, sol_cap) in AREAS.items():
            demand_shape = 0.82 + 0.18 * math.sin(math.pi * (hour - 5) / 12) if 5 <= hour <= 23 else 0.78
            weekend = 0.9 if local.weekday() >= 5 else 1.0
            consumption = base * demand_shape * weekend * rng.uniform(0.97, 1.03)
            offshore = off_cap * wind_state * rng.uniform(0.6, 0.75)
            onshore = on_cap * wind_state * rng.uniform(0.35, 0.5)
            solar = sol_cap * daylight * rng.uniform(0.25, 0.45)
            residual = consumption - offshore - onshore - solar
            eur = max(-15.0, 55 + residual * 0.035 + rng.gauss(0, 8))
            prices.append({
                "HourUTC": _fmt(utc),
                "HourDK": _fmt(local.replace(tzinfo=None)),
                "PriceArea": area,
                "SpotPriceDKK": round(eur * EUR_DKK, 2),
                "SpotPriceEUR": round(eur, 2),
            })
            production.append({
                "HourUTC": _fmt(utc),
                "HourDK": _fmt(local.replace(tzinfo=None)),
                "PriceArea": area,
                "GrossConsumptionMWh": round(consumption, 2),
                "OffshoreWindMWh": round(offshore, 2),
                "OnshoreWindMWh": round(onshore, 2),
                "SolarPowerMWh": round(solar, 2),
            })

    # --- faults, the way real feeds deliver them ---
    prices.append(dict(prices[40]))                      # duplicate hour
    del prices[100]                                       # missing hour
    prices[200]["SpotPriceEUR"] = None                    # null price
    prices[200]["SpotPriceDKK"] = None
    production[150]["GrossConsumptionMWh"] *= 1000        # kWh sent as MWh
    rng.shuffle(prices)                                   # arrival order is not time order
    return prices, production


def write(directory: Path, seed: int = 7) -> None:
    prices, production = generate(seed)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "elspotprices.json").write_text(json.dumps({"records": prices}, indent=1))
    (directory / "production_consumption.json").write_text(json.dumps({"records": production}, indent=1))


if __name__ == "__main__":
    write(Path(__file__).resolve().parent.parent / "data" / "sample")

r"""The pipeline itself: raw -> staging -> marts, with data quality checks.

    raw_prices ------> stg_prices ------\                 /--> mart_daily
                                         >--> mart_hourly
    raw_production --> stg_production --/                 \--> price_model

Raw tables keep the data exactly as delivered. Staging is where it gets
cleaned: duplicates removed, bad rows moved to a `rejected_rows` table with a
reason (never silently dropped). Marts are what dashboards and the API read.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .assets import Registry

DK = ZoneInfo("Europe/Copenhagen")
DATA_DIR = Path(os.environ.get("GRIDPIPE_DATA", Path(__file__).resolve().parent.parent / "data" / "sample"))

# A price outside this band is almost certainly a feed error, not a market
# price. The day-ahead market's own limits are -500 and 4000 EUR/MWh.
PRICE_MIN_EUR, PRICE_MAX_EUR = -500.0, 4000.0
# A consumption value this many times the area's median is treated as a unit
# error (kWh sent as MWh) and quarantined.
UNIT_ERROR_FACTOR = 20

pipeline = Registry()


def expected_hours(local_day: date) -> int:
    """Hours in a local Danish day: 24 normally, 23 or 25 on DST change days.

    Both ends are converted to UTC before subtracting. Subtracting two
    datetimes that share the same tzinfo does wall-clock arithmetic in
    Python and ignores the DST jump, so it always says 24.
    """
    start = datetime(local_day.year, local_day.month, local_day.day, tzinfo=DK)
    end = datetime.combine(local_day + timedelta(days=1), datetime.min.time(), tzinfo=DK)
    return int((end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds() // 3600)


def _load_records(filename: str) -> list[dict]:
    payload = json.loads((DATA_DIR / filename).read_text())
    return payload["records"] if isinstance(payload, dict) else payload


def _reject(conn, source, key, reason, payload):
    conn.execute(
        "INSERT INTO rejected_rows (source, record_key, reason, payload) VALUES (?, ?, ?, ?)",
        (source, key, reason, json.dumps(payload)),
    )


# ---------------------------------------------------------------- raw layer

@pipeline.asset(description="Day-ahead spot prices, exactly as delivered")
def raw_prices(conn: sqlite3.Connection) -> int:
    rows = _load_records("elspotprices.json")
    conn.execute("DROP TABLE IF EXISTS raw_prices")
    conn.execute("CREATE TABLE raw_prices (hour_utc TEXT, hour_dk TEXT, price_area TEXT, price_dkk REAL, price_eur REAL)")
    conn.executemany(
        "INSERT INTO raw_prices VALUES (?, ?, ?, ?, ?)",
        [(r["HourUTC"], r["HourDK"], r["PriceArea"], r["SpotPriceDKK"], r["SpotPriceEUR"]) for r in rows],
    )
    return len(rows)


@pipeline.asset(description="Hourly consumption and renewable production, exactly as delivered")
def raw_production(conn: sqlite3.Connection) -> int:
    rows = _load_records("production_consumption.json")
    conn.execute("DROP TABLE IF EXISTS raw_production")
    conn.execute(
        "CREATE TABLE raw_production (hour_utc TEXT, hour_dk TEXT, price_area TEXT, "
        "consumption_mwh REAL, offshore_mwh REAL, onshore_mwh REAL, solar_mwh REAL)"
    )
    conn.executemany(
        "INSERT INTO raw_production VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (r["HourUTC"], r["HourDK"], r["PriceArea"], r["GrossConsumptionMWh"],
             r["OffshoreWindMWh"], r["OnshoreWindMWh"], r["SolarPowerMWh"])
            for r in rows
        ],
    )
    return len(rows)


# ------------------------------------------------------------ staging layer

def _ensure_rejected_table(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS rejected_rows (source TEXT, record_key TEXT, reason TEXT, payload TEXT, "
        "rejected_at TEXT DEFAULT (datetime('now')))"
    )


@pipeline.asset(deps=("raw_prices",), description="One clean price per area and hour")
def stg_prices(conn: sqlite3.Connection) -> int:
    _ensure_rejected_table(conn)
    conn.execute("DELETE FROM rejected_rows WHERE source = 'prices'")
    conn.execute("DROP TABLE IF EXISTS stg_prices")
    conn.execute(
        "CREATE TABLE stg_prices (hour_utc TEXT, local_date TEXT, price_area TEXT, price_eur REAL, price_dkk REAL, "
        "PRIMARY KEY (hour_utc, price_area))"
    )
    seen = set()
    kept = 0
    for hour_utc, hour_dk, area, dkk, eur in conn.execute(
        "SELECT hour_utc, hour_dk, price_area, price_dkk, price_eur FROM raw_prices ORDER BY hour_utc, price_area"
    ).fetchall():
        key = (hour_utc, area)
        record = {"hour_utc": hour_utc, "area": area, "price_eur": eur}
        if key in seen:
            _reject(conn, "prices", f"{area} {hour_utc}", "duplicate hour", record)
            continue
        seen.add(key)
        if eur is None:
            _reject(conn, "prices", f"{area} {hour_utc}", "missing price", record)
            continue
        conn.execute(
            "INSERT INTO stg_prices VALUES (?, ?, ?, ?, ?)", (hour_utc, hour_dk[:10], area, eur, dkk)
        )
        kept += 1
    return kept


@pipeline.asset(deps=("raw_production",), description="One clean production row per area and hour")
def stg_production(conn: sqlite3.Connection) -> int:
    _ensure_rejected_table(conn)
    conn.execute("DELETE FROM rejected_rows WHERE source = 'production'")
    conn.execute("DROP TABLE IF EXISTS stg_production")
    conn.execute(
        "CREATE TABLE stg_production (hour_utc TEXT, local_date TEXT, price_area TEXT, consumption_mwh REAL, "
        "wind_mwh REAL, solar_mwh REAL, PRIMARY KEY (hour_utc, price_area))"
    )
    rows = conn.execute("SELECT * FROM raw_production ORDER BY hour_utc, price_area").fetchall()

    medians = {}
    for area in {r[2] for r in rows}:
        values = sorted(r[3] for r in rows if r[2] == area and r[3] is not None)
        medians[area] = values[len(values) // 2]

    seen, kept = set(), 0
    for hour_utc, hour_dk, area, cons, off, on, sol in rows:
        key = (hour_utc, area)
        record = {"hour_utc": hour_utc, "area": area, "consumption_mwh": cons}
        if key in seen:
            _reject(conn, "production", f"{area} {hour_utc}", "duplicate hour", record)
            continue
        seen.add(key)
        if cons is None or cons > medians[area] * UNIT_ERROR_FACTOR:
            _reject(conn, "production", f"{area} {hour_utc}",
                    f"consumption {cons} is over {UNIT_ERROR_FACTOR}x the median; likely kWh sent as MWh", record)
            continue
        conn.execute(
            "INSERT INTO stg_production VALUES (?, ?, ?, ?, ?, ?)",
            (hour_utc, hour_dk[:10], area, cons, (off or 0) + (on or 0), sol or 0),
        )
        kept += 1
    return kept


@pipeline.check(asset="stg_prices")
def prices_in_market_range(conn) -> tuple[bool, str]:
    bad = conn.execute(
        "SELECT COUNT(*) FROM stg_prices WHERE price_eur < ? OR price_eur > ?", (PRICE_MIN_EUR, PRICE_MAX_EUR)
    ).fetchone()[0]
    return bad == 0, f"{bad} prices outside {PRICE_MIN_EUR:g}..{PRICE_MAX_EUR:g} EUR/MWh"


@pipeline.check(asset="stg_prices", blocking=False)
def prices_complete_per_day(conn) -> tuple[bool, str]:
    return _completeness(conn, "stg_prices")


@pipeline.check(asset="stg_production", blocking=False)
def production_complete_per_day(conn) -> tuple[bool, str]:
    return _completeness(conn, "stg_production")


def _completeness(conn, table: str) -> tuple[bool, str]:
    gaps = []
    for local_date, area, n in conn.execute(
        f"SELECT local_date, price_area, COUNT(*) FROM {table} GROUP BY local_date, price_area ORDER BY 1, 2"
    ):
        want = expected_hours(date.fromisoformat(local_date))
        if n != want:
            gaps.append(f"{area} {local_date}: {n}/{want} hours")
    return not gaps, "all days complete" if not gaps else "; ".join(gaps)


# --------------------------------------------------------------- mart layer

@pipeline.asset(deps=("stg_prices", "stg_production"), description="Price, demand and renewables per area and hour")
def mart_hourly(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS mart_hourly")
    conn.execute(
        """
        CREATE TABLE mart_hourly AS
        SELECT p.hour_utc,
               p.local_date,
               p.price_area,
               p.price_eur,
               p.price_dkk,
               d.consumption_mwh,
               d.wind_mwh,
               d.solar_mwh,
               ROUND((d.wind_mwh + d.solar_mwh) / d.consumption_mwh, 4) AS renewable_share,
               ROUND(d.consumption_mwh - d.wind_mwh - d.solar_mwh, 2)  AS residual_load_mwh
        FROM stg_prices p
        JOIN stg_production d USING (hour_utc, price_area)
        """
    )
    return conn.execute("SELECT COUNT(*) FROM mart_hourly").fetchone()[0]


@pipeline.asset(deps=("mart_hourly",), description="Daily summary per area, by Danish local date")
def mart_daily(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS mart_daily")
    conn.execute(
        """
        CREATE TABLE mart_daily (
            local_date TEXT, price_area TEXT, avg_price_eur REAL, min_price_eur REAL, max_price_eur REAL,
            negative_price_hours INTEGER, consumption_mwh REAL, renewable_share REAL,
            hours_covered INTEGER, expected_hours INTEGER,
            PRIMARY KEY (local_date, price_area)
        )
        """
    )
    rows = conn.execute(
        """
        SELECT local_date, price_area,
               ROUND(AVG(price_eur), 2), MIN(price_eur), MAX(price_eur),
               SUM(price_eur < 0),
               ROUND(SUM(consumption_mwh), 1),
               ROUND(SUM(wind_mwh + solar_mwh) / SUM(consumption_mwh), 4),
               COUNT(*)
        FROM mart_hourly GROUP BY local_date, price_area
        """
    ).fetchall()
    conn.executemany(
        "INSERT INTO mart_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [r + (expected_hours(date.fromisoformat(r[0])),) for r in rows],
    )
    return len(rows)


@pipeline.check(asset="mart_hourly")
def renewable_share_is_plausible(conn) -> tuple[bool, str]:
    # Denmark can produce more wind than it uses (share > 1) but not 5x more.
    bad = conn.execute("SELECT COUNT(*) FROM mart_hourly WHERE renewable_share < 0 OR renewable_share > 5").fetchone()[0]
    return bad == 0, f"{bad} hours with an impossible renewable share"


# ------------------------------------------------------------ model layer
#
# A small price model, as an example of moving a data science prototype into
# the pipeline: it is rebuilt on every run, evaluated the same way every time,
# and checked against a naive baseline before anyone relies on it.

TEST_DAYS = 2  # the last two local days are held out for testing


def _fit_line(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Ordinary least squares for y = a + b * x."""
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    b = sxy / sxx
    return my - b * mx, b


@pipeline.asset(deps=("mart_hourly",), description="Price model: spot price from residual load, chronological test split")
def price_model(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS model_predictions")
    conn.execute(
        "CREATE TABLE model_predictions (hour_utc TEXT, price_area TEXT, split TEXT, actual_eur REAL, "
        "predicted_eur REAL, naive_eur REAL)"
    )
    conn.execute("DROP TABLE IF EXISTS model_metrics")
    conn.execute(
        "CREATE TABLE model_metrics (price_area TEXT PRIMARY KEY, train_hours INTEGER, test_hours INTEGER, "
        "intercept REAL, slope_eur_per_mwh REAL, mae_model REAL, mae_naive REAL, r2_test REAL)"
    )
    written = 0
    for (area,) in conn.execute("SELECT DISTINCT price_area FROM mart_hourly ORDER BY 1").fetchall():
        rows = conn.execute(
            "SELECT hour_utc, local_date, residual_load_mwh, price_eur FROM mart_hourly "
            "WHERE price_area = ? ORDER BY hour_utc", (area,)
        ).fetchall()
        days = sorted({r[1] for r in rows})
        test_days = set(days[-TEST_DAYS:])
        # Split by time, never at random: a random split lets the model learn
        # from hours that come after the ones it is tested on.
        train = [r for r in rows if r[1] not in test_days]
        test = [r for r in rows if r[1] in test_days]
        a, b = _fit_line([r[2] for r in train], [r[3] for r in train])

        # Naive baseline: the price at the same hour one day earlier.
        price_at = {r[0]: r[3] for r in rows}

        def naive(hour_utc: str):
            prev = (datetime.fromisoformat(hour_utc) - timedelta(days=1)).isoformat(timespec="seconds")
            return price_at.get(prev)

        errors_model, errors_naive, actuals = [], [], []
        for split, part in (("train", train), ("test", test)):
            for hour_utc, _, residual, actual in part:
                predicted = a + b * residual
                base = naive(hour_utc)
                conn.execute(
                    "INSERT INTO model_predictions VALUES (?, ?, ?, ?, ?, ?)",
                    (hour_utc, area, split, actual, round(predicted, 2), base),
                )
                written += 1
                if split == "test" and base is not None:
                    errors_model.append(abs(actual - predicted))
                    errors_naive.append(abs(actual - base))
                    actuals.append((actual, predicted))
        mean_actual = sum(x for x, _ in actuals) / len(actuals)
        ss_res = sum((x - p) ** 2 for x, p in actuals)
        ss_tot = sum((x - mean_actual) ** 2 for x, _ in actuals)
        conn.execute(
            "INSERT INTO model_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (area, len(train), len(test), round(a, 3), round(b, 5),
             round(sum(errors_model) / len(errors_model), 2), round(sum(errors_naive) / len(errors_naive), 2),
             round(1 - ss_res / ss_tot, 3)),
        )
    return written


@pipeline.check(asset="price_model", blocking=False)
def model_beats_naive_baseline(conn) -> tuple[bool, str]:
    rows = conn.execute("SELECT price_area, mae_model, mae_naive, r2_test FROM model_metrics ORDER BY 1").fetchall()
    worse = [r[0] for r in rows if r[1] >= r[2]]
    detail = "; ".join(f"{a}: MAE {m} vs naive {n} EUR/MWh, test R2 {r2}" for a, m, n, r2 in rows)
    return not worse, detail

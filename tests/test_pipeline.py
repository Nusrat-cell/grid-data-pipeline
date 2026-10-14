import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from unittest import mock

from gridpipe import pipeline as pl
from gridpipe import sample
from gridpipe.api import serve
from gridpipe.assets import Registry, run


def quiet(*_args, **_kwargs):
    pass


class ExpectedHoursTest(unittest.TestCase):
    def test_normal_and_dst_days(self):
        self.assertEqual(pl.expected_hours(date(2026, 3, 28)), 24)
        self.assertEqual(pl.expected_hours(date(2026, 3, 29)), 23)  # clocks go forward
        self.assertEqual(pl.expected_hours(date(2026, 10, 25)), 25)  # clocks go back


class RegistryTest(unittest.TestCase):
    def test_order_respects_dependencies(self):
        reg = Registry()

        @reg.asset(deps=("raw",))
        def clean(conn):
            return 0

        @reg.asset()
        def raw(conn):
            return 0

        @reg.asset(deps=("clean", "raw"))
        def report(conn):
            return 0

        order = reg.order()
        self.assertLess(order.index("raw"), order.index("clean"))
        self.assertLess(order.index("clean"), order.index("report"))

    def test_cycle_is_reported(self):
        reg = Registry()

        @reg.asset(deps=("b",))
        def a(conn):
            return 0

        @reg.asset(deps=("a",))
        def b(conn):
            return 0

        with self.assertRaisesRegex(ValueError, "Cycle"):
            reg.order()

    def test_failure_skips_downstream_but_not_siblings(self):
        reg = Registry()

        @reg.asset()
        def broken(conn):
            raise RuntimeError("source unavailable")

        @reg.asset(deps=("broken",))
        def child(conn):
            return 1

        @reg.asset()
        def independent(conn):
            return 5

        with closing(sqlite3.connect(":memory:")) as conn:
            result = run(reg, conn, log=quiet)
        status = {a.name: a.status for a in result.assets}
        self.assertEqual(result.status, "failed")
        self.assertEqual(status, {"broken": "failed", "child": "skipped", "independent": "success"})

    def test_blocking_check_stops_downstream(self):
        reg = Registry()

        @reg.asset()
        def source(conn):
            return 1

        @reg.asset(deps=("source",))
        def report(conn):
            return 1

        @reg.check(asset="source")
        def always_fails(conn):
            return False, "bad data"

        with closing(sqlite3.connect(":memory:")) as conn:
            result = run(reg, conn, log=quiet)
        self.assertEqual({a.name: a.status for a in result.assets}["report"], "skipped")


class FullPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        data = Path(cls.tmp.name) / "data"
        sample.write(data)
        cls.db_path = str(Path(cls.tmp.name) / "grid.db")
        cls.conn = sqlite3.connect(cls.db_path, check_same_thread=False)
        with mock.patch.object(pl, "DATA_DIR", data):
            cls.result = run(pl.pipeline, cls.conn, log=quiet)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.tmp.cleanup()

    def test_run_succeeds(self):
        self.assertEqual(self.result.status, "success")
        self.assertTrue(all(a.status == "success" for a in self.result.assets))

    def test_every_planted_fault_is_caught_with_a_reason(self):
        reasons = [r[0] for r in self.conn.execute("SELECT reason FROM rejected_rows")]
        self.assertIn("duplicate hour", reasons)
        self.assertIn("missing price", reasons)
        self.assertTrue(any("kWh" in r for r in reasons))
        self.assertEqual(len(reasons), 3)

    def test_staging_has_one_row_per_area_and_hour(self):
        dupes = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM stg_prices GROUP BY hour_utc, price_area HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        self.assertEqual(dupes, 0)

    def test_gaps_are_reported_not_hidden(self):
        checks = {c.check: c for c in self.result.checks}
        self.assertFalse(checks["prices_complete_per_day"].passed)
        self.assertIn("23/24", checks["prices_complete_per_day"].detail)
        # The DST day itself is complete for DK2: 23 of 23 hours is not a gap.
        self.assertNotIn("DK2 2026-03-29", checks["prices_complete_per_day"].detail)

    def test_daily_mart_knows_the_dst_day_is_short(self):
        row = self.conn.execute(
            "SELECT hours_covered, expected_hours FROM mart_daily WHERE local_date = '2026-03-29' AND price_area = 'DK2'"
        ).fetchone()
        self.assertEqual(row, (23, 23))

    def test_mart_values_are_consistent(self):
        bad = self.conn.execute(
            "SELECT COUNT(*) FROM mart_hourly "
            "WHERE ABS(residual_load_mwh - (consumption_mwh - wind_mwh - solar_mwh)) > 0.01"
        ).fetchone()[0]
        self.assertEqual(bad, 0)

    def test_model_is_tested_on_later_days_only(self):
        last_train = self.conn.execute("SELECT MAX(hour_utc) FROM model_predictions WHERE split = 'train'").fetchone()[0]
        first_test = self.conn.execute("SELECT MIN(hour_utc) FROM model_predictions WHERE split = 'test'").fetchone()[0]
        self.assertLess(last_train, first_test)

    def test_model_beats_naive_baseline(self):
        for area, mae_model, mae_naive in self.conn.execute("SELECT price_area, mae_model, mae_naive FROM model_metrics"):
            self.assertLess(mae_model, mae_naive, area)

    def test_run_is_logged(self):
        rows = self.conn.execute("SELECT COUNT(*) FROM asset_materializations WHERE run_id = ?", (self.result.run_id,))
        self.assertEqual(rows.fetchone()[0], len(pl.pipeline.assets))

    def test_api(self):
        server = serve(self.db_path, port=0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            health = json.load(urllib.request.urlopen(base + "/health"))
            self.assertTrue(health["ok"])
            hourly = json.load(urllib.request.urlopen(base + "/hourly?area=DK2&date=2026-03-29"))
            self.assertEqual(len(hourly), 23)
            model = json.load(urllib.request.urlopen(base + "/model"))
            self.assertEqual(len(model["metrics"]), 2)
            quality = json.load(urllib.request.urlopen(base + "/quality"))
            self.assertEqual(len(quality["rejected_rows"]), 3)
            page = urllib.request.urlopen(base + "/").read().decode()
            self.assertIn("Grid Data Dashboard", page)
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(base + "/hourly?area=SE3")
            self.assertEqual(ctx.exception.code, 400)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()

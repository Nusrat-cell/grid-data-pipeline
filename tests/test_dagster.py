"""Runs the whole pipeline through Dagster. Skipped when Dagster is not installed;
CI installs it and runs this test on every push."""

import importlib.util
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
# CI sets REQUIRE_DAGSTER=1 so a missing install fails the build instead of skipping.
RUN_TEST = HAS_DAGSTER or os.environ.get("REQUIRE_DAGSTER") == "1"


@unittest.skipUnless(RUN_TEST, "dagster not installed")
class DagsterTest(unittest.TestCase):
    def test_materialize_all_assets_and_checks(self):
        from dagster import materialize

        from gridpipe import pipeline as pl
        from gridpipe import sample
        from gridpipe.dagster_defs import assets, checks

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            sample.write(data)
            db = str(Path(tmp) / "grid.db")
            with mock.patch.object(pl, "DATA_DIR", data), mock.patch.dict(os.environ, {"GRIDPIPE_DB": db}):
                result = materialize([*assets, *checks])
            self.assertTrue(result.success)

            materialized = {e.asset_key.path[-1] for e in result.get_asset_materialization_events()}
            self.assertEqual(materialized, set(pl.pipeline.assets))

            check_results = {e.asset_check_key.name: e.passed for e in result.get_asset_check_evaluations()}
            self.assertTrue(check_results["prices_in_market_range"])
            self.assertFalse(check_results["prices_complete_per_day"])  # the planted gap is reported

            conn = sqlite3.connect(db)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM model_metrics").fetchone()[0], 2)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()

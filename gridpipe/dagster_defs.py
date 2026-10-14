"""The same pipeline, run by Dagster.

Every asset and check in pipeline.py becomes a Dagster asset or asset check
with the same name and dependencies, so the Dagster UI shows the same graph,
row counts become materialization metadata, and warnings show up as asset
check results. The pipeline code itself does not change.

    pip install dagster dagster-webserver
    dagster dev -m gridpipe.dagster_defs

The database path comes from GRIDPIPE_DB (default grid.db).
"""

from __future__ import annotations

import os
import sqlite3

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    Definitions,
    MaterializeResult,
    asset,
    asset_check,
)

from .pipeline import pipeline


def _db_path() -> str:
    return os.environ.get("GRIDPIPE_DB", "grid.db")


def _make_asset(spec):
    @asset(
        name=spec.name,
        deps=[AssetKey(d) for d in spec.deps],
        description=spec.description or None,
        group_name="grid",
    )
    def _materialize() -> MaterializeResult:
        conn = sqlite3.connect(_db_path())
        try:
            with conn:
                rows = spec.fn(conn)
        finally:
            conn.close()
        return MaterializeResult(metadata={"rows": rows or 0})

    return _materialize


def _make_check(spec):
    @asset_check(asset=AssetKey(spec.asset), name=spec.name)
    def _check() -> AssetCheckResult:
        conn = sqlite3.connect(_db_path())
        try:
            passed, detail = spec.fn(conn)
        finally:
            conn.close()
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if spec.blocking else AssetCheckSeverity.WARN,
            metadata={"detail": detail},
        )

    return _check


assets = [_make_asset(pipeline.assets[name]) for name in pipeline.order()]
checks = [_make_check(c) for c in pipeline.checks]

defs = Definitions(assets=assets, asset_checks=checks)

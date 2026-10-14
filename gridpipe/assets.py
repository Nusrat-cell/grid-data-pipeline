"""A small asset-based pipeline runner.

Each step of the pipeline is an "asset": a function that produces one table
and declares which other assets it reads from. The runner works out the
order from those declarations, runs each asset once, records how long it took
and how many rows it wrote, and stops everything downstream of a failure.

It follows the same idea as Dagster's software-defined assets (and the same
words, so the design carries over), but without the dependency.
"""

from __future__ import annotations

import sqlite3
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable


@dataclass
class Asset:
    name: str
    fn: Callable[[sqlite3.Connection], int]
    deps: tuple[str, ...] = ()
    description: str = ""


@dataclass
class CheckResult:
    asset: str
    check: str
    passed: bool
    detail: str = ""
    blocking: bool = True


@dataclass
class Check:
    name: str
    asset: str
    fn: Callable[[sqlite3.Connection], tuple[bool, str]]
    blocking: bool = True  # a failed blocking check stops downstream assets


@dataclass
class AssetResult:
    name: str
    status: str  # "success", "failed", "skipped"
    rows: int = 0
    seconds: float = 0.0
    error: str = ""


@dataclass
class RunResult:
    run_id: int
    status: str
    assets: list[AssetResult] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)


class Registry:
    def __init__(self) -> None:
        self.assets: dict[str, Asset] = {}
        self.checks: list[Check] = []

    def asset(self, deps: tuple[str, ...] = (), description: str = ""):
        def register(fn):
            if fn.__name__ in self.assets:
                raise ValueError(f"Asset {fn.__name__} defined twice")
            self.assets[fn.__name__] = Asset(fn.__name__, fn, tuple(deps), description)
            return fn
        return register

    def check(self, asset: str, blocking: bool = True):
        def register(fn):
            self.checks.append(Check(fn.__name__, asset, fn, blocking))
            return fn
        return register

    def order(self) -> list[str]:
        """Topological order of assets. Raises on unknown deps or cycles."""
        for a in self.assets.values():
            for d in a.deps:
                if d not in self.assets:
                    raise ValueError(f"{a.name} depends on unknown asset {d}")
        ordered, state = [], {}

        def visit(name: str, path: tuple[str, ...]):
            if state.get(name) == "done":
                return
            if state.get(name) == "visiting":
                raise ValueError("Cycle: " + " -> ".join(path + (name,)))
            state[name] = "visiting"
            for d in self.assets[name].deps:
                visit(d, path + (name,))
            state[name] = "done"
            ordered.append(name)

        for name in sorted(self.assets):
            visit(name, ())
        return ordered

    def downstream(self, name: str) -> set[str]:
        out, frontier = set(), {name}
        while frontier:
            nxt = {a.name for a in self.assets.values() if set(a.deps) & frontier} - out
            out |= nxt
            frontier = nxt
        return out


RUN_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT
);
CREATE TABLE IF NOT EXISTS asset_materializations (
    run_id   INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    asset    TEXT NOT NULL,
    status   TEXT NOT NULL,
    rows     INTEGER,
    seconds  REAL,
    error    TEXT
);
CREATE TABLE IF NOT EXISTS check_results (
    run_id   INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    asset    TEXT NOT NULL,
    check_name TEXT NOT NULL,
    passed   INTEGER NOT NULL,
    blocking INTEGER NOT NULL,
    detail   TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run(registry: Registry, conn: sqlite3.Connection, log=print) -> RunResult:
    conn.executescript(RUN_LOG_SCHEMA)
    run_id = conn.execute(
        "INSERT INTO pipeline_runs (started_at, status) VALUES (?, 'running')", (_now(),)
    ).lastrowid
    conn.commit()

    result = RunResult(run_id, "success")
    blocked: dict[str, str] = {}  # asset -> reason it was skipped

    for name in registry.order():
        asset = registry.assets[name]
        if name in blocked:
            result.assets.append(AssetResult(name, "skipped", error=blocked[name]))
            log(f"  skip   {name:<24} ({blocked[name]})")
            continue

        start = time.perf_counter()
        try:
            with conn:  # each asset commits on success or rolls back on error
                rows = asset.fn(conn)
            outcome = AssetResult(name, "success", rows or 0, time.perf_counter() - start)
            log(f"  ok     {name:<24} {outcome.rows:>7} rows  {outcome.seconds*1000:7.1f} ms")
        except Exception as exc:  # noqa: BLE001 - we want to record any failure
            outcome = AssetResult(name, "failed", 0, time.perf_counter() - start, f"{type(exc).__name__}: {exc}")
            log(f"  FAILED {name:<24} {outcome.error}")
            log(traceback.format_exc(limit=3))
            result.status = "failed"
            for d in registry.downstream(name):
                blocked.setdefault(d, f"upstream {name} failed")
        result.assets.append(outcome)

        if outcome.status != "success":
            continue
        for chk in (c for c in registry.checks if c.asset == name):
            passed, detail = chk.fn(conn)
            cr = CheckResult(name, chk.name, passed, detail, chk.blocking)
            result.checks.append(cr)
            mark = "pass" if passed else ("FAIL" if chk.blocking else "warn")
            log(f"    {mark:<5} {chk.name}: {detail}")
            if not passed and chk.blocking:
                result.status = "failed"
                for d in registry.downstream(name):
                    blocked.setdefault(d, f"check {chk.name} failed")

    with conn:
        conn.executemany(
            "INSERT INTO asset_materializations VALUES (?, ?, ?, ?, ?, ?)",
            [(run_id, a.name, a.status, a.rows, round(a.seconds, 4), a.error) for a in result.assets],
        )
        conn.executemany(
            "INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?)",
            [(run_id, c.asset, c.check, int(c.passed), int(c.blocking), c.detail) for c in result.checks],
        )
        conn.execute(
            "UPDATE pipeline_runs SET finished_at = ?, status = ? WHERE run_id = ?",
            (_now(), result.status, run_id),
        )
    return result

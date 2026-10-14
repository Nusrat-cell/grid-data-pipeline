"""A read-only JSON API over the marts and the run log, using only the standard library."""

from __future__ import annotations

import json
import re
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DASHBOARD = Path(__file__).resolve().parent / "dashboard.html"

AREAS = {"DK1", "DK2"}
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _area(params) -> str | None:
    area = params.get("area", [None])[0]
    if area is not None and area not in AREAS:
        raise ApiError(400, f"area must be one of {sorted(AREAS)}")
    return area


def _date(params, name) -> str | None:
    value = params.get(name, [None])[0]
    if value is not None and not DATE_RE.match(value):
        raise ApiError(400, f"{name} must look like 2026-03-29")
    return value


def route(conn: sqlite3.Connection, path: str, query: str):
    params = parse_qs(query)
    if path == "/health":
        last = _rows(conn, "SELECT run_id, status, finished_at FROM pipeline_runs ORDER BY run_id DESC LIMIT 1")
        if not last:
            raise ApiError(503, "pipeline has never run")
        return {"ok": last[0]["status"] == "success", "last_run": last[0]}

    if path == "/hourly":
        area, day = _area(params), _date(params, "date")
        sql, args = "SELECT * FROM mart_hourly WHERE 1=1", []
        if area:
            sql += " AND price_area = ?"; args.append(area)
        if day:
            sql += " AND local_date = ?"; args.append(day)
        return _rows(conn, sql + " ORDER BY hour_utc, price_area LIMIT 1000", args)

    if path == "/daily":
        area = _area(params)
        sql, args = "SELECT * FROM mart_daily", []
        if area:
            sql += " WHERE price_area = ?"; args.append(area)
        return _rows(conn, sql + " ORDER BY local_date, price_area", args)

    if path == "/quality":
        return {
            "rejected_rows": _rows(conn, "SELECT source, record_key, reason FROM rejected_rows ORDER BY source, record_key"),
            "checks": _rows(
                conn,
                "SELECT asset, check_name, passed, blocking, detail FROM check_results "
                "WHERE run_id = (SELECT MAX(run_id) FROM pipeline_runs)",
            ),
        }

    if path == "/model":
        return {
            "metrics": _rows(conn, "SELECT * FROM model_metrics ORDER BY price_area"),
            "test_predictions": _rows(
                conn,
                "SELECT hour_utc, price_area, actual_eur, predicted_eur, naive_eur FROM model_predictions "
                "WHERE split = 'test' ORDER BY hour_utc, price_area",
            ),
        }

    if path == "/runs":
        runs = _rows(conn, "SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT 20")
        for r in runs:
            r["assets"] = _rows(
                conn, "SELECT asset, status, rows, seconds, error FROM asset_materializations WHERE run_id = ?", (r["run_id"],)
            )
        return runs

    raise ApiError(404, "not found; try /, /health, /hourly, /daily, /quality, /model or /runs")


def make_handler(db_path: str):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - name required by BaseHTTPRequestHandler
            url = urlparse(self.path)
            if url.path in ("/", "/dashboard"):
                page = DASHBOARD.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)
                return
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            try:
                status, body = 200, route(conn, url.path.rstrip("/") or "/", url.query)
            except ApiError as err:
                status, body = err.status, {"error": str(err)}
            except sqlite3.OperationalError as err:
                status, body = 503, {"error": f"data not ready: {err}"}
            finally:
                conn.close()
            payload = json.dumps(body, indent=1).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt, *args):
            pass

    return Handler


def serve(db_path: str, port: int = 8000, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(db_path))

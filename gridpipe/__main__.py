"""Command line: python -m gridpipe run | serve | sample"""

from __future__ import annotations

import argparse
import sqlite3
import sys

from . import sample
from .assets import run
from .pipeline import DATA_DIR, pipeline


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="gridpipe")
    parser.add_argument("--db", default="grid.db", help="SQLite file to write (default: grid.db)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="materialize all assets and run the checks")
    serve_p = sub.add_parser("serve", help="serve the JSON API")
    serve_p.add_argument("--port", type=int, default=8000)
    sub.add_parser("sample", help="regenerate the sample data")
    args = parser.parse_args(argv)

    if args.command == "sample":
        sample.write(DATA_DIR)
        print(f"Sample data written to {DATA_DIR}")
        return 0

    if args.command == "run":
        conn = sqlite3.connect(args.db)
        print(f"Run started, data from {DATA_DIR}")
        try:
            result = run(pipeline, conn)
        finally:
            conn.close()
        print(f"Run {result.run_id}: {result.status}")
        return 0 if result.status == "success" else 1

    if args.command == "serve":
        from .api import serve
        server = serve(args.db, args.port)
        print(f"API on http://127.0.0.1:{args.port}  (/health /hourly /daily /quality /runs)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

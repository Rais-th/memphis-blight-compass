"""Full ingestion + scoring pipeline against Neon Postgres.

Used by the nightly GitHub Action and locally on demand.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from db.pg import init_schema
from ingest import memphis_311
from ingest import code_enforcement
from ingest import landbank
from ingest import fema
from score.scorer import compute_scores


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--fema-limit", type=int, default=None)
    parser.add_argument("--skip-fema", action="store_true")
    parser.add_argument("--skip-311", action="store_true")
    parser.add_argument("--skip-ce", action="store_true")
    parser.add_argument("--skip-landbank", action="store_true")
    parser.add_argument("--skip-score", action="store_true")
    args = parser.parse_args()

    init_schema()
    t0 = time.time()

    def step(label, fn):
        t = time.time()
        print(f"[{datetime.now(timezone.utc).isoformat()}] {label}...", flush=True)
        try:
            result = fn()
            print(f"  ok  ({time.time() - t:.1f}s): {result}", flush=True)
        except Exception as e:
            print(f"  FAIL ({time.time() - t:.1f}s): {e}", flush=True)
            raise

    if not args.skip_311:
        step("311", lambda: memphis_311.ingest(days_back=args.days))
    if not args.skip_ce:
        step("code enforcement", lambda: code_enforcement.ingest(days_back=args.days))
    if not args.skip_landbank:
        step("land bank", landbank.ingest)
    if not args.skip_fema:
        step("FEMA", lambda: fema.ingest(limit=args.fema_limit))
    if not args.skip_score:
        step("scoring", compute_scores)

    print(f"Total: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()

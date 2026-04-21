"""Run the full ingestion pipeline."""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from db.db import init_db
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
    args = parser.parse_args()

    init_db()
    t0 = time.time()

    print(f"[{datetime.now(timezone.utc).isoformat()}] 311...")
    print(" ", memphis_311.ingest(days_back=args.days))

    print(f"[{datetime.now(timezone.utc).isoformat()}] code enforcement...")
    print(" ", code_enforcement.ingest(days_back=args.days))

    print(f"[{datetime.now(timezone.utc).isoformat()}] land bank...")
    print(" ", landbank.ingest())

    if not args.skip_fema:
        print(f"[{datetime.now(timezone.utc).isoformat()}] FEMA...")
        print(" ", fema.ingest(limit=args.fema_limit))

    print(f"[{datetime.now(timezone.utc).isoformat()}] scoring...")
    print(" ", compute_scores())

    print(f"Total: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()

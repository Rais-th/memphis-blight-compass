"""One-time migration: local SQLite -> Neon Postgres.

Reads data/compass.db and upserts every row into Neon. Idempotent: re-running
just overwrites matching primary keys.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from psycopg import sql

from db.pg import connect as pg_connect

ROOT = Path(__file__).resolve().parent.parent
SQLITE = ROOT / "data" / "compass.db"

TABLES: dict[str, tuple[str, list[str]]] = {
    "requests_311": (
        "incident_number",
        [
            "incident_number", "objectid", "parcel_id", "parcel_norm", "category",
            "group_name", "department", "division", "request_type", "request_status",
            "request_priority", "reported_date", "closed_date", "resolved_date",
            "days_old", "address", "zipcode", "neighborhood", "council_district",
            "creation_channel", "lat", "lng",
        ],
    ),
    "code_violations": (
        "id",
        [
            "id", "source_layer", "case_number", "parcel_id", "parcel_norm",
            "violation_type", "status", "open_date", "close_date", "address",
            "zipcode", "lat", "lng", "raw_json",
        ],
    ),
    "landbank_inventory": (
        "parcel_id",
        [
            "parcel_id", "parcel_norm", "address", "zipcode", "current_status",
            "available", "asking_price", "acres", "parcel_length", "parcel_width",
            "improvement_type", "lat", "lng", "last_seen_at", "raw_json",
        ],
    ),
    "flood_zones": (
        "parcel_id",
        [
            "parcel_id", "parcel_norm", "flood_zone", "sfha_tf", "static_bfe",
            "checked_at",
        ],
    ),
    "scores": (
        "parcel_id",
        [
            "parcel_id", "parcel_norm", "score", "chronic_complaints",
            "code_violations", "flood_safe", "affordable", "buildable",
            "lat", "lng", "computed_at",
        ],
    ),
}

BATCH = 1000


def migrate_table(name: str, pk: str, cols: list[str]) -> int:
    src = sqlite3.connect(SQLITE)
    src.row_factory = sqlite3.Row
    # Some sqlite rows may lack a column (e.g. case_number was never inserted). Probe.
    present = {r[1] for r in src.execute(f"PRAGMA table_info({name})")}
    effective_cols = [c for c in cols if c in present]

    rows = list(src.execute(
        f"SELECT {','.join(effective_cols)} FROM {name}"
    ).fetchall())
    src.close()
    if not rows:
        print(f"  {name}: 0 rows")
        return 0

    col_list = sql.SQL(",").join(sql.Identifier(c) for c in effective_cols)
    placeholders = sql.SQL(",").join(sql.Placeholder() * len(effective_cols))
    non_pk_cols = [c for c in effective_cols if c != pk]
    set_clause = sql.SQL(",").join(
        sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in non_pk_cols
    ) if non_pk_cols else sql.SQL("")

    if non_pk_cols:
        stmt = sql.SQL(
            "INSERT INTO {t} ({cols}) VALUES ({ph}) "
            "ON CONFLICT ({pk}) DO UPDATE SET {set_clause}"
        ).format(
            t=sql.Identifier(name),
            cols=col_list,
            ph=placeholders,
            pk=sql.Identifier(pk),
            set_clause=set_clause,
        )
    else:
        stmt = sql.SQL(
            "INSERT INTO {t} ({cols}) VALUES ({ph}) ON CONFLICT ({pk}) DO NOTHING"
        ).format(t=sql.Identifier(name), cols=col_list, ph=placeholders, pk=sql.Identifier(pk))

    inserted = 0
    with pg_connect(autocommit=False) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(rows), BATCH):
                batch = [tuple(r[c] for c in effective_cols) for r in rows[i:i+BATCH]]
                cur.executemany(stmt, batch)
                inserted += len(batch)
            conn.commit()
    return inserted


def main():
    if not SQLITE.exists():
        raise SystemExit(f"Missing local DB at {SQLITE}")
    # Truncate in reverse order of deps (scores, flood, lb, cv, 311)
    with pg_connect() as c:
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE scores, flood_zones, landbank_inventory, "
                "code_violations, requests_311 RESTART IDENTITY"
            )
    t0 = time.time()
    for name, (pk, cols) in TABLES.items():
        t = time.time()
        n = migrate_table(name, pk, cols)
        print(f"  {name}: {n:,} rows in {time.time()-t:.1f}s")
    print(f"Total: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

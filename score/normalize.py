"""Shelby County parcel ID normalization.

Three sources publish the same parcel IDs with different formatting:
  - Land Bank (ePropertyPlus): "00701900000290"   (14 chars, packed)
  - 311 (ArcGIS OPM):           "045118  00009"    (double-space separated)
  - Code Enforcement (ArcGIS):  "001043  00001C"   (double-space + suffix)

Canonical form: BLOCK(3) + MAP(3) + int(parcel) + optional suffix letter.
  "00701900000290"  -> "007019290"  (no suffix because trailing is '0' digit)
  "0110150000005C" -> "0110155C"
  "001043  00001C" -> "0010431C"

This lets us cross-reference parcels across sources.
"""
from __future__ import annotations

import re

from db.db import connect


def normalize(pid: str | None) -> str | None:
    if not pid:
        return None
    s = re.sub(r"\s+", "", pid.strip().upper())
    if len(s) < 6:
        return None
    block = s[:3]
    mapp = s[3:6]
    rest = s[6:]
    suffix = ""
    if rest and rest[-1].isalpha():
        suffix = rest[-1]
        rest = rest[:-1]
    if not rest or not rest.isdigit():
        return None
    parcel_num = str(int(rest))
    return f"{block}{mapp}{parcel_num}{suffix}"


def ensure_column(cur, table: str):
    cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table})")}
    if "parcel_norm" not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN parcel_norm TEXT")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_pnorm ON {table}(parcel_norm)")


def backfill() -> dict:
    conn = connect()
    cur = conn.cursor()
    counts = {}
    for table in ("landbank_inventory", "requests_311", "code_violations", "flood_zones", "scores"):
        ensure_column(cur, table)
        rows = list(cur.execute(f"SELECT rowid, parcel_id FROM {table}"))
        n = 0
        for rid, pid in rows:
            norm = normalize(pid)
            if norm:
                cur.execute(f"UPDATE {table} SET parcel_norm=? WHERE rowid=?", (norm, rid))
                n += 1
        counts[table] = n
    conn.close()
    return counts


if __name__ == "__main__":
    print(backfill())

"""Blight Compass scoring engine.

Formula (PRD §4.2):
  score = (chronic_complaints * 3)        -- 311 complaints last 12mo, if >= 3
        + (code_violations * 2)            -- count of code enforcement cases
        + (1 if flood safe else 0)         -- Zone X / SFHA False
        + (1 if asking_price < 2000 else 0)
        + (1 if width >= 30 AND length >= 50 else 0)

Scoring scope: every parcel that appears in any source (landbank inventory,
311 requests with parcel_id, code_violations with parcel_id). Parcels not
in the Land Bank inventory get 0 on the price/size/flood components but
still score on complaint density (useful for the equity dashboard).
"""
from __future__ import annotations

from datetime import datetime, timezone

from db.db import connect

SQL_SCORE = """
INSERT OR REPLACE INTO scores
  (parcel_id, score, chronic_complaints, code_violations,
   flood_safe, affordable, buildable, computed_at)
WITH
  complaints AS (
    SELECT parcel_id, COUNT(*) AS n
    FROM requests_311
    WHERE parcel_id IS NOT NULL
      AND reported_date >= datetime('now', '-365 days')
    GROUP BY parcel_id
  ),
  violations AS (
    SELECT parcel_id, COUNT(*) AS n
    FROM code_violations
    WHERE parcel_id IS NOT NULL
    GROUP BY parcel_id
  ),
  parcels AS (
    SELECT parcel_id FROM landbank_inventory
    UNION
    SELECT parcel_id FROM complaints
    UNION
    SELECT parcel_id FROM violations
  )
SELECT
  p.parcel_id,
  (CASE WHEN COALESCE(c.n, 0) >= 3 THEN COALESCE(c.n, 0) ELSE 0 END) * 3.0
    + COALESCE(v.n, 0) * 2.0
    + (CASE WHEN f.flood_zone IN ('X', 'X500') OR f.sfha_tf = 'F' THEN 1 ELSE 0 END)
    + (CASE WHEN l.asking_price IS NOT NULL AND l.asking_price < 2000 THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(l.parcel_width, 0) >= 30 AND COALESCE(l.parcel_length, 0) >= 50 THEN 1 ELSE 0 END)
    AS score,
  CASE WHEN COALESCE(c.n, 0) >= 3 THEN COALESCE(c.n, 0) ELSE 0 END AS chronic_complaints,
  COALESCE(v.n, 0) AS code_violations,
  CASE WHEN f.flood_zone IN ('X', 'X500') OR f.sfha_tf = 'F' THEN 1 ELSE 0 END AS flood_safe,
  CASE WHEN l.asking_price IS NOT NULL AND l.asking_price < 2000 THEN 1 ELSE 0 END AS affordable,
  CASE WHEN COALESCE(l.parcel_width, 0) >= 30 AND COALESCE(l.parcel_length, 0) >= 50 THEN 1 ELSE 0 END AS buildable,
  ? AS computed_at
FROM parcels p
LEFT JOIN complaints c USING (parcel_id)
LEFT JOIN violations v USING (parcel_id)
LEFT JOIN flood_zones f USING (parcel_id)
LEFT JOIN landbank_inventory l USING (parcel_id)
WHERE p.parcel_id IS NOT NULL
"""


def compute_scores() -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (?, ?, ?)",
        ("scoring", started_at, "running"),
    )
    log_id = cur.lastrowid
    try:
        cur.execute("DELETE FROM scores")
        cur.execute(SQL_SCORE, (started_at,))
        row = cur.execute("SELECT COUNT(*), MAX(score), AVG(score) FROM scores").fetchone()
        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, records_inserted=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), row[0], "ok", log_id),
        )
        return {"scored": row[0], "max_score": row[1], "avg_score": row[2]}
    except Exception as e:
        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, status=?, error_message=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), "error", str(e), log_id),
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print(compute_scores())

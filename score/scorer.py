"""Blight Compass scoring engine (Postgres / Neon).

Formula (PRD §4.2) with CE-dedup:
  score = (chronic_complaints * 3)     -- 311 tickets NOT in code_violations, >= 3
        + (code_violations * 2)         -- count of CE cases
        + (1 if flood safe else 0)      -- FEMA Zone X / SFHA = F
        + (1 if asking_price < 2000)
        + (1 if width >= 30 AND length >= 50)

311 tickets that also appear in code_violations are excluded from the 311
count so they are not counted twice.
"""
from __future__ import annotations

from datetime import datetime, timezone

from db.pg import connect

SQL = """
TRUNCATE scores;

WITH
  non_ce_311 AS (
    SELECT r.parcel_norm, COUNT(*) AS n,
           AVG(CASE WHEN r.lat BETWEEN 34.9 AND 35.4 THEN r.lat END) AS avg_lat,
           AVG(CASE WHEN r.lng BETWEEN -90.3 AND -89.5 THEN r.lng END) AS avg_lng
    FROM requests_311 r
    LEFT JOIN code_violations v ON v.case_number = r.incident_number
    WHERE r.parcel_norm IS NOT NULL
      AND r.reported_date >= NOW() - INTERVAL '365 days'
      AND v.id IS NULL
    GROUP BY r.parcel_norm
  ),
  ce_counts AS (
    SELECT parcel_norm, COUNT(*) AS n,
           AVG(CASE WHEN lat BETWEEN 34.9 AND 35.4 THEN lat END) AS avg_lat,
           AVG(CASE WHEN lng BETWEEN -90.3 AND -89.5 THEN lng END) AS avg_lng
    FROM code_violations
    WHERE parcel_norm IS NOT NULL
    GROUP BY parcel_norm
  ),
  lb AS (
    SELECT parcel_norm, parcel_id, asking_price, parcel_length, parcel_width, lat, lng
    FROM landbank_inventory WHERE parcel_norm IS NOT NULL
  ),
  flood AS (
    SELECT parcel_norm, flood_zone, sfha_tf
    FROM flood_zones WHERE parcel_norm IS NOT NULL
  ),
  parcels AS (
    SELECT parcel_norm FROM lb
    UNION SELECT parcel_norm FROM non_ce_311
    UNION SELECT parcel_norm FROM ce_counts
  )
INSERT INTO scores (parcel_id, parcel_norm, score, chronic_complaints,
                    code_violations, flood_safe, affordable, buildable,
                    lat, lng, computed_at)
SELECT
  COALESCE(l.parcel_id, p.parcel_norm),
  p.parcel_norm,
  (CASE WHEN COALESCE(c.n, 0) >= 3 THEN COALESCE(c.n, 0) ELSE 0 END) * 3.0
    + COALESCE(v.n, 0) * 2.0
    + (CASE WHEN f.flood_zone IN ('X', 'X500') OR f.sfha_tf = 'F' THEN 1 ELSE 0 END)
    + (CASE WHEN l.asking_price IS NOT NULL AND l.asking_price < 2000 THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(l.parcel_width, 0) >= 30
             AND COALESCE(l.parcel_length, 0) >= 50 THEN 1 ELSE 0 END),
  CASE WHEN COALESCE(c.n, 0) >= 3 THEN COALESCE(c.n, 0) ELSE 0 END,
  COALESCE(v.n, 0),
  CASE WHEN f.flood_zone IN ('X', 'X500') OR f.sfha_tf = 'F' THEN 1 ELSE 0 END,
  CASE WHEN l.asking_price IS NOT NULL AND l.asking_price < 2000 THEN 1 ELSE 0 END,
  CASE WHEN COALESCE(l.parcel_width, 0) >= 30
        AND COALESCE(l.parcel_length, 0) >= 50 THEN 1 ELSE 0 END,
  COALESCE(l.lat, v.avg_lat, c.avg_lat),
  COALESCE(l.lng, v.avg_lng, c.avg_lng),
  NOW()
FROM parcels p
LEFT JOIN non_ce_311 c USING (parcel_norm)
LEFT JOIN ce_counts v  USING (parcel_norm)
LEFT JOIN flood f      USING (parcel_norm)
LEFT JOIN lb l         USING (parcel_norm)
WHERE p.parcel_norm IS NOT NULL;
"""


def compute_scores() -> dict:
    started_at = datetime.now(timezone.utc)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion_log (source, started_at, status) VALUES (%s, %s, %s) RETURNING id",
            ("scoring", started_at, "running"),
        )
        log_id = cur.fetchone()["id"]
        try:
            cur.execute(SQL)
            cur.execute("SELECT COUNT(*) AS n, MAX(score) AS mx, AVG(score) AS av FROM scores")
            row = cur.fetchone()
            cur.execute(
                "UPDATE ingestion_log SET finished_at=%s, records_inserted=%s, status=%s WHERE id=%s",
                (datetime.now(timezone.utc), row["n"], "ok", log_id),
            )
            return {"scored": row["n"], "max_score": float(row["mx"] or 0), "avg_score": float(row["av"] or 0)}
        except Exception as e:
            cur.execute(
                "UPDATE ingestion_log SET finished_at=%s, status=%s, error_message=%s WHERE id=%s",
                (datetime.now(timezone.utc), "error", str(e)[:500], log_id),
            )
            raise


if __name__ == "__main__":
    print(compute_scores())

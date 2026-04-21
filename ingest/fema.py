"""FEMA NFHL flood zone lookup by point.

Queries FEMA's public NFHL MapServer layer 28 (Flood Hazard Zones).
No auth, no API key. We check every parcel in landbank_inventory that
doesn't yet have a flood zone entry.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx

from db.db import connect

NFHL_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"


def check_point(lat: float, lng: float, client: httpx.Client) -> dict | None:
    params = {
        "geometry": f'{{"x":{lng},"y":{lat},"spatialReference":{{"wkid":4326}}}}',
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "FLD_ZONE,SFHA_TF,STATIC_BFE,ZONE_SUBTY",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        r = client.get(NFHL_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except (httpx.HTTPError, ValueError):
        return None
    feats = data.get("features") or []
    if not feats:
        return {"flood_zone": "X", "sfha_tf": "F", "static_bfe": None}
    a = feats[0].get("attributes") or {}
    return {
        "flood_zone": a.get("FLD_ZONE"),
        "sfha_tf": a.get("SFHA_TF"),
        "static_bfe": a.get("STATIC_BFE"),
    }


def ingest(limit: int | None = None, throttle: float = 0.15) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (?, ?, ?)",
        ("fema_nfhl", started_at, "running"),
    )
    log_id = cur.lastrowid

    sql = """
        SELECT l.parcel_id, l.lat, l.lng
        FROM landbank_inventory l
        LEFT JOIN flood_zones f USING(parcel_id)
        WHERE l.lat IS NOT NULL AND l.lng IS NOT NULL
          AND f.parcel_id IS NULL
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = list(cur.execute(sql))

    checked = 0
    try:
        with httpx.Client(headers={"User-Agent": "memphis-blight-compass/1.0"}) as client:
            for r in rows:
                pid, lat, lng = r
                result = check_point(lat, lng, client)
                if result is None:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO flood_zones
                      (parcel_id, flood_zone, sfha_tf, static_bfe, checked_at)
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        pid,
                        result["flood_zone"],
                        result["sfha_tf"],
                        result["static_bfe"],
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                checked += 1
                if throttle:
                    time.sleep(throttle)
        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, records_inserted=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), checked, "ok", log_id),
        )
    except Exception as e:
        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, status=?, error_message=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), "error", str(e), log_id),
        )
        raise
    finally:
        conn.close()
    return {"checked": checked, "total_candidates": len(rows)}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    print(ingest(limit=args.limit))

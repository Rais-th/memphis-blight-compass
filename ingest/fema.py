"""FEMA NFHL flood zone lookup by point, into Neon Postgres."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx

from db.pg import connect

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
    started_at = datetime.now(timezone.utc)
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (%s, %s, %s) RETURNING id",
        ("fema_nfhl", started_at, "running"),
    )
    log_id = cur.fetchone()["id"]

    sql = """
        SELECT l.parcel_id, l.parcel_norm, l.lat, l.lng
        FROM landbank_inventory l
        LEFT JOIN flood_zones f ON f.parcel_id = l.parcel_id
        WHERE l.lat IS NOT NULL AND l.lng IS NOT NULL
          AND f.parcel_id IS NULL
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    rows = cur.fetchall()

    insert_sql = """
        INSERT INTO flood_zones (parcel_id, parcel_norm, flood_zone, sfha_tf, static_bfe, checked_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (parcel_id) DO UPDATE SET
            parcel_norm=EXCLUDED.parcel_norm,
            flood_zone=EXCLUDED.flood_zone,
            sfha_tf=EXCLUDED.sfha_tf,
            static_bfe=EXCLUDED.static_bfe,
            checked_at=EXCLUDED.checked_at
    """

    checked = 0
    try:
        with httpx.Client(headers={"User-Agent": "memphis-blight-compass/1.0"}) as client:
            for r in rows:
                result = check_point(r["lat"], r["lng"], client)
                if result is None:
                    continue
                cur.execute(
                    insert_sql,
                    (
                        r["parcel_id"],
                        r["parcel_norm"],
                        result["flood_zone"],
                        result["sfha_tf"],
                        result["static_bfe"],
                        datetime.now(timezone.utc),
                    ),
                )
                checked += 1
                if throttle:
                    time.sleep(throttle)
        cur.execute(
            "UPDATE ingestion_log SET finished_at=%s, records_inserted=%s, status=%s WHERE id=%s",
            (datetime.now(timezone.utc), checked, "ok", log_id),
        )
    except Exception as e:
        cur.execute(
            "UPDATE ingestion_log SET finished_at=%s, status=%s, error_message=%s WHERE id=%s",
            (datetime.now(timezone.utc), "error", str(e)[:500], log_id),
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

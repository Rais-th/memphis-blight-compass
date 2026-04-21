"""Ingest Memphis Code Enforcement cases.

Two layers on maps.memphistn.gov/mapping/rest are relevant:
  - PublicWorks/Code_Grounds_Services: overgrown lots, tall grass, junky yards
  - PublicWorks/Code_EnvEnf_Services_Prod: environmental enforcement, dumping

Both expose PARCEL_ID, REPORTED_DATE, REQUEST_TYPE, REQUEST_STATUS etc.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from db.db import connect
from ingest.arcgis import iso_millis, iter_features

LAYERS = {
    "code_grounds": "https://maps.memphistn.gov/mapping/rest/services/PublicWorks/Code_Grounds_Services/FeatureServer/0",
    "code_envenf": "https://maps.memphistn.gov/mapping/rest/services/PublicWorks/Code_EnvEnf_Services_Prod/FeatureServer/0",
}

OUT_FIELDS = ",".join([
    "OBJECTID", "INCIDENT_NUMBER", "PARCEL_ID", "REQUEST_TYPE", "REQUEST_STATUS",
    "REPORTED_DATE", "CLOSE_DATE", "ADDRESS1", "POSTAL_CODE",
    "CATEGORY", "GROUP_NAME",
])


def since_date_literal(days: int) -> str:
    d = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return f"DATE '{d}'"


def ingest_layer(name: str, url: str, days_back: int, max_features: int | None) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    where = f"REPORTED_DATE >= {since_date_literal(days_back)}"

    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (?, ?, ?)",
        (f"code_enforcement:{name}", started_at, "running"),
    )
    log_id = cur.lastrowid

    inserted = updated = 0
    try:
        for feat in iter_features(
            url,
            where=where,
            out_fields=OUT_FIELDS,
            order_by="REPORTED_DATE DESC",
            max_features=max_features,
        ):
            a = feat.get("attributes") or {}
            geom = feat.get("geometry") or {}
            inum = a.get("INCIDENT_NUMBER")
            if not inum:
                continue
            uid = f"{name}:{inum}"
            row = (
                uid,
                name,
                inum,
                a.get("REQUEST_TYPE"),
                a.get("REQUEST_STATUS"),
                iso_millis(a.get("REPORTED_DATE")),
                iso_millis(a.get("CLOSE_DATE")),
                a.get("ADDRESS1"),
                a.get("POSTAL_CODE"),
                (a.get("PARCEL_ID") or "").strip() or None,
                geom.get("y"),
                geom.get("x"),
                json.dumps({k: a.get(k) for k in ("CATEGORY", "GROUP_NAME")}),
            )
            cur.execute(
                """
                INSERT INTO code_violations (
                    id, source_layer, case_number, violation_type, status,
                    open_date, close_date, address, zipcode, parcel_id, lat, lng, raw_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    violation_type=excluded.violation_type,
                    status=excluded.status,
                    open_date=excluded.open_date,
                    close_date=excluded.close_date,
                    address=excluded.address,
                    zipcode=excluded.zipcode,
                    parcel_id=excluded.parcel_id,
                    lat=excluded.lat,
                    lng=excluded.lng,
                    raw_json=excluded.raw_json
                """,
                row,
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1

        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, records_inserted=?, records_updated=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), inserted, updated, "ok", log_id),
        )
    except Exception as e:
        cur.execute(
            "UPDATE ingestion_log SET finished_at=?, status=?, error_message=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), "error", str(e), log_id),
        )
        raise
    finally:
        conn.close()

    return {"layer": name, "inserted": inserted, "updated": updated}


def ingest(days_back: int = 365, max_features: int | None = None) -> list[dict]:
    results = []
    for name, url in LAYERS.items():
        results.append(ingest_layer(name, url, days_back, max_features))
    return results


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--max", type=int, default=None)
    args = p.parse_args()
    print(ingest(days_back=args.days, max_features=args.max))

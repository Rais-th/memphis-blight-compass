"""Ingest Memphis 311 service requests.

Source: City of Memphis Enterprise GIS (EGIS), public FeatureServer layer
`OPM/COM_311_REQUESTS_OPM`. Each record carries a PARCEL_ID field so no
spatial join is required. We pull the last N days (default 365) and upsert
by INCIDENT_NUMBER.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db.db import connect
from ingest.arcgis import iso_millis, iter_features

LAYER_URL = (
    "https://maps.memphistn.gov/mapping/rest/services/"
    "OPM/COM_311_REQUESTS_OPM/FeatureServer/0"
)

OUT_FIELDS = ",".join([
    "OBJECTID", "INCIDENT_NUMBER", "PARCEL_ID", "CATEGORY", "GROUP_NAME",
    "DEPARTMENT", "DIVISION", "REQUEST_TYPE", "REQUEST_STATUS", "REQUEST_PRIORITY",
    "REPORTED_DATE", "Closed_Date", "RESOLVED_DATE", "DAYS_OLD",
    "Location_Address", "ZipCode", "neigh_desc", "cd_desc",
    "SR_CREATION_CHANNEL",
])


def since_date_literal(days: int) -> str:
    d = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return f"DATE '{d}'"


def ingest(days_back: int = 365, max_features: int | None = None) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    where = f"REPORTED_DATE >= {since_date_literal(days_back)}"

    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (?, ?, ?)",
        ("memphis_311", started_at, "running"),
    )
    log_id = cur.lastrowid

    inserted = updated = 0
    try:
        for feat in iter_features(
            LAYER_URL,
            where=where,
            out_fields=OUT_FIELDS,
            order_by="REPORTED_DATE DESC",
            max_features=max_features,
        ):
            a = feat.get("attributes") or {}
            geom = feat.get("geometry") or {}
            lat = geom.get("y")
            lng = geom.get("x")

            row = (
                a.get("INCIDENT_NUMBER"),
                a.get("OBJECTID"),
                (a.get("PARCEL_ID") or "").strip() or None,
                a.get("CATEGORY"),
                a.get("GROUP_NAME"),
                a.get("DEPARTMENT"),
                a.get("DIVISION"),
                a.get("REQUEST_TYPE"),
                a.get("REQUEST_STATUS"),
                a.get("REQUEST_PRIORITY"),
                iso_millis(a.get("REPORTED_DATE")),
                iso_millis(a.get("Closed_Date")),
                iso_millis(a.get("RESOLVED_DATE")),
                a.get("DAYS_OLD"),
                a.get("Location_Address"),
                a.get("ZipCode"),
                a.get("neigh_desc"),
                a.get("cd_desc"),
                a.get("SR_CREATION_CHANNEL"),
                lat,
                lng,
            )
            if not row[0]:
                continue

            cur.execute(
                """
                INSERT INTO requests_311 (
                    incident_number, objectid, parcel_id, category, group_name,
                    department, division, request_type, request_status,
                    request_priority, reported_date, closed_date, resolved_date,
                    days_old, address, zipcode, neighborhood, council_district,
                    creation_channel, lat, lng
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(incident_number) DO UPDATE SET
                    objectid=excluded.objectid,
                    parcel_id=excluded.parcel_id,
                    category=excluded.category,
                    group_name=excluded.group_name,
                    department=excluded.department,
                    division=excluded.division,
                    request_type=excluded.request_type,
                    request_status=excluded.request_status,
                    request_priority=excluded.request_priority,
                    reported_date=excluded.reported_date,
                    closed_date=excluded.closed_date,
                    resolved_date=excluded.resolved_date,
                    days_old=excluded.days_old,
                    address=excluded.address,
                    zipcode=excluded.zipcode,
                    neighborhood=excluded.neighborhood,
                    council_district=excluded.council_district,
                    creation_channel=excluded.creation_channel,
                    lat=excluded.lat,
                    lng=excluded.lng
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

    return {"inserted": inserted, "updated": updated, "days_back": days_back}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--max", type=int, default=None)
    args = p.parse_args()
    result = ingest(days_back=args.days, max_features=args.max)
    print(result)

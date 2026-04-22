"""Ingest Memphis 311 service requests into Neon Postgres.

Source: City of Memphis EGIS FeatureServer layer OPM/COM_311_REQUESTS_OPM.
Each record carries PARCEL_ID so no spatial join. Upsert by INCIDENT_NUMBER.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from db.pg import connect
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

BATCH = 500


def since_date_literal(days: int) -> str:
    d = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return f"DATE '{d}'"


def normalize_parcel(pid: str | None) -> str | None:
    if not pid:
        return None
    s = re.sub(r"\s+", "", pid.strip().upper())
    if len(s) < 6:
        return None
    block, mapp, rest = s[:3], s[3:6], s[6:]
    suffix = ""
    if rest and rest[-1].isalpha():
        suffix = rest[-1]
        rest = rest[:-1]
    if not rest or not rest.isdigit():
        return None
    return f"{block}{mapp}{int(rest)}{suffix}"


def ingest(days_back: int = 365, max_features: int | None = None) -> dict:
    started_at = datetime.now(timezone.utc)
    where = f"REPORTED_DATE >= {since_date_literal(days_back)}"

    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (%s, %s, %s) RETURNING id",
        ("memphis_311", started_at, "running"),
    )
    log_id = cur.fetchone()["id"]

    insert_sql = """
        INSERT INTO requests_311 (
            incident_number, objectid, parcel_id, parcel_norm, category,
            group_name, department, division, request_type, request_status,
            request_priority, reported_date, closed_date, resolved_date,
            days_old, address, zipcode, neighborhood, council_district,
            creation_channel, lat, lng
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (incident_number) DO UPDATE SET
            objectid=EXCLUDED.objectid,
            parcel_id=EXCLUDED.parcel_id,
            parcel_norm=EXCLUDED.parcel_norm,
            category=EXCLUDED.category,
            group_name=EXCLUDED.group_name,
            department=EXCLUDED.department,
            division=EXCLUDED.division,
            request_type=EXCLUDED.request_type,
            request_status=EXCLUDED.request_status,
            request_priority=EXCLUDED.request_priority,
            reported_date=EXCLUDED.reported_date,
            closed_date=EXCLUDED.closed_date,
            resolved_date=EXCLUDED.resolved_date,
            days_old=EXCLUDED.days_old,
            address=EXCLUDED.address,
            zipcode=EXCLUDED.zipcode,
            neighborhood=EXCLUDED.neighborhood,
            council_district=EXCLUDED.council_district,
            creation_channel=EXCLUDED.creation_channel,
            lat=EXCLUDED.lat,
            lng=EXCLUDED.lng
    """

    total = 0
    batch: list[tuple] = []
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
            inum = a.get("INCIDENT_NUMBER")
            if not inum:
                continue
            parcel_id = (a.get("PARCEL_ID") or "").strip() or None
            row = (
                inum,
                a.get("OBJECTID"),
                parcel_id,
                normalize_parcel(parcel_id),
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
                geom.get("y"),
                geom.get("x"),
            )
            batch.append(row)
            if len(batch) >= BATCH:
                cur.executemany(insert_sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            total += len(batch)

        cur.execute(
            "UPDATE ingestion_log SET finished_at=%s, records_inserted=%s, status=%s WHERE id=%s",
            (datetime.now(timezone.utc), total, "ok", log_id),
        )
    except Exception as e:
        cur.execute(
            "UPDATE ingestion_log SET finished_at=%s, status=%s, error_message=%s WHERE id=%s",
            (datetime.now(timezone.utc), "error", str(e)[:500], log_id),
        )
        raise
    finally:
        conn.close()

    return {"processed": total, "days_back": days_back}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--max", type=int, default=None)
    args = p.parse_args()
    print(ingest(days_back=args.days, max_features=args.max))

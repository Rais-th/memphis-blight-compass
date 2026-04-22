"""Ingest Memphis Code Enforcement cases into Neon Postgres.

Layers: PublicWorks/Code_Grounds_Services and PublicWorks/Code_EnvEnf_Services_Prod.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

from db.pg import connect
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


def ingest_layer(name: str, url: str, days_back: int, max_features: int | None) -> dict:
    started_at = datetime.now(timezone.utc)
    where = f"REPORTED_DATE >= {since_date_literal(days_back)}"

    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (%s, %s, %s) RETURNING id",
        (f"code_enforcement:{name}", started_at, "running"),
    )
    log_id = cur.fetchone()["id"]

    insert_sql = """
        INSERT INTO code_violations (
            id, source_layer, case_number, parcel_id, parcel_norm,
            violation_type, status, open_date, close_date, address,
            zipcode, lat, lng, raw_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            violation_type=EXCLUDED.violation_type,
            status=EXCLUDED.status,
            open_date=EXCLUDED.open_date,
            close_date=EXCLUDED.close_date,
            address=EXCLUDED.address,
            zipcode=EXCLUDED.zipcode,
            parcel_id=EXCLUDED.parcel_id,
            parcel_norm=EXCLUDED.parcel_norm,
            lat=EXCLUDED.lat,
            lng=EXCLUDED.lng,
            raw_json=EXCLUDED.raw_json
    """

    total = 0
    batch: list[tuple] = []
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
            parcel_id = (a.get("PARCEL_ID") or "").strip() or None
            row = (
                f"{name}:{inum}",
                name,
                inum,
                parcel_id,
                normalize_parcel(parcel_id),
                a.get("REQUEST_TYPE"),
                a.get("REQUEST_STATUS"),
                iso_millis(a.get("REPORTED_DATE")),
                iso_millis(a.get("CLOSE_DATE")),
                a.get("ADDRESS1"),
                a.get("POSTAL_CODE"),
                geom.get("y"),
                geom.get("x"),
                json.dumps({k: a.get(k) for k in ("CATEGORY", "GROUP_NAME")}),
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

    return {"layer": name, "processed": total}


def ingest(days_back: int = 365, max_features: int | None = None) -> list[dict]:
    return [ingest_layer(n, u, days_back, max_features) for n, u in LAYERS.items()]


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--max", type=int, default=None)
    args = p.parse_args()
    print(ingest(days_back=args.days, max_features=args.max))

"""Ingest Shelby County Land Bank FOR SALE inventory into Neon Postgres."""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from db.pg import connect

BASE_URL = "https://public-sctn.epropertyplus.com/landmgmtpub/remote/public/property/getPublishedProperties"

MEMPHIS_ZIPS = [
    "38103", "38104", "38105", "38106", "38107", "38108", "38109",
    "38111", "38112", "38114", "38115", "38116", "38117", "38118",
    "38119", "38120", "38122", "38125", "38126", "38127", "38128",
    "38131", "38132", "38133", "38134", "38135", "38138", "38139",
    "38141",
]


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


def fetch_zip(zip_code: str, client: httpx.Client) -> list[dict[str, Any]]:
    criteria = json.dumps({"criterias": [
        {"name": "postalCode", "value": zip_code},
        {"name": "currentStatus", "value": "FOR SALE"},
    ]})
    sort = json.dumps([{"property": "askingPrice", "direction": "ASC"}])
    params = {
        "limit": 500,
        "sEcho": 1,
        "iDisplayStart": 0,
        "iDisplayLength": 500,
        "page": 1,
        "json": criteria,
        "sort": sort,
        "favoriteProperties": "",
    }
    r = client.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    try:
        d = r.json()
    except json.JSONDecodeError:
        return []
    return d.get("aaData") or d.get("rows") or d.get("data") or []


def ingest() -> dict:
    started_at = datetime.now(timezone.utc)
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (%s, %s, %s) RETURNING id",
        ("landbank", started_at, "running"),
    )
    log_id = cur.fetchone()["id"]

    insert_sql = """
        INSERT INTO landbank_inventory (
            parcel_id, parcel_norm, address, zipcode, current_status, available,
            asking_price, acres, parcel_length, parcel_width,
            improvement_type, lat, lng, last_seen_at, raw_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (parcel_id) DO UPDATE SET
            parcel_norm=EXCLUDED.parcel_norm,
            address=EXCLUDED.address,
            zipcode=EXCLUDED.zipcode,
            current_status=EXCLUDED.current_status,
            available=EXCLUDED.available,
            asking_price=EXCLUDED.asking_price,
            acres=EXCLUDED.acres,
            parcel_length=EXCLUDED.parcel_length,
            parcel_width=EXCLUDED.parcel_width,
            improvement_type=EXCLUDED.improvement_type,
            lat=EXCLUDED.lat,
            lng=EXCLUDED.lng,
            last_seen_at=EXCLUDED.last_seen_at,
            raw_json=EXCLUDED.raw_json
    """

    total = 0
    try:
        with httpx.Client(headers={"User-Agent": "memphis-blight-compass/1.0"}) as client:
            for zc in MEMPHIS_ZIPS:
                try:
                    records = fetch_zip(zc, client)
                except httpx.HTTPError:
                    continue
                for r in records:
                    if r.get("currentStatus") != "FOR SALE":
                        continue
                    pn = r.get("parcelNumber")
                    if not pn:
                        continue
                    row = (
                        pn,
                        normalize_parcel(pn),
                        r.get("propertyAddress1"),
                        r.get("postalCode") or zc,
                        r.get("currentStatus"),
                        r.get("available"),
                        r.get("askingPrice"),
                        float(r.get("s_custom_0032") or 0) or None,
                        r.get("parcelLength"),
                        r.get("parcelWidth"),
                        r.get("s_custom_0049"),
                        r.get("latitude"),
                        r.get("longitude"),
                        datetime.now(timezone.utc),
                        json.dumps(r)[:20000],
                    )
                    cur.execute(insert_sql, row)
                    total += 1
                time.sleep(0.3)

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

    return {"processed": total}


if __name__ == "__main__":
    print(ingest())

"""Ingest Shelby County Land Bank FOR SALE inventory.

Public endpoint (ePropertyPlus backend), no auth. We loop Memphis zips and
upsert by parcelNumber.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from db.db import connect

BASE_URL = "https://public-sctn.epropertyplus.com/landmgmtpub/remote/public/property/getPublishedProperties"

MEMPHIS_ZIPS = [
    "38103", "38104", "38105", "38106", "38107", "38108", "38109",
    "38111", "38112", "38114", "38115", "38116", "38117", "38118",
    "38119", "38120", "38122", "38125", "38126", "38127", "38128",
    "38131", "38132", "38133", "38134", "38135", "38138", "38139",
    "38141",
]

SKIP_IMPROVEMENT = {"LAND LOCKED", "INSEPARABLE PCL", "VCNT STRIP", "DITCH"}


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
    started_at = datetime.now(timezone.utc).isoformat()
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingestion_log (source, started_at, status) VALUES (?, ?, ?)",
        ("landbank", started_at, "running"),
    )
    log_id = cur.lastrowid

    inserted = updated = 0
    total_seen = 0

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
                    imp = r.get("s_custom_0049")
                    pn = r.get("parcelNumber")
                    if not pn:
                        continue
                    total_seen += 1
                    row = (
                        pn,
                        r.get("propertyAddress1"),
                        r.get("postalCode") or zc,
                        r.get("currentStatus"),
                        r.get("available"),
                        r.get("askingPrice"),
                        float(r.get("s_custom_0032") or 0) or None,
                        r.get("parcelLength"),
                        r.get("parcelWidth"),
                        imp,
                        r.get("latitude"),
                        r.get("longitude"),
                        datetime.now(timezone.utc).isoformat(),
                        json.dumps(r)[:20000],
                    )
                    cur.execute(
                        """
                        INSERT INTO landbank_inventory (
                            parcel_id, address, zipcode, current_status, available,
                            asking_price, acres, parcel_length, parcel_width,
                            improvement_type, lat, lng, last_seen_at, raw_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(parcel_id) DO UPDATE SET
                            address=excluded.address,
                            zipcode=excluded.zipcode,
                            current_status=excluded.current_status,
                            available=excluded.available,
                            asking_price=excluded.asking_price,
                            acres=excluded.acres,
                            parcel_length=excluded.parcel_length,
                            parcel_width=excluded.parcel_width,
                            improvement_type=excluded.improvement_type,
                            lat=excluded.lat,
                            lng=excluded.lng,
                            last_seen_at=excluded.last_seen_at,
                            raw_json=excluded.raw_json
                        """,
                        row,
                    )
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        updated += 1
                time.sleep(0.3)

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

    return {"inserted": inserted, "updated": updated, "total_seen": total_seen}


if __name__ == "__main__":
    print(ingest())

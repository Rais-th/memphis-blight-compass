"""Memphis Blight Compass API (Postgres / Neon)."""
from __future__ import annotations

import csv
import io
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, EmailStr

from db.pg import connect

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
PUBLIC = ROOT / "public"
STATIC_DIR = PUBLIC if PUBLIC.exists() else WEB

app = FastAPI(title="Memphis Blight Compass", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/api/stats")
def stats():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM scores")
        parcels = cur.fetchone()["n"]
        cur.execute(
            "SELECT COUNT(*) AS n FROM landbank_inventory "
            "WHERE current_status = 'FOR SALE'"
        )
        landbank = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM requests_311")
        c311 = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM code_violations")
        cv = cur.fetchone()["n"]
        cur.execute(
            "SELECT source, finished_at, records_inserted, records_updated, status "
            "FROM ingestion_log ORDER BY id DESC LIMIT 10"
        )
        last_run = cur.fetchall()
        cur.execute(
            "SELECT zipcode, COUNT(*) AS n FROM requests_311 "
            "WHERE zipcode IS NOT NULL AND TRIM(zipcode) <> '' "
            "GROUP BY zipcode ORDER BY n DESC LIMIT 1"
        )
        top_zip = cur.fetchone()
        cur.execute(
            "SELECT AVG(EXTRACT(EPOCH FROM (closed_date - reported_date)) / 86400.0) AS d "
            "FROM requests_311 WHERE closed_date IS NOT NULL AND reported_date IS NOT NULL"
        )
        avg_days = cur.fetchone()["d"]
        return {
            "parcels_scored": parcels,
            "landbank_for_sale": landbank,
            "requests_311": c311,
            "code_violations": cv,
            "top_zip_by_complaints": top_zip,
            "avg_response_days_citywide": round(float(avg_days), 2) if avg_days else None,
            "recent_ingestion": [dict(r) for r in last_run],
            "as_of": datetime.now(timezone.utc).isoformat(),
        }


@app.get("/api/parcels/top")
def parcels_top(limit: int = 50, min_score: float = 1.0, acquirable: bool = True):
    if limit > 500:
        limit = 500
    where = ["s.score >= %s"]
    params: list[Any] = [min_score]
    if acquirable:
        where.append(
            "l.parcel_id IS NOT NULL AND l.current_status = 'FOR SALE' "
            "AND l.improvement_type NOT IN ('LAND LOCKED','INSEPARABLE PCL','VCNT STRIP','DITCH')"
        )
    sql = f"""
        SELECT
          s.parcel_id, s.score, s.chronic_complaints, s.code_violations,
          s.flood_safe, s.affordable, s.buildable,
          l.address, l.zipcode, l.asking_price, l.acres, l.parcel_length,
          l.parcel_width, l.improvement_type,
          COALESCE(l.lat, s.lat) AS lat,
          COALESCE(l.lng, s.lng) AS lng,
          f.flood_zone
        FROM scores s
        LEFT JOIN landbank_inventory l ON l.parcel_norm = s.parcel_norm
        LEFT JOIN flood_zones f        ON f.parcel_norm = s.parcel_norm
        WHERE {' AND '.join(where)}
        ORDER BY s.score DESC, l.asking_price ASC NULLS LAST
        LIMIT %s
    """
    params.append(limit)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        features = []
        for d in cur.fetchall():
            lat = d.pop("lat")
            lng = d.pop("lng")
            feat = {"type": "Feature", "properties": dict(d)}
            if lat is not None and lng is not None:
                feat["geometry"] = {"type": "Point", "coordinates": [float(lng), float(lat)]}
            else:
                feat["geometry"] = None
            features.append(feat)
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/parcels/{parcel_id}")
def parcel_detail(parcel_id: str):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.*, l.address, l.zipcode, l.asking_price, l.acres,
                   l.parcel_length, l.parcel_width, l.improvement_type,
                   l.current_status, l.available,
                   COALESCE(l.lat, s.lat) AS lat,
                   COALESCE(l.lng, s.lng) AS lng,
                   f.flood_zone, f.sfha_tf
            FROM scores s
            LEFT JOIN landbank_inventory l ON l.parcel_norm = s.parcel_norm
            LEFT JOIN flood_zones f        ON f.parcel_norm = s.parcel_norm
            WHERE s.parcel_id = %s OR s.parcel_norm = %s
            LIMIT 1
            """,
            (parcel_id, parcel_id),
        )
        base = cur.fetchone()
        if not base:
            raise HTTPException(404, f"Parcel {parcel_id} not found")
        norm = base["parcel_norm"]
        cur.execute(
            """
            SELECT incident_number, category, group_name, department,
                   request_status, reported_date, closed_date, address, zipcode
            FROM requests_311
            WHERE parcel_norm = %s
            ORDER BY reported_date DESC NULLS LAST
            LIMIT 200
            """,
            (norm,),
        )
        complaints = cur.fetchall()
        cur.execute(
            """
            SELECT id, source_layer, case_number, violation_type, status,
                   open_date, close_date, address, zipcode
            FROM code_violations
            WHERE parcel_norm = %s
            ORDER BY open_date DESC NULLS LAST
            LIMIT 200
            """,
            (norm,),
        )
        violations = cur.fetchall()
        return {
            "parcel": dict(base),
            "complaints": [dict(r) for r in complaints],
            "violations": [dict(r) for r in violations],
        }


@app.get("/api/equity")
def equity():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              zipcode,
              COUNT(*) AS total_requests,
              AVG(EXTRACT(EPOCH FROM (closed_date - reported_date)) / 86400.0) AS avg_days_to_close,
              SUM(CASE WHEN closed_date IS NULL THEN 1 ELSE 0 END) AS still_open
            FROM requests_311
            WHERE zipcode IS NOT NULL AND TRIM(zipcode) <> ''
            GROUP BY zipcode
            HAVING COUNT(*) >= 10
            ORDER BY avg_days_to_close DESC NULLS LAST
            """
        )
        zips = []
        for r in cur.fetchall():
            d = dict(r)
            if d.get("avg_days_to_close") is not None:
                d["avg_days_to_close"] = round(float(d["avg_days_to_close"]), 2)
            zips.append(d)
    return {"zips": zips}


class Subscribe(BaseModel):
    email: EmailStr


@app.post("/api/subscribe")
def subscribe(body: Subscribe):
    token = secrets.token_urlsafe(32)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscribers (email, subscribed_at, unsubscribe_token)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET subscribed_at = EXCLUDED.subscribed_at
            """,
            (body.email, datetime.now(timezone.utc), token),
        )
    return {"ok": True}


@app.get("/api/csv/weekly")
def csv_weekly(limit: int = 100):
    lim = min(limit, 1000)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.parcel_id, s.score, s.chronic_complaints, s.code_violations,
                   l.address, l.zipcode, l.asking_price, l.acres, l.parcel_length,
                   l.parcel_width, l.improvement_type,
                   COALESCE(l.lat, s.lat) AS lat,
                   COALESCE(l.lng, s.lng) AS lng,
                   f.flood_zone
            FROM scores s
            JOIN landbank_inventory l ON l.parcel_norm = s.parcel_norm
            LEFT JOIN flood_zones f   ON f.parcel_norm = s.parcel_norm
            WHERE l.current_status = 'FOR SALE'
              AND l.improvement_type NOT IN ('LAND LOCKED','INSEPARABLE PCL','VCNT STRIP','DITCH')
            ORDER BY s.score DESC, l.asking_price ASC NULLS LAST
            LIMIT %s
            """,
            (lim,),
        )
        rows = cur.fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    cols = [
        "parcel_id", "score", "chronic_complaints", "code_violations",
        "address", "zipcode", "asking_price", "acres",
        "parcel_length", "parcel_width", "improvement_type",
        "lat", "lng", "flood_zone",
    ]
    writer.writerow(cols)
    for r in rows:
        writer.writerow([r.get(c) for c in cols])
    buf.seek(0)

    fname = f"blight-compass-top-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/map", include_in_schema=False)
def page_map():
    return FileResponse(STATIC_DIR / "map.html")


@app.get("/top", include_in_schema=False)
def page_top():
    return FileResponse(STATIC_DIR / "top.html")


@app.get("/equity", include_in_schema=False)
def page_equity():
    return FileResponse(STATIC_DIR / "equity.html")


@app.get("/about", include_in_schema=False)
def page_about():
    return FileResponse(STATIC_DIR / "about.html")


@app.get("/subscribe", include_in_schema=False)
def page_subscribe():
    return FileResponse(STATIC_DIR / "subscribe.html")


@app.get("/parcel/{parcel_id}", include_in_schema=False)
def page_parcel(parcel_id: str):
    return FileResponse(STATIC_DIR / "parcel.html")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

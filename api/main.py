"""Memphis Blight Compass API."""
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

from db.db import connect

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"

app = FastAPI(title="Memphis Blight Compass", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def row_to_dict(r) -> dict[str, Any]:
    return {k: r[k] for k in r.keys()}


@app.get("/api/stats")
def stats():
    conn = connect()
    try:
        cur = conn.cursor()
        parcels = cur.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
        landbank = cur.execute(
            "SELECT COUNT(*) FROM landbank_inventory WHERE current_status='FOR SALE'"
        ).fetchone()[0]
        c311 = cur.execute("SELECT COUNT(*) FROM requests_311").fetchone()[0]
        cv = cur.execute("SELECT COUNT(*) FROM code_violations").fetchone()[0]
        last_run = cur.execute(
            "SELECT source, finished_at, records_inserted, records_updated, status "
            "FROM ingestion_log ORDER BY id DESC LIMIT 10"
        ).fetchall()
        top_zip = cur.execute(
            "SELECT zipcode, COUNT(*) AS n FROM requests_311 "
            "WHERE zipcode IS NOT NULL AND TRIM(zipcode) != '' "
            "GROUP BY zipcode ORDER BY n DESC LIMIT 1"
        ).fetchone()
        median_response = cur.execute(
            """
            SELECT AVG(julianday(closed_date) - julianday(reported_date))
            FROM requests_311
            WHERE closed_date IS NOT NULL AND reported_date IS NOT NULL
            """
        ).fetchone()[0]
        return {
            "parcels_scored": parcels,
            "landbank_for_sale": landbank,
            "requests_311": c311,
            "code_violations": cv,
            "top_zip_by_complaints": row_to_dict(top_zip) if top_zip else None,
            "avg_response_days_citywide": round(median_response, 2) if median_response else None,
            "recent_ingestion": [row_to_dict(r) for r in last_run],
            "as_of": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


@app.get("/api/parcels/top")
def parcels_top(limit: int = 50, min_score: float = 1.0, acquirable: bool = True):
    if limit > 500:
        limit = 500
    conn = connect()
    try:
        cur = conn.cursor()
        where = ["s.score >= ?"]
        params: list[Any] = [min_score]
        if acquirable:
            where.append(
                "l.parcel_id IS NOT NULL AND l.current_status='FOR SALE' "
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
            LEFT JOIN flood_zones f ON f.parcel_norm = s.parcel_norm
            WHERE {' AND '.join(where)}
            ORDER BY s.score DESC, l.asking_price ASC
            LIMIT ?
        """
        params.append(limit)
        features = []
        for r in cur.execute(sql, params):
            d = row_to_dict(r)
            lat, lng = d.pop("lat"), d.pop("lng")
            feat = {"type": "Feature", "properties": d}
            if lat is not None and lng is not None:
                feat["geometry"] = {"type": "Point", "coordinates": [lng, lat]}
            else:
                feat["geometry"] = None
            features.append(feat)
        return {"type": "FeatureCollection", "features": features}
    finally:
        conn.close()


@app.get("/api/parcels/{parcel_id}")
def parcel_detail(parcel_id: str):
    conn = connect()
    try:
        cur = conn.cursor()
        base = cur.execute(
            """
            SELECT s.*, l.address, l.zipcode, l.asking_price, l.acres,
                   l.parcel_length, l.parcel_width, l.improvement_type,
                   l.current_status, l.available, l.lat, l.lng,
                   f.flood_zone, f.sfha_tf
            FROM scores s
            LEFT JOIN landbank_inventory l ON l.parcel_norm = s.parcel_norm
            LEFT JOIN flood_zones f ON f.parcel_norm = s.parcel_norm
            WHERE s.parcel_id = ? OR s.parcel_norm = ?
            LIMIT 1
            """,
            (parcel_id, parcel_id),
        ).fetchone()
        if not base:
            raise HTTPException(404, f"Parcel {parcel_id} not found")
        norm = base["parcel_norm"]
        complaints = cur.execute(
            """
            SELECT incident_number, category, group_name, department,
                   request_status, reported_date, closed_date, address, zipcode
            FROM requests_311
            WHERE parcel_norm = ?
            ORDER BY reported_date DESC
            LIMIT 200
            """,
            (norm,),
        ).fetchall()
        violations = cur.execute(
            """
            SELECT id, source_layer, case_number, violation_type, status,
                   open_date, close_date, address, zipcode
            FROM code_violations
            WHERE parcel_norm = ?
            ORDER BY open_date DESC
            LIMIT 200
            """,
            (norm,),
        ).fetchall()
        return {
            "parcel": row_to_dict(base),
            "complaints": [row_to_dict(r) for r in complaints],
            "violations": [row_to_dict(r) for r in violations],
        }
    finally:
        conn.close()


@app.get("/api/equity")
def equity():
    conn = connect()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT
              zipcode,
              COUNT(*) AS total_requests,
              AVG(julianday(closed_date) - julianday(reported_date)) AS avg_days_to_close,
              SUM(CASE WHEN closed_date IS NULL THEN 1 ELSE 0 END) AS still_open
            FROM requests_311
            WHERE zipcode IS NOT NULL AND TRIM(zipcode) != ''
            GROUP BY zipcode
            HAVING total_requests >= 10
            ORDER BY avg_days_to_close DESC
            """
        ).fetchall()
        return {"zips": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


class Subscribe(BaseModel):
    email: EmailStr


@app.post("/api/subscribe")
def subscribe(body: Subscribe):
    token = secrets.token_urlsafe(32)
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO subscribers (email, subscribed_at, unsubscribe_token)
            VALUES (?, ?, ?)
            ON CONFLICT(email) DO UPDATE SET subscribed_at=excluded.subscribed_at
            """,
            (body.email, datetime.now(timezone.utc).isoformat(), token),
        )
    finally:
        conn.close()
    return {"ok": True}


@app.get("/api/csv/weekly")
def csv_weekly(limit: int = 100):
    conn = connect()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT s.parcel_id, s.score, s.chronic_complaints, s.code_violations,
                   l.address, l.zipcode, l.asking_price, l.acres, l.parcel_length,
                   l.parcel_width, l.improvement_type, l.lat, l.lng, f.flood_zone
            FROM scores s
            JOIN landbank_inventory l ON l.parcel_norm = s.parcel_norm
            LEFT JOIN flood_zones f ON f.parcel_norm = s.parcel_norm
            WHERE l.current_status='FOR SALE'
              AND l.improvement_type NOT IN ('LAND LOCKED','INSEPARABLE PCL','VCNT STRIP','DITCH')
            ORDER BY s.score DESC, l.asking_price ASC
            LIMIT ?
            """,
            (min(limit, 1000),),
        ).fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "parcel_id", "score", "chronic_complaints", "code_violations",
        "address", "zipcode", "asking_price", "acres",
        "parcel_length", "parcel_width", "improvement_type",
        "lat", "lng", "flood_zone",
    ])
    for r in rows:
        writer.writerow([r[k] for k in r.keys()])
    buf.seek(0)

    fname = f"blight-compass-top-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


app.mount("/static", StaticFiles(directory=WEB), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(WEB / "index.html")


@app.get("/map", include_in_schema=False)
def page_map():
    return FileResponse(WEB / "map.html")


@app.get("/top", include_in_schema=False)
def page_top():
    return FileResponse(WEB / "top.html")


@app.get("/equity", include_in_schema=False)
def page_equity():
    return FileResponse(WEB / "equity.html")


@app.get("/about", include_in_schema=False)
def page_about():
    return FileResponse(WEB / "about.html")


@app.get("/subscribe", include_in_schema=False)
def page_subscribe():
    return FileResponse(WEB / "subscribe.html")


@app.get("/parcel/{parcel_id}", include_in_schema=False)
def page_parcel(parcel_id: str):
    return FileResponse(WEB / "parcel.html")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

"""ArcGIS FeatureServer / MapServer client.

Memphis exposes 311 + Code Enforcement via public ArcGIS Feature / Map servers
under https://maps.memphistn.gov/mapping/rest/services. Each layer supports the
standard Esri query endpoint.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

USER_AGENT = "memphis-blight-compass/1.0 (+https://github.com/Rais-th/memphis-blight-compass)"


def iso_millis(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError, TypeError):
        return None


def count_records(layer_url: str, where: str = "1=1", client: httpx.Client | None = None) -> int:
    c = client or httpx.Client(timeout=60, headers={"User-Agent": USER_AGENT})
    try:
        r = c.get(f"{layer_url}/query", params={"where": where, "returnCountOnly": "true", "f": "json"})
        r.raise_for_status()
        data = r.json()
        return int(data.get("count") or 0)
    finally:
        if client is None:
            c.close()


def iter_features(
    layer_url: str,
    where: str = "1=1",
    out_fields: str = "*",
    order_by: str = "OBJECTID ASC",
    page_size: int = 2000,
    return_geometry: bool = True,
    out_sr: int = 4326,
    max_features: int | None = None,
    throttle: float = 0.25,
) -> Iterator[dict[str, Any]]:
    """Yield ArcGIS features page by page using resultOffset.

    Works against any public FeatureServer/MapServer layer query endpoint.
    """
    offset = 0
    yielded = 0
    with httpx.Client(timeout=120, headers={"User-Agent": USER_AGENT}) as client:
        while True:
            if max_features is not None and yielded >= max_features:
                return
            params = {
                "where": where,
                "outFields": out_fields,
                "orderByFields": order_by,
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "returnGeometry": "true" if return_geometry else "false",
                "outSR": out_sr,
                "f": "json",
            }
            data: dict[str, Any] = {}
            for attempt in range(4):
                try:
                    r = client.get(f"{layer_url}/query", params=params)
                    r.raise_for_status()
                    data = r.json()
                    if "error" in data:
                        raise RuntimeError(f"ArcGIS error: {data['error']}")
                    break
                except (httpx.HTTPError, json.JSONDecodeError):
                    if attempt == 3:
                        raise
                    time.sleep(2 ** attempt)
            features = data.get("features") or []
            if not features:
                return
            for feat in features:
                yield feat
                yielded += 1
                if max_features is not None and yielded >= max_features:
                    return
            if not data.get("exceededTransferLimit") and len(features) < page_size:
                return
            offset += len(features)
            if throttle:
                time.sleep(throttle)

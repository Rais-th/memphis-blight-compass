# Memphis Blight Compass

Public tool that joins Memphis 311 complaints, Code Enforcement violations, Shelby County Land Bank inventory, and FEMA flood zones to help triage blight and surface acquisition-ready parcels.

Built by [Rais Thelemuka](https://github.com/Rais-th) / Popuzar LLC.

## Problem

The Bloomberg Harvard City Leadership Initiative publicly documented in Oct 2023 that "the 311 integration has not yet been solved" in Memphis. 311 complaints don't route effectively between Code Enforcement, Public Works, and Solid Waste. Shelby County Land Bank inventory isn't cross-referenced with 311 data either. Parcels sell for $400 to $2000 in zones where neighbors have complained for years.

This tool closes that gap.

## Features

- **Live public map** of parcels colored by blight score
- **Ranked top-50 acquisition-ready parcels** (score + price + size + flood)
- **Equity dashboard**: median 311 response time by zip code
- **Weekly CSV export** for the Memphis Blight Strike Team
- **Per-parcel detail** with complaint history, violations, flood zone, "How to apply" link

## Data sources

- Memphis Open Data Hub (Socrata): 311 requests, Code Enforcement violations (CC BY 4.0)
- Shelby County Land Bank (ePropertyPlus public API)
- FEMA National Flood Hazard Layer
- Google Maps Geocoding API (for addresses missing coordinates)

## Local dev

```bash
cp .env.example .env
# fill in SOCRATA_APP_TOKEN (free at data.memphistn.gov), GOOGLE_MAPS_API_KEY, MAPBOX_TOKEN

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 -m db.init
python3 -m scripts.run_ingest
python3 -m uvicorn api.main:app --reload --port 8000
```

Open http://localhost:8000

## Deploy

Fly.io. See `fly.toml` and `.github/workflows/deploy.yml`.

## Legal

All data is public record from documented sources. Not legal or real estate advice. MIT license.

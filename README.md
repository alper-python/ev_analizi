# DomiFrame
**Property Intelligence**

Current public module: **Nearby Access Score — Belgium**

Nearby Access Score analyzes access to selected everyday amenities and services around an address in Belgium. It reports separate scores for:

- Market
- School
- Health
- Transit
- Park
- Sport

Public application: [https://domiframe.com](https://domiframe.com)

The interface is available in Turkish, Dutch, and English.

## Product scope

The application measures nearby access to selected everyday amenities and services. It is not a complete liveability score, property valuation, safety or crime score, socioeconomic assessment, or route-planning service.

A low score does not mean that a location is a bad place to live. Many important aspects of a home and neighborhood are outside the model.

## Scoring model

Each category has its own score. The overall score is a secondary, experimental summary using these weights:

`Overall = 25% Market + 25% School + 20% Health + 15% Transit + 10% Park + 5% Sport`

| Category | Weight |
| --- | ---: |
| Market | 25% |
| School | 25% |
| Health | 20% |
| Transit | 15% |
| Park | 10% |
| Sport | 5% |

The category scoring radii are fixed:

| Category component | Scoring radius |
| --- | ---: |
| Market | 2.5 km |
| School | 2.5 km |
| Health — local access | 2.5 km |
| Health — hospital access | 20 km |
| Transit — local access | 1 km |
| Transit — rail access | 7.5 km |
| Park | 2.5 km |
| Sport | 3 km |

The map/display radius selected by the user controls which nearby places are displayed. It does not change these fixed scoring radii.

## Distance and time estimates

Displayed walking and driving estimates are derived from straight-line distance using approximation factors. They are not produced by a routing engine and should not be treated as road-network or journey-time calculations.

## Data sources

- [OpenStreetMap](https://www.openstreetmap.org/copyright) provides POI and geographic source data.
- The [Belgian Mobility Open Data Portal](https://data.belgianmobility.io/) provides public transport data for De Lijn, STIB/MIVB, TEC, and SNCB/NMBS.
- [Geoapify](https://www.geoapify.com/) supports address autocomplete and geocoding.
- [Stadia Maps](https://stadiamaps.com/), [OpenMapTiles](https://openmaptiles.org/), and OpenStreetMap provide the map background and attribution chain.

Transit GTFS source provenance and dataset update timestamps are stored in Parquet metadata and surfaced in the application's **Data Sources** dialog. Consult that dialog for current source attribution and update information; update dates are intentionally not hardcoded here.

## Privacy

- No user account is required.
- Analysis results are not intentionally persisted by the application code.
- Recent address history is not persisted between page sessions.
- Language and theme preferences may be stored in browser `localStorage` for up to 30 days.
- Address autocomplete uses Geoapify.
- Map tiles are loaded from Stadia Maps.
- Hosting is provided through Render.

See the in-app **Privacy** notice for details, including information about technical requests and third-party services.

## Architecture

- **Frontend:** single-page public interface in `belgium-location/static/index.html`, with a Leaflet map.
- **Backend:** Flask, served by Gunicorn in production.
- **Analysis:** DuckDB, Pandas, PyArrow, Shapely, and supporting geospatial helpers.
- **Deployment:** Render in the Frankfurt region, with a persistent disk for runtime caches. Production currently uses one Gunicorn worker with two threads.

## Runtime caches

The production runtime requires exactly these seven Parquet files under `belgium-location/cache/`:

```text
cache/be_poi.parquet
cache/be_poi_poly.parquet
cache/be_transit_service_stops.parquet
cache/be_transit_service_summary.parquet
cache/be_rail_service.parquet
cache/be_park_destinations.parquet
cache/be_sport_destinations.parquet
```

Runtime readiness is fail-closed. If required data is missing, corrupt, or schema-invalid, analysis returns a service-unavailable response instead of silently producing partial scores. Build-only intermediate files are not runtime requirements.

## Local development

Use Python 3.12.x. The repository's `.python-version` pins Python 3.12.14.

From the repository root, create and activate a virtual environment if desired, then install the public runtime dependencies:

```bash
python -m pip install -r belgium-location/requirements-runtime.txt
```

The full cache-building workflows have additional builder dependencies and are separate from this basic runtime installation.

## Run locally

Ensure the seven runtime caches listed above exist in `belgium-location/cache/`, then run:

```bash
cd belgium-location
python server.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000).

Address autocomplete requires a `GEOAPIFY_API_KEY` environment variable. Never commit the key.

Windows CMD:

```bat
set GEOAPIFY_API_KEY=your_key_here
python server.py
```

macOS/Linux:

```bash
export GEOAPIFY_API_KEY=your_key_here
python server.py
```

## Health endpoint

`GET /api/health` validates readiness of the required runtime caches. A ready response is conceptually:

```json
{
  "status": "ok",
  "ready": true
}
```

The public response does not expose cache filesystem paths.

## Tests

Run the project test suite from the repository root:

```bash
python -m unittest discover -s tests -p "test_*.py" -q
```

## Repository and generated data

Generated datasets, downloaded GTFS ZIPs, OSM PBF files, and runtime cache artifacts should not be committed to Git. Generate and manage these artifacts outside version control; do not add production caches to commits.

## Deployment

The Render Blueprint is defined in `render.yaml`. Production deploys from `main`; automatic deployment is intentionally disabled, and deployment is performed manually after validation.

## Current limitations

- The application covers Belgium only.
- Walking and driving figures are approximations; real routing is not implemented.
- Transit scoring does not use realtime service data.
- Scores cover selected nearby-access dimensions only.
- The overall weighting is experimental.
- Results depend on the completeness and currentness of source data.

## Development status

This is an actively developed, early public version of the project.

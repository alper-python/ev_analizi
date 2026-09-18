"""Netherlands Solar data adapter.

Current prototype scope:
- discover the PDOK 3D building download(s) covering a location
- select the newest available dataset version

Geometry download and CityJSON parsing are intentionally separate steps.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PDOK_3D_API = (
    "https://api.pdok.nl/"
    "kadaster/3d-basisvoorziening/ogc/v1"
)

BUILDING_COLLECTION = "basisbestand_gebouwen"


@dataclass(frozen=True)
class PdokBuildingTile:
    feature_id: str
    sheet_id: str | None
    download_url: str
    imagery_year: int | None
    start_date: str | None
    end_date: str | None


def point_bbox(
    *,
    lat: float,
    lon: float,
    radius_m: float = 25.0,
) -> tuple[float, float, float, float]:
    """Approximate a CRS84 bbox around a WGS84 point."""

    if radius_m <= 0:
        raise ValueError("radius_m must be positive.")

    lat_delta = radius_m / 111_320.0

    cos_lat = math.cos(math.radians(lat))

    if abs(cos_lat) < 1e-12:
        raise ValueError("Longitude bbox is undefined near the poles.")

    lon_delta = radius_m / (111_320.0 * cos_lat)

    return (
        lon - lon_delta,
        lat - lat_delta,
        lon + lon_delta,
        lat + lat_delta,
    )


def _year_or_none(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_tile_features(payload: dict[str, Any]) -> list[PdokBuildingTile]:
    """Normalize PDOK OGC Feature results."""

    results: list[PdokBuildingTile] = []

    for feature in payload.get("features", []):
        properties = feature.get("properties") or {}

        download_url = properties.get("download_link")

        if not download_url:
            continue

        results.append(
            PdokBuildingTile(
                feature_id=str(feature.get("id", "")),
                sheet_id=properties.get("bladnr"),
                download_url=str(download_url),
                imagery_year=_year_or_none(
                    properties.get("jaargang_luchtfoto")
                ),
                start_date=properties.get("startdatum"),
                end_date=properties.get("einddatum"),
            )
        )

    return results


def newest_tiles(
    tiles: list[PdokBuildingTile],
) -> list[PdokBuildingTile]:
    """Return all tiles belonging to the newest available imagery year."""

    if not tiles:
        return []

    known_years = [
        tile.imagery_year
        for tile in tiles
        if tile.imagery_year is not None
    ]

    if not known_years:
        return tiles

    newest_year = max(known_years)

    return [
        tile
        for tile in tiles
        if tile.imagery_year == newest_year
    ]


def discover_building_tiles(
    *,
    lat: float,
    lon: float,
    radius_m: float = 25.0,
    timeout_s: float = 30.0,
) -> list[PdokBuildingTile]:
    """Query PDOK for building download tiles around a location."""

    bbox = point_bbox(
        lat=lat,
        lon=lon,
        radius_m=radius_m,
    )

    query = urlencode(
        {
            "bbox": ",".join(
                f"{coordinate:.8f}"
                for coordinate in bbox
            ),
            "limit": 100,
            "f": "json",
        }
    )

    url = (
        f"{PDOK_3D_API}/collections/"
        f"{BUILDING_COLLECTION}/items?{query}"
    )

    request = Request(
        url,
        headers={
            "Accept": "application/geo+json, application/json",
            "User-Agent": "DomiFrame-Solar-Prototype/0.1",
        },
    )

    with urlopen(
        request,
        timeout=timeout_s,
    ) as response:
        payload = json.load(response)

    return newest_tiles(
        parse_tile_features(payload)
    )

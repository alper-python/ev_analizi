"""Streaming helpers for PDOK CityJSON building tiles.

The PDOK building tiles can be large after decompression. This module avoids
loading an entire CityJSON document into memory when only a small local
neighbourhood is required for Solar Analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, BinaryIO

import ijson
from pyproj import Transformer


_WGS84_TO_RD = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:28992",
    always_xy=True,
)


@dataclass(frozen=True)
class NearbyBuilding:
    object_id: str
    object_type: str
    extent: tuple[float, float, float, float, float, float]
    distance_m: float
    identification: str | None
    construction_year: int | None
    geometry_lods: tuple[str, ...]
    geometry_types: tuple[str, ...]
    pointcloud_source: str | None
    pointcloud_year: int | None
    pointcloud_unusable: bool | None


def wgs84_to_rd(
    *,
    lat: float,
    lon: float,
) -> tuple[float, float]:
    """Convert WGS84 longitude/latitude to Dutch RD New XY coordinates."""

    x, y = _WGS84_TO_RD.transform(
        float(lon),
        float(lat),
    )

    return float(x), float(y)


def distance_to_extent_2d(
    *,
    x: float,
    y: float,
    extent: list[float] | tuple[float, ...],
) -> float:
    """Shortest horizontal distance from a point to a 3D bbox."""

    if len(extent) < 6:
        raise ValueError(
            "CityJSON geographicalExtent must contain six coordinates."
        )

    min_x, min_y = float(extent[0]), float(extent[1])
    max_x, max_y = float(extent[3]), float(extent[4])

    dx = max(
        min_x - x,
        0.0,
        x - max_x,
    )

    dy = max(
        min_y - y,
        0.0,
        y - max_y,
    )

    return math.hypot(dx, dy)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def summarize_city_object(
    *,
    object_id: str,
    city_object: dict[str, Any],
    target_x: float,
    target_y: float,
) -> NearbyBuilding | None:
    """Create the lightweight metadata needed for neighbourhood discovery."""

    extent = city_object.get("geographicalExtent")

    if not isinstance(extent, list) or len(extent) < 6:
        return None

    attributes = city_object.get("attributes") or {}
    geometries = city_object.get("geometry") or []

    lods: list[str] = []
    geometry_types: list[str] = []

    for geometry in geometries:
        lod = geometry.get("lod")

        if lod is not None:
            lods.append(str(lod))

        geometry_type = geometry.get("type")

        if geometry_type is not None:
            geometry_types.append(str(geometry_type))

    normalized_extent = tuple(
        float(value)
        for value in extent[:6]
    )

    return NearbyBuilding(
        object_id=str(object_id),
        object_type=str(
            city_object.get("type", "")
        ),
        extent=normalized_extent,
        distance_m=distance_to_extent_2d(
            x=target_x,
            y=target_y,
            extent=extent,
        ),
        identification=attributes.get(
            "identificatie"
        ),
        construction_year=_optional_int(
            attributes.get("oorspronkelijkBouwjaar")
        ),
        geometry_lods=tuple(sorted(set(lods))),
        geometry_types=tuple(
            sorted(set(geometry_types))
        ),
        pointcloud_source=attributes.get(
            "rf_pc_source"
        ),
        pointcloud_year=_optional_int(
            attributes.get("rf_pc_year")
        ),
        pointcloud_unusable=attributes.get(
            "rf_pointcloud_unusable"
        ),
    )


def nearby_buildings_from_stream(
    source: BinaryIO,
    *,
    target_x: float,
    target_y: float,
    radius_m: float,
) -> list[NearbyBuilding]:
    """Stream CityObjects and retain only objects near the target point."""

    if radius_m <= 0:
        raise ValueError(
            "radius_m must be positive."
        )

    results: list[NearbyBuilding] = []

    for object_id, city_object in ijson.kvitems(
        source,
        "CityObjects",
    ):
        summary = summarize_city_object(
            object_id=object_id,
            city_object=city_object,
            target_x=target_x,
            target_y=target_y,
        )

        if summary is None:
            continue

        if summary.object_type not in {
            "Building",
            "BuildingPart",
        }:
            continue

        if summary.distance_m <= radius_m:
            results.append(summary)

    results.sort(
        key=lambda building: (
            building.distance_m,
            building.object_id,
        )
    )

    return results

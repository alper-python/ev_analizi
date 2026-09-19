"""Directional grouping and area-weighted Solar surface aggregation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from solar.geometry.real_surfaces import AnalyzableSurface


COMPASS_ORDER = (
    "N",
    "NE",
    "E",
    "SE",
    "S",
    "SW",
    "W",
    "NW",
)


@dataclass(frozen=True)
class SurfaceGroup:
    surface_type: str
    direction: str
    surfaces: tuple[AnalyzableSurface, ...]
    total_area_m2: float


def compass_sector(
    azimuth_deg: float | None,
) -> str:
    """Map azimuth to the nearest 8-way compass direction."""

    if azimuth_deg is None:
        return "FLAT"

    azimuth = float(azimuth_deg) % 360.0

    index = int(
        ((azimuth + 22.5) % 360.0) // 45.0
    )

    return COMPASS_ORDER[index]


def group_surfaces(
    surfaces: Iterable[AnalyzableSurface],
) -> list[SurfaceGroup]:
    """Group roofs/facades by semantic type and compass orientation."""

    grouped = defaultdict(list)

    for surface in surfaces:
        direction = compass_sector(
            surface.azimuth_deg
        )

        grouped[
            (
                surface.surface_type,
                direction,
            )
        ].append(surface)

    result = []

    for (
        surface_type,
        direction,
    ), members in grouped.items():
        members = tuple(members)

        result.append(
            SurfaceGroup(
                surface_type=surface_type,
                direction=direction,
                surfaces=members,
                total_area_m2=sum(
                    item.area_m2
                    for item in members
                ),
            )
        )

    direction_order = {
        name: index
        for index, name in enumerate(
            COMPASS_ORDER + ("FLAT",)
        )
    }

    result.sort(
        key=lambda item: (
            item.surface_type,
            direction_order[item.direction],
        )
    )

    return result


def area_weighted_percentage(
    values: Iterable[tuple[float, float]],
) -> float:
    """Combine per-surface percentages using physical surface area."""

    values = list(values)

    total_area = sum(
        area
        for area, _ in values
    )

    if total_area <= 0:
        return 0.0

    return sum(
        area * percentage
        for area, percentage in values
    ) / total_area

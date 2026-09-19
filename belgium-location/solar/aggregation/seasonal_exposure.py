"""Seasonal and time-of-day sunlight aggregation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

import numpy as np
import pandas as pd
import trimesh

from solar.aggregation.surface_groups import (
    SurfaceGroup,
    area_weighted_percentage,
)
from solar.geometry.raycast import ShadowEngine
from solar.position.solar_position import sun_direction


SEASON_MONTHS = {
    "Winter": {12, 1, 2},
    "Spring": {3, 4, 5},
    "Summer": {6, 7, 8},
    "Autumn": {9, 10, 11},
}

DAYPARTS = {
    "Morning": (6, 10),
    "Midday": (10, 14),
    "Afternoon": (14, 18),
    "Evening": (18, 22),
}


@dataclass(frozen=True)
class SurfaceSamples:
    surface_id: str
    points: np.ndarray


def season_for_month(month: int) -> str:
    for season, months in SEASON_MONTHS.items():
        if month in months:
            return season

    raise ValueError(
        f"Invalid month: {month}"
    )


def daypart_for_hour(hour: int) -> str | None:
    for name, (start, end) in DAYPARTS.items():
        if start <= hour < end:
            return name

    return None


def _stable_seed(surface_id: str) -> int:
    digest = hashlib.sha256(
        surface_id.encode("utf-8")
    ).digest()

    return int.from_bytes(
        digest[:4],
        byteorder="big",
        signed=False,
    )


def sample_group_surfaces(
    groups: list[SurfaceGroup],
    *,
    points_per_m2: float = 0.75,
    min_samples: int = 12,
    max_samples: int = 150,
) -> dict[str, SurfaceSamples]:
    """Create deterministic samples for every analysis surface."""

    result: dict[str, SurfaceSamples] = {}

    for group in groups:
        for surface in group.surfaces:
            if surface.surface_id in result:
                continue

            count = int(
                round(
                    surface.area_m2
                    * points_per_m2
                )
            )

            count = max(
                min_samples,
                min(
                    max_samples,
                    count,
                ),
            )

            points, _ = (
                trimesh.sample.sample_surface(
                    surface.mesh,
                    count=count,
                    seed=_stable_seed(
                        surface.surface_id
                    ),
                )
            )

            result[
                surface.surface_id
            ] = SurfaceSamples(
                surface_id=surface.surface_id,
                points=np.asarray(
                    points,
                    dtype=float,
                ),
            )

    return result


def calculate_hourly_group_exposure(
    positions: pd.DataFrame,
    *,
    groups: list[SurfaceGroup],
    samples: dict[str, SurfaceSamples],
    own_engine: ShadowEngine,
    full_engine: ShadowEngine,
) -> pd.DataFrame:
    """Calculate area-weighted hourly sunlight for every surface group."""

    rows = []

    for timestamp, position in positions.iterrows():
        azimuth = float(
            position["azimuth"]
        )

        elevation = float(
            position["apparent_elevation"]
        )

        if elevation > 0:
            direction = sun_direction(
                azimuth_deg=azimuth,
                elevation_deg=elevation,
            )
        else:
            direction = None

        for group in groups:
            open_values = []
            own_values = []
            full_values = []

            for surface in group.surfaces:
                area = surface.area_m2

                normal = np.asarray(
                    surface.normal,
                    dtype=float,
                )

                if (
                    direction is None
                    or float(
                        np.dot(
                            normal,
                            direction,
                        )
                    ) <= 1e-12
                ):
                    open_pct = 0.0
                    own_pct = 0.0
                    full_pct = 0.0

                else:
                    open_pct = 100.0

                    sample_points = samples[
                        surface.surface_id
                    ].points

                    own_pct = own_engine.sunlit_area_pct(
                        sample_points,
                        surface_normal=normal,
                        sun_azimuth_deg=azimuth,
                        sun_elevation_deg=elevation,
                    )

                    full_pct = full_engine.sunlit_area_pct(
                        sample_points,
                        surface_normal=normal,
                        sun_azimuth_deg=azimuth,
                        sun_elevation_deg=elevation,
                    )

                open_values.append(
                    (
                        area,
                        open_pct,
                    )
                )

                own_values.append(
                    (
                        area,
                        own_pct,
                    )
                )

                full_values.append(
                    (
                        area,
                        full_pct,
                    )
                )

            open_group = area_weighted_percentage(
                open_values
            )

            own_group = area_weighted_percentage(
                own_values
            )

            full_group = area_weighted_percentage(
                full_values
            )

            rows.append(
                {
                    "timestamp": timestamp,
                    "surface_type": group.surface_type,
                    "direction": group.direction,
                    "area_m2": group.total_area_m2,
                    "open_pct": open_group,
                    "own_pct": own_group,
                    "full_pct": full_group,
                    "self_shade_pp": (
                        open_group - own_group
                    ),
                    "neighbour_shade_pp": (
                        own_group - full_group
                    ),
                }
            )

    result = pd.DataFrame(
        rows
    )

    if result.empty:
        return result

    result["season"] = result[
        "timestamp"
    ].map(
        lambda value: season_for_month(
            value.month
        )
    )

    result["daypart"] = result[
        "timestamp"
    ].map(
        lambda value: daypart_for_hour(
            value.hour
        )
    )

    result["date"] = result[
        "timestamp"
    ].map(
        lambda value: value.date()
    )

    return result


def _daily_equivalent_hours(
    rows: pd.DataFrame,
) -> tuple[float, float, float]:
    if rows.empty:
        return (
            0.0,
            0.0,
            0.0,
        )

    daily = (
        rows.groupby(
            "date",
            as_index=False,
        )[
            [
                "open_pct",
                "own_pct",
                "full_pct",
            ]
        ]
        .sum()
    )

    # Hourly resolution:
    # 100% sunlit for one row = one equivalent full-sun hour.
    return (
        float(
            daily["open_pct"].mean()
            / 100.0
        ),
        float(
            daily["own_pct"].mean()
            / 100.0
        ),
        float(
            daily["full_pct"].mean()
            / 100.0
        ),
    )


def seasonal_summary(
    hourly: pd.DataFrame,
    *,
    seasons: set[str] | None = None,
) -> pd.DataFrame:
    """Summarize average equivalent sunlight hours per day."""

    if hourly.empty:
        return pd.DataFrame()

    requested = (
        seasons
        if seasons is not None
        else set(SEASON_MONTHS)
    )

    rows = []

    keys = (
        hourly[
            hourly["season"].isin(
                requested
            )
        ][
            [
                "season",
                "surface_type",
                "direction",
                "area_m2",
            ]
        ]
        .drop_duplicates()
    )

    for item in keys.itertuples(
        index=False
    ):
        base = hourly[
            (hourly["season"] == item.season)
            & (
                hourly["surface_type"]
                == item.surface_type
            )
            & (
                hourly["direction"]
                == item.direction
            )
        ]

        periods = [
            (
                "Whole day",
                base,
            )
        ]

        for daypart in DAYPARTS:
            periods.append(
                (
                    daypart,
                    base[
                        base["daypart"]
                        == daypart
                    ],
                )
            )

        for period, subset in periods:
            (
                open_hours,
                own_hours,
                full_hours,
            ) = _daily_equivalent_hours(
                subset
            )

            rows.append(
                {
                    "season": item.season,
                    "surface_type": item.surface_type,
                    "direction": item.direction,
                    "area_m2": float(
                        item.area_m2
                    ),
                    "period": period,
                    "open_hours_per_day": open_hours,
                    "own_hours_per_day": own_hours,
                    "full_hours_per_day": full_hours,
                    "self_shade_hours_per_day": (
                        open_hours - own_hours
                    ),
                    "neighbour_shade_hours_per_day": (
                        own_hours - full_hours
                    ),
                }
            )

    return pd.DataFrame(
        rows
    )

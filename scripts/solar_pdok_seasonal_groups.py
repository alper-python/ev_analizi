"""Analyze whole-season sunlight for real PDOK roof/facade groups."""

from __future__ import annotations

import time
import sys
from pathlib import Path

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_scene import (
    load_cached_pdok_scene,
)
from solar.aggregation.seasonal_exposure import (
    calculate_hourly_group_exposure,
    sample_group_surfaces,
    seasonal_summary,
)
from solar.aggregation.surface_groups import (
    group_surfaces,
)
from solar.geometry.raycast import ShadowEngine
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)
from solar.position.solar_position import (
    hourly_solar_positions_for_year,
)


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"
YEAR = 2026

TARGET_ID = (
    "NL.IMBAG.Pand.0344100000024581"
)

RADIUS_M = 75.0

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)

ANALYZE_SEASONS = {
    "Summer",
    "Winter",
}

# Debug-output filter only.
# All surfaces remain part of the calculation.
DISPLAY_MIN_GROUP_AREA_M2 = 5.0


def main():
    started = time.perf_counter()

    print(
        "DomiFrame Solar — whole-season real building analysis"
    )
    print("=" * 78)

    scene = load_cached_pdok_scene(
        archive_path=ARCHIVE,
        lat=LAT,
        lon=LON,
        radius_m=RADIUS_M,
    )

    if TARGET_ID not in scene.meshes:
        raise RuntimeError(
            f"Target not found in scene: {TARGET_ID}"
        )

    surfaces = extract_analyzable_surfaces(
        scene.geometries,
        vertices=scene.vertices,
        parent_id=TARGET_ID,
        local_origin=(
            scene.x,
            scene.y,
            0.0,
        ),
    )

    groups = group_surfaces(
        surfaces
    )

    samples = sample_group_surfaces(
        groups
    )

    print()
    print(
        f"Scene buildings:     {len(scene.meshes)}"
    )
    print(
        f"Target surfaces:     {len(surfaces)}"
    )
    print(
        f"Direction groups:    {len(groups)}"
    )
    print(
        f"Surface samples:     "
        f"{sum(len(item.points) for item in samples.values())}"
    )

    positions = hourly_solar_positions_for_year(
        year=YEAR,
        lat=LAT,
        lon=LON,
        timezone=TZ,
    )

    positions = positions[
        positions.index.map(
            lambda timestamp:
            (
                "Summer"
                if timestamp.month in {
                    6,
                    7,
                    8,
                }
                else (
                    "Winter"
                    if timestamp.month in {
                        12,
                        1,
                        2,
                    }
                    else None
                )
            )
            in ANALYZE_SEASONS
        )
    ]

    print(
        f"Hourly timestamps:   {len(positions)}"
    )
    print()
    print("Running ray casting...")

    own_engine = ShadowEngine(
        [
            scene.meshes[
                TARGET_ID
            ]
        ]
    )

    full_engine = ShadowEngine(
        list(
            scene.meshes.values()
        )
    )

    hourly = calculate_hourly_group_exposure(
        positions,
        groups=groups,
        samples=samples,
        own_engine=own_engine,
        full_engine=full_engine,
    )

    summary = seasonal_summary(
        hourly,
        seasons=ANALYZE_SEASONS,
    )

    print()
    print(
        f"Calculation finished in "
        f"{time.perf_counter() - started:.1f} s"
    )

    direction_order = {
        "N": 0,
        "NE": 1,
        "E": 2,
        "SE": 3,
        "S": 4,
        "SW": 5,
        "W": 6,
        "NW": 7,
        "FLAT": 8,
    }

    for season in (
        "Summer",
        "Winter",
    ):
        print()
        print("=" * 78)
        print(season.upper())
        print("=" * 78)

        season_rows = summary[
            (
                summary["season"]
                == season
            )
            & (
                summary["area_m2"]
                >= DISPLAY_MIN_GROUP_AREA_M2
            )
        ]

        groups_for_display = (
            season_rows[
                [
                    "surface_type",
                    "direction",
                    "area_m2",
                ]
            ]
            .drop_duplicates()
            .sort_values(
                by=[
                    "surface_type",
                    "direction",
                ],
                key=lambda column: (
                    column.map(
                        direction_order
                    )
                    if column.name
                    == "direction"
                    else column
                ),
            )
        )

        for group in groups_for_display.itertuples(
            index=False
        ):
            print()
            print(
                f"{group.surface_type} "
                f"{group.direction} "
                f"({group.area_m2:.1f} m2)"
            )

            print(
                "period        "
                "open sky   "
                "own bldg   "
                "real scene  "
                "neighbour loss"
            )

            rows = season_rows[
                (
                    season_rows[
                        "surface_type"
                    ]
                    == group.surface_type
                )
                & (
                    season_rows[
                        "direction"
                    ]
                    == group.direction
                )
            ]

            period_order = [
                "Whole day",
                "Morning",
                "Midday",
                "Afternoon",
                "Evening",
            ]

            for period in period_order:
                row = rows[
                    rows["period"]
                    == period
                ]

                if row.empty:
                    continue

                item = row.iloc[0]

                print(
                    f"{period:<13}"
                    f"{item['open_hours_per_day']:7.2f} h   "
                    f"{item['own_hours_per_day']:7.2f} h   "
                    f"{item['full_hours_per_day']:7.2f} h   "
                    f"{item['neighbour_shade_hours_per_day']:7.2f} h"
                )


if __name__ == "__main__":
    main()

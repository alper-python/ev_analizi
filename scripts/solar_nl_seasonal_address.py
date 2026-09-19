"""Run whole-season Solar exposure for any supported Dutch address."""

from __future__ import annotations

import argparse
import time

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_pipeline import (
    load_netherlands_address_scene,
)
from solar.aggregation.seasonal_exposure import (
    calculate_hourly_group_exposure,
    sample_group_surfaces,
    seasonal_summary,
    season_for_month,
)
from solar.aggregation.surface_groups import (
    group_surfaces,
)
from solar.geometry.facade_exposure import (
    FacadeExposureClass,
    measure_facade_exposure,
    remove_party_walls,
)
from solar.geometry.raycast import (
    ShadowEngine,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)
from solar.position.solar_position import (
    hourly_solar_positions_for_year,
)


TIMEZONE = "Europe/Amsterdam"

DEFAULT_SEASONS = (
    "Winter",
    "Spring",
    "Summer",
    "Autumn",
)

DIRECTION_ORDER = {
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

SURFACE_TYPE_ORDER = {
    "RoofSurface": 0,
    "WallSurface": 1,
}

PERIOD_ORDER = (
    "Whole day",
    "Morning",
    "Midday",
    "Afternoon",
    "Evening",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run whole-season DomiFrame Solar exposure "
            "analysis for a Dutch BAG address."
        )
    )

    parser.add_argument(
        "--postcode",
        required=True,
    )

    parser.add_argument(
        "--number",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--letter",
        default=None,
    )

    parser.add_argument(
        "--addition",
        default=None,
    )

    parser.add_argument(
        "--label",
        default=None,
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2026,
    )

    parser.add_argument(
        "--radius",
        type=float,
        default=75.0,
    )

    parser.add_argument(
        "--seasons",
        nargs="+",
        choices=DEFAULT_SEASONS,
        default=list(
            DEFAULT_SEASONS
        ),
    )

    parser.add_argument(
        "--display-min-area",
        type=float,
        default=5.0,
        help=(
            "Hide direction groups smaller than this "
            "area in console output only. They remain "
            "part of the calculation."
        ),
    )

    return parser.parse_args()


def group_sort_key(
    group,
):
    return (
        SURFACE_TYPE_ORDER.get(
            group.surface_type,
            99,
        ),
        DIRECTION_ORDER.get(
            group.direction,
            99,
        ),
    )


def main():
    args = parse_args()

    started = time.perf_counter()

    analyze_seasons = set(
        args.seasons
    )

    print(
        "DomiFrame Solar — Netherlands seasonal exposure"
    )
    print("=" * 88)

    if args.label:
        print(
            f"Validation case:   {args.label}"
        )

    print(
        f"Input:             "
        f"{args.postcode} {args.number}"
    )

    print(
        f"Year:              {args.year}"
    )

    print(
        f"Seasons:           "
        f"{', '.join(args.seasons)}"
    )

    print()
    print(
        "Resolving address and loading PDOK 3D scene..."
    )

    scene = load_netherlands_address_scene(
        postcode=args.postcode,
        house_number=args.number,
        house_letter=args.letter,
        addition=args.addition,
        radius_m=args.radius,
    )

    target_pand = (
        scene.address.panden[
            0
        ]
    )

    print()
    print("ADDRESS")
    print("-" * 88)

    print(
        f"Resolved:          "
        f"{scene.address.street} "
        f"{scene.address.house_number}, "
        f"{scene.address.postcode} "
        f"{scene.address.city}"
    )

    print(
        f"Coordinates:       "
        f"{scene.lat:.8f}, "
        f"{scene.lon:.8f}"
    )

    print(
        f"Pand:              "
        f"{scene.target_id}"
    )

    print(
        f"Pand VBO count:    "
        f"{target_pand.verblijfsobject_count}"
    )

    print(
        f"Shared BAG Pand:   "
        f"{target_pand.is_shared_building}"
    )

    if target_pand.is_shared_building:
        print()
        print("RESULT")
        print("-" * 88)

        print(
            "LIMITATION — this BAG Pand contains "
            f"{target_pand.verblijfsobject_count} VBOs."
        )

        print(
            "Building-level geometry exists, but "
            "property-specific roof/facade Solar exposure "
            "cannot be inferred reliably."
        )

        return

    target_scene = (
        scene.target_tile_scene
    )

    surfaces = extract_analyzable_surfaces(
        target_scene.geometries,
        vertices=target_scene.vertices,
        parent_id=scene.target_id,
        local_origin=(
            scene.x,
            scene.y,
            0.0,
        ),
    )

    neighbour_meshes = [
        mesh
        for building_id, mesh
        in scene.meshes.items()
        if building_id
        != scene.target_id
    ]

    facade_reports = (
        measure_facade_exposure(
            surfaces,
            neighbour_meshes=neighbour_meshes,
        )
    )

    party_walls = [
        surface
        for surface in surfaces
        if (
            surface.surface_type
            == "WallSurface"
            and facade_reports[
                surface.surface_id
            ].classification
            == FacadeExposureClass.PARTY_WALL
        )
    ]

    analysis_surfaces = (
        remove_party_walls(
            surfaces,
            facade_reports,
        )
    )

    groups = group_surfaces(
        analysis_surfaces
    )

    samples = sample_group_surfaces(
        groups
    )

    total_sample_points = sum(
        len(
            item.points
        )
        for item in samples.values()
    )

    print()
    print("SCENE")
    print("-" * 88)

    print(
        f"PDOK tiles:        "
        f"{len(scene.tiles)}"
    )

    print(
        f"Scene buildings:   "
        f"{len(scene.meshes)}"
    )

    print(
        f"Raw surfaces:      "
        f"{len(surfaces)}"
    )

    print(
        f"Party walls:       "
        f"{len(party_walls)} "
        f"("
        f"{sum(item.area_m2 for item in party_walls):.2f}"
        f" m2)"
    )

    print(
        f"Analysis surfaces: "
        f"{len(analysis_surfaces)}"
    )

    print(
        f"Direction groups:  "
        f"{len(groups)}"
    )

    print(
        f"Surface samples:   "
        f"{total_sample_points}"
    )

    print()
    print("GROUPS")
    print("-" * 88)

    for group in sorted(
        groups,
        key=group_sort_key,
    ):
        print(
            f"{group.surface_type:<12} "
            f"{group.direction:>4} "
            f"{group.total_area_m2:8.2f} m2 "
            f"({len(group.surfaces)} surfaces)"
        )

    print()
    print(
        "Calculating hourly solar positions..."
    )

    positions = (
        hourly_solar_positions_for_year(
            year=args.year,
            lat=scene.lat,
            lon=scene.lon,
            timezone=TIMEZONE,
        )
    )

    positions = positions[
        positions.index.map(
            lambda timestamp:
            season_for_month(
                timestamp.month
            )
            in analyze_seasons
        )
    ]

    print(
        f"Hourly timestamps: {len(positions)}"
    )

    print()
    print(
        "Building ray-casting engines..."
    )

    own_engine = ShadowEngine(
        [
            scene.target_mesh
        ]
    )

    full_engine = ShadowEngine(
        list(
            scene.meshes.values()
        )
    )

    print(
        "Running hourly ray casting..."
    )

    ray_started = (
        time.perf_counter()
    )

    hourly = (
        calculate_hourly_group_exposure(
            positions,
            groups=groups,
            samples=samples,
            own_engine=own_engine,
            full_engine=full_engine,
        )
    )

    ray_seconds = (
        time.perf_counter()
        - ray_started
    )

    summary = seasonal_summary(
        hourly,
        seasons=analyze_seasons,
    )

    print()
    print(
        f"Ray casting time:  "
        f"{ray_seconds:.1f} s"
    )

    print(
        f"Total runtime:      "
        f"{time.perf_counter() - started:.1f} s"
    )

    for season in DEFAULT_SEASONS:
        if season not in analyze_seasons:
            continue

        print()
        print("=" * 88)
        print(
            season.upper()
        )
        print("=" * 88)

        season_rows = summary[
            summary[
                "season"
            ]
            == season
        ]

        for group in sorted(
            groups,
            key=group_sort_key,
        ):
            if (
                group.total_area_m2
                < args.display_min_area
            ):
                continue

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

            if rows.empty:
                continue

            print()
            print(
                f"{group.surface_type} "
                f"{group.direction} "
                f"({group.total_area_m2:.1f} m2)"
            )

            print(
                "period        "
                "open sky   "
                "own bldg   "
                "bldg scene  "
                "neighbour loss"
            )

            for period in PERIOD_ORDER:
                row = rows[
                    rows[
                        "period"
                    ]
                    == period
                ]

                if row.empty:
                    continue

                item = row.iloc[
                    0
                ]

                print(
                    f"{period:<13}"
                    f"{item['open_hours_per_day']:7.2f} h   "
                    f"{item['own_hours_per_day']:7.2f} h   "
                    f"{item['full_hours_per_day']:7.2f} h   "
                    f"{item['neighbour_shade_hours_per_day']:7.2f} h"
                )

    print()
    print("=" * 88)
    print("RESULT")
    print("=" * 88)

    print(
        "PASS — property-level seasonal Solar exposure "
        "from building geometry completed from address only."
    )

    print(
        "NOTE — current obstruction scene includes buildings only; "
        "terrain and vegetation are not yet included."
    )


if __name__ == "__main__":
    main()

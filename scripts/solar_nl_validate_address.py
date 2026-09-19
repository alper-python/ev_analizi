"""Validate the Netherlands Solar pipeline for any BAG address."""

from __future__ import annotations

import argparse
import time

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_pipeline import (
    load_netherlands_address_scene,
)
from solar.aggregation.surface_groups import (
    group_surfaces,
)
from solar.geometry.facade_exposure import (
    FacadeExposureClass,
    measure_facade_exposure,
    remove_party_walls,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Build and inspect a real DomiFrame Solar "
            "scene for a Dutch address."
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
        "--radius",
        type=float,
        default=75.0,
    )

    parser.add_argument(
        "--label",
        default=None,
        help="Optional validation label such as NL-A.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    started = time.perf_counter()

    print(
        "DomiFrame Solar — Netherlands address validation"
    )
    print("=" * 82)

    if args.label:
        print(
            f"Validation case: {args.label}"
        )

    print(
        f"Input:           {args.postcode} {args.number}"
    )

    scene = load_netherlands_address_scene(
        postcode=args.postcode,
        house_number=args.number,
        house_letter=args.letter,
        addition=args.addition,
        radius_m=args.radius,
    )

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

    facade_reports = measure_facade_exposure(
        surfaces,
        neighbour_meshes=neighbour_meshes,
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

    partial_walls = [
        surface
        for surface in surfaces
        if (
            surface.surface_type
            == "WallSurface"
            and facade_reports[
                surface.surface_id
            ].classification
            == FacadeExposureClass.PARTIALLY_ATTACHED
        )
    ]

    analysis_surfaces = remove_party_walls(
        surfaces,
        facade_reports,
    )

    groups = group_surfaces(
        analysis_surfaces
    )

    roofs = [
        surface
        for surface in analysis_surfaces
        if surface.surface_type
        == "RoofSurface"
    ]

    walls = [
        surface
        for surface in analysis_surfaces
        if surface.surface_type
        == "WallSurface"
    ]

    print()
    print("ADDRESS")
    print("-" * 82)

    print(
        f"Resolved:        "
        f"{scene.address.street} "
        f"{scene.address.house_number}, "
        f"{scene.address.postcode} "
        f"{scene.address.city}"
    )

    print(
        f"Coordinates:     "
        f"{scene.lat:.8f}, {scene.lon:.8f}"
    )

    print(
        f"VBO:             "
        f"{scene.address.verblijfsobject_identification}"
    )

    print(
        f"Pand:            {scene.target_id}"
    )

    target_pand = (
        scene.address.panden[
            0
        ]
    )

    if (
        target_pand.verblijfsobject_count
        is None
    ):
        vbo_count_text = "unknown"
        shared_text = "unknown"
    else:
        vbo_count_text = str(
            target_pand.verblijfsobject_count
        )

        shared_text = (
            "YES"
            if target_pand.is_shared_building
            else "NO"
        )

    print(
        f"Pand VBO count:  {vbo_count_text}"
    )

    print(
        f"Shared BAG Pand: {shared_text}"
    )

    print()
    print("DATA PROVENANCE")
    print("-" * 82)

    print(
        f"PDOK tiles:      {len(scene.tiles)}"
    )

    for tile in scene.tiles:
        print(
            f"  {tile.sheet_id}: "
            f"imagery={tile.imagery_year} "
            f"start={tile.start_date} "
            f"end={tile.end_date}"
        )

    print()
    print("3D SCENE")
    print("-" * 82)

    print(
        f"Radius:          {scene.radius_m:.1f} m"
    )

    print(
        f"Buildings:       {len(scene.meshes)}"
    )

    print(
        f"Duplicate IDs:   "
        f"{len(scene.duplicate_building_ids)}"
    )

    print(
        f"Target faces:    "
        f"{len(scene.target_mesh.faces)}"
    )

    print(
        f"Target area:     "
        f"{scene.target_mesh.area:.2f} m2"
    )

    print()
    print("SURFACES")
    print("-" * 82)

    print(
        f"Raw surfaces:    {len(surfaces)}"
    )

    print(
        f"Party walls:     {len(party_walls)} "
        f"({sum(item.area_m2 for item in party_walls):.2f} m2)"
    )

    print(
        f"Partial walls:   {len(partial_walls)} "
        f"({sum(item.area_m2 for item in partial_walls):.2f} m2)"
    )

    print(
        f"Analyzed roofs:  {len(roofs)} "
        f"({sum(item.area_m2 for item in roofs):.2f} m2)"
    )

    print(
        f"Analyzed walls:  {len(walls)} "
        f"({sum(item.area_m2 for item in walls):.2f} m2)"
    )

    print()
    print("DIRECTION GROUPS")
    print("-" * 82)

    for group in groups:
        print(
            f"{group.surface_type:<12} "
            f"{group.direction:>4} "
            f"{group.total_area_m2:8.2f} m2 "
            f"({len(group.surfaces)} surfaces)"
        )

    print()
    print("PARTY WALL DETAILS")
    print("-" * 82)

    if not party_walls:
        print(
            "none"
        )

    for surface in sorted(
        party_walls,
        key=lambda item:
        -item.area_m2,
    ):
        report = facade_reports[
            surface.surface_id
        ]

        print(
            f"{surface.surface_id}"
        )

        print(
            f"  area:      "
            f"{surface.area_m2:.2f} m2"
        )

        print(
            f"  azimuth:   "
            f"{surface.azimuth_deg:.1f}°"
        )

        print(
            f"  attachment:"
            f" {report.near_zero_pct:.1f}%"
        )

    print()
    print("RESULT")
    print("-" * 82)

    if target_pand.is_shared_building:
        print(
            "LIMITATION — exact address and building geometry "
            "were resolved, but this BAG Pand contains "
            f"{target_pand.verblijfsobject_count} VBOs."
        )

        print(
            "Solar geometry is building-level only; "
            "unit-specific roof/facade exposure must not "
            "be inferred from this scene."
        )

    else:
        print(
            "PASS — exact address resolved and complete "
            "property-level Solar geometry was produced."
        )

    print(
        f"Runtime:         "
        f"{time.perf_counter() - started:.2f} s"
    )


if __name__ == "__main__":
    main()

"""Inventory analyzable roofs and facades on the real Utrecht target."""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.adapters.pdok_cityjson import (
    nearby_buildings_from_stream,
    wgs84_to_rd,
)
from solar.adapters.pdok_geometry import (
    load_selected_parts,
    load_selected_vertices,
    read_cityjson_context,
    required_vertex_indices,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)


LAT = 52.0907
LON = 5.1214

RADIUS_M = 75.0

TARGET_ID = (
    "NL.IMBAG.Pand.0344100000024581"
)

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)


def main():
    x, y = wgs84_to_rd(
        lat=LAT,
        lon=LON,
    )

    with ZipFile(ARCHIVE) as archive:
        members = [
            info.filename
            for info in archive.infolist()
            if info.filename.lower().endswith(
                ".city.json"
            )
        ]

        if len(members) != 1:
            raise RuntimeError(
                "Expected exactly one CityJSON member."
            )

        member = members[0]

        with archive.open(member) as source:
            buildings = nearby_buildings_from_stream(
                source,
                target_x=x,
                target_y=y,
                radius_m=RADIUS_M,
            )

        child_to_parent = {
            child_id: building.object_id
            for building in buildings
            for child_id in building.child_ids
        }

        with archive.open(member) as source:
            geometries = load_selected_parts(
                source,
                child_to_parent=child_to_parent,
            )

        indices = required_vertex_indices(
            geometries
        )

        with archive.open(member) as transform_source, archive.open(
            member
        ) as metadata_source:
            context = read_cityjson_context(
                transform_source,
                metadata_source,
            )

        with archive.open(member) as source:
            vertices = load_selected_vertices(
                source,
                indices=indices,
                context=context,
            )

    surfaces = extract_analyzable_surfaces(
        geometries,
        vertices=vertices,
        parent_id=TARGET_ID,
        local_origin=(
            x,
            y,
            0.0,
        ),
    )

    roofs = [
        item
        for item in surfaces
        if item.surface_type == "RoofSurface"
    ]

    walls = [
        item
        for item in surfaces
        if item.surface_type == "WallSurface"
    ]

    print(
        "DomiFrame Solar — real target surface inventory"
    )
    print("=" * 72)
    print()

    print(
        f"Target: {TARGET_ID}"
    )

    print(
        f"Analyzable surfaces: {len(surfaces)}"
    )

    print(
        f"Roof surfaces:       {len(roofs)}"
    )

    print(
        f"Wall surfaces:       {len(walls)}"
    )

    print(
        f"Roof area:           "
        f"{sum(item.area_m2 for item in roofs):.2f} m2"
    )

    print(
        f"Wall area:           "
        f"{sum(item.area_m2 for item in walls):.2f} m2"
    )

    print()
    print("ROOFS")
    print("-" * 72)

    for surface in roofs:
        azimuth = (
            "flat"
            if surface.azimuth_deg is None
            else f"{surface.azimuth_deg:.1f}°"
        )

        print(
            f"{surface.surface_id}"
        )

        print(
            f"  area:    {surface.area_m2:.2f} m2"
        )

        print(
            f"  azimuth: {azimuth}"
        )

        print(
            f"  tilt:    {surface.tilt_deg:.1f}°"
        )

    print()
    print("LARGEST WALLS")
    print("-" * 72)

    for surface in sorted(
        walls,
        key=lambda item: item.area_m2,
        reverse=True,
    )[:12]:
        print(
            f"{surface.surface_id}"
        )

        print(
            f"  area:    {surface.area_m2:.2f} m2"
        )

        print(
            f"  azimuth: "
            f"{surface.azimuth_deg:.1f}°"
        )

        print(
            f"  tilt:    {surface.tilt_deg:.1f}°"
        )


if __name__ == "__main__":
    main()

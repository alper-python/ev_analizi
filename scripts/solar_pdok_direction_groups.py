"""Group the Utrecht prototype building's real roofs/facades by direction."""

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
from solar.aggregation.surface_groups import (
    group_surfaces,
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
        member = next(
            info.filename
            for info in archive.infolist()
            if info.filename.lower().endswith(
                ".city.json"
            )
        )

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

    groups = group_surfaces(
        surfaces
    )

    print(
        "DomiFrame Solar — directional surface groups"
    )
    print("=" * 70)
    print()
    print(f"Target: {TARGET_ID}")
    print()

    for group in groups:
        print(
            f"{group.surface_type:<12} "
            f"{group.direction:>4} "
            f"area={group.total_area_m2:8.2f} m2 "
            f"surfaces={len(group.surfaces)}"
        )

        for surface in sorted(
            group.surfaces,
            key=lambda item: item.area_m2,
            reverse=True,
        ):
            azimuth = (
                "flat"
                if surface.azimuth_deg is None
                else f"{surface.azimuth_deg:.1f}°"
            )

            print(
                f"    {surface.area_m2:7.2f} m2"
                f"   az={azimuth:>7}"
                f"   tilt={surface.tilt_deg:5.1f}°"
            )


if __name__ == "__main__":
    main()

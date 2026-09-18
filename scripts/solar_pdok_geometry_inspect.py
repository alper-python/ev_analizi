"""Extract real local LoD geometry from the cached Utrecht PDOK tile."""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.pdok_cityjson import (
    nearby_buildings_from_stream,
    wgs84_to_rd,
)
from solar.adapters.pdok_geometry import (
    load_selected_parts,
    load_selected_vertices,
    read_cityjson_context,
    required_vertex_indices,
    semantic_surface_counts,
)


LAT = 52.0907
LON = 5.1214
RADIUS_M = 75.0

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)


def bounds(vertices):
    xs = [value[0] for value in vertices.values()]
    ys = [value[1] for value in vertices.values()]
    zs = [value[2] for value in vertices.values()]

    return (
        min(xs),
        min(ys),
        min(zs),
        max(xs),
        max(ys),
        max(zs),
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
            if info.filename.lower().endswith(".city.json")
        ]

        if len(members) != 1:
            raise SystemExit(
                f"Expected one CityJSON member, found {len(members)}."
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

        with archive.open(member) as transform_source, archive.open(
            member
        ) as metadata_source:
            context = read_cityjson_context(
                transform_source,
                metadata_source,
            )

        with archive.open(member) as source:
            geometries = load_selected_parts(
                source,
                child_to_parent=child_to_parent,
            )

        vertex_indices = required_vertex_indices(
            geometries
        )

        with archive.open(member) as source:
            vertices = load_selected_vertices(
                source,
                indices=vertex_indices,
                context=context,
            )

    lod_counts = Counter(
        item.lod
        for item in geometries
    )

    geometry_type_counts = Counter(
        item.geometry_type
        for item in geometries
    )

    surface_counts = Counter()

    for geometry in geometries:
        surface_counts.update(
            semantic_surface_counts(
                geometry
            )
        )

    print("DomiFrame Solar — real PDOK LoD geometry extraction")
    print("=" * 68)
    print()
    print(f"Location:        {LAT}, {LON}")
    print(f"RD:              {x:.3f}, {y:.3f}")
    print(f"Buildings:       {len(buildings)}")
    print(f"Requested parts: {len(child_to_parent)}")
    print(f"Geometry objs:   {len(geometries)}")
    print(f"Unique vertices: {len(vertices)}")
    print()
    print(f"CRS:       {context.reference_system}")
    print(f"Scale:     {context.scale}")
    print(f"Translate: {context.translate}")
    print()
    print("Selected LoD:")
    for lod, count in sorted(lod_counts.items()):
        print(f"  {lod}: {count}")

    print()
    print("Geometry types:")
    for geometry_type, count in sorted(
        geometry_type_counts.items()
    ):
        print(f"  {geometry_type}: {count}")

    print()
    print("Semantic surface primitives:")
    for surface_type, count in sorted(
        surface_counts.items()
    ):
        print(f"  {surface_type}: {count}")

    print()
    local_bounds = bounds(vertices)

    print(
        "Selected vertex bounds: "
        + ", ".join(
            f"{value:.3f}"
            for value in local_bounds
        )
    )

    print()
    print("Nearest building:")
    target = buildings[0]

    print(f"  {target.object_id}")
    print(f"  children: {target.child_ids}")
    print(f"  parent extent: {target.extent}")

    target_geometries = [
        item
        for item in geometries
        if item.parent_id == target.object_id
    ]

    print(
        "  selected geometry: "
        + ", ".join(
            f"{item.geometry_type} LoD {item.lod}"
            for item in target_geometries
        )
    )

    target_surface_counts = Counter()

    for item in target_geometries:
        target_surface_counts.update(
            semantic_surface_counts(item)
        )

    print(
        "  semantic primitives: "
        + ", ".join(
            f"{name}={count}"
            for name, count in sorted(
                target_surface_counts.items()
            )
        )
    )


if __name__ == "__main__":
    main()

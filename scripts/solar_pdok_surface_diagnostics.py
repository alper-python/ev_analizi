"""Diagnose PDOK semantic surfaces that cannot be triangulated."""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from zipfile import ZipFile

import numpy as np


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
)
from solar.geometry.cityjson_mesh import (
    semantic_surfaces,
    triangulate_surface,
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


def unique_points(points: np.ndarray, tolerance: float = 1e-9):
    result = []

    for point in points:
        if not any(
            np.linalg.norm(point - other) <= tolerance
            for other in result
        ):
            result.append(point)

    return result


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

        indices = required_vertex_indices(
            geometries
        )

        with archive.open(member) as source:
            vertices = load_selected_vertices(
                source,
                indices=indices,
                context=context,
            )

    total = 0
    successful = 0
    failed = []

    failure_types = Counter()
    surface_types = Counter()
    failed_surface_types = Counter()

    for geometry in geometries:
        for surface in semantic_surfaces(geometry):
            total += 1
            surface_types[surface.surface_type] += 1

            try:
                mesh = triangulate_surface(
                    surface,
                    vertices=vertices,
                    local_origin=(x, y, 0.0),
                )

                if mesh.is_empty:
                    raise ValueError("Triangulation returned empty mesh.")

                successful += 1

            except Exception as exc:
                message = str(exc)
                failure_types[type(exc).__name__] += 1
                failed_surface_types[surface.surface_type] += 1

                ring_info = []

                for ring_index, ring in enumerate(surface.rings):
                    points = np.array(
                        [
                            vertices[index]
                            for index in ring
                        ],
                        dtype=float,
                    )

                    unique = unique_points(points)

                    ring_info.append(
                        {
                            "ring_index": ring_index,
                            "index_count": len(ring),
                            "unique_point_count": len(unique),
                            "indices": ring,
                            "points": points.tolist(),
                        }
                    )

                failed.append(
                    {
                        "parent": surface.parent_id,
                        "part": surface.part_id,
                        "surface_type": surface.surface_type,
                        "exception_type": type(exc).__name__,
                        "message": message,
                        "rings": ring_info,
                    }
                )

    print("DomiFrame Solar — PDOK surface diagnostics")
    print("=" * 72)
    print()
    print(f"Buildings:            {len(buildings)}")
    print(f"Geometry objects:     {len(geometries)}")
    print(f"Semantic surfaces:    {total}")
    print(f"Triangulated:         {successful}")
    print(f"Failed:               {len(failed)}")

    if total:
        print(
            f"Failure rate:         "
            f"{len(failed) / total * 100.0:.4f}%"
        )

    print()
    print("All surface types:")
    for name, count in sorted(surface_types.items()):
        print(f"  {name}: {count}")

    print()
    print("Failed surface types:")
    if failed_surface_types:
        for name, count in sorted(failed_surface_types.items()):
            print(f"  {name}: {count}")
    else:
        print("  none")

    print()
    print("Failure exception types:")
    if failure_types:
        for name, count in sorted(failure_types.items()):
            print(f"  {name}: {count}")
    else:
        print("  none")

    print()
    print("First failing surfaces")
    print("-" * 72)

    for index, failure in enumerate(failed[:10], start=1):
        print()
        print(f"{index}. {failure['parent']}")
        print(f"   part:    {failure['part']}")
        print(f"   type:    {failure['surface_type']}")
        print(f"   error:   {failure['message']}")

        for ring in failure["rings"]:
            print(
                f"   ring {ring['ring_index']}: "
                f"indices={ring['index_count']} "
                f"unique_points={ring['unique_point_count']}"
            )

            print(
                f"      vertex indices: {ring['indices']}"
            )

            for point in ring["points"]:
                print(
                    "      "
                    + ", ".join(
                        f"{value:.6f}"
                        for value in point
                    )
                )


if __name__ == "__main__":
    main()

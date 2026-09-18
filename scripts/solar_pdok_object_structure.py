"""Inspect the CityJSON hierarchy of one PDOK building.

This is a diagnostic script only. It intentionally avoids printing geometry
boundaries or vertices, which may be very large.
"""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import ijson


ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)

TARGET_ID = "NL.IMBAG.Pand.0344100000039864"


def geometry_summary(geometry):
    semantics = geometry.get("semantics") or {}

    surface_types = []

    for surface in semantics.get("surfaces") or []:
        surface_type = surface.get("type")

        if surface_type:
            surface_types.append(str(surface_type))

    boundaries = geometry.get("boundaries")

    return {
        "type": geometry.get("type"),
        "lod": geometry.get("lod"),
        "surface_types": sorted(set(surface_types)),
        "top_level_boundary_count": (
            len(boundaries)
            if isinstance(boundaries, list)
            else None
        ),
    }


def print_object(object_id, obj, indent=""):
    print(f"{indent}ID:       {object_id}")
    print(f"{indent}Type:     {obj.get('type')}")
    print(f"{indent}Parents:  {obj.get('parents')}")
    print(f"{indent}Children: {obj.get('children')}")
    print(f"{indent}Extent:   {obj.get('geographicalExtent')}")

    geometries = obj.get("geometry") or []

    print(f"{indent}Geometry count: {len(geometries)}")

    for index, geometry in enumerate(geometries, start=1):
        summary = geometry_summary(geometry)

        print(
            f"{indent}  Geometry {index}:"
            f" type={summary['type']}"
            f" lod={summary['lod']}"
            f" boundary_count={summary['top_level_boundary_count']}"
        )

        print(
            f"{indent}    semantic surfaces:"
            f" {summary['surface_types'] or 'none'}"
        )


def find_object(archive, member, object_id):
    with archive.open(member) as source:
        for current_id, obj in ijson.kvitems(
            source,
            "CityObjects",
        ):
            if current_id == object_id:
                return obj

    return None


def find_objects(archive, member, object_ids):
    wanted = set(object_ids)
    found = {}

    if not wanted:
        return found

    with archive.open(member) as source:
        for current_id, obj in ijson.kvitems(
            source,
            "CityObjects",
        ):
            if current_id in wanted:
                found[current_id] = obj

                if len(found) == len(wanted):
                    break

    return found


def read_metadata(archive, member):
    with archive.open(member) as source:
        iterator = ijson.items(
            source,
            "metadata",
        )

        return next(iterator, None)


def main():
    if not ARCHIVE.exists():
        raise SystemExit(
            f"Archive not found: {ARCHIVE}"
        )

    with ZipFile(ARCHIVE) as archive:
        members = [
            info.filename
            for info in archive.infolist()
            if info.filename.lower().endswith(".city.json")
        ]

        if len(members) != 1:
            raise SystemExit(
                f"Expected one CityJSON file, found {len(members)}."
            )

        member = members[0]

        print("DomiFrame Solar — PDOK CityJSON hierarchy")
        print("=" * 60)
        print()
        print("TARGET BUILDING")
        print("---------------")

        target = find_object(
            archive,
            member,
            TARGET_ID,
        )

        if target is None:
            raise SystemExit(
                f"Target object not found: {TARGET_ID}"
            )

        print_object(
            TARGET_ID,
            target,
        )

        child_ids = target.get("children") or []

        print()
        print(
            f"DIRECT CHILDREN: {len(child_ids)}"
        )
        print("-" * 60)

        children = find_objects(
            archive,
            member,
            child_ids,
        )

        for child_id in child_ids:
            print()

            child = children.get(child_id)

            if child is None:
                print(f"Missing child: {child_id}")
                continue

            print_object(
                child_id,
                child,
                indent="  ",
            )

        print()
        print("CITYJSON METADATA")
        print("-----------------")

        metadata = read_metadata(
            archive,
            member,
        )

        if metadata is None:
            print("No top-level metadata object found.")
        else:
            for key, value in metadata.items():
                print(f"{key}: {value}")


if __name__ == "__main__":
    main()

"""Build a real local Trimesh scene from the Utrecht PDOK LoD2 tile."""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import trimesh


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
    build_parent_meshes,
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

OUTPUT = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "utrecht_75m_lod22.glb"
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
                f"Expected one CityJSON file, found {len(members)}."
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

    print("DomiFrame Solar — real PDOK Trimesh scene")
    print("=" * 65)
    print()
    print(f"Buildings selected: {len(buildings)}")
    print(f"LoD geometries:     {len(geometries)}")
    print(f"Source vertices:    {len(vertices)}")
    print()
    print("Triangulating semantic surfaces...")

    meshes = build_parent_meshes(
        geometries,
        vertices=vertices,
        # Shift the large RD coordinates to a local scene around the
        # analysis point. Z remains NAP-based.
        local_origin=(
            x,
            y,
            0.0,
        ),
    )

    print()
    print(f"Building meshes: {len(meshes)}")

    total_vertices = sum(
        len(mesh.vertices)
        for mesh in meshes.values()
    )

    total_faces = sum(
        len(mesh.faces)
        for mesh in meshes.values()
    )

    print(f"Mesh vertices:   {total_vertices}")
    print(f"Triangle faces:  {total_faces}")

    scene = trimesh.Scene()

    for building_id, mesh in meshes.items():
        scene.add_geometry(
            mesh,
            node_name=building_id,
            geom_name=building_id,
        )

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT.write_bytes(
        scene.export(
            file_type="glb"
        )
    )

    print()
    print(f"Scene bounds:")
    print(scene.bounds)

    print()
    print(f"GLB written to:")
    print(OUTPUT)

    print()
    print("Target candidate:")
    target = buildings[0]

    print(target.object_id)

    target_mesh = meshes.get(
        target.object_id
    )

    if target_mesh is None:
        print(
            "WARNING: no target mesh was produced."
        )
    else:
        print(
            f"  vertices: {len(target_mesh.vertices)}"
        )
        print(
            f"  faces:    {len(target_mesh.faces)}"
        )
        print(
            f"  bounds:   {target_mesh.bounds}"
        )
        print(
            f"  area:     {target_mesh.area:.2f} m2"
        )


if __name__ == "__main__":
    main()

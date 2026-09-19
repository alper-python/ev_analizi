"""Reusable loader for a local Netherlands PDOK Solar scene."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

import trimesh

from solar.adapters.pdok_cityjson import (
    NearbyBuilding,
    nearby_buildings_from_stream,
    wgs84_to_rd,
)
from solar.adapters.pdok_geometry import (
    CityJsonContext,
    SelectedPartGeometry,
    load_selected_parts,
    load_selected_vertices,
    read_cityjson_context,
    required_vertex_indices,
)
from solar.geometry.cityjson_mesh import build_parent_meshes


@dataclass
class NetherlandsLocalScene:
    x: float
    y: float
    context: CityJsonContext
    buildings: list[NearbyBuilding]
    geometries: list[SelectedPartGeometry]
    vertices: dict[int, tuple[float, float, float]]
    meshes: dict[str, trimesh.Trimesh]


def load_cached_pdok_scene(
    *,
    archive_path: Path,
    lat: float,
    lon: float,
    radius_m: float,
) -> NetherlandsLocalScene:
    """Load a local LoD2 scene around one WGS84 point."""

    x, y = wgs84_to_rd(
        lat=lat,
        lon=lon,
    )

    if not archive_path.exists():
        raise FileNotFoundError(
            f"PDOK archive not found: {archive_path}"
        )

    with ZipFile(archive_path) as archive:
        members = [
            info.filename
            for info in archive.infolist()
            if info.filename.lower().endswith(
                ".city.json"
            )
        ]

        if len(members) != 1:
            raise ValueError(
                "Expected exactly one CityJSON member, "
                f"found {len(members)}."
            )

        member = members[0]

        with archive.open(member) as source:
            buildings = nearby_buildings_from_stream(
                source,
                target_x=x,
                target_y=y,
                radius_m=radius_m,
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

    meshes = build_parent_meshes(
        geometries,
        vertices=vertices,
        local_origin=(
            x,
            y,
            0.0,
        ),
    )

    return NetherlandsLocalScene(
        x=x,
        y=y,
        context=context,
        buildings=buildings,
        geometries=geometries,
        vertices=vertices,
        meshes=meshes,
    )

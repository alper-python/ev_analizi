"""Build a real Netherlands Solar scene from an address only."""

from __future__ import annotations

from pathlib import Path

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_pipeline import (
    load_netherlands_address_scene,
)
from solar.geometry.facade_exposure import (
    FacadeExposureClass,
    measure_facade_exposure,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)


POSTCODE = "3512JE"
HOUSE_NUMBER = 22
RADIUS_M = 75.0


def main():
    print(
        "DomiFrame Solar — address to real Netherlands 3D scene"
    )
    print("=" * 78)

    scene = load_netherlands_address_scene(
        postcode=POSTCODE,
        house_number=HOUSE_NUMBER,
        radius_m=RADIUS_M,
    )

    print()
    print("ADDRESS")
    print("-" * 78)

    print(
        f"Street:          {scene.address.street}"
    )

    print(
        f"Number:          {scene.address.house_number}"
    )

    print(
        f"Postcode:        {scene.address.postcode}"
    )

    print(
        f"City:            {scene.address.city}"
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
        f"Target Pand:     {scene.target_id}"
    )

    print()
    print("PDOK 3D TILES")
    print("-" * 78)

    for tile, archive in zip(
        scene.tiles,
        scene.archive_paths,
    ):
        print(
            f"Sheet:           {tile.sheet_id}"
        )

        print(
            f"Imagery year:    {tile.imagery_year}"
        )

        print(
            f"Archive:         {archive}"
        )

        print()

    print("LOCAL SCENE")
    print("-" * 78)

    print(
        f"Radius:          {scene.radius_m:.1f} m"
    )

    print(
        f"Tiles:           {len(scene.tiles)}"
    )

    print(
        f"Unique buildings:{len(scene.meshes):>8}"
    )

    print(
        f"Duplicate IDs:   "
        f"{len(scene.duplicate_building_ids)}"
    )

    target_mesh = (
        scene.target_mesh
    )

    print(
        f"Target faces:    {len(target_mesh.faces)}"
    )

    print(
        f"Target area:     {target_mesh.area:.2f} m2"
    )

    print(
        f"Target bounds:   {target_mesh.bounds}"
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

    roofs = [
        item
        for item in surfaces
        if item.surface_type
        == "RoofSurface"
    ]

    walls = [
        item
        for item in surfaces
        if item.surface_type
        == "WallSurface"
    ]

    neighbour_meshes = [
        mesh
        for building_id, mesh
        in scene.meshes.items()
        if building_id
        != scene.target_id
    ]

    reports = measure_facade_exposure(
        surfaces,
        neighbour_meshes=neighbour_meshes,
    )

    party_walls = [
        wall
        for wall in walls
        if (
            reports[
                wall.surface_id
            ].classification
            == FacadeExposureClass.PARTY_WALL
        )
    ]

    print()
    print("TARGET SURFACES")
    print("-" * 78)

    print(
        f"Analyzable:      {len(surfaces)}"
    )

    print(
        f"Roofs:           {len(roofs)} "
        f"({sum(item.area_m2 for item in roofs):.2f} m2)"
    )

    print(
        f"Walls:           {len(walls)} "
        f"({sum(item.area_m2 for item in walls):.2f} m2)"
    )

    print(
        f"Party walls:     {len(party_walls)} "
        f"({sum(item.area_m2 for item in party_walls):.2f} m2)"
    )

    print()
    print("PIPELINE")
    print("-" * 78)

    print(
        "PASS — address -> BAG Pand -> PDOK tile -> "
        "LoD2 scene -> exact target surfaces"
    )


if __name__ == "__main__":
    main()

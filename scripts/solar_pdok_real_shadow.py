"""Run the first real DomiFrame Solar shadow analysis on PDOK LoD2 data.

The test uses an ordinary-scale building near the Utrecht development point
rather than the nearest object, which is a very tall historic structure.

Three visibility states are compared:

1. open sky:
   only solar geometry and surface orientation

2. own building:
   self-shading from the target building

3. full scene:
   target building plus surrounding PDOK buildings

This lets us separate local-neighbour obstruction from the building's own
geometry.
"""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd
import trimesh


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
from solar.geometry.cityjson_mesh import (
    build_parent_meshes,
    semantic_surfaces,
    triangulate_surface,
)
from solar.geometry.raycast import ShadowEngine
from solar.position.solar_position import (
    solar_positions,
)


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"

RADIUS_M = 75.0

# Ordinary-scale building from the discovered Utrecht neighbourhood.
# This is only a technical prototype target, not an NL-A validation home.
TARGET_ID = "NL.IMBAG.Pand.0344100000024581"

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)

SAMPLE_COUNT = 500
SAMPLE_SEED = 42


def load_scene():
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

        with archive.open(
            member
        ) as transform_source, archive.open(
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

    return (
        x,
        y,
        buildings,
        geometries,
        vertices,
        meshes,
    )


def area_weighted_normal(
    mesh: trimesh.Trimesh,
) -> np.ndarray:
    normal = (
        mesh.face_normals
        * mesh.area_faces[:, None]
    ).sum(axis=0)

    length = np.linalg.norm(normal)

    if length <= 1e-12:
        raise ValueError(
            "Surface has no stable area-weighted normal."
        )

    return normal / length


def choose_largest_roof(
    *,
    geometries,
    vertices,
    x,
    y,
):
    target_geometries = [
        geometry
        for geometry in geometries
        if geometry.parent_id == TARGET_ID
    ]

    if not target_geometries:
        raise RuntimeError(
            f"No LoD geometry found for target {TARGET_ID}."
        )

    candidates = []

    for geometry in target_geometries:
        for surface in semantic_surfaces(
            geometry
        ):
            if surface.surface_type != "RoofSurface":
                continue

            mesh = triangulate_surface(
                surface,
                vertices=vertices,
                local_origin=(
                    x,
                    y,
                    0.0,
                ),
            )

            if mesh.is_empty:
                continue

            candidates.append(
                (
                    float(mesh.area),
                    surface,
                    mesh,
                )
            )

    if not candidates:
        raise RuntimeError(
            "No usable RoofSurface found for target."
        )

    candidates.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return candidates[0]


def positions_for(date: str):
    times = pd.date_range(
        f"{date} 00:00",
        periods=24,
        freq="1h",
        tz=TZ,
    )

    return solar_positions(
        times,
        lat=LAT,
        lon=LON,
    )


def analyze_day(
    date: str,
    *,
    sample_points: np.ndarray,
    surface_normal: np.ndarray,
    open_engine: ShadowEngine,
    own_engine: ShadowEngine,
    full_engine: ShadowEngine,
):
    positions = positions_for(
        date
    )

    rows = []

    for timestamp, row in positions.iterrows():
        azimuth = float(
            row["azimuth"]
        )

        elevation = float(
            row["apparent_elevation"]
        )

        open_pct = open_engine.sunlit_area_pct(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        own_pct = own_engine.sunlit_area_pct(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        full_pct = full_engine.sunlit_area_pct(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        rows.append(
            {
                "timestamp": timestamp,
                "azimuth": azimuth,
                "elevation": elevation,
                "open": open_pct,
                "own": own_pct,
                "full": full_pct,
                "self_loss": (
                    open_pct - own_pct
                ),
                "neighbour_loss": (
                    own_pct - full_pct
                ),
            }
        )

    return pd.DataFrame(
        rows
    ).set_index(
        "timestamp"
    )


def print_day(
    date: str,
    result: pd.DataFrame,
):
    print()
    print(date)
    print("-" * 104)

    print(
        "time   azimuth   elevation"
        "   open sky   own building"
        "   full scene   neighbour loss"
    )

    relevant = result[
        result["open"] > 0.0
    ]

    for timestamp, row in relevant.iterrows():
        print(
            f"{timestamp:%H:%M}"
            f"   {row['azimuth']:7.2f}°"
            f"   {row['elevation']:8.2f}°"
            f"   {row['open']:8.2f}%"
            f"   {row['own']:11.2f}%"
            f"   {row['full']:10.2f}%"
            f"   {row['neighbour_loss']:12.2f} pp"
        )

    print()

    open_hours = (
        result["open"].sum()
        / 100.0
    )

    own_hours = (
        result["own"].sum()
        / 100.0
    )

    full_hours = (
        result["full"].sum()
        / 100.0
    )

    print(
        "Sampled equivalent direct-sun exposure"
    )

    print(
        f"  open sky:          "
        f"{open_hours:.2f} h"
    )

    print(
        f"  own building:      "
        f"{own_hours:.2f} h"
    )

    print(
        f"  full surroundings: "
        f"{full_hours:.2f} h"
    )

    print(
        f"  neighbour impact:  "
        f"{own_hours - full_hours:.2f} h"
    )


def main():
    print(
        "DomiFrame Solar — first real PDOK shadow analysis"
    )
    print("=" * 72)

    (
        x,
        y,
        buildings,
        geometries,
        vertices,
        meshes,
    ) = load_scene()

    if TARGET_ID not in meshes:
        raise RuntimeError(
            f"Target mesh not available: {TARGET_ID}"
        )

    roof_area, surface, roof_mesh = (
        choose_largest_roof(
            geometries=geometries,
            vertices=vertices,
            x=x,
            y=y,
        )
    )

    surface_normal = area_weighted_normal(
        roof_mesh
    )

    sample_points, _ = (
        trimesh.sample.sample_surface(
            roof_mesh,
            count=SAMPLE_COUNT,
            seed=SAMPLE_SEED,
        )
    )

    own_mesh = meshes[
        TARGET_ID
    ]

    full_meshes = list(
        meshes.values()
    )

    open_engine = ShadowEngine()

    own_engine = ShadowEngine(
        [own_mesh]
    )

    full_engine = ShadowEngine(
        full_meshes
    )

    print()
    print(f"Scene buildings:   {len(buildings)}")
    print(f"Target:            {TARGET_ID}")
    print(f"Target mesh area:  {own_mesh.area:.2f} m2")
    print(f"Selected roof:     {roof_area:.2f} m2")
    print(f"Sample points:     {len(sample_points)}")

    print(
        "Roof normal:       "
        + ", ".join(
            f"{value:.4f}"
            for value in surface_normal
        )
    )

    horizontal_length = np.hypot(
        surface_normal[0],
        surface_normal[1],
    )

    tilt = np.degrees(
        np.arctan2(
            horizontal_length,
            surface_normal[2],
        )
    )

    azimuth = (
        np.degrees(
            np.arctan2(
                surface_normal[0],
                surface_normal[1],
            )
        )
        + 360.0
    ) % 360.0

    print(
        f"Roof azimuth:      {azimuth:.2f}°"
    )

    print(
        f"Roof tilt:         {tilt:.2f}°"
    )

    for date in (
        "2026-06-21",
        "2026-12-21",
    ):
        result = analyze_day(
            date,
            sample_points=sample_points,
            surface_normal=surface_normal,
            open_engine=open_engine,
            own_engine=own_engine,
            full_engine=full_engine,
        )

        print_day(
            date,
            result,
        )


if __name__ == "__main__":
    main()

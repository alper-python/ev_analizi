"""Extract analyzable roof and facade surfaces from real LoD geometry."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

import numpy as np
import trimesh

from solar.adapters.pdok_geometry import SelectedPartGeometry
from solar.geometry.cityjson_mesh import (
    DegenerateSurfaceError,
    semantic_surfaces,
    triangulate_surface,
)


@dataclass(frozen=True)
class AnalyzableSurface:
    surface_id: str
    parent_id: str
    part_id: str
    surface_type: str
    area_m2: float
    azimuth_deg: float | None
    tilt_deg: float
    normal: tuple[float, float, float]
    mesh: trimesh.Trimesh


def area_weighted_normal(
    mesh: trimesh.Trimesh,
) -> np.ndarray:
    """Return a normalized area-weighted surface normal."""

    if mesh.is_empty or len(mesh.faces) == 0:
        raise ValueError(
            "Cannot calculate a normal for an empty mesh."
        )

    vector = (
        mesh.face_normals
        * mesh.area_faces[:, None]
    ).sum(axis=0)

    length = float(
        np.linalg.norm(vector)
    )

    if length <= 1e-12:
        raise ValueError(
            "Surface has no stable area-weighted normal."
        )

    return vector / length


def normal_to_orientation(
    normal: np.ndarray,
) -> tuple[float | None, float]:
    """Convert an outward normal into azimuth and tilt.

    Azimuth:
        0 = North
        90 = East
        180 = South
        270 = West

    Tilt:
        0 = horizontal/upward
        90 = vertical

    Horizontal surfaces have no meaningful azimuth and return None.
    """

    vector = np.asarray(
        normal,
        dtype=float,
    )

    if vector.shape != (3,):
        raise ValueError(
            "normal must have shape (3,)."
        )

    length = float(
        np.linalg.norm(vector)
    )

    if length <= 1e-12:
        raise ValueError(
            "normal cannot have zero length."
        )

    vector = vector / length

    horizontal = float(
        np.hypot(
            vector[0],
            vector[1],
        )
    )

    tilt = math.degrees(
        math.atan2(
            horizontal,
            vector[2],
        )
    )

    # A very small roof slope can produce an arbitrary-looking azimuth.
    # Keep the true normal and tilt for physical calculations, but do not
    # present an orientation to users for effectively flat surfaces.
    if tilt < 5.0:
        azimuth = None
    else:
        azimuth = (
            math.degrees(
                math.atan2(
                    vector[0],
                    vector[1],
                )
            )
            + 360.0
        ) % 360.0

    return azimuth, tilt


def extract_analyzable_surfaces(
    geometries: Iterable[SelectedPartGeometry],
    *,
    vertices: dict[int, tuple[float, float, float]],
    parent_id: str,
    local_origin: tuple[float, float, float],
    include_surface_types: set[str] | None = None,
    min_area_m2: float = 0.05,
) -> list[AnalyzableSurface]:
    """Extract individual real roof/facade surfaces for one building."""

    allowed = (
        include_surface_types
        if include_surface_types is not None
        else {
            "RoofSurface",
            "WallSurface",
        }
    )

    result: list[AnalyzableSurface] = []

    surface_index = 0

    for geometry in geometries:
        if geometry.parent_id != parent_id:
            continue

        for surface in semantic_surfaces(
            geometry
        ):
            if surface.surface_type not in allowed:
                continue

            try:
                mesh = triangulate_surface(
                    surface,
                    vertices=vertices,
                    local_origin=local_origin,
                )
            except DegenerateSurfaceError:
                continue

            area = float(
                mesh.area
            )

            if area < min_area_m2:
                continue

            normal = area_weighted_normal(
                mesh
            )

            azimuth, tilt = normal_to_orientation(
                normal
            )

            surface_index += 1

            result.append(
                AnalyzableSurface(
                    surface_id=(
                        f"{parent_id}:"
                        f"{surface.surface_type}:"
                        f"{surface_index}"
                    ),
                    parent_id=parent_id,
                    part_id=surface.part_id,
                    surface_type=surface.surface_type,
                    area_m2=area,
                    azimuth_deg=azimuth,
                    tilt_deg=tilt,
                    normal=tuple(
                        float(value)
                        for value in normal
                    ),
                    mesh=mesh,
                )
            )

    result.sort(
        key=lambda item: (
            item.surface_type,
            -item.area_m2,
            item.surface_id,
        )
    )

    return result

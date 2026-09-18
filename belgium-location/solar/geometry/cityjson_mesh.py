"""Convert selected PDOK CityJSON semantic surfaces into Trimesh geometry."""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Integral
from typing import Any, Iterable

import numpy as np
import trimesh
from shapely import make_valid
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.validation import explain_validity

from solar.adapters.pdok_geometry import SelectedPartGeometry


class DegenerateSurfaceError(ValueError):
    """A CityJSON surface has no measurable polygon area.

    These surfaces may safely be excluded from the shadow mesh because they
    cannot represent a physical area capable of blocking or receiving direct
    sunlight.
    """


@dataclass(frozen=True)
class SemanticSurface:
    parent_id: str
    part_id: str
    surface_type: str
    rings: tuple[tuple[int, ...], ...]


def _semantic_index(
    values: Any,
    shell_index: int,
    surface_index: int,
) -> int | None:
    try:
        value = values[shell_index][surface_index]
    except (IndexError, TypeError):
        return None

    while isinstance(value, list) and len(value) == 1:
        value = value[0]

    if isinstance(value, Integral):
        return int(value)

    return None


def semantic_surfaces(
    geometry: SelectedPartGeometry,
) -> list[SemanticSurface]:
    """Extract semantic polygon surfaces from a CityJSON Solid."""

    if geometry.geometry_type != "Solid":
        return []

    boundaries = geometry.boundaries or []
    semantics = geometry.semantics or {}

    definitions = semantics.get("surfaces") or []
    values = semantics.get("values") or []

    results: list[SemanticSurface] = []

    for shell_index, shell in enumerate(boundaries):
        if not isinstance(shell, list):
            continue

        for surface_index, rings in enumerate(shell):
            if not isinstance(rings, list):
                continue

            semantic_index = _semantic_index(
                values,
                shell_index,
                surface_index,
            )

            if (
                semantic_index is None
                or semantic_index < 0
                or semantic_index >= len(definitions)
            ):
                surface_type = "Unknown"
            else:
                surface_type = str(
                    definitions[semantic_index].get(
                        "type",
                        "Unknown",
                    )
                )

            normalized_rings = []

            for ring in rings:
                if not isinstance(ring, list):
                    continue

                indices = tuple(
                    int(value)
                    for value in ring
                    if isinstance(value, Integral)
                )

                if len(indices) >= 3:
                    normalized_rings.append(indices)

            if normalized_rings:
                results.append(
                    SemanticSurface(
                        parent_id=geometry.parent_id,
                        part_id=geometry.part_id,
                        surface_type=surface_type,
                        rings=tuple(normalized_rings),
                    )
                )

    return results


def _newell_normal(
    points: np.ndarray,
) -> np.ndarray:
    """Calculate a stable polygon normal using Newell's method."""

    normal = np.zeros(3, dtype=float)

    for index, current in enumerate(points):
        nxt = points[
            (index + 1) % len(points)
        ]

        normal[0] += (
            current[1] - nxt[1]
        ) * (
            current[2] + nxt[2]
        )

        normal[1] += (
            current[2] - nxt[2]
        ) * (
            current[0] + nxt[0]
        )

        normal[2] += (
            current[0] - nxt[0]
        ) * (
            current[1] + nxt[1]
        )

    length = np.linalg.norm(normal)

    if length <= 1e-12:
        raise ValueError(
            "Degenerate CityJSON polygon has no stable normal."
        )

    return normal / length


def _plane_basis(
    exterior: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    origin = exterior[0]
    normal = _newell_normal(exterior)

    reference = np.array(
        [0.0, 0.0, 1.0],
        dtype=float,
    )

    if abs(float(np.dot(normal, reference))) > 0.95:
        reference = np.array(
            [0.0, 1.0, 0.0],
            dtype=float,
        )

    axis_u = np.cross(reference, normal)
    axis_u /= np.linalg.norm(axis_u)

    axis_v = np.cross(normal, axis_u)
    axis_v /= np.linalg.norm(axis_v)

    return origin, axis_u, axis_v


def _project_ring(
    points: np.ndarray,
    *,
    origin: np.ndarray,
    axis_u: np.ndarray,
    axis_v: np.ndarray,
) -> list[tuple[float, float]]:
    relative = points - origin

    return [
        (
            float(np.dot(point, axis_u)),
            float(np.dot(point, axis_v)),
        )
        for point in relative
    ]



def _deduplicate_ring(
    points: np.ndarray,
    *,
    tolerance: float = 1e-9,
) -> np.ndarray:
    """Remove repeated consecutive points from a polygon ring."""

    if len(points) == 0:
        return points

    cleaned = [points[0]]

    for point in points[1:]:
        if np.linalg.norm(point - cleaned[-1]) > tolerance:
            cleaned.append(point)

    result = np.asarray(
        cleaned,
        dtype=float,
    )

    # CityJSON may explicitly repeat the first vertex at the end.
    if (
        len(result) >= 2
        and np.linalg.norm(result[0] - result[-1]) <= tolerance
    ):
        result = result[:-1]

    return result


def _polygon_parts(geometry) -> list[Polygon]:
    """Extract usable Polygon components from repaired geometry."""

    if isinstance(geometry, Polygon):
        return (
            [geometry]
            if not geometry.is_empty and geometry.area > 1e-10
            else []
        )

    if isinstance(geometry, MultiPolygon):
        return [
            polygon
            for polygon in geometry.geoms
            if not polygon.is_empty and polygon.area > 1e-10
        ]

    if isinstance(geometry, GeometryCollection):
        result: list[Polygon] = []

        for item in geometry.geoms:
            result.extend(
                _polygon_parts(item)
            )

        return result

    return []


def triangulate_surface(
    surface: SemanticSurface,
    *,
    vertices: dict[int, tuple[float, float, float]],
    local_origin: tuple[float, float, float] = (
        0.0,
        0.0,
        0.0,
    ),
) -> trimesh.Trimesh:
    """Triangulate one semantic CityJSON surface.

    Real LoD building data can contain repeated ring points or polygons
    which become self-intersecting after planar projection. Invalid
    projected polygons are repaired with Shapely before triangulation.
    """

    rings_3d: list[np.ndarray] = []

    for ring in surface.rings:
        points = np.array(
            [
                vertices[index]
                for index in ring
            ],
            dtype=float,
        )

        points = _deduplicate_ring(
            points
        )

        if len(points) < 3:
            continue

        rings_3d.append(points)

    if not rings_3d:
        raise DegenerateSurfaceError(
            "Semantic surface contains no valid polygon rings: "
            f"parent={surface.parent_id} "
            f"part={surface.part_id} "
            f"type={surface.surface_type}"
        )

    exterior = rings_3d[0]

    origin, axis_u, axis_v = _plane_basis(
        exterior
    )

    exterior_2d = _project_ring(
        exterior,
        origin=origin,
        axis_u=axis_u,
        axis_v=axis_v,
    )

    holes_2d = [
        _project_ring(
            ring,
            origin=origin,
            axis_u=axis_u,
            axis_v=axis_v,
        )
        for ring in rings_3d[1:]
    ]

    polygon = Polygon(
        exterior_2d,
        holes_2d,
    )

    if polygon.is_empty:
        raise DegenerateSurfaceError(
            "Projected semantic surface is empty: "
            f"parent={surface.parent_id} "
            f"part={surface.part_id} "
            f"type={surface.surface_type}"
        )

    original_validity = (
        None
        if polygon.is_valid
        else explain_validity(polygon)
    )

    repaired = (
        polygon
        if polygon.is_valid
        else make_valid(polygon)
    )

    polygon_parts = _polygon_parts(
        repaired
    )

    if not polygon_parts:
        raise ValueError(
            "Projected semantic surface could not be repaired: "
            f"parent={surface.parent_id} "
            f"part={surface.part_id} "
            f"type={surface.surface_type} "
            f"reason={original_validity}"
        )

    local_origin_array = np.asarray(
        local_origin,
        dtype=float,
    )

    meshes: list[trimesh.Trimesh] = []

    for polygon_part in polygon_parts:
        vertices_2d, faces = (
            trimesh.creation.triangulate_polygon(
                polygon_part,
                engine="earcut",
            )
        )

        vertices_2d = np.asarray(
            vertices_2d,
            dtype=float,
        )

        faces = np.asarray(
            faces,
            dtype=np.int64,
        )

        if (
            len(vertices_2d) == 0
            or len(faces) == 0
        ):
            continue

        vertices_3d = np.array(
            [
                origin
                + xy[0] * axis_u
                + xy[1] * axis_v
                - local_origin_array
                for xy in vertices_2d
            ],
            dtype=float,
        )

        mesh = trimesh.Trimesh(
            vertices=vertices_3d,
            faces=faces,
            process=False,
        )

        if not mesh.is_empty:
            meshes.append(mesh)

    if not meshes:
        raise DegenerateSurfaceError(
            "Semantic surface produced no triangles: "
            f"parent={surface.parent_id} "
            f"part={surface.part_id} "
            f"type={surface.surface_type}"
        )

    if len(meshes) == 1:
        return meshes[0]

    return trimesh.util.concatenate(
        meshes
    )


def build_parent_meshes(
    geometries: Iterable[SelectedPartGeometry],
    *,
    vertices: dict[int, tuple[float, float, float]],
    local_origin: tuple[float, float, float],
    include_surface_types: set[str] | None = None,
) -> dict[str, trimesh.Trimesh]:
    """Build one combined mesh for each parent Building."""

    allowed = (
        include_surface_types
        if include_surface_types is not None
        else {
            "RoofSurface",
            "WallSurface",
            "GroundSurface",
        }
    )

    grouped: dict[str, list[trimesh.Trimesh]] = {}

    for geometry in geometries:
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
                # PDOK may contain zero-area primitives such as rings with
                # repeated identical vertices. They cannot block sunlight
                # and are safe to exclude from the ray-casting mesh.
                continue

            if mesh.is_empty:
                continue

            grouped.setdefault(
                surface.parent_id,
                [],
            ).append(mesh)

    result: dict[str, trimesh.Trimesh] = {}

    for parent_id, meshes in grouped.items():
        if len(meshes) == 1:
            result[parent_id] = meshes[0]
        else:
            result[parent_id] = (
                trimesh.util.concatenate(meshes)
            )

    return result

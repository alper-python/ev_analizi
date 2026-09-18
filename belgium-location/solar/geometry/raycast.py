"""Direct-sun visibility and shadow calculations."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import trimesh

from solar.position.solar_position import sun_direction


_RAY_ORIGIN_EPSILON_M = 1e-5
_FACING_EPSILON = 1e-12


def _normalize_surface_normal(surface_normal: np.ndarray) -> np.ndarray:
    normal = np.asarray(surface_normal, dtype=float)

    if normal.shape != (3,):
        raise ValueError("surface_normal must have shape (3,).")

    norm = np.linalg.norm(normal)

    if norm == 0:
        raise ValueError("surface_normal cannot have zero length.")

    return normal / norm


class ShadowEngine:
    """Determine whether surface samples receive direct sunlight."""

    def __init__(
        self,
        obstacles: Iterable[trimesh.Trimesh] | None = None,
    ) -> None:
        obstacle_list = list(obstacles or [])

        self._has_obstacles = bool(obstacle_list)
        self._intersector = None

        if not obstacle_list:
            return

        meshes: list[trimesh.Trimesh] = []

        for obstacle in obstacle_list:
            if not isinstance(obstacle, trimesh.Trimesh):
                raise TypeError(
                    "Every obstacle must be a trimesh.Trimesh instance."
                )

            if obstacle.is_empty:
                continue

            meshes.append(obstacle)

        if not meshes:
            self._has_obstacles = False
            return

        if len(meshes) == 1:
            scene_mesh = meshes[0].copy()
        else:
            scene_mesh = trimesh.util.concatenate(
                [mesh.copy() for mesh in meshes]
            )

        try:
            from trimesh.ray.ray_pyembree import RayMeshIntersector

            self._intersector = RayMeshIntersector(scene_mesh)

        except ImportError as exc:
            raise RuntimeError(
                "The Solar prototype requires the embreex-backed "
                "trimesh ray intersector."
            ) from exc

    def sunlight_mask(
        self,
        sample_points: np.ndarray,
        *,
        surface_normal: np.ndarray,
        sun_azimuth_deg: float,
        sun_elevation_deg: float,
    ) -> np.ndarray:
        """Return one boolean per sample point.

        A point receives direct sunlight only if:

        1. the sun is above the horizon;
        2. the surface faces the sun;
        3. no obstacle intersects the ray toward the sun.
        """

        points = np.asarray(sample_points, dtype=float)

        if points.ndim != 2 or points.shape[1] != 3:
            raise ValueError(
                "sample_points must have shape (n, 3)."
            )

        if len(points) == 0:
            return np.zeros(0, dtype=bool)

        if sun_elevation_deg <= 0:
            return np.zeros(len(points), dtype=bool)

        direction = sun_direction(
            azimuth_deg=sun_azimuth_deg,
            elevation_deg=sun_elevation_deg,
        )

        normal = _normalize_surface_normal(surface_normal)

        # A planar surface receives direct beam radiation only from the
        # hemisphere its outward normal faces.
        if float(np.dot(normal, direction)) <= _FACING_EPSILON:
            return np.zeros(len(points), dtype=bool)

        if not self._has_obstacles:
            return np.ones(len(points), dtype=bool)

        directions = np.repeat(
            direction[np.newaxis, :],
            len(points),
            axis=0,
        )

        # Avoid numerical self-intersection when target geometry is later
        # included in the full 3D scene.
        origins = points + directions * _RAY_ORIGIN_EPSILON_M

        blocked = np.asarray(
            self._intersector.intersects_any(
                ray_origins=origins,
                ray_directions=directions,
            ),
            dtype=bool,
        )

        return ~blocked

    def sunlit_area_pct(
        self,
        sample_points: np.ndarray,
        *,
        surface_normal: np.ndarray,
        sun_azimuth_deg: float,
        sun_elevation_deg: float,
    ) -> float:
        """Approximate directly sunlit percentage of a sampled surface."""

        mask = self.sunlight_mask(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=sun_azimuth_deg,
            sun_elevation_deg=sun_elevation_deg,
        )

        if len(mask) == 0:
            return 0.0

        return float(mask.mean() * 100.0)

"""Classify real building walls as exposed, partial or party-wall surfaces.

PDOK LoD geometry can contain WallSurface primitives which are coincident
with an adjacent building. Those surfaces are valid solid boundaries but
must not be presented to users as sun-exposed facades.

The classifier samples each wall and casts rays along the outward normal
against OTHER buildings only.

A coincident neighbouring wall is reached almost immediately and therefore
has near-zero facade clearance.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
from typing import Iterable

import numpy as np
import trimesh
from trimesh.ray.ray_pyembree import RayMeshIntersector

from solar.geometry.real_surfaces import (
    AnalyzableSurface,
)


DEFAULT_PROBE_DEPTH_M = 0.05
DEFAULT_PARTY_CLEARANCE_M = 0.10
DEFAULT_PARTY_COVERAGE_THRESHOLD = 0.80


class FacadeExposureClass(str, Enum):
    EXPOSED = "exposed"
    PARTIALLY_ATTACHED = "partially_attached"
    PARTY_WALL = "party_wall"


@dataclass(frozen=True)
class FacadeExposureReport:
    surface_id: str
    classification: FacadeExposureClass
    sample_count: int
    near_zero_fraction: float
    no_hit_fraction: float
    median_clearance_m: float | None
    min_clearance_m: float | None

    @property
    def near_zero_pct(self) -> float:
        return self.near_zero_fraction * 100.0

    @property
    def no_hit_pct(self) -> float:
        return self.no_hit_fraction * 100.0


def classify_attachment(
    near_zero_fraction: float,
    *,
    party_coverage_threshold: float = (
        DEFAULT_PARTY_COVERAGE_THRESHOLD
    ),
) -> FacadeExposureClass:
    """Classify a wall from the fraction coincident with another building."""

    value = float(
        near_zero_fraction
    )

    if not 0.0 <= value <= 1.0:
        raise ValueError(
            "near_zero_fraction must be between 0 and 1."
        )

    if not 0.0 < party_coverage_threshold <= 1.0:
        raise ValueError(
            "party_coverage_threshold must be in (0, 1]."
        )

    if value >= party_coverage_threshold:
        return FacadeExposureClass.PARTY_WALL

    if value > 0.0:
        return FacadeExposureClass.PARTIALLY_ATTACHED

    return FacadeExposureClass.EXPOSED


def _stable_seed(
    value: str,
) -> int:
    digest = hashlib.sha256(
        value.encode("utf-8")
    ).digest()

    return int.from_bytes(
        digest[:4],
        byteorder="big",
        signed=False,
    )


def _sample_wall(
    surface: AnalyzableSurface,
    *,
    min_samples: int,
    max_samples: int,
) -> np.ndarray:
    count = int(
        round(
            surface.area_m2
        )
    )

    count = max(
        min_samples,
        min(
            max_samples,
            count,
        ),
    )

    points, _ = (
        trimesh.sample.sample_surface(
            surface.mesh,
            count=count,
            seed=_stable_seed(
                surface.surface_id
            ),
        )
    )

    return np.asarray(
        points,
        dtype=float,
    )


def _clearances(
    *,
    intersector: RayMeshIntersector,
    points: np.ndarray,
    normal: np.ndarray,
    probe_depth_m: float,
) -> np.ndarray:
    normal = np.asarray(
        normal,
        dtype=float,
    )

    normal_length = float(
        np.linalg.norm(
            normal
        )
    )

    if normal_length <= 1e-12:
        raise ValueError(
            "Facade normal cannot have zero length."
        )

    normal = (
        normal
        / normal_length
    )

    # Begin slightly inside the target building.
    #
    # The target itself is excluded from the intersector. If another
    # building has a coincident wall, that wall will be reached after
    # roughly probe_depth_m. Subtracting the probe depth therefore yields
    # approximately zero metres of exterior clearance.
    origins = (
        points
        - normal[None, :]
        * probe_depth_m
    )

    directions = np.repeat(
        normal[None, :],
        len(points),
        axis=0,
    )

    locations, ray_indices, _ = (
        intersector.intersects_location(
            ray_origins=origins,
            ray_directions=directions,
            multiple_hits=False,
        )
    )

    result = np.full(
        len(points),
        np.inf,
        dtype=float,
    )

    if len(ray_indices) == 0:
        return result

    distances = np.linalg.norm(
        locations
        - origins[
            ray_indices
        ],
        axis=1,
    )

    result[
        ray_indices
    ] = np.maximum(
        distances
        - probe_depth_m,
        0.0,
    )

    return result


def measure_facade_exposure(
    surfaces: Iterable[AnalyzableSurface],
    *,
    neighbour_meshes: Iterable[trimesh.Trimesh],
    probe_depth_m: float = DEFAULT_PROBE_DEPTH_M,
    party_clearance_m: float = DEFAULT_PARTY_CLEARANCE_M,
    party_coverage_threshold: float = (
        DEFAULT_PARTY_COVERAGE_THRESHOLD
    ),
    min_samples: int = 30,
    max_samples: int = 150,
) -> dict[str, FacadeExposureReport]:
    """Measure attachment/exposure for all WallSurface items."""

    walls = [
        surface
        for surface in surfaces
        if surface.surface_type
        == "WallSurface"
    ]

    neighbours = [
        mesh
        for mesh in neighbour_meshes
        if not mesh.is_empty
    ]

    if not walls:
        return {}

    # With no surrounding building geometry every wall is exposed.
    if not neighbours:
        return {
            surface.surface_id: FacadeExposureReport(
                surface_id=surface.surface_id,
                classification=FacadeExposureClass.EXPOSED,
                sample_count=0,
                near_zero_fraction=0.0,
                no_hit_fraction=1.0,
                median_clearance_m=None,
                min_clearance_m=None,
            )
            for surface in walls
        }

    neighbour_mesh = (
        neighbours[0]
        if len(neighbours) == 1
        else trimesh.util.concatenate(
            neighbours
        )
    )

    intersector = RayMeshIntersector(
        neighbour_mesh
    )

    result: dict[
        str,
        FacadeExposureReport,
    ] = {}

    for surface in walls:
        points = _sample_wall(
            surface,
            min_samples=min_samples,
            max_samples=max_samples,
        )

        clearances = _clearances(
            intersector=intersector,
            points=points,
            normal=np.asarray(
                surface.normal,
                dtype=float,
            ),
            probe_depth_m=probe_depth_m,
        )

        finite = clearances[
            np.isfinite(
                clearances
            )
        ]

        near_zero_fraction = float(
            np.mean(
                clearances
                <= party_clearance_m
            )
        )

        no_hit_fraction = float(
            np.mean(
                ~np.isfinite(
                    clearances
                )
            )
        )

        classification = classify_attachment(
            near_zero_fraction,
            party_coverage_threshold=(
                party_coverage_threshold
            ),
        )

        result[
            surface.surface_id
        ] = FacadeExposureReport(
            surface_id=surface.surface_id,
            classification=classification,
            sample_count=len(points),
            near_zero_fraction=(
                near_zero_fraction
            ),
            no_hit_fraction=(
                no_hit_fraction
            ),
            median_clearance_m=(
                float(
                    np.median(
                        finite
                    )
                )
                if len(finite)
                else None
            ),
            min_clearance_m=(
                float(
                    np.min(
                        finite
                    )
                )
                if len(finite)
                else None
            ),
        )

    return result


def remove_party_walls(
    surfaces: Iterable[AnalyzableSurface],
    reports: dict[str, FacadeExposureReport],
) -> list[AnalyzableSurface]:
    """Remove only confidently classified party walls.

    Partially attached walls remain in the analysis until a later geometry
    step can split their exposed and attached portions.
    """

    result = []

    for surface in surfaces:
        if (
            surface.surface_type
            != "WallSurface"
        ):
            result.append(
                surface
            )
            continue

        report = reports.get(
            surface.surface_id
        )

        if report is None:
            raise ValueError(
                "Missing facade exposure report for "
                f"{surface.surface_id}."
            )

        if (
            report.classification
            == FacadeExposureClass.PARTY_WALL
        ):
            continue

        result.append(
            surface
        )

    return result

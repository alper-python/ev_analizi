"""Diagnose whether PDOK WallSurfaces are actually exposed facades.

A Building solid can contain WallSurface polygons that coincide with an
adjacent building. Such party walls should not be presented to users as
sun-exposed facades.

For every target wall we sample points and cast rays along its outward
normal against all OTHER buildings. The ray starts slightly inside the
target building so a coincident neighbouring wall is detected close to
zero metres of facade clearance.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import trimesh
from trimesh.ray.ray_pyembree import RayMeshIntersector

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_scene import (
    load_cached_pdok_scene,
)
from solar.aggregation.surface_groups import (
    compass_sector,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)


LAT = 52.0907
LON = 5.1214

TARGET_ID = (
    "NL.IMBAG.Pand.0344100000024581"
)

RADIUS_M = 75.0

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)

PROBE_DEPTH_M = 0.05

THRESHOLDS_M = (
    0.10,
    0.25,
    0.50,
    1.00,
    2.00,
)


def stable_seed(
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


def sample_surface(
    surface,
) -> np.ndarray:
    count = max(
        30,
        min(
            150,
            int(
                round(
                    surface.area_m2
                )
            ),
        ),
    )

    points, _ = (
        trimesh.sample.sample_surface(
            surface.mesh,
            count=count,
            seed=stable_seed(
                surface.surface_id
            ),
        )
    )

    return np.asarray(
        points,
        dtype=float,
    )


def first_hit_clearances(
    *,
    intersector: RayMeshIntersector,
    points: np.ndarray,
    normal: np.ndarray,
) -> np.ndarray:
    """Return distance from facade plane to first OTHER-building hit."""

    normal = np.asarray(
        normal,
        dtype=float,
    )

    normal /= np.linalg.norm(
        normal
    )

    # Start just inside the target building.
    #
    # Because the target mesh is excluded from the intersector, a
    # neighbouring surface coincident with this facade is encountered
    # after roughly PROBE_DEPTH_M. Subtracting the probe depth yields a
    # facade clearance close to zero.
    origins = (
        points
        - normal[None, :]
        * PROBE_DEPTH_M
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

    clearances = np.full(
        len(points),
        np.inf,
        dtype=float,
    )

    if len(ray_indices):
        raw_distances = np.linalg.norm(
            locations
            - origins[
                ray_indices
            ],
            axis=1,
        )

        facade_clearances = np.maximum(
            raw_distances
            - PROBE_DEPTH_M,
            0.0,
        )

        clearances[
            ray_indices
        ] = facade_clearances

    return clearances


def fmt_distance(
    value: float,
) -> str:
    if not np.isfinite(
        value
    ):
        return "open"

    return f"{value:.3f} m"


def main():
    print(
        "DomiFrame Solar — facade exposure clearance diagnostic"
    )
    print("=" * 88)

    scene = load_cached_pdok_scene(
        archive_path=ARCHIVE,
        lat=LAT,
        lon=LON,
        radius_m=RADIUS_M,
    )

    surfaces = extract_analyzable_surfaces(
        scene.geometries,
        vertices=scene.vertices,
        parent_id=TARGET_ID,
        local_origin=(
            scene.x,
            scene.y,
            0.0,
        ),
    )

    walls = [
        surface
        for surface in surfaces
        if surface.surface_type
        == "WallSurface"
    ]

    other_meshes = [
        mesh
        for building_id, mesh
        in scene.meshes.items()
        if building_id != TARGET_ID
    ]

    if not other_meshes:
        raise RuntimeError(
            "No neighbouring building meshes available."
        )

    neighbour_mesh = (
        trimesh.util.concatenate(
            other_meshes
        )
    )

    intersector = RayMeshIntersector(
        neighbour_mesh
    )

    print()
    print(
        f"Scene buildings:       {len(scene.meshes)}"
    )
    print(
        f"Target walls:          {len(walls)}"
    )
    print(
        f"Neighbour mesh faces:  {len(neighbour_mesh.faces)}"
    )
    print(
        f"Probe depth:           {PROBE_DEPTH_M:.2f} m"
    )

    results = []

    for surface in walls:
        points = sample_surface(
            surface
        )

        clearances = first_hit_clearances(
            intersector=intersector,
            points=points,
            normal=np.asarray(
                surface.normal,
                dtype=float,
            ),
        )

        finite = clearances[
            np.isfinite(
                clearances
            )
        ]

        threshold_values = {
            threshold: (
                float(
                    np.mean(
                        clearances
                        <= threshold
                    )
                    * 100.0
                )
            )
            for threshold
            in THRESHOLDS_M
        }

        near_zero_pct = (
            threshold_values[
                0.10
            ]
        )

        results.append(
            {
                "surface": surface,
                "direction": compass_sector(
                    surface.azimuth_deg
                ),
                "samples": len(points),
                "min": (
                    float(
                        np.min(
                            finite
                        )
                    )
                    if len(finite)
                    else np.inf
                ),
                "median": (
                    float(
                        np.median(
                            finite
                        )
                    )
                    if len(finite)
                    else np.inf
                ),
                "no_hit_pct": float(
                    np.mean(
                        ~np.isfinite(
                            clearances
                        )
                    )
                    * 100.0
                ),
                "thresholds": threshold_values,
                # Diagnostic only. Do NOT use this as a production
                # classification rule yet.
                "party_candidate": (
                    near_zero_pct
                    >= 80.0
                ),
            }
        )

    results.sort(
        key=lambda item: (
            item["direction"],
            -item["surface"].area_m2,
        )
    )

    print()
    print(
        "surface                         "
        "dir     area   samples   "
        "<=0.10m  <=0.25m  <=0.50m  <=1m   <=2m   "
        "median clearance   no hit"
    )
    print("-" * 140)

    for item in results:
        surface = item[
            "surface"
        ]

        thresholds = item[
            "thresholds"
        ]

        print(
            f"{surface.surface_id.split(':')[-1]:<31}"
            f"{item['direction']:>4}"
            f"{surface.area_m2:9.2f}"
            f"{item['samples']:10d}"
            f"{thresholds[0.10]:10.1f}%"
            f"{thresholds[0.25]:10.1f}%"
            f"{thresholds[0.50]:10.1f}%"
            f"{thresholds[1.00]:8.1f}%"
            f"{thresholds[2.00]:8.1f}%"
            f"{fmt_distance(item['median']):>19}"
            f"{item['no_hit_pct']:9.1f}%"
        )

    print()
    print("=" * 88)
    print("PARTY-WALL CANDIDATES — diagnostic only")
    print("=" * 88)

    candidates = [
        item
        for item in results
        if item[
            "party_candidate"
        ]
    ]

    if not candidates:
        print(
            "No wall has >=80% of samples with <=0.10 m clearance."
        )
    else:
        for item in sorted(
            candidates,
            key=lambda value:
            -value["surface"].area_m2,
        ):
            surface = item[
                "surface"
            ]

            print(
                f"{surface.surface_id}"
            )

            print(
                f"  direction: "
                f"{item['direction']}"
            )

            print(
                f"  azimuth:   "
                f"{surface.azimuth_deg:.1f}°"
            )

            print(
                f"  area:      "
                f"{surface.area_m2:.2f} m2"
            )

            print(
                f"  <= 0.10 m: "
                f"{item['thresholds'][0.10]:.1f}%"
            )

            print(
                f"  <= 0.25 m: "
                f"{item['thresholds'][0.25]:.1f}%"
            )

            print()


if __name__ == "__main__":
    main()

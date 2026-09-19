"""Diagnose grazing-angle ray self-intersection in ShadowEngine.

Compares:
- current ShadowEngine
- raw Embree rays offset toward the sun
- raw Embree rays offset along the outward surface normal

The target is the exposed south facade where the existing blocker
attribution revealed a late-afternoon mismatch.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import trimesh
from trimesh.ray.ray_pyembree import RayMeshIntersector

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.netherlands_scene import (
    load_cached_pdok_scene,
)
from solar.geometry.raycast import (
    ShadowEngine,
    sun_direction,
)
from solar.geometry.real_surfaces import (
    extract_analyzable_surfaces,
)
from solar.position.solar_position import (
    solar_positions,
)


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"

TARGET_ID = (
    "NL.IMBAG.Pand.0344100000024581"
)

TARGET_SURFACE_SUFFIX = (
    ":WallSurface:4"
)

RADIUS_M = 75.0

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)

SAMPLE_COUNT = 1000

EPSILONS = (
    1e-8,
    1e-7,
    1e-6,
    1e-5,
    1e-4,
    1e-3,
    1e-2,
)

HOURS = (
    12,
    13,
    14,
    15,
    16,
    17,
)

DATE = "2026-06-21"


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


def raw_sunlit_pct(
    *,
    intersector,
    points,
    sun_vector,
    surface_normal,
    epsilon,
    mode,
):
    if mode == "sun":
        offset_vector = sun_vector

    elif mode == "normal":
        offset_vector = surface_normal

    elif mode == "combined":
        vector = (
            surface_normal
            + sun_vector
        )

        vector /= np.linalg.norm(
            vector
        )

        offset_vector = vector

    else:
        raise ValueError(
            f"Unknown offset mode: {mode}"
        )

    origins = (
        points
        + offset_vector[None, :]
        * epsilon
    )

    directions = np.repeat(
        sun_vector[None, :],
        len(points),
        axis=0,
    )

    blocked = intersector.intersects_any(
        origins,
        directions,
    )

    return (
        float(
            np.mean(
                ~blocked
            )
        )
        * 100.0
    )


def main():
    print(
        "DomiFrame Solar — grazing-angle epsilon diagnostic"
    )
    print("=" * 108)

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

    matches = [
        surface
        for surface in surfaces
        if surface.surface_id.endswith(
            TARGET_SURFACE_SUFFIX
        )
    ]

    if len(matches) != 1:
        raise RuntimeError(
            "Expected exactly one target surface."
        )

    surface = matches[0]

    points, _ = trimesh.sample.sample_surface(
        surface.mesh,
        count=SAMPLE_COUNT,
        seed=stable_seed(
            surface.surface_id
        ),
    )

    points = np.asarray(
        points,
        dtype=float,
    )

    normal = np.asarray(
        surface.normal,
        dtype=float,
    )

    own_mesh = scene.meshes[
        TARGET_ID
    ]

    full_mesh = trimesh.util.concatenate(
        list(
            scene.meshes.values()
        )
    )

    own_intersector = RayMeshIntersector(
        own_mesh
    )

    full_intersector = RayMeshIntersector(
        full_mesh
    )

    own_engine = ShadowEngine(
        [own_mesh]
    )

    full_engine = ShadowEngine(
        list(
            scene.meshes.values()
        )
    )

    times = pd.DatetimeIndex(
        [
            pd.Timestamp(
                f"{DATE} {hour:02d}:00",
                tz=TZ,
            )
            for hour in HOURS
        ]
    )

    positions = solar_positions(
        times,
        lat=LAT,
        lon=LON,
    )

    print()
    print(
        f"Surface:   {surface.surface_id}"
    )
    print(
        f"Area:      {surface.area_m2:.2f} m2"
    )
    print(
        f"Azimuth:   {surface.azimuth_deg:.2f}°"
    )
    print(
        f"Normal:    {normal}"
    )
    print(
        f"Samples:   {len(points)}"
    )

    for timestamp, position in positions.iterrows():
        azimuth = float(
            position["azimuth"]
        )

        elevation = float(
            position[
                "apparent_elevation"
            ]
        )

        sun = sun_direction(
            azimuth_deg=azimuth,
            elevation_deg=elevation,
        )

        facing = float(
            np.dot(
                normal,
                sun,
            )
        )

        if facing <= 0.0:
            continue

        engine_own = own_engine.sunlit_area_pct(
            points,
            surface_normal=normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        engine_full = full_engine.sunlit_area_pct(
            points,
            surface_normal=normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        incidence_angle = np.degrees(
            np.arccos(
                np.clip(
                    facing,
                    -1.0,
                    1.0,
                )
            )
        )

        print()
        print("=" * 108)
        print(
            f"{timestamp:%H:%M}  "
            f"sun az={azimuth:.2f}°  "
            f"el={elevation:.2f}°  "
            f"dot={facing:.6f}  "
            f"incidence={incidence_angle:.2f}°"
        )

        print(
            f"Current ShadowEngine: "
            f"own={engine_own:.2f}% "
            f"full={engine_full:.2f}%"
        )

        print()
        print(
            "epsilon       "
            "sun-own   normal-own   combined-own   "
            "sun-full  normal-full  combined-full"
        )
        print("-" * 108)

        for epsilon in EPSILONS:
            own_sun = raw_sunlit_pct(
                intersector=own_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="sun",
            )

            own_normal = raw_sunlit_pct(
                intersector=own_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="normal",
            )

            own_combined = raw_sunlit_pct(
                intersector=own_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="combined",
            )

            full_sun = raw_sunlit_pct(
                intersector=full_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="sun",
            )

            full_normal = raw_sunlit_pct(
                intersector=full_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="normal",
            )

            full_combined = raw_sunlit_pct(
                intersector=full_intersector,
                points=points,
                sun_vector=sun,
                surface_normal=normal,
                epsilon=epsilon,
                mode="combined",
            )

            print(
                f"{epsilon:10.0e}   "
                f"{own_sun:7.2f}%   "
                f"{own_normal:10.2f}%   "
                f"{own_combined:12.2f}%   "
                f"{full_sun:7.2f}%   "
                f"{full_normal:10.2f}%   "
                f"{full_combined:12.2f}%"
            )


if __name__ == "__main__":
    main()

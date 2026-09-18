"""Manual synthetic-shadow smoke test."""

import sys
from pathlib import Path

import numpy as np
import trimesh


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.geometry.raycast import ShadowEngine
from solar.geometry.sampling import sample_vertical_facade


def main():
    facade = sample_vertical_facade(
        x_min=-2.0,
        x_max=2.0,
        y=0.0,
        z_min=0.5,
        z_max=3.0,
        horizontal_samples=20,
        vertical_samples=12,
    )

    obstacle = trimesh.creation.box(
        extents=(8.0, 1.0, 8.0)
    )
    obstacle.apply_translation([0.0, -5.0, 4.0])

    engine = ShadowEngine([obstacle])

    south_facing = np.array([0.0, -1.0, 0.0])

    print("Synthetic narrow-street shadow test")
    print("-----------------------------------")

    for elevation in (5, 10, 15, 20, 30, 40, 50, 60, 70):
        pct = engine.sunlit_area_pct(
            facade,
            surface_normal=south_facing,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=elevation,
        )

        print(
            f"South sun elevation {elevation:>2}°"
            f" -> facade sunlit {pct:6.2f}%"
        )


if __name__ == "__main__":
    main()

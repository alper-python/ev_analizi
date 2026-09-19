import sys
import unittest
from pathlib import Path

import numpy as np
import trimesh


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.geometry.real_surfaces import (  # noqa: E402
    area_weighted_normal,
    normal_to_orientation,
)


class RealSurfaceOrientationTests(unittest.TestCase):
    def test_flat_upward_surface_has_zero_tilt(self):
        azimuth, tilt = normal_to_orientation(
            np.array([0.0, 0.0, 1.0])
        )

        self.assertIsNone(
            azimuth
        )

        self.assertAlmostEqual(
            tilt,
            0.0,
        )

    def test_south_vertical_surface(self):
        azimuth, tilt = normal_to_orientation(
            np.array([0.0, -1.0, 0.0])
        )

        self.assertAlmostEqual(
            azimuth,
            180.0,
        )

        self.assertAlmostEqual(
            tilt,
            90.0,
        )

    def test_east_vertical_surface(self):
        azimuth, tilt = normal_to_orientation(
            np.array([1.0, 0.0, 0.0])
        )

        self.assertAlmostEqual(
            azimuth,
            90.0,
        )

        self.assertAlmostEqual(
            tilt,
            90.0,
        )

    def test_area_weighted_normal_of_horizontal_mesh_points_up(self):
        mesh = trimesh.Trimesh(
            vertices=np.array(
                [
                    [0.0, 0.0, 0.0],
                    [2.0, 0.0, 0.0],
                    [2.0, 2.0, 0.0],
                    [0.0, 2.0, 0.0],
                ]
            ),
            faces=np.array(
                [
                    [0, 1, 2],
                    [0, 2, 3],
                ]
            ),
            process=False,
        )

        normal = area_weighted_normal(
            mesh
        )

        np.testing.assert_allclose(
            normal,
            [0.0, 0.0, 1.0],
            atol=1e-10,
        )


if __name__ == "__main__":
    unittest.main()

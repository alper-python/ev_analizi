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

from solar.geometry.raycast import (  # noqa: E402
    RAY_ORIGIN_OFFSET_M,
    ShadowEngine,
    _offset_ray_origins,
)


class ShadowRayOffsetTests(unittest.TestCase):
    def test_origin_offset_follows_surface_normal(self):
        points = np.array(
            [
                [0.0, 1.0, 2.0],
                [0.0, 3.0, 4.0],
            ],
            dtype=float,
        )

        normal = np.array(
            [2.0, 0.0, 0.0],
            dtype=float,
        )

        origins = _offset_ray_origins(
            points,
            normal,
        )

        displacement = (
            origins
            - points
        )

        np.testing.assert_allclose(
            displacement[:, 0],
            RAY_ORIGIN_OFFSET_M,
            atol=1e-12,
        )

        np.testing.assert_allclose(
            displacement[:, 1:],
            0.0,
            atol=1e-12,
        )

    def test_origin_offset_is_independent_of_sun_incidence(self):
        points = np.array(
            [
                [0.0, 0.5, 0.5],
            ],
            dtype=float,
        )

        normal = np.array(
            [1.0, 0.0, 0.0],
            dtype=float,
        )

        origins = _offset_ray_origins(
            points,
            normal,
        )

        self.assertAlmostEqual(
            float(
                origins[0, 0]
            ),
            RAY_ORIGIN_OFFSET_M,
            places=10,
        )

    def test_zero_normal_is_rejected(self):
        with self.assertRaises(
            ValueError
        ):
            _offset_ray_origins(
                np.zeros(
                    (1, 3),
                    dtype=float,
                ),
                np.zeros(
                    3,
                    dtype=float,
                ),
            )

    def test_surface_does_not_shadow_itself_at_grazing_incidence(self):
        # Vertical plane at X=0, outward normal +X.
        #
        # Vertex ordering makes both triangle normals point +X.
        mesh = trimesh.Trimesh(
            vertices=np.array(
                [
                    [0.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                    [0.0, 1.0, 1.0],
                    [0.0, 0.0, 1.0],
                ],
                dtype=float,
            ),
            faces=np.array(
                [
                    [0, 1, 2],
                    [0, 2, 3],
                ],
                dtype=np.int64,
            ),
            process=False,
        )

        points = np.array(
            [
                [0.0, 0.20, 0.20],
                [0.0, 0.40, 0.40],
                [0.0, 0.60, 0.60],
                [0.0, 0.80, 0.80],
            ],
            dtype=float,
        )

        engine = ShadowEngine(
            [mesh]
        )

        # Sun has only a very small outward component relative to the
        # facade, reproducing the geometry of a grazing-angle case.
        percentage = engine.sunlit_area_pct(
            points,
            surface_normal=np.array(
                [1.0, 0.0, 0.0],
                dtype=float,
            ),
            sun_azimuth_deg=1.0,
            sun_elevation_deg=45.0,
        )

        self.assertAlmostEqual(
            percentage,
            100.0,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()

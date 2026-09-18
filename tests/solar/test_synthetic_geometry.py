import sys
import unittest
from pathlib import Path

import numpy as np
import trimesh


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.geometry.raycast import ShadowEngine  # noqa: E402
from solar.geometry.sampling import sample_vertical_facade  # noqa: E402
from solar.position.solar_position import sun_direction  # noqa: E402


SOUTH_FACING = np.array([0.0, -1.0, 0.0])
NORTH_FACING = np.array([0.0, 1.0, 0.0])


class SolarDirectionTests(unittest.TestCase):
    def test_north(self):
        direction = sun_direction(0.0, 0.0)
        np.testing.assert_allclose(
            direction,
            [0.0, 1.0, 0.0],
            atol=1e-10,
        )

    def test_east(self):
        direction = sun_direction(90.0, 0.0)
        np.testing.assert_allclose(
            direction,
            [1.0, 0.0, 0.0],
            atol=1e-10,
        )

    def test_south(self):
        direction = sun_direction(180.0, 0.0)
        np.testing.assert_allclose(
            direction,
            [0.0, -1.0, 0.0],
            atol=1e-10,
        )

    def test_west(self):
        direction = sun_direction(270.0, 0.0)
        np.testing.assert_allclose(
            direction,
            [-1.0, 0.0, 0.0],
            atol=1e-10,
        )


class SyntheticShadowTests(unittest.TestCase):
    def setUp(self):
        self.facade = sample_vertical_facade(
            x_min=-2.0,
            x_max=2.0,
            y=0.0,
            z_min=0.5,
            z_max=3.0,
            horizontal_samples=20,
            vertical_samples=12,
        )

    @staticmethod
    def obstacle_at(
        *,
        y: float,
        height: float = 8.0,
    ) -> trimesh.Trimesh:
        obstacle = trimesh.creation.box(
            extents=(8.0, 1.0, height)
        )

        obstacle.apply_translation(
            [0.0, y, height / 2.0]
        )

        return obstacle

    def test_no_obstacle_means_full_direct_sun_for_facing_surface(self):
        engine = ShadowEngine()

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=15.0,
        )

        self.assertAlmostEqual(pct, 100.0)

    def test_surface_facing_away_from_sun_gets_no_direct_sun(self):
        engine = ShadowEngine()

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=NORTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=30.0,
        )

        self.assertAlmostEqual(pct, 0.0)

    def test_sun_below_horizon_means_no_direct_sun(self):
        engine = ShadowEngine()

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=-2.0,
        )

        self.assertAlmostEqual(pct, 0.0)

    def test_high_building_blocks_low_southern_sun(self):
        obstacle = self.obstacle_at(y=-5.0)

        engine = ShadowEngine([obstacle])

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=15.0,
        )

        self.assertAlmostEqual(pct, 0.0)

    def test_partial_facade_emerges_from_shadow(self):
        obstacle = self.obstacle_at(y=-5.0)

        engine = ShadowEngine([obstacle])

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=50.0,
        )

        # At this geometry only the top 2 of the 12 sampled facade rows
        # clear the 8 m obstacle.
        self.assertAlmostEqual(pct, 100.0 * 2.0 / 12.0)

    def test_high_sun_passes_over_same_building(self):
        obstacle = self.obstacle_at(y=-5.0)

        engine = ShadowEngine([obstacle])

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=70.0,
        )

        self.assertAlmostEqual(pct, 100.0)

    def test_building_behind_surface_does_not_block_southern_sun(self):
        obstacle = self.obstacle_at(y=5.0)

        engine = ShadowEngine([obstacle])

        pct = engine.sunlit_area_pct(
            self.facade,
            surface_normal=SOUTH_FACING,
            sun_azimuth_deg=180.0,
            sun_elevation_deg=15.0,
        )

        self.assertAlmostEqual(pct, 100.0)


if __name__ == "__main__":
    unittest.main()

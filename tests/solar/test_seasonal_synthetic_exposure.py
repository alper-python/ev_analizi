import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import trimesh


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.geometry.exposure import hourly_surface_exposure  # noqa: E402
from solar.geometry.raycast import ShadowEngine  # noqa: E402
from solar.geometry.sampling import sample_vertical_facade  # noqa: E402
from solar.position.solar_position import solar_positions  # noqa: E402


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"

SOUTH_FACING = np.array([0.0, -1.0, 0.0])


def day_positions(date: str) -> pd.DataFrame:
    times = pd.date_range(
        f"{date} 00:00",
        periods=24,
        freq="1h",
        tz=TZ,
    )

    return solar_positions(
        times,
        lat=LAT,
        lon=LON,
    )


def wide_building_opposite() -> trimesh.Trimesh:
    # Wide on purpose: this test isolates vertical obstruction behaviour
    # rather than side-edge effects.
    obstacle = trimesh.creation.box(
        extents=(20.0, 1.0, 8.0)
    )

    obstacle.apply_translation(
        [0.0, -5.0, 4.0]
    )

    return obstacle


class SeasonalSyntheticExposureTests(unittest.TestCase):
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

        self.theoretical = ShadowEngine()
        self.actual = ShadowEngine(
            [wide_building_opposite()]
        )

    def exposure_for(self, date: str) -> pd.DataFrame:
        return hourly_surface_exposure(
            day_positions(date),
            sample_points=self.facade,
            surface_normal=SOUTH_FACING,
            theoretical_engine=self.theoretical,
            actual_engine=self.actual,
        )

    def test_obstacles_never_increase_direct_sun(self):
        summer = self.exposure_for("2026-06-21")

        self.assertTrue(
            (
                summer["actual_sunlit_area_pct"]
                <= summer["theoretical_sunlit_area_pct"]
            ).all()
        )

    def test_summer_high_sun_can_clear_opposite_building(self):
        summer = self.exposure_for("2026-06-21")

        row = summer.loc[
            pd.Timestamp(
                "2026-06-21 14:00",
                tz=TZ,
            )
        ]

        self.assertAlmostEqual(
            row["theoretical_sunlit_area_pct"],
            100.0,
        )

        self.assertAlmostEqual(
            row["actual_sunlit_area_pct"],
            100.0,
        )

    def test_winter_midday_sun_is_blocked_by_opposite_building(self):
        winter = self.exposure_for("2026-12-21")

        row = winter.loc[
            pd.Timestamp(
                "2026-12-21 13:00",
                tz=TZ,
            )
        ]

        self.assertAlmostEqual(
            row["theoretical_sunlit_area_pct"],
            100.0,
        )

        self.assertAlmostEqual(
            row["actual_sunlit_area_pct"],
            0.0,
        )

    def test_same_house_gets_more_actual_direct_sun_in_summer(self):
        summer = self.exposure_for("2026-06-21")
        winter = self.exposure_for("2026-12-21")

        summer_equivalent = (
            summer["actual_sunlit_area_pct"].sum()
            / 100.0
        )

        winter_equivalent = (
            winter["actual_sunlit_area_pct"].sum()
            / 100.0
        )

        self.assertGreater(
            summer_equivalent,
            winter_equivalent,
        )


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.position.solar_position import (  # noqa: E402
    hourly_solar_positions_for_year,
    solar_positions,
)


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"


class RealSolarPositionTests(unittest.TestCase):
    def test_timezone_is_required(self):
        times = pd.date_range(
            "2026-06-21 00:00",
            periods=24,
            freq="1h",
        )

        with self.assertRaises(ValueError):
            solar_positions(
                times,
                lat=LAT,
                lon=LON,
            )

    def test_summer_sun_is_much_higher_than_winter_sun(self):
        summer_times = pd.date_range(
            "2026-06-21 00:00",
            periods=24,
            freq="1h",
            tz=TZ,
        )

        winter_times = pd.date_range(
            "2026-12-21 00:00",
            periods=24,
            freq="1h",
            tz=TZ,
        )

        summer = solar_positions(
            summer_times,
            lat=LAT,
            lon=LON,
        )

        winter = solar_positions(
            winter_times,
            lat=LAT,
            lon=LON,
        )

        summer_max = float(summer["apparent_elevation"].max())
        winter_max = float(winter["apparent_elevation"].max())

        self.assertGreater(summer_max, 55.0)
        self.assertLess(winter_max, 20.0)
        self.assertGreater(
            summer_max - winter_max,
            35.0,
        )

    def test_summer_has_more_daylight_hours_than_winter(self):
        summer_times = pd.date_range(
            "2026-06-21 00:00",
            periods=24,
            freq="1h",
            tz=TZ,
        )

        winter_times = pd.date_range(
            "2026-12-21 00:00",
            periods=24,
            freq="1h",
            tz=TZ,
        )

        summer = solar_positions(
            summer_times,
            lat=LAT,
            lon=LON,
        )

        winter = solar_positions(
            winter_times,
            lat=LAT,
            lon=LON,
        )

        summer_daylight = int(
            (summer["apparent_elevation"] > 0).sum()
        )

        winter_daylight = int(
            (winter["apparent_elevation"] > 0).sum()
        )

        self.assertGreater(
            summer_daylight,
            winter_daylight,
        )

    def test_sun_is_near_south_at_daily_maximum_elevation(self):
        times = pd.date_range(
            "2026-06-21 00:00",
            periods=24,
            freq="1h",
            tz=TZ,
        )

        positions = solar_positions(
            times,
            lat=LAT,
            lon=LON,
        )

        peak_index = positions["apparent_elevation"].idxmax()
        peak_azimuth = float(
            positions.loc[peak_index, "azimuth"]
        )

        self.assertGreater(peak_azimuth, 150.0)
        self.assertLess(peak_azimuth, 210.0)

    def test_non_leap_year_has_8760_hourly_positions(self):
        positions = hourly_solar_positions_for_year(
            year=2026,
            lat=LAT,
            lon=LON,
            timezone=TZ,
        )

        self.assertEqual(len(positions), 8760)
        self.assertIsNotNone(positions.index.tz)


if __name__ == "__main__":
    unittest.main()

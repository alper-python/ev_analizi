import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.aggregation.seasonal_exposure import (  # noqa: E402
    daypart_for_hour,
    season_for_month,
    seasonal_summary,
)


class SeasonalExposureTests(unittest.TestCase):
    def test_season_mapping(self):
        self.assertEqual(
            season_for_month(1),
            "Winter",
        )
        self.assertEqual(
            season_for_month(4),
            "Spring",
        )
        self.assertEqual(
            season_for_month(7),
            "Summer",
        )
        self.assertEqual(
            season_for_month(10),
            "Autumn",
        )

    def test_daypart_mapping(self):
        self.assertEqual(
            daypart_for_hour(7),
            "Morning",
        )
        self.assertEqual(
            daypart_for_hour(11),
            "Midday",
        )
        self.assertEqual(
            daypart_for_hour(15),
            "Afternoon",
        )
        self.assertEqual(
            daypart_for_hour(19),
            "Evening",
        )
        self.assertIsNone(
            daypart_for_hour(2)
        )

    def test_summary_returns_average_daily_equivalent_hours(self):
        timestamps = pd.to_datetime(
            [
                "2026-06-01 10:00",
                "2026-06-01 11:00",
                "2026-06-02 10:00",
                "2026-06-02 11:00",
            ],
            utc=True,
        )

        hourly = pd.DataFrame(
            {
                "timestamp": timestamps,
                "surface_type": [
                    "WallSurface"
                ] * 4,
                "direction": [
                    "S"
                ] * 4,
                "area_m2": [
                    100.0
                ] * 4,
                "open_pct": [
                    100.0
                ] * 4,
                "own_pct": [
                    100.0
                ] * 4,
                "full_pct": [
                    50.0
                ] * 4,
            }
        )

        hourly["season"] = "Summer"
        hourly["daypart"] = "Midday"
        hourly["date"] = hourly[
            "timestamp"
        ].dt.date

        result = seasonal_summary(
            hourly,
            seasons={"Summer"},
        )

        whole_day = result[
            result["period"]
            == "Whole day"
        ].iloc[0]

        self.assertAlmostEqual(
            whole_day[
                "open_hours_per_day"
            ],
            2.0,
        )

        self.assertAlmostEqual(
            whole_day[
                "full_hours_per_day"
            ],
            1.0,
        )

        self.assertAlmostEqual(
            whole_day[
                "neighbour_shade_hours_per_day"
            ],
            1.0,
        )


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.models import (  # noqa: E402
    ConfidenceLevel,
    HourlyExposure,
    LocationContext,
    SurfaceRef,
    SurfaceType,
)


class SolarModelTests(unittest.TestCase):
    def test_location_context_keeps_region_separate_from_country(self):
        location = LocationContext(
            address="Example address",
            lat=50.0,
            lon=4.0,
            country_code="be",
            region="flanders",
        )
        self.assertEqual(location.country_code, "be")
        self.assertEqual(location.region, "flanders")

    def test_surface_types_cover_v1_analysis_targets(self):
        self.assertEqual(
            {item.value for item in SurfaceType},
            {"roof", "facade", "garden"},
        )

    def test_hourly_exposure_uses_sunlit_area_as_raw_measurement(self):
        observation = HourlyExposure(
            timestamp=datetime(2026, 6, 15, 14, 0),
            surface_id="rear_garden",
            sun_azimuth_deg=215.0,
            sun_elevation_deg=52.0,
            sunlit_area_pct=73.5,
            theoretical_sun=True,
            actual_sun=True,
        )
        self.assertEqual(observation.sunlit_area_pct, 73.5)

    def test_confidence_levels_are_descriptive_not_numeric_scores(self):
        self.assertEqual(ConfidenceLevel.HIGH.value, "high")


if __name__ == "__main__":
    unittest.main()

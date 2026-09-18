import io
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.pdok_cityjson import (  # noqa: E402
    distance_to_extent_2d,
    nearby_buildings_from_stream,
    wgs84_to_rd,
)


class PdokCityJsonTests(unittest.TestCase):
    def test_utrecht_wgs84_converts_into_expected_rd_region(self):
        x, y = wgs84_to_rd(
            lat=52.0907,
            lon=5.1214,
        )

        self.assertGreater(x, 130_000)
        self.assertLess(x, 145_000)
        self.assertGreater(y, 450_000)
        self.assertLess(y, 465_000)

    def test_point_inside_extent_has_zero_distance(self):
        distance = distance_to_extent_2d(
            x=10.0,
            y=20.0,
            extent=[
                0.0, 10.0, 0.0,
                20.0, 30.0, 10.0,
            ],
        )

        self.assertAlmostEqual(
            distance,
            0.0,
        )

    def test_point_outside_extent_gets_horizontal_distance(self):
        distance = distance_to_extent_2d(
            x=23.0,
            y=34.0,
            extent=[
                0.0, 10.0, 0.0,
                20.0, 30.0, 10.0,
            ],
        )

        self.assertAlmostEqual(
            distance,
            5.0,
        )

    def test_streaming_reader_filters_by_radius(self):
        payload = {
            "type": "CityJSON",
            "version": "2.0",
            "CityObjects": {
                "near": {
                    "type": "Building",
                    "geographicalExtent": [
                        0, 0, 0,
                        10, 10, 8,
                    ],
                    "attributes": {
                        "identificatie": "near",
                        "oorspronkelijkBouwjaar": 1990,
                        "rf_pc_source": "AHN5",
                        "rf_pc_year": 2023,
                        "rf_pointcloud_unusable": False,
                    },
                    "geometry": [
                        {
                            "type": "Solid",
                            "lod": "2.2",
                            "boundaries": [],
                        }
                    ],
                },
                "far": {
                    "type": "Building",
                    "geographicalExtent": [
                        100, 100, 0,
                        110, 110, 8,
                    ],
                    "attributes": {
                        "identificatie": "far",
                    },
                    "geometry": [],
                },
            },
            "vertices": [],
        }

        source = io.BytesIO(
            json.dumps(payload).encode("utf-8")
        )

        result = nearby_buildings_from_stream(
            source,
            target_x=5.0,
            target_y=5.0,
            radius_m=30.0,
        )

        self.assertEqual(
            len(result),
            1,
        )
        self.assertEqual(
            result[0].object_id,
            "near",
        )
        self.assertEqual(
            result[0].geometry_lods,
            ("2.2",),
        )


if __name__ == "__main__":
    unittest.main()

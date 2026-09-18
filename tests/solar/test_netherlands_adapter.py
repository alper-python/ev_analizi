import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.netherlands import (  # noqa: E402
    newest_tiles,
    parse_tile_features,
    point_bbox,
)


class NetherlandsAdapterTests(unittest.TestCase):
    def test_point_bbox_contains_original_point(self):
        lat = 52.0907
        lon = 5.1214

        west, south, east, north = point_bbox(
            lat=lat,
            lon=lon,
            radius_m=25.0,
        )

        self.assertLess(west, lon)
        self.assertGreater(east, lon)
        self.assertLess(south, lat)
        self.assertGreater(north, lat)

    def test_parse_pdok_features(self):
        payload = {
            "features": [
                {
                    "id": "abc",
                    "properties": {
                        "bladnr": "tile-1",
                        "download_link": (
                            "https://example.test/tile.zip"
                        ),
                        "jaargang_luchtfoto": 2025,
                        "startdatum": "2025-01-01T00:00:00Z",
                        "einddatum": "2025-12-31T23:59:59Z",
                    },
                }
            ]
        }

        result = parse_tile_features(payload)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].sheet_id, "tile-1")
        self.assertEqual(result[0].imagery_year, 2025)
        self.assertEqual(
            result[0].download_url,
            "https://example.test/tile.zip",
        )

    def test_features_without_download_link_are_ignored(self):
        payload = {
            "features": [
                {
                    "id": "abc",
                    "properties": {
                        "bladnr": "tile-1",
                    },
                }
            ]
        }

        self.assertEqual(
            parse_tile_features(payload),
            [],
        )

    def test_newest_year_is_selected(self):
        payload = {
            "features": [
                {
                    "id": "old",
                    "properties": {
                        "bladnr": "old-tile",
                        "download_link": "https://example.test/old.zip",
                        "jaargang_luchtfoto": 2021,
                    },
                },
                {
                    "id": "new",
                    "properties": {
                        "bladnr": "new-tile",
                        "download_link": "https://example.test/new.zip",
                        "jaargang_luchtfoto": 2025,
                    },
                },
            ]
        }

        tiles = newest_tiles(
            parse_tile_features(payload)
        )

        self.assertEqual(len(tiles), 1)
        self.assertEqual(
            tiles[0].imagery_year,
            2025,
        )


if __name__ == "__main__":
    unittest.main()

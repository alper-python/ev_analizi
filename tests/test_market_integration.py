import math
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    import market_scoring as market
finally:
    sys.path.pop(0)


TEST_LAT = 50.0
TEST_LON = 4.0
EARTH_RADIUS_M = 6371000.0


def north_of_origin(distance_m):
    return TEST_LAT + math.degrees(distance_m / EARTH_RADIUS_M), TEST_LON


NODE_SCHEMA = pa.schema([
    ("id", pa.int64()), ("cat", pa.string()), ("name", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()), ("landuse", pa.string()),
    ("sport", pa.string()), ("school_level", pa.string()), ("isced_level", pa.string()),
])

POLYGON_SCHEMA = pa.schema([
    ("uid", pa.string()), ("cat", pa.string()), ("name", pa.string()),
    ("brand", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()),
    ("public_transport", pa.string()), ("leisure", pa.string()),
    ("boundary", pa.string()), ("landuse", pa.string()), ("sport", pa.string()),
    ("school_level", pa.string()), ("isced_level", pa.string()),
])


def node_row(identifier, name, distance_m, shop=None, amenity=None):
    lat, lon = north_of_origin(distance_m)
    return {
        "id": identifier, "cat": "market", "name": name, "lat": lat, "lon": lon,
        "amenity": amenity, "shop": shop, "healthcare": None, "railway": None,
        "highway": None, "public_transport": None, "leisure": None, "boundary": None,
        "landuse": None, "sport": None, "school_level": None, "isced_level": None,
    }


def polygon_row(identifier, name, distance_m, shop=None, amenity=None, brand=None):
    lat, lon = north_of_origin(distance_m)
    return {
        "uid": identifier, "cat": "market", "name": name, "brand": brand,
        "lat": lat, "lon": lon, "amenity": amenity, "shop": shop,
        "healthcare": None, "railway": None, "highway": None,
        "public_transport": None, "leisure": None, "boundary": None,
        "landuse": None, "sport": None, "school_level": None, "isced_level": None,
    }


class MarketDuckDBIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(cls.temp_dir.name)
        cls.nodes_path = temp_path / "nodes.parquet"
        cls.polygons_path = temp_path / "polygons.parquet"

        nodes = [
            node_row(1, "Solo Super", 300, shop="supermarket"),
            node_row(2, "Quick Shop", 100, shop="convenience"),
            node_row(3, "  Duplicate   Mart ", 500, shop="supermarket"),
            node_row(4, "Distinct Alpha", 600, shop="supermarket"),
            node_row(5, "Distinct Beta", 650, shop="supermarket"),
            node_row(6, "Weekly Market", 50, amenity="marketplace"),
            node_row(7, "Dual Tagged Market", 70, shop="supermarket", amenity="marketplace"),
            node_row(8, "Beyond Score Radius", 3000, shop="supermarket"),
            node_row(9, "Mid Radius Convenience", 1800, shop="convenience"),
            node_row(10, "Just Inside", 2499, shop="supermarket"),
            node_row(11, "Just Outside", 2501, shop="supermarket"),
        ]
        polygons = [
            polygon_row("p1", "duplicate mart", 530, shop="supermarket",
                        brand="Duplicate Brand"),
        ]
        pq.write_table(pa.Table.from_pylist(nodes, schema=NODE_SCHEMA), cls.nodes_path)
        pq.write_table(pa.Table.from_pylist(polygons, schema=POLYGON_SCHEMA), cls.polygons_path)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def candidates(self, radius):
        with duckdb.connect() as con:
            return app.query_market_candidates(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, radius)

    def test_real_query_schema_union_filters_and_radius_boundary(self):
        rows = self.candidates(2500)
        names = {row["name"].strip() for row in rows}
        self.assertEqual(len(rows), 8)
        self.assertEqual({row["source"] for row in rows}, {"node", "polygon"})
        self.assertIn("Just Inside", names)
        self.assertNotIn("Just Outside", names)
        self.assertNotIn("Beyond Score Radius", names)
        self.assertNotIn("Weekly Market", names)
        self.assertNotIn("Dual Tagged Market", names)
        self.assertTrue(all(row["amenity"] != "marketplace" for row in rows))

    def test_real_query_dedup_and_score_components(self):
        raw_rows = self.candidates(2500)
        deduplicated = market.deduplicate_market_pois(raw_rows)
        effective_count = sum(
            market.MARKET_TYPE_WEIGHTS[market.market_type(row)] for row in deduplicated)
        best_weighted_proximity = max(
            market.MARKET_TYPE_WEIGHTS[market.market_type(row)]
            * max(0.0, 1.0 - row["d_lin"] / market.MARKET_SCORING_RADIUS_M)
            for row in deduplicated)
        proximity_points = 7.0 * best_weighted_proximity
        count_points = min(effective_count, 3.0)
        score = market.calc_market_score(deduplicated)

        self.assertEqual(len(raw_rows), 8)
        self.assertEqual(len(deduplicated), 7)
        self.assertAlmostEqual(effective_count, 6.2)
        self.assertAlmostEqual(proximity_points, 6.16, places=6)
        self.assertAlmostEqual(count_points, 3.0)
        self.assertAlmostEqual(score, 9.16, places=6)

        duplicate_rows = [
            row for row in deduplicated
            if market.normalize_market_name(row["name"]) == "duplicate mart"
        ]
        self.assertEqual(len(duplicate_rows), 1)
        self.assertEqual(duplicate_rows[0]["source"], "node")
        self.assertAlmostEqual(duplicate_rows[0]["d_lin"], 500.0, places=5)

    def test_fixed_score_radius_and_display_populations_end_to_end(self):
        results = {}
        with duckdb.connect() as con:
            for radius in (1000, 2500, 5000):
                frame, score, count, nearest = app.analyze_market(
                    con, str(self.nodes_path), str(self.polygons_path),
                    TEST_LAT, TEST_LON, radius, 20)
                results[radius] = (frame, score, count, nearest)

        self.assertEqual(
            [results[radius][1] for radius in (1000, 2500, 5000)],
            [results[1000][1]] * 3)
        self.assertAlmostEqual(results[1000][1], 9.16, places=6)
        self.assertEqual([results[radius][2] for radius in (1000, 2500, 5000)],
                         [5, 7, 9])

        for radius in (1000, 2500, 5000):
            names = set(results[radius][0]["name"])
            if radius == 1000:
                self.assertNotIn("Mid Radius Convenience", names)
            else:
                self.assertIn("Mid Radius Convenience", names)
            self.assertNotIn("Weekly Market", names)
            self.assertNotIn("Dual Tagged Market", names)

        duplicate = results[2500][0][
            results[2500][0]["name"].str.strip() == "Duplicate   Mart"
        ]
        self.assertEqual(len(duplicate), 1)
        self.assertAlmostEqual(float(duplicate.iloc[0]["d_lin"]), 500.0, places=5)

    def test_public_analyze_coordinates_real_sql_and_parquet(self):
        result = app.analyze(
            lat=TEST_LAT, lon=TEST_LON, radius=1000, topn=20,
            nodes_path=str(self.nodes_path), polys_path=str(self.polygons_path))

        self.assertEqual(result["scores"]["Market"], 9.2)
        self.assertEqual(result["overall"], 2.3)
        self.assertEqual(len(result["results"]["market"]), 5)
        self.assertEqual(result["results"]["school"], [])
        self.assertEqual(result["results"]["health"], [])
        self.assertEqual(result["results"]["transit"], [])
        self.assertEqual(result["results"]["park"], [])
        self.assertEqual(result["results"]["sport"], [])
        for item in result["results"]["market"]:
            self.assertEqual(
                set(item), {"name", "walk_m", "walk_s", "drive_m", "drive_s"})


if __name__ == "__main__":
    unittest.main()

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
    import school_scoring as school
finally:
    sys.path.pop(0)


TEST_LAT = 50.0
TEST_LON = 4.0
EARTH_RADIUS_M = 6371000.0


def north_of_origin(distance_m):
    return TEST_LAT + math.degrees(distance_m / EARTH_RADIUS_M), TEST_LON


def offset_from_origin(distance_m, bearing_degrees):
    angular = distance_m / EARTH_RADIUS_M
    bearing = math.radians(bearing_degrees)
    lat1 = math.radians(TEST_LAT)
    lon1 = math.radians(TEST_LON)
    lat2 = math.asin(math.sin(lat1) * math.cos(angular)
                     + math.cos(lat1) * math.sin(angular) * math.cos(bearing))
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular) * math.cos(lat1),
        math.cos(angular) - math.sin(lat1) * math.sin(lat2))
    return math.degrees(lat2), math.degrees(lon2)


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


def school_row(identifier, name, distance_m, amenity, polygon=False,
               isced_level=None, school_level=None):
    lat, lon = north_of_origin(distance_m)
    schema = POLYGON_SCHEMA if polygon else NODE_SCHEMA
    row = {column: None for column in schema.names}
    row.update({"cat": "school", "name": name, "lat": lat, "lon": lon,
                "amenity": amenity, "isced_level": isced_level,
                "school_level": school_level})
    row["uid" if polygon else "id"] = identifier
    return row


class SchoolDuckDBIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(cls.temp_dir.name)
        cls.nodes_path = temp_path / "nodes.parquet"
        cls.polygons_path = temp_path / "polygons.parquet"
        nodes = [
            school_row(1, "Nearest School", 300, "school"),
            school_row(2, "Duplicate School", 500, "school"),
            school_row(3, "Nearby Kindergarten", 700, "kindergarten"),
            school_row(4, "Excluded College", 50, "college"),
            school_row(5, "Excluded University", 60, "university"),
            school_row(6, "Ambiguous ISCED", 70, None, isced_level="1"),
            school_row(7, "Far School", 3500, "school"),
        ]
        polygons = [
            school_row("p1", " duplicate   school ", 530, "school", polygon=True,
                       isced_level="1"),
            school_row("p2", "Polygon School", 1200, "school", polygon=True),
        ]
        pq.write_table(pa.Table.from_pylist(nodes, schema=NODE_SCHEMA), cls.nodes_path)
        pq.write_table(pa.Table.from_pylist(polygons, schema=POLYGON_SCHEMA),
                       cls.polygons_path)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def candidates(self, radius):
        with duckdb.connect() as con:
            return app.query_school_candidates(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, radius)

    def test_sql_union_and_explicit_amenity_filters(self):
        rows = self.candidates(2500)
        self.assertEqual({row["source"] for row in rows}, {"node", "polygon"})
        names = {str(row["name"]).strip() for row in rows}
        self.assertNotIn("Excluded College", names)
        self.assertNotIn("Excluded University", names)
        self.assertNotIn("Ambiguous ISCED", names)
        self.assertTrue(all(row["amenity"] in ("school", "kindergarten") for row in rows))

    def test_conservative_bounding_box_keeps_all_cardinal_edge_pois(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            nodes_path = Path(temp_dir) / "boundary-nodes.parquet"
            rows = []
            for identifier, bearing in enumerate((0, 90, 180, 270), start=1):
                row = school_row(identifier, f"Boundary {bearing}", 0, "school")
                row["lat"], row["lon"] = offset_from_origin(2499.9, bearing)
                rows.append(row)
            pq.write_table(pa.Table.from_pylist(rows, schema=NODE_SCHEMA), nodes_path)
            with duckdb.connect() as con:
                found = app.query_school_candidates(
                    con, str(nodes_path), None, TEST_LAT, TEST_LON, 2500)
        self.assertEqual({row["name"] for row in found},
                         {"Boundary 0", "Boundary 90", "Boundary 180", "Boundary 270"})
        self.assertTrue(all(row["d_lin"] <= 2500 for row in found))

    def test_deduplication_and_components_through_real_query_path(self):
        deduplicated = school.deduplicate_school_pois(self.candidates(2500))
        self.assertEqual(len(deduplicated), 4)
        duplicate = [row for row in deduplicated
                     if school.normalize_school_name(row["name"]) == "duplicate school"]
        self.assertEqual(len(duplicate), 1)
        self.assertEqual(duplicate[0]["source"], "node")
        self.assertAlmostEqual(duplicate[0]["d_lin"], 500.0, places=4)

        components = school.school_score_components(deduplicated)
        expected_choice = ((1 - 300 / 2500) + (1 - 500 / 2500)
                           + 0.4 * (1 - 700 / 2500) + (1 - 1200 / 2500))
        self.assertAlmostEqual(components["proximity_points"], 7 * (1 - 300 / 2500),
                               places=5)
        self.assertAlmostEqual(components["effective_choice"], expected_choice, places=5)
        self.assertEqual(components["scoring_population_size"], 4)

    def test_fixed_score_radius_and_display_population(self):
        results = {}
        with duckdb.connect() as con:
            for radius in (1000, 2500, 5000):
                results[radius] = app.analyze_school(
                    con, str(self.nodes_path), str(self.polygons_path),
                    TEST_LAT, TEST_LON, radius, 20)
        self.assertEqual([results[r][1] for r in (1000, 2500, 5000)],
                         [results[1000][1]] * 3)
        self.assertEqual([results[r][2] for r in (1000, 2500, 5000)], [3, 4, 5])
        self.assertEqual(list(results[5000][0]["name"]),
                         ["Nearest School", "Duplicate School", "Nearby Kindergarten",
                          "Polygon School", "Far School"])

    def test_public_analyze_uses_dedicated_school_path(self):
        result = app.analyze(
            lat=TEST_LAT, lon=TEST_LON, radius=1000, topn=20,
            nodes_path=str(self.nodes_path), polys_path=str(self.polygons_path))
        self.assertGreater(result["scores"]["Okul"], 0)
        self.assertEqual([item["name"] for item in result["results"]["school"]],
                         ["Nearest School", "Duplicate School", "Nearby Kindergarten"])
        self.assertEqual([item["type"] for item in result["results"]["school"]],
                         ["school", "school", "kindergarten"])
        self.assertEqual(result["results"]["market"], [])
        self.assertEqual(result["results"]["health"], [])

    def test_public_analyze_score_is_display_radius_invariant(self):
        results = [app.analyze(
            lat=TEST_LAT, lon=TEST_LON, radius=radius, topn=20,
            nodes_path=str(self.nodes_path), polys_path=str(self.polygons_path))
                   for radius in (1000, 2500, 5000)]
        self.assertEqual([result["scores"]["Okul"] for result in results],
                         [results[0]["scores"]["Okul"]] * 3)
        self.assertEqual([len(result["results"]["school"]) for result in results],
                         [3, 4, 5])

    def test_public_analyze_kindergarten_only_uses_choice_score(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            nodes_path = Path(temp_dir) / "kindergarten-nodes.parquet"
            rows = [school_row(1, "Only Kindergarten", 300, "kindergarten")]
            pq.write_table(pa.Table.from_pylist(rows, schema=NODE_SCHEMA), nodes_path)
            result = app.analyze(
                lat=TEST_LAT, lon=TEST_LON, radius=2500, topn=20,
                nodes_path=str(nodes_path), polys_path=None)
        expected = 0.4 * (1 - 300 / 2500)
        self.assertEqual(result["scores"]["Okul"], float(f"{expected:.1f}"))
        self.assertEqual(result["results"]["school"][0]["type"], "kindergarten")


class RealBelgiumSchoolIntegrationTests(unittest.TestCase):
    nodes_path = SOURCE_DIR / "cache" / "be_poi.parquet"
    polygons_path = SOURCE_DIR / "cache" / "be_poi_poly.parquet"

    @unittest.skipUnless(nodes_path.is_file() or polygons_path.is_file(),
                         "real Belgium Parquet caches are not available")
    def test_gijmelstraat_real_cache_executes_and_is_display_radius_invariant(self):
        nodes = str(self.nodes_path) if self.nodes_path.is_file() else None
        polygons = str(self.polygons_path) if self.polygons_path.is_file() else None
        scores = []
        counts = []
        with duckdb.connect() as con:
            for radius in (1000, 2500, 5000):
                frame, score, count, _nearest = app.analyze_school(
                    con, nodes, polygons, 51.0035, 4.8405, radius, 1000)
                scores.append(score)
                counts.append(count)
                self.assertTrue(all(frame["amenity"].isin(["school", "kindergarten"])))
        self.assertEqual(scores, [scores[0]] * 3)
        self.assertLessEqual(counts[0], counts[1])
        self.assertLessEqual(counts[1], counts[2])


if __name__ == "__main__":
    unittest.main()

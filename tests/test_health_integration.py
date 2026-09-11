import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    import build_park_cache as park_cache
    import health_scoring as health
    import server as preview_server
finally:
    sys.path.pop(0)


TEST_LAT = 50.0
TEST_LON = 4.0
EARTH_RADIUS_M = 6371000.0


def offset(distance_m, bearing_degrees=0):
    angular = distance_m / EARTH_RADIUS_M
    bearing = math.radians(bearing_degrees)
    lat1, lon1 = math.radians(TEST_LAT), math.radians(TEST_LON)
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
    ("railway", pa.string()), ("highway", pa.string()),
    ("public_transport", pa.string()), ("leisure", pa.string()),
    ("boundary", pa.string()), ("landuse", pa.string()), ("sport", pa.string()),
    ("school_level", pa.string()), ("isced_level", pa.string()),
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


def row(identifier, name, distance, polygon=False, bearing=0, **tags):
    schema = POLYGON_SCHEMA if polygon else NODE_SCHEMA
    result = {field: None for field in schema.names}
    lat, lon = offset(distance, bearing)
    result.update({"uid" if polygon else "id": identifier, "cat": "health",
                   "name": name, "lat": lat, "lon": lon})
    result.update(tags)
    return result


class HealthDuckDBIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        target = Path(self.temp.name)
        self.nodes_path = target / "nodes.parquet"
        self.polygons_path = target / "polygons.parquet"
        self.parks_path = target / "be_park_destinations.parquet"
        nodes = [
            row(1, "Central Care", 300, amenity="doctors"),
            row(2, "Local Pharmacy", 500, amenity="pharmacy"),
            row(3, "Visible Dentist", 600, amenity="dentist", healthcare="dentist"),
            row(4, "Rehabilitation Campus", 900, amenity="hospital",
                healthcare="rehabilitation"),
            row(5, "Display-only Doctor", 3000, amenity="doctors"),
            row(6, "Qualifying Hospital", 15000, healthcare="hospital"),
        ]
        polygons = [
            row("p1", "central care", 320, polygon=True, amenity="clinic"),
            row("p2", "Displayed Physiotherapist", 700, polygon=True,
                healthcare="physiotherapist"),
        ]
        pq.write_table(pa.Table.from_pylist(nodes, schema=NODE_SCHEMA), self.nodes_path)
        pq.write_table(pa.Table.from_pylist(polygons, schema=POLYGON_SCHEMA),
                       self.polygons_path)
        pq.write_table(pa.Table.from_pylist([], schema=park_cache.park_schema()),
                       self.parks_path)

    def tearDown(self):
        self.temp.cleanup()

    def analyze(self, radius):
        with duckdb.connect() as con:
            return app.analyze_health(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, radius, 20)

    def test_dedicated_sql_unions_nodes_and_polygons(self):
        with duckdb.connect() as con:
            rows = app.query_health_candidates(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, 2500)
        self.assertEqual({item["source"] for item in rows}, {"node", "polygon"})
        self.assertIn("Displayed Physiotherapist", {item["name"] for item in rows})

    def test_fixed_scoring_queries_and_display_radius_are_separate(self):
        results = {radius: self.analyze(radius) for radius in (1000, 2500, 5000)}
        scores = [results[radius][1] for radius in (1000, 2500, 5000)]
        self.assertEqual(scores, [scores[0]] * 3)
        self.assertEqual([results[radius][2] for radius in (1000, 2500, 5000)],
                         [6, 6, 7])
        self.assertNotIn("Display-only Doctor", set(results[2500][0]["name"]))
        self.assertIn("Display-only Doctor", set(results[5000][0]["name"]))
        for result in results.values():
            components = result[4]
            self.assertAlmostEqual(components["nearest_clinical_m"], 300, places=3)
            self.assertAlmostEqual(components["nearest_hospital_m"], 15000, places=3)

    def test_scoring_deduplicates_node_polygon_but_display_preserves_rows(self):
        frame, _score, count, _nearest, components = self.analyze(2500)
        self.assertEqual(count, 6)
        self.assertEqual(sum(frame["name"].str.casefold() == "central care"), 2)
        self.assertEqual(components["clinical_count"], 1)
        self.assertEqual(components["pharmacy_count"], 1)

    def test_excluded_specialists_display_but_do_not_score(self):
        frame, _score, _count, _nearest, components = self.analyze(2500)
        self.assertIn("Visible Dentist", set(frame["name"]))
        self.assertIn("Displayed Physiotherapist", set(frame["name"]))
        expected_choice = (1 - 300 / 2500) + 0.4 * (1 - 500 / 2500)
        self.assertAlmostEqual(components["choice_points"], expected_choice, places=4)

    def test_display_rows_are_distance_sorted_before_topn_without_type_priority(self):
        candidates = [
            {"name": "Far Hospital", "brand": None, "lat": TEST_LAT,
             "lon": TEST_LON, "amenity": "hospital", "shop": None,
             "healthcare": "hospital", "source": "node", "d_lin": 300.0},
            {"name": "Near Dentist", "brand": None, "lat": TEST_LAT,
             "lon": TEST_LON, "amenity": "dentist", "shop": None,
             "healthcare": "dentist", "source": "node", "d_lin": 100.0},
            {"name": "Middle Pharmacy", "brand": None, "lat": TEST_LAT,
             "lon": TEST_LON, "amenity": "pharmacy", "shop": None,
             "healthcare": None, "source": "node", "d_lin": 200.0},
        ]

        with mock.patch.object(app, "query_health_candidates",
                               return_value=[dict(item) for item in candidates]):
            frame, score, count, nearest, components = app.analyze_health(
                object(), "nodes", "polys", TEST_LAT, TEST_LON, 1000, 2)
        with mock.patch.object(app, "query_health_candidates",
                               return_value=[dict(item) for item in candidates]):
            _full_frame, full_score, full_count, full_nearest, full_components = (
                app.analyze_health(
                    object(), "nodes", "polys", TEST_LAT, TEST_LON, 1000, 20))

        self.assertEqual(list(frame["name"]), ["Near Dentist", "Middle Pharmacy"])
        self.assertEqual(list(frame["d_lin"]), [100.0, 200.0])
        self.assertEqual(count, 3)
        self.assertEqual(nearest, 100.0)
        self.assertEqual(score, components["score"])
        self.assertEqual((score, count, nearest, components),
                         (full_score, full_count, full_nearest, full_components))

    def test_display_distance_ties_are_deterministic(self):
        candidates = [
            {"name": name, "brand": None, "lat": TEST_LAT, "lon": TEST_LON,
             "amenity": "dentist", "shop": None, "healthcare": "dentist",
             "source": source, "d_lin": 100.0}
            for name, source in (("Zulu", "node"), ("Alpha", "polygon"))
        ]
        orders = []
        for rows in (candidates, list(reversed(candidates))):
            with mock.patch.object(app, "query_health_candidates",
                                   return_value=[dict(item) for item in rows]):
                frame, *_rest = app.analyze_health(
                    object(), "nodes", "polys", TEST_LAT, TEST_LON, 1000, 20)
            orders.append(list(frame["name"]))
        self.assertEqual(orders, [["Alpha", "Zulu"], ["Alpha", "Zulu"]])

    def test_hospital_outside_display_still_scores_and_rehabilitation_does_not(self):
        frame, _score, _count, _nearest, components = self.analyze(1000)
        self.assertNotIn("Qualifying Hospital", set(frame["name"]))
        self.assertIn("Rehabilitation Campus", set(frame["name"]))
        self.assertAlmostEqual(components["hospital_points"], 0.5, places=4)
        self.assertAlmostEqual(components["nearest_hospital_m"], 15000, places=3)

    def test_public_analyze_uses_health_v1(self):
        result = app.analyze(
            lat=TEST_LAT, lon=TEST_LON, radius=1000, topn=20,
            nodes_path=str(self.nodes_path), polys_path=str(self.polygons_path))
        _frame, score, _count, _nearest, _components = self.analyze(1000)
        self.assertEqual(result["scores"]["Sağlık"], float(f"{score:.1f}"))
        self.assertIn("Visible Dentist",
                      {item["name"] for item in result["results"]["health"]})
        self.assertNotIn("Qualifying Hospital",
                         {item["name"] for item in result["results"]["health"]})

    def test_safe_bounding_boxes_include_near_boundary_rows(self):
        with duckdb.connect() as con:
            local = app.query_health_candidates(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, 2500)
            hospital = app.query_health_candidates(
                con, str(self.nodes_path), str(self.polygons_path),
                TEST_LAT, TEST_LON, 20000)
        self.assertIn("Central Care", {item["name"] for item in local})
        self.assertIn("Qualifying Hospital", {item["name"] for item in hospital})

    def test_cardinal_query_boundaries_for_local_and_hospital_radii(self):
        boundary_path = Path(self.temp.name) / "boundary-nodes.parquet"
        rows = []
        identifier = 100
        directions = (("north", 0), ("east", 90), ("south", 180), ("west", 270))
        for radius, prefix, tags in (
                (health.HEALTH_LOCAL_RADIUS_M, "local", {"amenity": "doctors"}),
                (health.HEALTH_HOSPITAL_RADIUS_M, "hospital",
                 {"healthcare": "hospital"})):
            for direction, bearing in directions:
                for position, distance in (
                        ("inside", radius - 0.01), ("exact", radius),
                        ("outside", radius + 0.01)):
                    identifier += 1
                    rows.append(row(identifier, f"{prefix}-{direction}-{position}",
                                    distance, bearing=bearing, **tags))
        pq.write_table(pa.Table.from_pylist(rows, schema=NODE_SCHEMA), boundary_path)

        with duckdb.connect() as con:
            local = app.query_health_candidates(
                con, str(boundary_path), None, TEST_LAT, TEST_LON,
                health.HEALTH_LOCAL_RADIUS_M)
            hospital = app.query_health_candidates(
                con, str(boundary_path), None, TEST_LAT, TEST_LON,
                health.HEALTH_HOSPITAL_RADIUS_M)

        local_names = {item["name"] for item in local if item["name"].startswith("local-")}
        hospital_names = {
            item["name"] for item in hospital if item["name"].startswith("hospital-")}
        for direction, _bearing in directions:
            self.assertIn(f"local-{direction}-inside", local_names)
            self.assertIn(f"local-{direction}-exact", local_names)
            self.assertNotIn(f"local-{direction}-outside", local_names)
            self.assertIn(f"hospital-{direction}-inside", hospital_names)
            self.assertIn(f"hospital-{direction}-exact", hospital_names)
            self.assertNotIn(f"hospital-{direction}-outside", hospital_names)
        self.assertIn("hospital-south-exact", hospital_names)

        exact_local = [item for item in local if item["name"] == "local-south-exact"]
        exact_hospital = [
            item for item in hospital if item["name"] == "hospital-south-exact"]
        components = health.health_score_components(exact_local, exact_hospital)
        self.assertAlmostEqual(components["clinical_proximity_points"], 0.0, places=10)
        self.assertAlmostEqual(components["choice_points"], 0.0, places=10)
        self.assertAlmostEqual(components["hospital_points"], 0.0, places=10)

    def test_generic_health_paths_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, "Health Score V1"):
            app.query_category(object(), None, None, "health", TEST_LAT, TEST_LON, 2500, 5)
        with self.assertRaisesRegex(ValueError, "Health Score V1"):
            app.calc_category_score("health", 1, 100, True)

    def test_orchestration_calls_fixed_local_and_hospital_radii(self):
        candidates = [
            {"name": "Doctor", "brand": None, "lat": TEST_LAT, "lon": TEST_LON,
             "amenity": "doctors", "shop": None, "healthcare": None,
             "source": "node", "d_lin": 300.0},
            {"name": "Hospital", "brand": None, "lat": TEST_LAT, "lon": TEST_LON,
             "amenity": "hospital", "shop": None, "healthcare": "hospital",
             "source": "node", "d_lin": 15000.0},
        ]

        def query(_con, _nodes, _polys, _lat, _lon, radius):
            return [dict(item) for item in candidates if item["d_lin"] <= radius]

        with mock.patch.object(app, "query_health_candidates", side_effect=query) as mocked:
            app.analyze_health(object(), "nodes", "polys", TEST_LAT, TEST_LON, 1000, 20)
        self.assertEqual([call.args[-1] for call in mocked.call_args_list],
                         [health.HEALTH_LOCAL_RADIUS_M,
                          health.HEALTH_HOSPITAL_RADIUS_M, 1000])

    def test_public_analyze_is_radius_invariant_and_uses_health_in_overall(self):
        public_results = [app.analyze(
            lat=TEST_LAT, lon=TEST_LON, radius=radius, topn=20,
            nodes_path=str(self.nodes_path), polys_path=str(self.polygons_path))
                          for radius in (1000, 2500, 5000)]
        dedicated = [self.analyze(radius) for radius in (1000, 2500, 5000)]
        health_scores = [result[1] for result in dedicated]
        components = [result[4] for result in dedicated]

        self.assertEqual(health_scores, [health_scores[0]] * 3)
        for field in ("clinical_proximity_points", "choice_points", "hospital_points",
                      "nearest_clinical_m", "nearest_hospital_m"):
            self.assertEqual([item[field] for item in components],
                             [components[0][field]] * 3)
        self.assertEqual([result["scores"]["Sağlık"] for result in public_results],
                         [float(f"{health_scores[0]:.1f}")] * 3)
        self.assertEqual(app.OVERALL_WEIGHTS["health"], 0.20)
        expected_overall = float(f"{health_scores[0] * 0.20:.1f}")
        self.assertEqual([result["overall"] for result in public_results],
                         [expected_overall] * 3)
        self.assertEqual([len(result["results"]["health"])
                          for result in public_results], [6, 6, 7])

    def test_legacy_display_metadata_remains_raw_and_broader_than_score(self):
        legacy_nodes = Path(self.temp.name) / "legacy-nodes.parquet"
        legacy_polygons = Path(self.temp.name) / "legacy-polygons.parquet"
        nodes = [
            row(20, "Nearest Pharmacy", 100, amenity="pharmacy"),
            row(21, "Duplicated Clinic", 800, amenity="clinic"),
            row(22, "Rehabilitation Hospital", 300, amenity="hospital",
                healthcare="rehabilitation"),
        ]
        polygons = [
            row("legacy-p1", "duplicated clinic", 820, polygon=True,
                healthcare="clinic"),
        ]
        pq.write_table(pa.Table.from_pylist(nodes, schema=NODE_SCHEMA), legacy_nodes)
        pq.write_table(pa.Table.from_pylist(polygons, schema=POLYGON_SCHEMA), legacy_polygons)

        with duckdb.connect() as con:
            frame, _score, count, nearest, components = app.analyze_health(
                con, str(legacy_nodes), str(legacy_polygons),
                TEST_LAT, TEST_LON, 2500, 20)
        self.assertEqual(nearest, min(frame["d_lin"]))
        self.assertAlmostEqual(nearest, 100, places=3)
        self.assertAlmostEqual(components["nearest_clinical_m"], 800, places=3)
        self.assertEqual(count, 4)
        self.assertEqual(components["clinical_count"], 1)
        self.assertEqual(components["pharmacy_count"], 1)
        self.assertTrue(bool(frame.iloc[0]["has_hospital_any"]))
        self.assertEqual(components["hospital_points"], 0.0)
        self.assertIsNone(components["nearest_hospital_m"])

        with (mock.patch.object(preview_server, "NODES_PATH", str(legacy_nodes)),
              mock.patch.object(preview_server, "POLYS_PATH", str(legacy_polygons)),
              duckdb.connect() as con):
            payload = preview_server._category_payload(
                con, "health", TEST_LAT, TEST_LON, 2500, 20, 0.0)
        self.assertEqual(payload["count"], 4)
        self.assertEqual(payload["nearest_m"], 100)
        self.assertTrue(payload["has_hospital"])
        self.assertEqual(payload["score_breakdown"]["hospital_points"], 0.0)
        self.assertAlmostEqual(
            payload["score_breakdown"]["nearest_clinical_m"], 800.0, places=3)


class RealBelgiumHealthIntegrationTests(unittest.TestCase):
    nodes_path = SOURCE_DIR / "cache" / "be_poi.parquet"
    polygons_path = SOURCE_DIR / "cache" / "be_poi_poly.parquet"

    @unittest.skipUnless(nodes_path.is_file() or polygons_path.is_file(),
                         "real Belgium Parquet caches are not available")
    def test_real_cache_executes_with_invariant_scores(self):
        nodes = str(self.nodes_path) if self.nodes_path.is_file() else None
        polygons = str(self.polygons_path) if self.polygons_path.is_file() else None
        scores = []
        with duckdb.connect() as con:
            for radius in (1000, 2500, 5000):
                _frame, score, _count, _nearest, components = app.analyze_health(
                    con, nodes, polygons, 51.0035, 4.8405, radius, 1000)
                scores.append(score)
                self.assertIsNotNone(components["nearest_clinical_m"])
                self.assertIsNotNone(components["nearest_hospital_m"])
        self.assertEqual(scores, [scores[0]] * 3)


if __name__ == "__main__":
    unittest.main()

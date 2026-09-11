import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import duckdb
from geographiclib.geodesic import Geodesic
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point, box


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    import build_park_cache as park_cache
    import park_scoring as park
finally:
    sys.path.pop(0)


TEST_LAT, TEST_LON = 50.0, 4.0


def candidate(identifier, distance, *, primary=True, choice=True,
              secondary=False, identity_type="osm", identity_value=None,
              size=1.0, confidence=1.0, secondary_weight=1.0,
              secondary_confidence=.8, parent=None):
    return {
        "park_id": f"park:{identifier}", "name": f"Park {identifier}",
        "display_name": f"Park {identifier}", "park_class": "park",
        "eligibility_tier": "primary", "distance_m": float(distance),
        "area_m2": 10000.0, "size_factor": size,
        "final_confidence": confidence, "access": "yes",
        "strong_identity_type": identity_type,
        "strong_identity_value": identity_value or str(identifier),
        "counts_as_primary": primary, "counts_as_choice": choice,
        "counts_as_secondary": secondary,
        "secondary_type_weight": secondary_weight,
        "secondary_confidence": secondary_confidence,
        "parent_park_id": parent,
    }


class ParkDistanceTests(unittest.TestCase):
    def test_distance_factor_boundaries(self):
        expected = {
            0: 1.0, 250: 1.0,
            250.000001: 1 - .000001 / 2250,
            1375: .5, 2499.999999: .000001 / 2250,
            2500: 0.0, 2501: 0.0,
        }
        for distance, value in expected.items():
            with self.subTest(distance=distance):
                self.assertAlmostEqual(park.park_distance_factor(distance), value)

    def test_inside_polygon_has_zero_distance(self):
        geometry = box(3.99, 49.99, 4.01, 50.01)
        self.assertEqual(
            park.geometry_distance_m(TEST_LAT, TEST_LON, geometry.wkb), 0.0)

    def test_point_geometry_uses_haversine_distance(self):
        geometry = Point(TEST_LON, TEST_LAT + 0.001)
        delta = math.radians(0.001)
        expected = (2.0 * park.EARTH_RADIUS_M
                    * math.asin(math.sin(delta / 2.0)))
        self.assertAlmostEqual(
            park.geometry_distance_m(TEST_LAT, TEST_LON, geometry.wkb),
            expected, places=9)

    def test_boundary_distance_is_used_instead_of_centroid(self):
        geometry = box(4.01, 49.999, 4.03, 50.001)
        boundary = park.geometry_distance_m(TEST_LAT, TEST_LON, geometry.wkb)
        centroid = Geodesic.WGS84.Inverse(
            TEST_LAT, TEST_LON, geometry.centroid.y,
            geometry.centroid.x)["s12"]
        self.assertGreater(boundary, 0.0)
        self.assertLess(boundary, centroid)


class ParkFormulaTests(unittest.TestCase):
    def test_primary_uses_distance_size_and_confidence(self):
        row = candidate("a", 1375, size=.6, confidence=.8)
        result = park.park_score_components([row])
        self.assertAlmostEqual(result["winner"]["utility"], .5 * .6 * .8)
        self.assertAlmostEqual(result["primary_points"], 7.5 * .5 * .6 * .8)

    def test_winner_selection_and_deterministic_tie(self):
        rows = [candidate("z", 500, size=.5),
                candidate("b", 300, size=.5),
                candidate("a", 300, size=.5)]
        result = park.park_score_components(rows)
        self.assertEqual(result["winner"]["park_id"], "park:a")

    def test_no_primary_state(self):
        result = park.park_score_components([
            candidate("s", 100, primary=False, choice=False, secondary=True)])
        self.assertIsNone(result["winner"])
        self.assertEqual(result["primary_points"], 0.0)

    def test_distinct_choice_identity_contributes_once(self):
        rows = [candidate("winner", 0),
                candidate("a1", 1375, identity_type="wikidata", identity_value="Q1"),
                candidate("a2", 1000, identity_type="wikidata", identity_value="Q1",
                          size=.4),
                candidate("b1", 1375, identity_type="wikipedia", identity_value="nl:B"),
                candidate("b2", 1800, identity_type="wikipedia", identity_value="nl:B")]
        result = park.park_score_components(rows)
        self.assertEqual(result["choice"]["distinct_alternative_identity_count"], 2)
        self.assertAlmostEqual(
            result["choice"]["effective_alternative_utility_sum"], 1.0)
        self.assertAlmostEqual(result["choice_points"], 1.0)

    def test_winning_identity_is_fully_excluded(self):
        rows = [candidate("winner", 0, identity_type="wikidata", identity_value="Q1"),
                candidate("duplicate", 500, identity_type="wikidata", identity_value="Q1"),
                candidate("alternative", 1375)]
        result = park.park_score_components(rows)
        self.assertEqual(result["choice"]["distinct_alternative_identity_count"], 1)
        self.assertAlmostEqual(result["choice_points"], .5)

    def test_attached_conditional_cannot_reenter_choice(self):
        rows = [candidate("winner", 0),
                candidate("child", 100, primary=False, choice=False,
                          parent="park:winner")]
        result = park.park_score_components(rows)
        self.assertEqual(result["choice_points"], 0.0)
        self.assertEqual(result["primary_candidate_count"], 1)

    def test_secondary_weights_cap_and_attached_contribution(self):
        playground = candidate(
            "play", 0, primary=False, choice=False, secondary=True,
            secondary_weight=1.0, secondary_confidence=.8,
            parent="park:parent")
        dog = candidate(
            "dog", 0, primary=False, choice=False, secondary=True,
            secondary_weight=.6, secondary_confidence=.8)
        result = park.park_score_components([playground, dog])
        self.assertAlmostEqual(result["secondary"]["effective_utility_sum"], 1.28)
        self.assertAlmostEqual(result["secondary_points"], .32)
        capped = park.park_score_components([playground, playground, dog, dog])
        self.assertEqual(capped["secondary_points"], .5)

    def test_full_precision_and_public_rounding(self):
        result = park.park_score_components([
            candidate("a", 777.123, size=.73, confidence=.81)])
        self.assertNotEqual(result["score_precise"], result["score_public"])
        self.assertEqual(result["score_public"], round(result["score_precise"], 1))

    def test_fixed_radius_excludes_boundary_and_outside_utility(self):
        result = park.park_score_components([
            candidate("inside", 0, size=.5),
            candidate("boundary", 2500, size=1.0),
            candidate("outside", 2501, size=1.0)])
        self.assertEqual(result["winner"]["park_id"], "park:inside")
        self.assertEqual(result["primary_candidate_count"], 2)
        self.assertEqual(result["score_precise"], 3.75)


def offset(distance_m, bearing_degrees):
    destination = Geodesic.WGS84.Direct(
        TEST_LAT, TEST_LON, bearing_degrees, distance_m)
    return destination["lat2"], destination["lon2"]


def cache_row(identifier, distance, bearing, *, secondary=False):
    lat, lon = offset(distance, bearing)
    geometry = Point(lon, lat).buffer(.000001)
    min_lon, min_lat, max_lon, max_lat = geometry.bounds
    row = {name: None for name in park_cache.park_schema().names}
    row.update({
        "park_id": f"park:osm:way:{identifier}", "osm_type": "way",
        "osm_id": identifier, "canonical_osm_type": "way",
        "canonical_osm_id": identifier, "member_of_relation_ids": [],
        "name": f"Cache Park {identifier}",
        "display_name": f"Cache Park {identifier}",
        "display_name_source": "canonical",
        "park_class": "playground" if secondary else "park",
        "source_classes": ["playground" if secondary else "park"],
        "eligibility_tier": "secondary" if secondary else "primary",
        "is_primary": not secondary, "is_conditional": False,
        "is_secondary": secondary, "counts_as_primary": not secondary,
        "counts_as_choice": not secondary, "counts_as_secondary": secondary,
        "strong_identity_type": "osm",
        "strong_identity_value": f"way:{identifier}",
        "access": "yes", "has_positive_access": True,
        "area_m2": 10000.0, "geometry_wkb": geometry.wkb,
        "geometry_valid": True, "geometry_was_valid": True,
        "centroid_lat": lat, "centroid_lon": lon,
        "representative_lat": lat, "representative_lon": lon,
        "bbox_min_lat": min_lat, "bbox_min_lon": min_lon,
        "bbox_max_lat": max_lat, "bbox_max_lon": max_lon,
        "size_factor": None if secondary else 1.0,
        "base_confidence": None if secondary else 1.0,
        "final_confidence": None if secondary else 1.0,
        "secondary_class": "playground" if secondary else None,
        "secondary_type_weight": 1.0 if secondary else None,
        "secondary_confidence": .95 if secondary else None,
        "absorbed_member_osm_keys": [],
        "source_osm_keys": [f"way:{identifier}"],
        "source_representations_json": "[]", "source_path": "synthetic",
        "source_size_bytes": 0, "builder_version": "test",
    })
    return row


class ParkDuckDBIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.path = Path(cls.temp.name) / "be_park_destinations.parquet"
        rows = [cache_row(1, 300, 0), cache_row(2, 700, 90),
                cache_row(3, 200, 180, secondary=True),
                cache_row(4, 4000, 270)]
        pq.write_table(pa.Table.from_pylist(
            rows, schema=park_cache.park_schema()), cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_fixed_scoring_and_display_radius_are_independent(self):
        with duckdb.connect() as con:
            results = [app.analyze_park(
                con, str(self.path), TEST_LAT, TEST_LON, radius, 20)
                for radius in (1000, 2500, 5000)]
        for index in (1, 2):
            self.assertEqual(results[index][1], results[0][1])
            self.assertEqual(results[index][4], results[0][4])
        self.assertEqual([result[2] for result in results], [3, 3, 4])

    def test_topn_does_not_affect_score(self):
        with duckdb.connect() as con:
            small = app.analyze_park(
                con, str(self.path), TEST_LAT, TEST_LON, 5000, 1)
            large = app.analyze_park(
                con, str(self.path), TEST_LAT, TEST_LON, 5000, 20)
        self.assertEqual(small[1], large[1])
        self.assertEqual(small[4], large[4])
        self.assertEqual(len(small[0]), 1)
        self.assertEqual(len(large[0]), 4)

    def test_query_uses_geometry_and_drops_wkb_from_results(self):
        with (duckdb.connect() as con,
              mock.patch.object(app, "geometry_distance_m",
                                wraps=app.geometry_distance_m) as distance):
            rows = app.query_park_candidates(
                con, str(self.path), TEST_LAT, TEST_LON, 1000)
        self.assertEqual(len(rows), 3)
        self.assertEqual(distance.call_count, 3)
        self.assertTrue(all("geometry_wkb" not in row for row in rows))

    def test_missing_cache_fails_clearly(self):
        missing = str(Path(self.temp.name) / "missing.parquet")
        with duckdb.connect() as con, self.assertRaisesRegex(
                FileNotFoundError, "Dedicated Park cache"):
            app.analyze_park(con, missing, TEST_LAT, TEST_LON, 2500, 5)

    def test_generic_park_paths_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, "dedicated analyze_park"):
            app.query_category(
                object(), None, None, "park", TEST_LAT, TEST_LON, 2500, 5)
        with self.assertRaisesRegex(ValueError, "dedicated Park Score V1"):
            app.calc_category_score("park", 2, 300)


class RealBelgiumParkRegressionTests(unittest.TestCase):
    path = SOURCE_DIR / "cache" / "be_park_destinations.parquet"
    locations = {
        "Gijmelstraat": (51.0034977, 4.8405107, 5.697331540,
                          "Gijmelbergwijk"),
        "Grote Markt Aarschot": (50.9843, 4.8367, 8.533889681,
                                  "Stadspark"),
        "Leuven": (50.8795, 4.7023, 8.648155664, "Sint-Donatuspark"),
        "Scherpenheuvel": (50.9949, 4.9778, 4.209516994,
                            "park:osm:relation:6363265"),
        "Diepenstraat": (51.0129, 4.8930, 1.033141391, "Langdonken"),
    }

    @unittest.skipUnless(path.is_file(), "real Park cache unavailable")
    def test_locked_scores_winners_and_radius_invariance(self):
        with duckdb.connect() as con:
            for label, (lat, lon, expected, winner) in self.locations.items():
                with self.subTest(location=label):
                    results = [app.analyze_park(
                        con, str(self.path), lat, lon, radius, 1000)
                        for radius in (1000, 2500, 5000)]
                    breakdowns = [result[4] for result in results]
                    invariant = ("score_precise", "score_public",
                                 "primary_points", "choice_points",
                                 "secondary_points", "winner")
                    for field in invariant:
                        self.assertEqual(
                            [value[field] for value in breakdowns],
                            [breakdowns[0][field]] * 3)
                    self.assertAlmostEqual(
                        breakdowns[0]["score_precise"], expected, places=8)
                    actual = (breakdowns[0]["winner"]["display_name"]
                              or breakdowns[0]["winner"]["park_id"])
                    self.assertEqual(actual, winner)


if __name__ == "__main__":
    unittest.main()

from pathlib import Path
import math
import sys
import tempfile
import unittest

import duckdb
from geographiclib.geodesic import Geodesic
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point, Polygon


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    from build_sport_cache import sport_schema
    from sport_scoring import (
        FACILITY_FACTORS,
        confidence_factor,
        destination_distance_m,
        destination_utility,
        facility_factor,
        sport_distance_factor,
        sport_score_components,
    )
finally:
    sys.path.pop(0)


def candidate(identifier="sport:1", **changes):
    row = {
        "sport_id": identifier,
        "canonical_osm_type": "node",
        "canonical_osm_id": int(identifier.rsplit(":", 1)[-1]),
        "display_name": identifier,
        "facility_class": "multi_sport_centre",
        "score_eligible": True,
        "eligibility_reason": "eligible",
        "specialized": False,
        "standalone": False,
        "sports": [],
        "sport_count": 0,
        "access_class": "positive_general",
        "is_commercial": False,
        "is_public_operator": False,
        "is_school_context": False,
        "distance_m": 100.0,
        "distance_method": "canonical_node",
    }
    row.update(changes)
    return row


class SportNumericTests(unittest.TestCase):
    def test_distance_factor_has_full_credit_through_400(self):
        for distance in (0, 100, 399.999, 400):
            self.assertEqual(sport_distance_factor(distance), 1.0)

    def test_distance_factor_linear_decay_and_zero_boundary(self):
        self.assertAlmostEqual(sport_distance_factor(1700), 0.5)
        self.assertAlmostEqual(sport_distance_factor(1000), 1 - 600 / 2600)
        for distance in (3000, 3000.001, 9000):
            self.assertEqual(sport_distance_factor(distance), 0.0)

    def test_all_locked_facility_factors(self):
        self.assertEqual(
            {key: facility_factor(key) for key in FACILITY_FACTORS},
            FACILITY_FACTORS)
        self.assertEqual(facility_factor("unknown"), 0.0)

    def test_confidence_precedence_and_missing_access_classes(self):
        cases = (
            ({"access_class": "positive_general"}, 1.0),
            ({"access_class": "missing", "is_public_operator": True}, 1.0),
            ({"access_class": "customers", "is_commercial": True,
              "facility_class": "fitness_gym"}, 0.95),
            ({"access_class": "missing"}, 0.85),
            ({"access_class": "members"}, 0.70),
            ({"access_class": "permit_limited"}, 0.65),
            ({"access_class": "missing", "specialized": True}, 0.60),
            ({"access_class": "missing", "specialized": True,
              "standalone": True, "name": "Named"}, 0.55),
            ({"access_class": "missing", "standalone": True,
              "operator": "Club"}, 0.55),
            ({"access_class": "missing", "standalone": True}, 0.0),
        )
        for changes, expected in cases:
            with self.subTest(changes=changes):
                self.assertEqual(confidence_factor(candidate(**changes)), expected)

    def test_ineligible_private_customer_and_school_restrictions_are_zero(self):
        rows = (
            candidate(score_eligible=False),
            candidate(access_class="private"),
            candidate(access_class="no"),
            candidate(access_class="customers", is_commercial=False),
            candidate(access_class="missing", is_school_context=True),
        )
        self.assertEqual([confidence_factor(row) for row in rows], [0.0] * 5)

    def test_fee_does_not_reduce_confidence(self):
        self.assertEqual(confidence_factor(candidate(fee="yes")), 1.0)

    def test_utility_is_product_of_three_factors(self):
        row = candidate(distance_m=1700, facility_class="stadium",
                        access_class="members")
        self.assertAlmostEqual(destination_utility(row), 0.5 * 0.8 * 0.7)

    def test_best_winner_and_deterministic_ties(self):
        rows = [candidate("sport:3", distance_m=200),
                candidate("sport:2", distance_m=100),
                candidate("sport:1", distance_m=100)]
        result = sport_score_components(rows)
        self.assertEqual(result["best"]["winner"]["sport_id"], "sport:1")
        self.assertEqual(result["best"]["points"], 7.5)

    def test_choice_weights_and_five_alternative_cap(self):
        rows = [candidate(f"sport:{index}") for index in range(1, 9)]
        result = sport_score_components(rows)
        choice = result["choice"]
        self.assertEqual(choice["weights"], [1.0, 0.7, 0.5, 0.35, 0.25])
        self.assertEqual(choice["alternatives_used"], 5)
        self.assertAlmostEqual(choice["choice_raw"], 2.8)
        self.assertEqual(choice["points"], 2.5)
        self.assertEqual(result["score_precise"], 10.0)

    def test_choice_uses_weighted_utility_without_diversity_points(self):
        result = sport_score_components([
            candidate("sport:1", sports=["football", "swimming"], sport_count=2),
            candidate("sport:2", facility_class="stadium"),
        ])
        self.assertAlmostEqual(result["choice"]["choice_raw"], 0.8)
        self.assertAlmostEqual(result["choice"]["points"], 2.5 * 0.8 / 2.2)
        self.assertNotIn("diversity", result)

    def test_duplicate_canonical_id_and_components_do_not_add_choices(self):
        result = sport_score_components([
            candidate("sport:1", component_count=9),
            candidate("sport:1", distance_m=200),
        ])
        self.assertEqual(result["choice"]["considered_count"], 0)
        self.assertEqual(result["choice"]["points"], 0.0)

    def test_public_rounding_and_precise_internal_score(self):
        result = sport_score_components([candidate(
            distance_m=1000, facility_class="stadium", access_class="members")])
        self.assertEqual(result["score"], round(result["score_precise"], 1))
        self.assertNotEqual(result["score"], result["score_precise"])

    def test_empty_and_boundary_only_locations_are_finite_zero(self):
        for rows in ([], [candidate(distance_m=3000)],
                     [candidate(distance_m=math.inf)]):
            result = sport_score_components(rows)
            self.assertEqual(result["score_precise"], 0.0)
            self.assertEqual(result["score"], 0.0)
            self.assertIsNone(result["best"]["winner"])
            self.assertEqual(result["choice"]["points"], 0.0)

    def test_breakdown_contract_and_no_wkb(self):
        result = sport_score_components([candidate()])
        self.assertEqual(result["scoring_radius_m"], 3000)
        self.assertEqual(result["best"]["max_points"], 7.5)
        self.assertEqual(result["choice"]["max_points"], 2.5)
        self.assertEqual(result["choice"]["saturation"], 2.2)
        winner = result["best"]["winner"]
        for key in ("sport_id", "canonical_osm_type", "canonical_osm_id",
                    "display_name", "facility_class", "distance_m",
                    "distance_method", "facility_factor", "confidence_factor",
                    "distance_factor", "utility", "sports", "sport_count",
                    "access_class", "is_commercial", "is_school_context"):
            self.assertIn(key, winner)
        self.assertNotIn("geometry_wkb", str(result))


class SportGeometryTests(unittest.TestCase):
    def test_point_uses_true_geodesic_distance(self):
        geometry = Point(4.84, 51.0)
        distance, method = destination_distance_m(
            51.001, 4.84, {"geometry_wkb": geometry.wkb})
        expected = Geodesic.WGS84.Inverse(51.001, 4.84, 51.0, 4.84)["s12"]
        self.assertAlmostEqual(distance, expected)
        self.assertEqual(method, "canonical_node")

    def test_polygon_boundary_and_inside_distance(self):
        polygon = Polygon([(4.84, 51.0), (4.842, 51.0),
                           (4.842, 51.002), (4.84, 51.002)])
        inside, method = destination_distance_m(
            51.001, 4.841, {"geometry_wkb": polygon.wkb})
        self.assertEqual(inside, 0.0)
        self.assertEqual(method, "canonical_polygon_boundary")
        outside, method = destination_distance_m(
            51.003, 4.841, {"geometry_wkb": polygon.wkb})
        self.assertGreater(outside, 100)
        self.assertEqual(method, "canonical_polygon_boundary")

    def test_validated_entrance_precedes_polygon_and_representative(self):
        polygon = Point(4.84, 51.0).buffer(0.01)
        distance, method = destination_distance_m(51.0, 4.84, {
            "entrance_lat": 51.001, "entrance_lon": 4.84,
            "geometry_wkb": polygon.wkb,
            "representative_lat": 51.0, "representative_lon": 4.84,
        })
        self.assertGreater(distance, 100)
        self.assertEqual(method, "validated_entrance")

    def test_representative_then_owned_component_fallback(self):
        distance, method = destination_distance_m(51.0, 4.84, {
            "geometry_wkb": b"bad", "representative_lat": 51.001,
            "representative_lon": 4.84,
        })
        self.assertGreater(distance, 100)
        self.assertEqual(method, "representative_point")
        distance, method = destination_distance_m(51.0, 4.84, {
            "geometry_wkb": b"bad",
            "owned_component_geometry_wkb": [Point(4.84, 51.002).wkb],
        })
        self.assertGreater(distance, 200)
        self.assertEqual(method, "owned_component_geometry")


def cache_row(identifier, lat, lon, *, name, facility_class, eligible=True,
              access="positive_general"):
    geometry = Point(lon, lat)
    row = {field: None for field in sport_schema().names}
    row.update({
        "sport_id": identifier, "canonical_osm_type": "node",
        "canonical_osm_id": int(identifier.rsplit(":", 1)[-1]),
        "name": name, "display_name": name,
        "display_name_source": "canonical_name",
        "facility_class": facility_class, "score_eligible": eligible,
        "eligibility_reason": "eligible" if eligible else "excluded_test",
        "specialized": False, "standalone": False,
        "sports": [], "direct_sports": [], "component_sports": [],
        "sport_count": 0, "component_count": 0,
        "access_class": access, "access_evidence": "test",
        "is_commercial": facility_class == "fitness_gym",
        "is_public_operator": False, "is_school_context": False,
        "geometry_wkb": geometry.wkb, "geometry_valid": True,
        "representative_lat": lat, "representative_lon": lon,
        "bbox_min_lat": lat, "bbox_min_lon": lon,
        "bbox_max_lat": lat, "bbox_max_lon": lon,
        "canonicalization_method": "canonical_source",
        "source_osm_keys": [], "absorbed_component_keys": [],
        "rejected_component_keys": [], "parent_relation_ids": [],
        "relation_membership_roles": [], "school_osm_keys": [],
        "build_version": "test", "source_pbf_timestamp": "test",
    })
    return row


class SportBackendIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "be_sport_destinations.parquet"
        origin = (51.0, 4.84)
        rows = []
        for index, distance in enumerate((500, 1500, 2800, 3500), 1):
            point = Geodesic.WGS84.Direct(*origin, 90, distance)
            rows.append(cache_row(
                f"sport:{index}", point["lat2"], point["lon2"],
                name=f"Facility {index}",
                facility_class="multi_sport_centre"))
        pq.write_table(pa.Table.from_pylist(rows, schema=sport_schema()), self.path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_display_radius_changes_items_but_not_score_or_breakdown(self):
        with duckdb.connect() as con:
            results = [app.analyze_sport(
                con, str(self.path), 51.0, 4.84, radius, 20)
                for radius in (1000, 2500, 5000)]
        self.assertEqual([result[2] for result in results], [1, 2, 4])
        self.assertEqual([result[1] for result in results], [results[0][1]] * 3)
        for key in ("score_precise", "score", "best", "choice"):
            self.assertEqual([result[4][key] for result in results],
                             [results[0][4][key]] * 3)
        self.assertEqual(results[0][0].iloc[0]["sport_id"], "sport:1")

    def test_legacy_generic_sport_paths_are_explicitly_rejected(self):
        with duckdb.connect() as con:
            with self.assertRaisesRegex(ValueError, "dedicated analyze_sport"):
                app.query_category(con, None, None, "sport", 51.0, 4.84, 1000, 5)
        with self.assertRaisesRegex(ValueError, "sport_score_components"):
            app.calc_category_score("sport", 1, 100)


class RealBelgiumSportRegressionTests(unittest.TestCase):
    cache_path = SOURCE_DIR / "cache" / "be_sport_destinations.parquet"
    locations = {
        "Gijmelstraat": (
            51.0034977, 4.8405107, 7.591257121047635,
            "sport:osm:node:5974621221"),
        "Grote Markt Aarschot": (
            50.9843, 4.8367, 10.0, "sport:osm:way:184905878"),
        "Leuven": (
            50.8795, 4.7023, 9.268028152946911,
            "sport:osm:node:3321391011"),
        "Scherpenheuvel": (
            50.9949, 4.9778, 2.9215857551105664,
            "sport:osm:way:100544935"),
        "Diepenstraat": (
            51.0129, 4.8930, 5.1433491649403775,
            "sport:osm:way:929253488"),
    }

    @unittest.skipUnless(cache_path.is_file(), "real Sport cache unavailable")
    def test_five_authoritative_scores_winners_and_display_invariance(self):
        with duckdb.connect() as con:
            for name, (lat, lon, expected, winner) in self.locations.items():
                with self.subTest(location=name):
                    results = [app.analyze_sport(
                        con, str(self.cache_path), lat, lon, radius, 20)
                        for radius in (1000, 2500, 5000)]
                    breakdowns = [result[4] for result in results]
                    self.assertTrue(all(
                        math.isclose(item["score_precise"], expected,
                                     rel_tol=0.0, abs_tol=1e-12)
                        for item in breakdowns))
                    self.assertEqual(
                        [item["best"]["winner"]["sport_id"]
                         for item in breakdowns], [winner] * 3)
                    for field in ("score_precise", "score", "best", "choice"):
                        self.assertEqual(
                            [item[field] for item in breakdowns],
                            [breakdowns[0][field]] * 3)

    @unittest.skipUnless(cache_path.is_file(), "real Sport cache unavailable")
    def test_locked_canonicalization_and_eligibility_semantics(self):
        with duckdb.connect() as con:
            rows = con.execute("""
                SELECT sport_id, display_name, facility_class, score_eligible,
                       access_class, component_count
                FROM read_parquet(?)
                WHERE sport_id IN (
                    'sport:osm:way:189639931',
                    'sport:osm:node:5974621221',
                    'sport:osm:way:929253488')
                   OR display_name IN (
                    'Laaksite', 'A.Tc.''82', 'KVC Langdorp',
                    'Sportoase Philipssite', 'K. Stade Leuven T.C.',
                    'King Power at Den Dreef')
            """, [str(self.cache_path)]).fetchall()
        by_id = {row[0]: row for row in rows}
        self.assertFalse(by_id["sport:osm:way:189639931"][3])
        self.assertTrue(by_id["sport:osm:node:5974621221"][3])
        diepenstraat = by_id["sport:osm:way:929253488"]
        self.assertEqual(diepenstraat[2:5],
                         ("general_sports_centre", True, "missing"))
        for name in ("Laaksite", "A.Tc.'82", "KVC Langdorp",
                     "Sportoase Philipssite", "K. Stade Leuven T.C.",
                     "King Power at Den Dreef"):
            self.assertEqual(sum(row[1] == name for row in rows), 1, name)


if __name__ == "__main__":
    unittest.main()

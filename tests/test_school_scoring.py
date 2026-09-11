import math
import itertools
from pathlib import Path
import sys
import unittest
from unittest import mock


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    import market_scoring as market
    import school_scoring as school
finally:
    sys.path.pop(0)


def poi(amenity, distance, name="School", source="node", lat=50.0, lon=4.0,
        isced_level=None, school_level=None):
    return {
        "name": name,
        "brand": None,
        "lat": lat,
        "lon": lon,
        "amenity": amenity,
        "shop": None,
        "healthcare": None,
        "school_level": school_level,
        "isced_level": isced_level,
        "source": source,
        "d_lin": float(distance),
    }


def moved(row, metres):
    result = dict(row)
    result["lat"] = float(row["lat"]) + math.degrees(metres / 6371000.0)
    return result


class SchoolScoreTests(unittest.TestCase):
    def test_no_school_pois_scores_zero(self):
        self.assertEqual(school.calc_school_score([]), 0.0)

    def test_one_school_at_300m(self):
        factor = 1 - 300 / 2500
        components = school.school_score_components([poi("school", 300)])
        self.assertAlmostEqual(components["proximity_points"], 7 * factor)
        self.assertAlmostEqual(components["choice_points"], factor)
        self.assertAlmostEqual(components["score"], 8 * factor)

    def test_school_distance_boundaries(self):
        cases = (
            (0, 7.0, 1.0, 8.0, 1),
            (2499, 0.0028, 0.0004, 0.0032, 1),
            (2500, 0.0, 0.0, 0.0, 1),
            (2501, 0.0, 0.0, 0.0, 0),
        )
        for distance, proximity, choice, score, population in cases:
            with self.subTest(distance=distance):
                components = school.school_score_components([poi("school", distance)])
                self.assertAlmostEqual(components["proximity_points"], proximity)
                self.assertAlmostEqual(components["choice_points"], choice)
                self.assertAlmostEqual(components["score"], score)
                self.assertEqual(components["scoring_population_size"], population)

    def test_one_kindergarten_at_300m_only_contributes_to_choice(self):
        components = school.school_score_components([poi("kindergarten", 300)])
        self.assertEqual(components["proximity_points"], 0.0)
        self.assertAlmostEqual(components["choice_points"], 0.4 * (1 - 300 / 2500))

    def test_school_and_kindergarten_use_weighted_distance_choice(self):
        rows = [poi("school", 500), poi("kindergarten", 500, name="Kindergarten")]
        components = school.school_score_components(rows)
        self.assertAlmostEqual(components["effective_choice"], 0.8 + 0.4 * 0.8)
        self.assertAlmostEqual(components["score"], 7 * 0.8 + 1.12)

    def test_college_is_excluded(self):
        self.assertEqual(school.calc_school_score([poi("college", 0)]), 0.0)
        self.assertEqual(school.deduplicate_school_pois([poi("college", 0)]), [])

    def test_university_is_excluded(self):
        self.assertEqual(school.calc_school_score([poi("university", 0)]), 0.0)

    def test_isced_only_ambiguous_row_is_excluded(self):
        row = poi(None, 0, isced_level="1")
        self.assertEqual(school.calc_school_score([row]), 0.0)
        self.assertEqual(school.deduplicate_school_pois([row]), [])

    def test_school_level_only_ambiguous_row_is_excluded(self):
        row = poi(None, 0, school_level="primary")
        self.assertEqual(school.calc_school_score([row]), 0.0)
        self.assertEqual(school.deduplicate_school_pois([row]), [])

    def test_poi_beyond_fixed_radius_is_excluded_from_score(self):
        self.assertEqual(school.calc_school_score([poi("school", 2500.01)]), 0.0)

    def test_score_is_always_bounded(self):
        self.assertEqual(school.calc_school_score([poi("school", -1)]), 0.0)
        many = [poi("school", 0, name=f"School {index}") for index in range(20)]
        self.assertEqual(school.calc_school_score(many), 10.0)


class SchoolDeduplicationTests(unittest.TestCase):
    def assert_duplicate_pair(self, first_source, second_source):
        first = poi("school", 80, name=" École   Centrale ", source=first_source)
        second = moved(poi("school", 50, name="école centrale", source=second_source), 40)
        result = school.deduplicate_school_pois([first, second])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["d_lin"], 50)
        self.assertEqual(result[0]["source"], second_source)

    def test_named_node_polygon_duplicate_counts_once(self):
        self.assert_duplicate_pair("node", "polygon")

    def test_named_polygon_polygon_duplicate_counts_once(self):
        self.assert_duplicate_pair("polygon", "polygon")

    def test_named_node_node_duplicate_counts_once(self):
        self.assert_duplicate_pair("node", "node")

    def test_same_name_beyond_75m_remains_separate(self):
        rows = [poi("school", 50, name="Alpha"), moved(poi("school", 60, name="alpha"), 76)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_named_exactly_75m_merges(self):
        rows = [poi("school", 50, name="Alpha"),
                moved(poi("school", 60, name="alpha"), 75)]
        self.assertAlmostEqual(school.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 75)
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 1)

    def test_named_just_over_75m_remains_separate(self):
        rows = [poi("school", 50, name="Alpha"),
                moved(poi("school", 60, name="alpha"), 75.001)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_different_names_inside_75m_remain_separate(self):
        rows = [poi("school", 50, name="Alpha"), moved(poi("school", 60, name="Beta"), 5)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_same_name_different_amenity_remains_separate(self):
        rows = [poi("school", 50, name="Alpha"),
                moved(poi("kindergarten", 60, name="alpha"), 5)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_unnamed_same_amenity_within_10m_merges(self):
        rows = [poi("school", 50, name=None), moved(poi("school", 40, name=""), 9)]
        result = school.deduplicate_school_pois(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["d_lin"], 40)

    def test_unnamed_records_beyond_10m_remain_separate(self):
        rows = [poi("school", 50, name=None), moved(poi("school", 40, name=""), 11)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_unnamed_exactly_10m_merges(self):
        rows = [poi("school", 50, name=None), moved(poi("school", 40, name=""), 10)]
        self.assertAlmostEqual(school.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 10)
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 1)

    def test_unnamed_just_over_10m_remains_separate(self):
        rows = [poi("school", 50, name=None),
                moved(poi("school", 40, name=""), 10.001)]
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def _pair_across_spatial_boundary(self, distance):
        def legacy_cell(row):
            lat = float(row["lat"])
            y = lat * 111320.0
            x = float(row["lon"]) * 111320.0 * math.cos(math.radians(lat))
            return math.floor(x / 10), math.floor(y / 10)

        metres_per_degree = 6371000.0 * math.pi / 180
        projected_scale = 111320.0 / metres_per_degree
        base_fraction = (50.0 * 111320.0) % 10
        offset = ((9.999 - base_fraction) % 10) / projected_scale
        first = moved(poi("school", 50, name=None), offset)
        second = moved(poi("school", 40, name=""), offset + distance)
        old_first, old_second = legacy_cell(first), legacy_cell(second)
        self.assertGreater(max(abs(a - b) for a, b in zip(old_first, old_second)), 1)
        new_first = school._spatial_cell(first, school.SCHOOL_UNNAMED_DEDUP_DISTANCE_M)
        new_second = school._spatial_cell(second, school.SCHOOL_UNNAMED_DEDUP_DISTANCE_M)
        self.assertLessEqual(max(abs(a - b) for a, b in zip(new_first, new_second)), 1)
        return first, second

    def test_unnamed_within_10m_merges_across_bucket_boundary(self):
        rows = self._pair_across_spatial_boundary(10)
        self.assertAlmostEqual(school.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 10)
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 1)

    def test_unnamed_over_10m_stays_separate_across_bucket_boundary(self):
        rows = self._pair_across_spatial_boundary(10.1)
        self.assertGreater(school.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 10)
        self.assertEqual(len(school.deduplicate_school_pois(rows)), 2)

    def test_complete_link_clustering_avoids_transitive_chain_merge(self):
        base = poi("school", 30, name="Chain")
        rows = [base, moved(poi("school", 20, name="chain"), 60),
                moved(poi("school", 10, name="CHAIN"), 120)]
        result = school.deduplicate_school_pois(rows)
        self.assertEqual(len(result), 2)

    def test_deduplication_is_deterministic_across_input_order(self):
        rows = [poi("school", 30, name="Alpha", source="node", school_level="1"),
                moved(poi("school", 20, name="alpha", source="polygon",
                          isced_level="1"), 20),
                moved(poi("school", 10, name="ALPHA", source="node"), 100)]
        expected = school.deduplicate_school_pois(rows)
        for ordering in itertools.permutations(rows):
            self.assertEqual(school.deduplicate_school_pois(ordering), expected)

    def test_duplicate_counts_once_for_proximity_and_choice(self):
        farther = moved(poi("school", 120, name="Duplicate", source="polygon"), 30)
        rows = school.deduplicate_school_pois([
            poi("school", 100, name="duplicate", source="node"), farther])
        components = school.school_score_components(rows)
        factor = 1 - 100 / 2500
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["d_lin"], 100)
        self.assertAlmostEqual(components["proximity_points"], 7 * factor)
        self.assertAlmostEqual(components["choice_points"], factor)


class SchoolOrchestrationTests(unittest.TestCase):
    def test_display_radius_does_not_change_score_but_changes_population(self):
        rows = [poi("school", 300, name="Near"),
                poi("kindergarten", 1800, name="Mid"),
                poi("school", 3500, name="Far")]

        def candidates(_con, _nodes, _polys, _lat, _lon, radius):
            return [dict(row) for row in rows if row["d_lin"] <= radius]

        scores, counts = [], []
        with mock.patch.object(app, "query_school_candidates", side_effect=candidates) as query:
            for radius in (1000, 2500, 5000):
                query.reset_mock()
                frame, score, count, _nearest = app.analyze_school(
                    object(), "nodes", "polys", 50.0, 4.0, radius, 20)
                scores.append(score)
                counts.append(count)
                called_radii = [call.args[-1] for call in query.call_args_list]
                self.assertEqual(called_radii,
                                 [2500] if radius == 2500 else [2500, radius])
                if radius == 5000:
                    self.assertIn("Far", set(frame["name"]))
        self.assertEqual(scores, [scores[0]] * 3)
        self.assertEqual(counts, [1, 2, 3])
        self.assertAlmostEqual(scores[0], school.calc_school_score(rows[:2]))

    def test_kindergarten_only_has_choice_without_proximity(self):
        rows = [poi("kindergarten", 300, name="Kindergarten")]
        with mock.patch.object(app, "query_school_candidates", return_value=rows):
            frame, score, count, nearest = app.analyze_school(
                object(), "nodes", "polys", 50.0, 4.0, 2500, 20)
        expected = 0.4 * (1 - 300 / 2500)
        self.assertAlmostEqual(score, expected)
        self.assertEqual(count, 1)
        self.assertEqual(nearest, 300)
        self.assertEqual(list(frame["amenity"]), ["kindergarten"])

    def test_empty_display_can_have_nonzero_fixed_score(self):
        rows = [poi("school", 1500, name="Outside display")]

        def candidates(_con, _nodes, _polys, _lat, _lon, radius):
            return [dict(row) for row in rows if row["d_lin"] <= radius]

        with mock.patch.object(app, "query_school_candidates", side_effect=candidates):
            frame, score, count, nearest = app.analyze_school(
                object(), "nodes", "polys", 50.0, 4.0, 1000, 20)
        self.assertGreater(score, 0)
        self.assertTrue(frame.empty)
        self.assertEqual(count, 0)
        self.assertIsNone(nearest)

    def test_results_are_ordered_by_distance_with_stable_ties(self):
        rows = [poi("school", 500, name="Zulu"),
                poi("kindergarten", 100, name="Near"),
                poi("school", 500, name="Alpha")]
        with mock.patch.object(app, "query_school_candidates", return_value=rows):
            frame, _score, _count, _nearest = app.analyze_school(
                object(), "nodes", "polys", 50.0, 4.0, 2500, 20)
        self.assertEqual(list(frame["name"]), ["Near", "Alpha", "Zulu"])

    def test_generic_school_calls_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, "School Score V1"):
            app.query_category(object(), None, None, "school", 50.0, 4.0, 2500, 5)
        with self.assertRaisesRegex(ValueError, "School Score V1"):
            app.calc_category_score("school", 1, 100)

    def test_non_school_scoring_configuration_and_formulas_are_unchanged(self):
        expected = {
            "sport": {"D0": 1500, "w_prox": 5.0, "w_count": 5.0, "Nsat": 3},
        }
        for category, config in expected.items():
            self.assertEqual(app.SCORING[category], config)
            raw = (1 - 300 / config["D0"]) * config["w_prox"]
            raw += min(2, config["Nsat"]) / config["Nsat"] * config["w_count"]
            self.assertAlmostEqual(
                app.calc_category_score(category, 2, 300),
                min(10.0, raw))
        with self.assertRaisesRegex(ValueError, "Transit Score V1"):
            app.calc_category_score("transit", 2, 300)
        with self.assertRaisesRegex(ValueError, "Park Score V1"):
            app.calc_category_score("park", 2, 300)
        market_row = {"name": "Market", "brand": None, "lat": 50.0, "lon": 4.0,
                      "amenity": None, "shop": "supermarket", "source": "node",
                      "d_lin": 300.0}
        self.assertAlmostEqual(market.calc_market_score([market_row]),
                               7 * (1 - 300 / 2500) + 1)


if __name__ == "__main__":
    unittest.main()

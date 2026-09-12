import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

from shapely.geometry import Point, Polygon


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "belgium-location" / "build_sport_cache.py"
SPEC = importlib.util.spec_from_file_location("build_sport_cache", MODULE_PATH)
sport_cache = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sport_cache
SPEC.loader.exec_module(sport_cache)


def square(x=4.0, y=50.0, size=0.01):
    return Polygon([
        (x, y), (x + size, y), (x + size, y + size),
        (x, y + size), (x, y),
    ])


def feature(osm_type, osm_id, tags, geometry=None):
    if geometry is None:
        geometry = Point(4.0 + osm_id / 1_000_000, 50.0)
    return {
        "osm_type": osm_type,
        "osm_id": osm_id,
        "tags": tags,
        "geometry": geometry,
    }


def build(*features, memberships=None, relation_tags=None):
    rows, diagnostics = sport_cache.build_destination_rows(
        list(features), memberships or {}, relation_tags or {},
        source_path="test.osm.pbf",
        source_timestamp="2026-09-12T00:00:00+00:00")
    return rows, diagnostics


def by_id(rows, osm_id):
    return next(row for row in rows if row["canonical_osm_id"] == osm_id)


class SportEligibilityTests(unittest.TestCase):
    def test_primary_destination_is_eligible(self):
        rows, _ = build(feature("way", 1, {"leisure": "sports_centre"}, square()))
        self.assertTrue(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["facility_class"], "general_sports_centre")

    def test_known_sports_make_multi_sport_centre(self):
        rows, _ = build(feature("way", 1, {
            "leisure": "sports_centre", "sport": "soccer;tennis"}, square()))
        self.assertEqual(rows[0]["facility_class"], "multi_sport_centre")
        self.assertEqual(rows[0]["sport_count"], 2)

    def test_literal_multi_does_not_manufacture_sport_count(self):
        rows, _ = build(feature("way", 1, {
            "leisure": "sports_centre", "sport": "multi"}, square()))
        self.assertEqual(rows[0]["facility_class"], "general_sports_centre")
        self.assertEqual(rows[0]["sports"], ["multi"])
        self.assertEqual(rows[0]["sport_count"], 0)

    def test_specialized_facility_classification(self):
        rows, _ = build(feature("way", 1, {
            "leisure": "sports_centre", "sport": "climbing"}, square()))
        self.assertTrue(rows[0]["specialized"])
        self.assertEqual(rows[0]["facility_class"], "specialized")

    def test_dedicated_sport_club_can_be_specialized(self):
        rows, _ = build(feature("way", 1, {
            "club": "sport", "sport": "archery", "name": "Archers"}, square()))
        self.assertTrue(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["facility_class"], "specialized")

    def test_bare_sport_tag_is_not_assumed_to_be_a_destination(self):
        rows, _ = build(feature("way", 1, {"sport": "soccer", "name": "Field"}, square()))
        self.assertFalse(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["eligibility_reason"], "excluded_unproven_sport_tag")

    def test_named_explicit_sport_site_is_dedicated(self):
        rows, _ = build(feature("relation", 1, {
            "type": "site", "sport": "rugby_union",
            "name": "Rugby Club"}, square()))
        self.assertTrue(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["facility_class"], "specialized")

    def test_animal_and_model_sports_are_excluded(self):
        for sport in ("dog_sport", "model_aerodrome"):
            with self.subTest(sport=sport):
                rows, _ = build(feature("way", 1, {
                    "club": "sport", "sport": sport, "name": "Club"}, square()))
                self.assertFalse(rows[0]["score_eligible"])

    def test_commercial_fitness_and_fee_are_eligible(self):
        rows, _ = build(feature("node", 1, {
            "leisure": "fitness_centre", "name": "Basic-Fit",
            "fee": "yes", "access": "customers"}))
        row = rows[0]
        self.assertTrue(row["score_eligible"])
        self.assertTrue(row["is_commercial"])
        self.assertEqual(row["facility_class"], "fitness_gym")
        self.assertEqual(row["fee"], "yes")

    def test_private_and_no_are_excluded(self):
        for access in ("private", "no"):
            with self.subTest(access=access):
                rows, _ = build(feature("way", 1, {
                    "leisure": "sports_hall", "access": access}, square()))
                self.assertFalse(rows[0]["score_eligible"])
                self.assertEqual(rows[0]["access_class"], access)

    def test_incidental_pub_sport_is_excluded(self):
        rows, _ = build(feature("node", 1, {
            "amenity": "pub", "sport": "darts", "name": "The Pub"}))
        self.assertFalse(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["eligibility_reason"],
                         "excluded_incidental_business_sport")

    def test_named_standalone_pitch_is_eligible(self):
        rows, _ = build(feature("way", 1, {
            "leisure": "pitch", "sport": "soccer", "name": "Local Pitch"}, square()))
        self.assertTrue(rows[0]["score_eligible"])
        self.assertTrue(rows[0]["standalone"])

    def test_unnamed_unknown_pitch_is_display_only(self):
        rows, _ = build(feature("way", 189639931, {
            "leisure": "pitch", "sport": "soccer"}, square()))
        self.assertFalse(rows[0]["score_eligible"])
        self.assertEqual(rows[0]["eligibility_reason"],
                         "excluded_unnamed_unknown_standalone")

    def test_school_context_requires_positive_general_access(self):
        school = feature("way", 10, {"amenity": "school"}, square())
        hall = feature("way", 11, {"leisure": "sports_hall", "name": "School Hall"},
                       square(4.002, 50.002, 0.002))
        rows, _ = build(school, hall)
        self.assertTrue(rows[0]["is_school_context"])
        self.assertFalse(rows[0]["score_eligible"])

    def test_school_context_with_public_access_is_eligible(self):
        school = feature("way", 10, {"amenity": "school"}, square())
        hall = feature("way", 11, {
            "leisure": "sports_hall", "name": "Community Hall", "access": "yes"},
            square(4.002, 50.002, 0.002))
        rows, _ = build(school, hall)
        self.assertTrue(rows[0]["is_school_context"])
        self.assertTrue(rows[0]["score_eligible"])


class SportCanonicalizationTests(unittest.TestCase):
    def test_parent_absorbs_pitch(self):
        parent = feature("way", 1, {"leisure": "sports_centre", "name": "Laaksite"}, square())
        pitch = feature("way", 2, {"leisure": "pitch", "sport": "soccer"},
                        square(4.002, 50.002, 0.002))
        rows, _ = build(parent, pitch)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["absorbed_component_keys"], ["way:2"])
        self.assertEqual(rows[0]["component_sports"], ["soccer"])

    def test_repeated_courts_are_components_not_sport_choices(self):
        parent = feature("way", 1, {"leisure": "sports_centre", "name": "Tennis Club"}, square())
        courts = [feature("way", index, {"leisure": "pitch", "sport": "tennis"},
                          square(4.001 + index / 10000, 50.001, 0.0001))
                  for index in range(2, 9)]
        rows, _ = build(parent, *courts)
        self.assertEqual(rows[0]["component_count"], 7)
        self.assertEqual(rows[0]["sport_count"], 1)
        self.assertEqual(rows[0]["component_sports"], ["tennis"])

    def test_different_component_sports_enrich_diversity(self):
        parent = feature("way", 1, {"leisure": "sports_centre"}, square())
        tennis = feature("way", 2, {"leisure": "pitch", "sport": "tennis"},
                          square(4.001, 50.001, 0.001))
        soccer = feature("way", 3, {"leisure": "pitch", "sport": "soccer"},
                          square(4.004, 50.004, 0.001))
        rows, _ = build(parent, tennis, soccer)
        self.assertEqual(rows[0]["sports"], ["soccer", "tennis"])
        self.assertEqual(rows[0]["facility_class"], "multi_sport_centre")

    def test_swimming_pool_is_absorbed(self):
        parent = feature("way", 1, {
            "leisure": "sports_centre", "name": "Sportoase"}, square())
        pool = feature("way", 2, {"leisure": "swimming_pool", "sport": "swimming"},
                       square(4.002, 50.002, 0.002))
        rows, _ = build(parent, pool)
        self.assertEqual(rows[0]["component_count"], 1)
        self.assertIn("swimming", rows[0]["component_sports"])

    def test_stadium_absorbs_inner_pitch(self):
        stadium = feature("way", 1, {"leisure": "stadium", "name": "King Power"}, square())
        pitch = feature("way", 2, {"leisure": "pitch", "sport": "soccer"},
                        square(4.002, 50.002, 0.005))
        rows, _ = build(stadium, pitch)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["facility_class"], "stadium")

    def test_explicit_site_relation_precedes_geometric_parent(self):
        site = feature("relation", 90, {
            "type": "site", "site": "sports", "leisure": "sports_centre",
            "name": "Explicit Site"}, square(5.0, 51.0))
        geometric = feature("way", 1, {
            "leisure": "sports_centre", "name": "Geometric Parent"}, square())
        pitch = feature("way", 2, {"leisure": "pitch", "sport": "soccer"},
                        square(4.002, 50.002, 0.002))
        memberships = {("relation", 90): [(('way', 2), 'facility')]}
        rows, _ = build(site, geometric, pitch, memberships=memberships)
        self.assertIn("way:2", by_id(rows, 90)["absorbed_component_keys"])
        self.assertNotIn("way:2", by_id(rows, 1)["absorbed_component_keys"])
        self.assertIn(
            "relation:90:member=way:2:role=facility",
            by_id(rows, 90)["relation_membership_roles"])

    def test_geometryless_site_relation_owns_facilities_and_components(self):
        centre = feature("way", 1, {
            "leisure": "sports_centre", "name": "Site Centre"}, square())
        hall = feature("way", 2, {"leisure": "sports_hall"},
                       square(4.002, 50.002, 0.003))
        pitch = feature("way", 3, {"leisure": "pitch", "sport": "soccer"},
                        square(4.003, 50.003, 0.001))
        memberships = {("relation", 90): [
            (("way", 1), "facility"), (("way", 2), "facility"),
            (("way", 3), "pitch")]}
        rows, _ = build(
            centre, hall, pitch, memberships=memberships,
            relation_tags={
                ("relation", 90): {
                    "type": "site", "site": "sports",
                    "leisure": "sports_centre", "name": "Whole Site"}})
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["canonical_osm_type"], "relation")
        self.assertEqual(row["canonical_osm_id"], 90)
        self.assertEqual(row["display_name"], "Whole Site")
        self.assertEqual(row["component_count"], 3)
        self.assertEqual(
            set(row["source_osm_keys"]),
            {"relation:90", "way:1", "way:2", "way:3"})
        self.assertIn(
            "relation:90:member=way:2:role=facility",
            row["relation_membership_roles"])

    def test_no_name_only_merge(self):
        first = feature("node", 1, {"leisure": "fitness_centre", "name": "Same"}, Point(4, 50))
        second = feature("node", 2, {"leisure": "fitness_centre", "name": "Same"}, Point(5, 51))
        rows, _ = build(first, second)
        self.assertEqual(len(rows), 2)

    def test_no_proximity_only_merge(self):
        first = feature("node", 1, {"leisure": "fitness_centre", "name": "One"}, Point(4, 50))
        second = feature("node", 2, {"leisure": "fitness_centre", "name": "Two"}, Point(4.00001, 50))
        rows, _ = build(first, second)
        self.assertEqual(len(rows), 2)

    def test_commercial_node_polygon_are_not_merged_by_name_and_distance(self):
        polygon = feature("way", 1, {"leisure": "fitness_centre", "name": "Basic-Fit"}, square())
        node = feature("node", 2, {"leisure": "fitness_centre", "name": "Basic-Fit"}, Point(4.005, 50.005))
        rows, _ = build(polygon, node)
        self.assertEqual(len(rows), 2)

    def test_strong_identity_merges_with_geographic_sanity(self):
        polygon = feature("way", 1, {
            "leisure": "sports_centre", "wikidata": "Q123"}, square())
        node = feature("node", 2, {
            "leisure": "sports_centre", "wikidata": "Q123"}, Point(4.005, 50.005))
        rows, _ = build(polygon, node)
        self.assertEqual(len(rows), 1)
        self.assertEqual(set(rows[0]["source_osm_keys"]), {"way:1", "node:2"})

    def test_strong_identity_does_not_merge_remote_objects(self):
        first = feature("node", 1, {"leisure": "sports_centre", "wikidata": "Q123"}, Point(4, 50))
        second = feature("node", 2, {"leisure": "sports_centre", "wikidata": "Q123"}, Point(5, 51))
        rows, _ = build(first, second)
        self.assertEqual(len(rows), 2)

    def test_nested_absorption_preserves_child_components(self):
        parent = feature("way", 1, {"leisure": "sports_centre", "name": "Sportoase"}, square())
        hall = feature("way", 2, {"leisure": "sports_hall", "name": "Sportoase Hall"},
                       square(4.002, 50.002, 0.004))
        court = feature("way", 3, {"leisure": "pitch", "sport": "basketball"},
                        square(4.003, 50.003, 0.001))
        rows, _ = build(parent, hall, court)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["component_count"], 2)
        self.assertIn("way:2", rows[0]["absorbed_component_keys"])
        self.assertIn("way:3", rows[0]["absorbed_component_keys"])
        self.assertIn("basketball", rows[0]["component_sports"])

    def test_display_name_operator_fallback_does_not_change_identity(self):
        rows, _ = build(feature("way", 11, {
            "leisure": "sports_hall", "operator": "Town Sports"}, square()))
        row = rows[0]
        self.assertEqual(row["display_name"], "Town Sports")
        self.assertEqual(row["display_name_source"], "operator")
        self.assertEqual(row["sport_id"], "sport:osm:way:11")

    def test_polygon_geometry_area_bbox_and_lineage_are_retained(self):
        rows, _ = build(feature("way", 1, {"leisure": "sports_centre"}, square()))
        row = rows[0]
        self.assertGreater(len(row["geometry_wkb"]), 20)
        self.assertGreater(row["area_m2"], 0)
        self.assertLess(row["bbox_min_lat"], row["bbox_max_lat"])
        self.assertEqual(row["source_osm_keys"], ["way:1"])
        self.assertEqual(row["source_pbf_timestamp"], "2026-09-12T00:00:00+00:00")

    def test_entrance_is_used_only_when_on_polygon_edge(self):
        parent = feature("way", 1, {"leisure": "sports_centre"}, square())
        entrance = feature("node", 2, {"entrance": "yes"}, Point(4.0, 50.005))
        rows, _ = build(parent, entrance)
        self.assertEqual(rows[0]["entrance_osm_key"], "node:2")

    def test_build_is_deterministic(self):
        features = [
            feature("way", 1, {"leisure": "sports_centre"}, square()),
            feature("way", 2, {"leisure": "pitch", "sport": "soccer"},
                    square(4.002, 50.002, 0.002)),
            feature("node", 3, {"leisure": "fitness_centre", "name": "Gym"}),
        ]
        first, first_diagnostics = sport_cache.build_destination_rows(features)
        second, second_diagnostics = sport_cache.build_destination_rows(list(reversed(features)))
        comparable = lambda rows: [
            {key: value for key, value in row.items()
             if key not in {"geometry_wkb", "source_representations"}}
            for row in rows]
        self.assertEqual(comparable(first), comparable(second))
        self.assertEqual(first_diagnostics, second_diagnostics)

    def test_safety_validation_detects_no_duplicate_owners(self):
        parent = feature("way", 1, {"leisure": "sports_centre"}, square())
        pitch = feature("way", 2, {"leisure": "pitch", "name": "Pitch"},
                        square(4.002, 50.002, 0.002))
        rows, _ = build(parent, pitch)
        self.assertTrue(all(value == 0 for value in sport_cache.validate_rows(rows).values()))

    def test_source_tags_are_stable_json(self):
        rows, _ = build(feature("node", 1, {
            "name": "Gym", "leisure": "fitness_centre", "fee": "yes"}))
        self.assertEqual(json.loads(rows[0]["source_tags"])["fee"], "yes")


class SportParquetTests(unittest.TestCase):
    def test_atomic_parquet_write_has_required_schema(self):
        rows, _ = build(feature("way", 1, {
            "leisure": "sports_centre", "name": "Centre"}, square()))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "sport.parquet"
            sport_cache._write_atomic(rows, output)
            import pyarrow.parquet as pq
            table = pq.read_table(output)
            self.assertEqual(table.num_rows, 1)
            required = {
                "sport_id", "facility_class", "score_eligible",
                "geometry_wkb", "sports", "source_osm_keys",
                "canonicalization_evidence", "build_version",
            }
            self.assertTrue(required <= set(table.schema.names))


if __name__ == "__main__":
    unittest.main()

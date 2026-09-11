from pathlib import Path
import json
import sys
import tempfile
import textwrap
import unittest

from shapely.geometry import Point, Polygon, box


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import build_park_cache as park
finally:
    sys.path.pop(0)

try:
    import osmium  # noqa: F401
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    osmium = None
    pq = None


def feature(identifier, tags, *, osm_type="way", geometry=None, area=1_000):
    if geometry is None:
        geometry = Point(4.0, 50.0) if osm_type == "node" else box(4, 50, 4.001, 50.001)
    value = {
        "osm_type": osm_type,
        "osm_id": identifier,
        "tags": tags,
        "geometry": geometry,
    }
    if osm_type != "node":
        value["area_m2"] = area
    return value


def rows_for(*features, memberships=None):
    return park.build_destination_rows(features, memberships or {})[0]


class ParkEligibilityTests(unittest.TestCase):
    def test_bare_non_destination_landuses_are_excluded(self):
        rows = rows_for(
            feature(1, {"landuse": "grass"}),
            feature(2, {"landuse": "forest"}),
            feature(3, {"landuse": "meadow"}),
        )
        self.assertEqual(rows, [])

    def test_restricted_access_always_excludes(self):
        for index, access in enumerate(("private", "no", "customers"), 1):
            with self.subTest(access=access):
                self.assertEqual(rows_for(feature(index, {
                    "leisure": "park", "access": access,
                    "name": "Restricted",
                })), [])

    def test_positive_access_values_are_normalized(self):
        for index, access in enumerate(("yes", "permissive", "public", "designated"), 1):
            with self.subTest(access=access):
                row = rows_for(feature(index, {
                    "leisure": "garden", "access": access,
                }, area=200))[0]
                self.assertTrue(row["has_positive_access"])

    def test_garden_thresholds(self):
        self.assertEqual(rows_for(feature(1, {
            "leisure": "garden", "access": "yes"}, area=199.999)), [])
        self.assertEqual(rows_for(feature(2, {
            "leisure": "garden", "name": "G", "operator": "O"}, area=499.999)), [])
        for identifier, tags, area in (
            (3, {"leisure": "garden", "access": "yes"}, 200),
            (4, {"leisure": "garden", "name": "G", "operator": "O"}, 500),
            (5, {"leisure": "garden", "name": "G", "opening_hours": "24/7"}, 500),
        ):
            with self.subTest(identifier=identifier):
                self.assertEqual(rows_for(feature(identifier, tags, area=area))[0]["park_class"], "garden")

    def test_recreation_and_village_green_thresholds(self):
        cases = (
            ({"leisure": "recreation_ground", "name": "R"}, 499.9, False),
            ({"leisure": "recreation_ground", "name": "R"}, 500, True),
            ({"leisure": "recreation_ground"}, 500, False),
            ({"landuse": "recreation_ground", "access": "yes"}, 999.9, False),
            ({"landuse": "recreation_ground", "access": "yes"}, 1000, True),
            ({"landuse": "village_green", "name": "V"}, 499.9, False),
            ({"landuse": "village_green", "name": "V"}, 500, True),
        )
        for identifier, (tags, area, expected) in enumerate(cases, 1):
            with self.subTest(tags=tags, area=area):
                self.assertEqual(bool(rows_for(feature(identifier, tags, area=area))), expected)

    def test_protected_area_threshold_and_evidence(self):
        base = {"boundary": "protected_area", "name": "Reserve"}
        self.assertEqual(rows_for(feature(1, {**base, "access": "yes"}, area=9999.9)), [])
        self.assertEqual(rows_for(feature(2, base, area=10000)), [])
        self.assertTrue(rows_for(feature(3, {**base, "access": "yes"}, area=10000)))
        self.assertTrue(rows_for(feature(4, {**base, "operator": "Agency"}, area=10000)))

    def test_secondary_evidence_and_weights(self):
        self.assertEqual(rows_for(feature(1, {"leisure": "playground"})), [])
        playground = rows_for(feature(2, {"leisure": "playground", "name": "Play"}))[0]
        dog_park = rows_for(feature(3, {"leisure": "dog_park", "access": "yes"}))[0]
        self.assertEqual((playground["secondary_type_weight"], playground["secondary_confidence"]), (1.0, 0.80))
        self.assertEqual((dog_park["secondary_type_weight"], dog_park["secondary_confidence"]), (0.6, 0.95))

    def test_node_policy(self):
        self.assertEqual(rows_for(feature(1, {"leisure": "park", "name": "Point"}, osm_type="node")), [])
        row = rows_for(feature(2, {"leisure": "playground", "name": "Play"}, osm_type="node"))[0]
        self.assertFalse(row["counts_as_primary"])
        self.assertFalse(row["counts_as_choice"])
        self.assertTrue(row["counts_as_secondary"])
        self.assertIsNone(row["area_m2"])
        self.assertIsNone(row["size_factor"])


class ParkCanonicalizationTests(unittest.TestCase):
    def test_relation_owns_same_tagged_outer_way(self):
        relation = feature(100, {"type": "multipolygon", "leisure": "park", "name": "Park"}, osm_type="relation", area=5000)
        member = feature(10, {"leisure": "park", "name": "Park outline"}, area=5000)
        rows = rows_for(relation, member, memberships={
            ("relation", 100): [(('way', 10), 'outer')],
        })
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["park_id"], "park:osm:relation:100")
        self.assertEqual(rows[0]["absorbed_member_osm_keys"], ["way:10"])

    def test_relation_ownership_does_not_depend_on_matching_source_class(self):
        relation = feature(100, {"type": "multipolygon", "leisure": "park", "name": "Park"}, osm_type="relation", area=5000)
        member = feature(10, {"leisure": "nature_reserve", "name": "Outer"}, area=5000)
        rows = rows_for(relation, member, memberships={
            ("relation", 100): [(('way', 10), 'outer')],
        })
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["canonical_osm_type"], "relation")

    def test_canonical_display_name_wins(self):
        relation = feature(100, {"leisure": "park", "name": "Canonical"}, osm_type="relation")
        member = feature(10, {"leisure": "park", "name": "Member"})
        row = rows_for(relation, member, memberships={
            ("relation", 100): [(('way', 10), 'outer')],
        })[0]
        self.assertEqual((row["display_name"], row["display_name_source"]),
                         ("Canonical", "canonical"))

    def test_one_unique_absorbed_display_name_is_recovered(self):
        relation = feature(100, {"leisure": "park"}, osm_type="relation")
        member = feature(10, {"leisure": "park", "name": "Member"})
        row = rows_for(relation, member, memberships={
            ("relation", 100): [(('way', 10), 'outer')],
        })[0]
        self.assertEqual((row["display_name"], row["display_name_source"]),
                         ("Member", "absorbed_member"))
        self.assertIsNone(row["name"])
        self.assertFalse(row["counts_as_choice"])

    def test_duplicate_absorbed_names_are_unambiguous(self):
        relation = feature(100, {"leisure": "park"}, osm_type="relation")
        members = [feature(identifier, {"leisure": "park", "name": "Same"})
                   for identifier in (10, 11)]
        row = rows_for(relation, *members, memberships={
            ("relation", 100): [(('way', 10), 'outer'),
                                (('way', 11), 'outer')],
        })[0]
        self.assertEqual((row["display_name"], row["display_name_source"]),
                         ("Same", "absorbed_member"))

    def test_conflicting_absorbed_names_are_ambiguous(self):
        relation = feature(100, {"leisure": "park"}, osm_type="relation")
        members = [feature(10, {"leisure": "park", "name": "Alpha"}),
                   feature(11, {"leisure": "park", "name": "Beta"})]
        row = rows_for(relation, *members, memberships={
            ("relation", 100): [(('way', 10), 'outer'),
                                (('way', 11), 'outer')],
        })[0]
        self.assertEqual((row["display_name"], row["display_name_source"]),
                         (None, "ambiguous"))
        self.assertFalse(row["counts_as_choice"])

    def test_explicit_relation_membership_precedes_spatial_attachment(self):
        parent = feature(100, {"leisure": "park", "name": "Parent"}, osm_type="relation", geometry=box(0, 0, 1, 1), area=10000)
        child = feature(10, {"leisure": "playground", "name": "Child"}, geometry=box(5, 5, 6, 6), area=1000)
        row = [row for row in rows_for(parent, child, memberships={
            ("relation", 100): [(('way', 10), 'inner')],
        }) if row["osm_id"] == 10][0]
        self.assertEqual(row["parent_park_id"], "park:osm:relation:100")
        self.assertEqual(row["attachment_method"], "relation")

    def test_spatial_attachment_at_80_percent_and_not_79_9(self):
        child_geometry = box(0, 0, 1, 1)
        child = feature(10, {"landuse": "recreation_ground", "name": "Child"}, geometry=child_geometry, area=1000)

        def longitude_for_geodesic_ratio(target):
            low, high = 0.0, 1.0
            child_area = park.geodesic_area_m2(child_geometry)
            for _ in range(60):
                middle = (low + high) / 2
                ratio = park.geodesic_area_m2(box(0, 0, middle, 1)) / child_area
                if ratio < target:
                    low = middle
                else:
                    high = middle
            return high

        parent80 = feature(1, {"leisure": "park", "name": "Parent"}, geometry=box(0, 0, longitude_for_geodesic_ratio(.8), 1), area=10000)
        attached = [row for row in rows_for(parent80, child) if row["osm_id"] == 10][0]
        self.assertAlmostEqual(attached["containment_ratio"], 0.8, delta=0.001)
        self.assertFalse(attached["counts_as_primary"])
        parent799 = feature(2, {"leisure": "park", "name": "Parent"}, geometry=box(0, 0, longitude_for_geodesic_ratio(.799), 1), area=10000)
        independent = [row for row in rows_for(parent799, child) if row["osm_id"] == 10][0]
        self.assertIsNone(independent["parent_park_id"])
        self.assertTrue(independent["counts_as_primary"])

    def test_multiple_parent_tie_uses_smallest_parent_area(self):
        child = feature(10, {"leisure": "playground", "name": "Child"}, osm_type="node", geometry=Point(.5, .5))
        large = feature(1, {"leisure": "park", "name": "Large"}, geometry=box(0, 0, 1, 1), area=100000)
        small = feature(2, {"leisure": "park", "name": "Small"}, geometry=box(.25, .25, .75, .75), area=10000)
        child_row = [row for row in rows_for(large, small, child) if row["osm_id"] == 10][0]
        self.assertEqual(child_row["parent_park_id"], "park:osm:way:2")

    def test_strong_identity_precedence_and_fallbacks(self):
        rows = rows_for(
            feature(1, {"leisure": "park", "name": "A", "wikidata": "Q1", "wikipedia": "en:A"}),
            feature(2, {"leisure": "park", "name": "B", "wikipedia": "nl:B"}),
            feature(3, {"leisure": "park", "name": "C"}),
        )
        values = {row["osm_id"]: (row["strong_identity_type"], row["strong_identity_value"]) for row in rows}
        self.assertEqual(values[1], ("wikidata", "Q1"))
        self.assertEqual(values[2], ("wikipedia", "nl:B"))
        self.assertEqual(values[3], ("osm", "way:3"))

    def test_same_name_does_not_merge_and_ids_are_order_stable(self):
        first = feature(1, {"leisure": "park", "name": "Same"})
        second = feature(2, {"leisure": "park", "name": "Same"})
        forward = rows_for(first, second)
        reverse = rows_for(second, first)
        self.assertEqual(len(forward), 2)
        self.assertEqual([row["park_id"] for row in forward], [row["park_id"] for row in reverse])

    def test_unnamed_osm_fallback_is_not_a_choice(self):
        row = rows_for(feature(1, {"leisure": "park"}))[0]
        self.assertEqual(row["strong_identity_type"], "osm")
        self.assertFalse(row["counts_as_choice"])


class ParkMetadataTests(unittest.TestCase):
    def test_size_factor_boundaries(self):
        expected = {0: .45, 499.999: .45, 500: .60, 1999.999: .60,
                    2000: .75, 9999.999: .75, 10000: .90,
                    49999.999: .90, 50000: 1.0}
        for area, value in expected.items():
            with self.subTest(area=area):
                self.assertEqual(park.size_factor(area), value)

    def test_confidence_bonus_order_and_reserve_caps(self):
        rich = {"name": "Reserve", "operator": "Agency", "opening_hours": "24/7"}
        self.assertEqual(park.confidence_for("nature_reserve", rich), (.70, .50))
        self.assertEqual(park.confidence_for("national_park", rich), (.58, .35))
        self.assertEqual(park.confidence_for("protected_area", rich), (.60, .35))
        base, final = park.confidence_for("nature_reserve", {**rich, "access": "yes"})
        self.assertEqual(base, .70)
        self.assertAlmostEqual(final, .85)
        self.assertEqual(park.confidence_for("park", {**rich, "access": "yes"}), (.93, 1.0))

    def test_malformed_or_non_polygon_geometry_is_rejected(self):
        bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        repaired = rows_for(feature(1, {"leisure": "park"}, geometry=bowtie))
        self.assertEqual(len(repaired), 1)
        self.assertFalse(repaired[0]["geometry_was_valid"])
        self.assertTrue(repaired[0]["geometry_valid"])
        rows, diagnostics = park.build_destination_rows([
            feature(2, {"leisure": "park"}, geometry=None),
        ])
        # The helper's default geometry is valid; an explicit empty polygon fails.
        rows, diagnostics = park.build_destination_rows([{
            "osm_type": "way", "osm_id": 3, "tags": {"leisure": "park"},
            "geometry": Polygon(),
        }])
        self.assertEqual(rows, [])
        self.assertEqual(diagnostics["geometry_failure"], 1)


@unittest.skipUnless(osmium is not None and pq is not None, "osmium/pyarrow unavailable")
class ParkCacheIntegrationTests(unittest.TestCase):
    def test_small_osm_extract_builds_contract_parquet(self):
        fixture = """
        <osm version="0.6" generator="park-cache-test">
          <node id="1" version="1" lat="50.0000" lon="4.0000"/>
          <node id="2" version="1" lat="50.0000" lon="4.0010"/>
          <node id="3" version="1" lat="50.0010" lon="4.0010"/>
          <node id="4" version="1" lat="50.0010" lon="4.0000"/>
          <node id="20" version="1" lat="50.0005" lon="4.0005">
            <tag k="leisure" v="playground"/><tag k="name" v="Play"/>
          </node>
          <way id="10" version="1">
            <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
            <tag k="leisure" v="park"/><tag k="name" v="Test Park"/>
          </way>
          <way id="30" version="1">
            <nd ref="1"/><nd ref="2"/>
            <tag k="leisure" v="park"/><tag k="name" v="Open Park Line"/>
          </way>
          <way id="31" version="1">
            <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
            <tag k="leisure" v="nature_reserve"/><tag k="area" v="no"/>
            <tag k="name" v="Closed Non-area Boundary"/>
          </way>
          <relation id="40" version="1">
            <member type="way" ref="10" role=""/>
            <tag k="type" v="site"/><tag k="leisure" v="park"/>
            <tag k="name" v="Unsupported Site"/>
          </relation>
        </osm>
        """
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "fixture.osm"
            output = Path(temporary) / "park.parquet"
            source.write_text(textwrap.dedent(fixture), encoding="utf-8")
            diagnostics = park.build_park_cache(source, output)
            table = pq.read_table(output)
            self.assertEqual(table.schema.names, park.park_schema().names)
            rows = table.to_pylist()
            self.assertEqual(len(rows), 2)
            polygon = next(row for row in rows if row["osm_type"] == "way")
            point = next(row for row in rows if row["osm_type"] == "node")
            self.assertGreater(polygon["area_m2"], 0)
            self.assertEqual(point["parent_park_id"], polygon["park_id"])
            self.assertEqual(diagnostics["geometry_failure_count"], 1)
            self.assertEqual(diagnostics["excluded_non_area_ways"], 2)
            self.assertEqual(diagnostics["open_way_count"], 1)
            self.assertEqual(diagnostics["closed_area_no_count"], 1)
            rejections = json.loads(diagnostics["geometry_rejections_json"])
            reasons = {row["reason"] for row in rejections}
            self.assertEqual(reasons, {
                "open_non_area_way", "closed_area_no",
                "unsupported_site_relation",
            })


if __name__ == "__main__":
    unittest.main()

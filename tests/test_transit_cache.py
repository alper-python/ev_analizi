import math
from pathlib import Path
import random
import sys
import unittest


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import build_transit_cache as transit
finally:
    sys.path.pop(0)


EARTH_RADIUS_M = 6371000.0


def point(distance_m):
    return math.degrees(distance_m / EARTH_RADIUS_M), 4.0


def member(identifier, distance_m=0, name="Stop", osm_type="node", **tags):
    lat, lon = point(distance_m)
    all_tags = {"name": name, **tags}
    return {
        "osm_type": osm_type, "osm_id": identifier, "name": name,
        "lat": lat, "lon": lon, "tags": all_tags,
        "route_keys": [], "route_modes": [], "route_refs": [],
    }


def stop_area(identifier, members, name="Stop"):
    return {"id": identifier, "name": name,
            "members": [(transit._object_key(item), "platform") for item in members]}


def ownership_counts(access_rows):
    counts = {}
    for row in access_rows:
        for key in row["member_osm_keys"]:
            counts[key] = counts.get(key, 0) + 1
    return counts


class TransitCacheGroupingTests(unittest.TestCase):
    def test_stop_area_group_collapses_two_stop_areas(self):
        first = member(1, 0, highway="bus_stop", bus="yes")
        second = member(2, 20, highway="bus_stop", bus="yes")
        areas = [stop_area(10, [first]), stop_area(11, [second])]
        groups = [{"id": 20, "name": "Grouped Stop", "stop_area_ids": [11, 10]}]
        result = transit.build_logical_access([first, second], areas, groups)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["logical_stop_id"], "stop_area_group:20")
        self.assertEqual(result[0]["stop_area_ids"], [10, 11])
        self.assertEqual(result[0]["identity_source"], "stop_area_group")

    def test_standalone_stop_area_becomes_one_logical_stop(self):
        platform = member(1, highway="bus_stop", bus="yes")
        stop_position = member(2, 3, public_transport="stop_position", bus="yes")
        result = transit.build_logical_access(
            [platform, stop_position], [stop_area(10, [platform, stop_position])])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["logical_stop_id"], "stop_area:10")
        self.assertEqual(result[0]["member_count"], 2)

    def test_rail_stop_area_uses_uic_identity_and_is_rail(self):
        station = member(1, name="Rail", railway="station", train="yes",
                         public_transport="station", uic_ref="0088")
        result = transit.build_logical_access([station], [stop_area(10, [station], "Rail")])
        self.assertEqual(result[0]["logical_stop_id"], "uic:0088")
        self.assertEqual(result[0]["identity_source"], "uic")
        self.assertTrue(result[0]["is_rail_station"])
        self.assertIn("RAIL", result[0]["modes"])

    def test_same_name_opposite_direction_stop_areas_merge_within_75m(self):
        rows = [member(1, 0, highway="bus_stop", bus="yes"),
                member(2, 75, highway="bus_stop", bus="yes")]
        result = transit.build_logical_access(
            rows, [stop_area(10, [rows[0]]), stop_area(11, [rows[1]])])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["stop_area_ids"], [10, 11])

    def test_same_name_stop_areas_beyond_75m_remain_separate(self):
        rows = [member(1, 0, highway="bus_stop", bus="yes"),
                member(2, 75.1, highway="bus_stop", bus="yes")]
        result = transit.build_logical_access(
            rows, [stop_area(10, [rows[0]]), stop_area(11, [rows[1]])])
        self.assertEqual(len(result), 2)

    def test_different_names_inside_75m_remain_separate(self):
        rows = [member(1, 0, "Alpha", highway="bus_stop", bus="yes"),
                member(2, 20, "Beta", highway="bus_stop", bus="yes")]
        result = transit.build_logical_access(
            rows, [stop_area(10, [rows[0]], "Alpha"),
                   stop_area(11, [rows[1]], "Beta")])
        self.assertEqual(len(result), 2)

    def test_complete_link_prevents_transitive_chain_merge(self):
        rows = [member(1, 0, highway="bus_stop", bus="yes"),
                member(2, 60, highway="bus_stop", bus="yes"),
                member(3, 120, highway="bus_stop", bus="yes")]
        result = transit.build_logical_access(rows)
        self.assertEqual(sorted(row["member_count"] for row in result), [1, 2])

    def test_unnamed_same_subtype_inside_10m_merges(self):
        rows = [member(1, 0, None, highway="bus_stop", bus="yes"),
                member(2, 10, None, highway="bus_stop", bus="yes")]
        result = transit.build_logical_access(rows)
        self.assertEqual(len(result), 1)

    def test_unnamed_beyond_10m_remains_separate(self):
        rows = [member(1, 0, None, highway="bus_stop", bus="yes"),
                member(2, 10.1, None, highway="bus_stop", bus="yes")]
        self.assertEqual(len(transit.build_logical_access(rows)), 2)

    def test_bus_and_rail_modes_survive_grouping(self):
        bus = member(1, highway="bus_stop", bus="yes")
        rail = member(2, 10, railway="station", train="yes", uic_ref="1")
        areas = [stop_area(10, [bus]), stop_area(11, [rail])]
        result = transit.build_logical_access(
            [bus, rail], areas,
            [{"id": 20, "name": "Interchange", "stop_area_ids": [10, 11]}])
        self.assertEqual(result[0]["modes"], ["BUS", "RAIL"])
        self.assertTrue(result[0]["is_rail_station"])

    def test_subway_only_station_is_not_heavy_rail(self):
        subway = member(1, railway="station", station="subway", subway="yes",
                        public_transport="station")
        result = transit.build_logical_access([subway])
        self.assertFalse(result[0]["is_rail_station"])
        self.assertEqual(result[0]["modes"], ["METRO"])

    def test_train_route_evidence_creates_heavy_rail_station(self):
        station = member(1, railway="station", public_transport="station")
        station["route_modes"] = ["RAIL"]
        station["route_keys"] = ["RAIL|nmbs|ref:l"]
        result = transit.build_logical_access([station])
        self.assertTrue(result[0]["is_rail_station"])

    def test_genuine_station_without_uic_remains_heavy_rail(self):
        station = member(1, railway="halt", public_transport="stop_position",
                         train="yes")
        result = transit.build_logical_access([station])
        self.assertEqual(result[0]["logical_stop_id"], "osm:node:1")
        self.assertTrue(result[0]["is_rail_station"])

    def test_track_level_train_objects_do_not_seed_station(self):
        cases = [
            member(1, railway="buffer_stop", train="yes"),
            member(2, railway="platform", public_transport="platform", train="yes"),
            member(3, railway="stop", public_transport="stop_position", train="yes"),
            member(4, railway="construction", construction="platform", train="yes"),
            member(5, railway="track", train="yes"),
        ]
        for item in cases:
            with self.subTest(osm_id=item["osm_id"]):
                result = transit.build_logical_access([item])
                self.assertEqual(len(result), 1)
                self.assertFalse(result[0]["is_rail_station"])
                self.assertIn("RAIL", result[0]["modes"])

    def test_authoritative_uic_on_stop_position_can_seed_station(self):
        stop = member(1, railway="stop", public_transport="stop_position",
                      train="yes", uic_ref="8717210")
        result = transit.build_logical_access([stop])
        self.assertEqual(result[0]["logical_stop_id"], "uic:8717210")
        self.assertTrue(result[0]["is_rail_station"])

    def test_shared_local_member_merges_relation_candidates(self):
        left = member(1, 0, "Interchange", highway="bus_stop", bus="yes")
        shared = member(2, 10, "Interchange", highway="bus_stop", bus="yes")
        right = member(3, 20, "Interchange", highway="bus_stop", bus="yes")
        areas = [stop_area(10, [left, shared], "Interchange"),
                 stop_area(11, [shared, right], "Interchange")]
        result = transit.build_logical_access([left, shared, right], areas)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["member_count"], 3)
        self.assertEqual(result[0]["stop_area_ids"], [10, 11])
        self.assertEqual(max(ownership_counts(result).values()), 1)

    def test_uic_station_absorbs_shared_secondary_rail_area(self):
        station = member(1, 0, "Oostende", railway="station",
                         public_transport="station", train="yes", uic_ref="8891702")
        platform = member(2, 10, None, osm_type="way", railway="platform",
                          public_transport="platform", train="yes")
        stop = member(3, 15, "Oostende", railway="stop",
                      public_transport="stop_position", train="yes")
        areas = [stop_area(100, [station, platform, stop], "Oostende"),
                 stop_area(101, [platform, stop], "Perron 3")]
        result = transit.build_logical_access([station, platform, stop], areas)
        self.assertEqual([row["logical_stop_id"] for row in result], ["uic:8891702"])
        self.assertEqual(result[0]["stop_area_ids"], [100, 101])
        self.assertEqual(result[0]["member_count"], 3)
        self.assertEqual(max(ownership_counts(result).values()), 1)

    def test_uic_absorbs_direct_exact_name_station_level_duplicate(self):
        uic = member(1, 0, "Londerzeel", railway="station",
                     public_transport="station", train="yes", uic_ref="0088")
        building = member(2, 24, "Londerzeel", osm_type="way",
                          public_transport="station", train="yes")
        result = transit.build_logical_access([uic, building])
        self.assertEqual([row["logical_stop_id"] for row in result], ["uic:0088"])
        self.assertEqual(result[0]["member_count"], 2)

    def test_uic_does_not_absorb_unsafe_station_candidates(self):
        uic = member(1, 0, "Central", railway="station",
                     public_transport="station", train="yes", uic_ref="0088")
        too_far = member(2, 75.1, "Central", railway="station", train="yes")
        different = member(3, 20, "Central East", railway="station", train="yes")
        unnamed = member(4, 20, None, railway="station", train="yes")
        result = transit.build_logical_access([uic, too_far, different, unnamed])
        self.assertEqual(len(result), 4)
        self.assertEqual(sum(row["logical_stop_id"] == "uic:0088" for row in result), 1)

    def test_leuven_station_wins_over_unowned_track_objects(self):
        station = member(5467988372, 100, "Leuven", railway="station",
                         public_transport="station", train="yes", uic_ref="8833001")
        platform = member(517394755, 110, None, osm_type="way",
                          railway="platform", public_transport="platform", train="yes")
        stop = member(150463269, 120, "Leuven", railway="stop",
                      public_transport="stop_position", train="yes")
        orphan_stop = member(10169311141, 50, "Leuven", railway="stop",
                             public_transport="stop_position", train="yes")
        buffer_stop = member(150469194, 60, None, railway="buffer_stop", train="yes")
        area = stop_area(11547753, [station, platform, stop], "Leuven")
        result = transit.build_logical_access(
            [station, platform, stop, orphan_stop, buffer_stop], [area])
        rail = [row for row in result if row["is_rail_station"]]
        self.assertEqual([row["logical_stop_id"] for row in rail], ["uic:8833001"])
        self.assertEqual(rail[0]["member_count"], 3)
        self.assertNotIn("osm:node:10169311141",
                         [row["logical_stop_id"] for row in rail])
        self.assertNotIn("osm:node:150469194",
                         [row["logical_stop_id"] for row in rail])

    def test_relation_owned_member_is_not_reclaimed_by_fallback(self):
        platform = member(1, highway="bus_stop", bus="yes")
        free = member(2, 20, highway="bus_stop", bus="yes")
        result = transit.build_logical_access(
            [platform, free], [stop_area(10, [platform])])
        self.assertEqual(ownership_counts(result)["node:1"], 1)
        self.assertEqual(max(ownership_counts(result).values()), 1)

    def test_shared_member_ownership_is_deterministic_when_shuffled(self):
        rows = [member(1, 0, "Hub", highway="bus_stop", bus="yes"),
                member(2, 20, "Hub", highway="bus_stop", bus="yes"),
                member(3, 40, "Hub", highway="bus_stop", bus="yes")]
        areas = [stop_area(11, [rows[1], rows[2]], "Hub"),
                 stop_area(10, [rows[0], rows[1]], "Hub")]
        expected = transit.build_logical_access(rows, areas)
        random.Random(8).shuffle(rows)
        random.Random(9).shuffle(areas)
        actual = transit.build_logical_access(rows, areas)
        self.assertEqual(actual, expected)
        self.assertEqual(max(ownership_counts(actual).values()), 1)

    def test_stop_area_identity_fallback_without_uic(self):
        bus = member(1, highway="bus_stop", bus="yes")
        result = transit.build_logical_access([bus], [stop_area(99, [bus])])
        self.assertEqual(result[0]["logical_stop_id"], "stop_area:99")

    def test_duplicate_uic_units_coalesce_into_one_station(self):
        first = member(1, 0, "Rail", railway="station", train="yes", uic_ref="0088")
        second = member(2, 100, "Rail", railway="station", train="yes", uic_ref="0088")
        result = transit.build_logical_access(
            [first, second], [stop_area(10, [first], "Rail"),
                              stop_area(11, [second], "Rail")])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["logical_stop_id"], "uic:0088")
        self.assertEqual(result[0]["stop_area_ids"], [10, 11])
        self.assertEqual(result[0]["member_count"], 2)

    def test_osm_identity_fallback_for_single_raw_object(self):
        bus = member(42, highway="bus_stop", bus="yes")
        result = transit.build_logical_access([bus])
        self.assertEqual(result[0]["logical_stop_id"], "osm:node:42")
        self.assertEqual(result[0]["identity_source"], "osm")

    def test_deterministic_output_with_shuffled_input(self):
        rows = [member(3, 40, highway="bus_stop", bus="yes"),
                member(1, 0, highway="bus_stop", bus="yes"),
                member(2, 20, highway="bus_stop", bus="yes")]
        expected = transit.build_logical_access(rows)
        random.Random(13).shuffle(rows)
        self.assertEqual(transit.build_logical_access(rows), expected)

    def test_passenger_platform_is_preferred_over_stop_position(self):
        stop_position = member(1, 0, public_transport="stop_position", bus="yes")
        platform = member(2, 30, highway="bus_stop", public_transport="platform",
                          bus="yes")
        result = transit.build_logical_access(
            [stop_position, platform], [stop_area(10, [stop_position, platform])])
        self.assertEqual(result[0]["representative_osm_id"], 2)
        self.assertAlmostEqual(result[0]["lat"], platform["lat"])

    def test_route_direction_variants_share_normalized_identity(self):
        first = transit.normalize_route_key(
            "bus", "De Lijn", None, "25", "A to B", 1)
        second = transit.normalize_route_key(
            "bus", "De Lijn", None, "25", "B to A", 2)
        self.assertEqual(first, second)
        platform = member(1, highway="bus_stop", bus="yes")
        platform["route_keys"] = [first, second]
        platform["route_modes"] = ["BUS"]
        platform["route_refs"] = ["25"]
        result = transit.build_logical_access([platform])
        self.assertEqual(result[0]["route_count"], 1)

    def test_unicode_and_whitespace_normalization_is_exact(self):
        self.assertEqual(transit.normalize_name("  ÉCOLE\tCENTRALE "),
                         transit.normalize_name("école centrale"))
        self.assertNotEqual(transit.normalize_name("Central"),
                            transit.normalize_name("Centraal"))


if __name__ == "__main__":
    unittest.main()

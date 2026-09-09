from pathlib import Path
import sys
import tempfile
import textwrap
import unittest


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import build_transit_cache as transit
finally:
    sys.path.pop(0)

try:
    import osmium  # noqa: F401
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised only in minimal environments
    osmium = None
    pq = None


OSM_FIXTURE = """
<osm version="0.6" generator="transit-cache-test">
  <node id="1" version="1" lat="50.000000" lon="4.000000">
    <tag k="name" v="Central"/><tag k="highway" v="bus_stop"/>
    <tag k="public_transport" v="platform"/><tag k="bus" v="yes"/>
    <tag k="network" v="Test Network"/><tag k="operator" v="Test Operator"/>
  </node>
  <node id="2" version="1" lat="50.000010" lon="4.000000">
    <tag k="public_transport" v="stop_position"/><tag k="bus" v="yes"/>
  </node>
  <node id="3" version="1" lat="50.000180" lon="4.000000">
    <tag k="name" v="Central"/><tag k="highway" v="bus_stop"/>
    <tag k="public_transport" v="platform"/><tag k="bus" v="yes"/>
  </node>
  <node id="4" version="1" lat="50.000190" lon="4.000000">
    <tag k="public_transport" v="stop_position"/><tag k="bus" v="yes"/>
  </node>
  <node id="10" version="1" lat="50.010000" lon="4.000000">
    <tag k="name" v="Test Rail"/><tag k="railway" v="station"/>
    <tag k="public_transport" v="station"/><tag k="train" v="yes"/>
    <tag k="uic_ref" v="0088001"/>
  </node>
  <node id="11" version="1" lat="50.010010" lon="4.000000">
    <tag k="name" v="Test Rail"/><tag k="railway" v="stop"/>
    <tag k="public_transport" v="stop_position"/><tag k="train" v="yes"/>
  </node>
  <node id="20" version="1" lat="50.020000" lon="4.000000">
    <tag k="name" v="Metro Only"/><tag k="railway" v="station"/>
    <tag k="public_transport" v="station"/><tag k="station" v="subway"/>
    <tag k="subway" v="yes"/>
  </node>
  <node id="30" version="1" lat="50.000005" lon="4.000005">
    <tag k="name" v="Central shelter label"/>
  </node>
  <node id="40" version="1" lat="50.009990" lon="3.999990"/>
  <node id="41" version="1" lat="50.009990" lon="4.000010"/>
  <node id="42" version="1" lat="50.010020" lon="4.000010"/>
  <node id="43" version="1" lat="50.010020" lon="3.999990"/>
  <way id="100" version="1">
    <nd ref="40"/><nd ref="41"/><nd ref="42"/><nd ref="43"/><nd ref="40"/>
    <tag k="railway" v="platform"/><tag k="public_transport" v="platform"/>
    <tag k="train" v="yes"/><tag k="ref" v="1"/>
  </way>
  <relation id="1000" version="1">
    <member type="node" ref="1" role="platform"/>
    <member type="node" ref="2" role="stop"/>
    <member type="node" ref="30" role="label"/>
    <tag k="type" v="public_transport"/>
    <tag k="public_transport" v="stop_area"/><tag k="name" v="Central"/>
  </relation>
  <relation id="1001" version="1">
    <member type="node" ref="3" role="platform"/>
    <member type="node" ref="4" role="stop"/>
    <tag k="type" v="public_transport"/>
    <tag k="public_transport" v="stop_area"/><tag k="name" v="Central"/>
  </relation>
  <relation id="2000" version="1">
    <member type="relation" ref="1000" role=""/>
    <member type="relation" ref="1001" role=""/>
    <tag k="type" v="public_transport"/>
    <tag k="public_transport" v="stop_area_group"/><tag k="name" v="Central"/>
  </relation>
  <relation id="3000" version="1">
    <member type="node" ref="10" role=""/>
    <member type="way" ref="100" role="platform"/>
    <member type="node" ref="11" role="stop"/>
    <tag k="type" v="public_transport"/>
    <tag k="public_transport" v="stop_area"/><tag k="name" v="Test Rail"/>
  </relation>
  <relation id="4000" version="1">
    <member type="node" ref="1" role="platform"/>
    <tag k="type" v="route"/><tag k="route" v="bus"/>
    <tag k="network" v="Test Network"/><tag k="ref" v="10"/>
    <tag k="name" v="Outbound"/>
  </relation>
  <relation id="4001" version="1">
    <member type="node" ref="3" role="platform"/>
    <tag k="type" v="route"/><tag k="route" v="bus"/>
    <tag k="network" v="Test Network"/><tag k="ref" v="10"/>
    <tag k="name" v="Inbound"/>
  </relation>
  <relation id="5000" version="1">
    <member type="node" ref="11" role="stop"/>
    <tag k="type" v="route"/><tag k="route" v="train"/>
    <tag k="network" v="Rail Network"/><tag k="ref" v="L1"/>
  </relation>
</osm>
"""


@unittest.skipUnless(osmium is not None and pq is not None,
                     "pyosmium and pyarrow are required for cache integration tests")
class TransitCacheIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.input_path = self.root / "fixture.osm"
        self.input_path.write_text(textwrap.dedent(OSM_FIXTURE).strip(), encoding="utf-8")

    def tearDown(self):
        self.temp_dir.cleanup()

    def build(self, suffix):
        members = self.root / f"members-{suffix}.parquet"
        access = self.root / f"access-{suffix}.parquet"
        diagnostics = transit.build_transit_caches(self.input_path, members, access)
        return diagnostics, pq.read_table(members), pq.read_table(access)

    def test_relation_parsing_memberships_modes_and_schemas(self):
        diagnostics, members_table, access_table = self.build("one")
        self.assertEqual(diagnostics["stop_area_count"], 3)
        self.assertEqual(diagnostics["stop_area_group_count"], 1)
        self.assertEqual(diagnostics["normalized_route_count"], 2)

        required_members = {
            "osm_type", "osm_id", "name", "lat", "lon", "railway", "highway",
            "public_transport", "amenity", "station", "bus", "tram", "train",
            "subway", "light_rail", "ferry", "construction", "network",
            "operator", "ref", "local_ref", "uic_ref", "stop_area_ids", "stop_area_roles",
            "stop_area_group_ids", "route_keys", "route_modes", "route_refs",
            "route_relation_ids", "route_roles",
        }
        self.assertEqual(set(members_table.schema.names), required_members)
        required_access = {
            "logical_stop_id", "identity_source", "name", "lat", "lon", "modes",
            "is_rail_station", "stop_area_ids", "stop_area_group_id", "member_count",
            "member_osm_keys", "representative_osm_type", "representative_osm_id",
            "route_count", "route_modes", "route_refs",
        }
        self.assertEqual(set(access_table.schema.names), required_access)

        members = {(row["osm_type"], row["osm_id"]): row
                   for row in members_table.to_pylist()}
        # Geometry-only way vertices must not leak into the passenger cache.
        self.assertFalse(any(key[1] in {40, 41, 42, 43} for key in members))
        platform = members[("node", 1)]
        self.assertEqual(platform["stop_area_ids"], [1000])
        self.assertEqual(platform["stop_area_roles"], ["platform"])
        self.assertEqual(platform["stop_area_group_ids"], [2000])
        self.assertEqual(platform["route_modes"], ["BUS"])
        self.assertEqual(platform["route_refs"], ["10"])
        self.assertEqual(platform["route_relation_ids"], [4000])
        self.assertEqual(platform["route_roles"], ["platform"])
        # Weak relation-proven member is retained even without transit tags.
        self.assertIn(("node", 30), members)
        self.assertEqual(members[("node", 30)]["stop_area_roles"], ["label"])
        self.assertIn(("way", 100), members)
        self.assertIsNotNone(members[("way", 100)]["lat"])

        access = {row["logical_stop_id"]: row for row in access_table.to_pylist()}
        self.assertEqual(set(access), {
            "stop_area_group:2000", "uic:0088001", "osm:node:20"})
        central = access["stop_area_group:2000"]
        self.assertEqual(central["stop_area_ids"], [1000, 1001])
        self.assertEqual(central["route_count"], 1)
        self.assertEqual(central["modes"], ["BUS"])
        self.assertEqual(central["representative_osm_type"], "node")
        rail = access["uic:0088001"]
        self.assertTrue(rail["is_rail_station"])
        self.assertEqual(rail["modes"], ["RAIL"])
        self.assertEqual(rail["representative_osm_id"], 10)
        subway = access["osm:node:20"]
        self.assertFalse(subway["is_rail_station"])
        self.assertEqual(subway["modes"], ["METRO"])

    def test_repeated_build_is_deterministic(self):
        first_diagnostics, first_members, first_access = self.build("first")
        second_diagnostics, second_members, second_access = self.build("second")
        self.assertEqual(first_diagnostics, second_diagnostics)
        self.assertEqual(first_members.schema, second_members.schema)
        self.assertEqual(first_access.schema, second_access.schema)
        self.assertEqual(first_members.to_pylist(), second_members.to_pylist())
        self.assertEqual(first_access.to_pylist(), second_access.to_pylist())


if __name__ == "__main__":
    unittest.main()

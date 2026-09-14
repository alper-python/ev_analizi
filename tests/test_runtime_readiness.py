from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import runtime_readiness as readiness
    import server
finally:
    sys.path.pop(0)


def _write(path, schema, row):
    pq.write_table(pa.Table.from_pylist([row], schema=schema), path)


class RuntimeReadinessApiTests(unittest.TestCase):
    def setUp(self):
        server.limiter.reset()
        self.temp = tempfile.TemporaryDirectory(prefix="runtime-readiness-")
        self.root = Path(self.temp.name)
        self.paths = self._valid_assets()
        readiness.reset_runtime_readiness_cache()
        server._result_cache.clear()
        self.client = server.app.test_client()

    def tearDown(self):
        readiness.reset_runtime_readiness_cache()
        self.temp.cleanup()

    def _valid_assets(self):
        generic_fields = [
            ("cat", pa.string()), ("name", pa.string()),
            ("lat", pa.float64()), ("lon", pa.float64()),
            ("amenity", pa.string()), ("shop", pa.string()),
            ("healthcare", pa.string()), ("railway", pa.string()),
            ("highway", pa.string()), ("public_transport", pa.string()),
            ("leisure", pa.string()), ("boundary", pa.string()),
            ("landuse", pa.string()), ("sport", pa.string()),
            ("school_level", pa.string()), ("isced_level", pa.string()),
        ]
        node_schema = pa.schema([("id", pa.int64()), *generic_fields])
        polygon_schema = pa.schema([
            ("uid", pa.string()), generic_fields[0], generic_fields[1],
            ("brand", pa.string()), *generic_fields[2:],
        ])
        base = {name: None for name, _type in generic_fields}
        base.update({"cat": "market", "name": "Fixture", "lat": 50.0,
                     "lon": 4.0, "shop": "supermarket"})
        nodes = self.root / "nodes.parquet"
        polygons = self.root / "polygons.parquet"
        _write(nodes, node_schema, {"id": 1, **base})
        _write(polygons, polygon_schema, {"uid": "way:1", "brand": None, **base})

        stops = self.root / "stops.parquet"
        _write(stops, pa.schema([
            ("logical_stop_id", pa.string()), ("operator", pa.string()),
            ("gtfs_stop_id", pa.string()), ("lat", pa.float64()),
            ("lon", pa.float64()),
        ]), {"logical_stop_id": "stop:1", "operator": "fixture",
             "gtfs_stop_id": "1", "lat": 50.0, "lon": 4.0})

        summary = self.root / "summary.parquet"
        _write(summary, pa.schema([
            ("logical_stop_id", pa.string()), ("display_name", pa.string()),
            ("operators", pa.list_(pa.string())), ("modes", pa.list_(pa.string())),
            ("weekday_departures", pa.int64()),
            ("saturday_departures", pa.int64()),
            ("sunday_departures", pa.int64()),
            ("seven_day_average", pa.float64()),
        ]), {"logical_stop_id": "stop:1", "display_name": "Fixture stop",
             "operators": ["fixture"], "modes": ["bus"],
             "weekday_departures": 1, "saturday_departures": 1,
             "sunday_departures": 1, "seven_day_average": 1.0})

        rail = self.root / "rail.parquet"
        _write(rail, pa.schema([
            ("logical_station_id", pa.string()), ("station_name", pa.string()),
            ("uic_code", pa.string()), ("lat", pa.float64()),
            ("lon", pa.float64()), ("weekday_departures", pa.int64()),
            ("saturday_departures", pa.int64()),
            ("sunday_departures", pa.int64()),
            ("seven_day_average", pa.float64()),
        ]), {"logical_station_id": "rail:1", "station_name": "Fixture rail",
             "uic_code": "1", "lat": 50.0, "lon": 4.0,
             "weekday_departures": 1, "saturday_departures": 1,
             "sunday_departures": 1, "seven_day_average": 1.0})

        park_types = {
            "osm_id": pa.int64(), "counts_as_primary": pa.bool_(),
            "counts_as_choice": pa.bool_(), "counts_as_secondary": pa.bool_(),
            "area_m2": pa.float64(), "size_factor": pa.float64(),
            "final_confidence": pa.float64(), "secondary_type_weight": pa.float64(),
            "secondary_confidence": pa.float64(), "geometry_wkb": pa.binary(),
            "representative_lat": pa.float64(), "representative_lon": pa.float64(),
            "bbox_min_lat": pa.float64(), "bbox_min_lon": pa.float64(),
            "bbox_max_lat": pa.float64(), "bbox_max_lon": pa.float64(),
        }
        park_columns = readiness.ASSET_SPECS["park_destinations"]["columns"]
        park_schema = pa.schema([
            (name, park_types.get(name, pa.string())) for name in sorted(park_columns)
        ])
        park_row = {name: None for name in park_columns}
        park_row.update({"park_id": "park:1", "osm_id": 1,
                         "counts_as_primary": True, "counts_as_choice": True,
                         "counts_as_secondary": False, "geometry_wkb": b"x",
                         "representative_lat": 50.0, "representative_lon": 4.0,
                         "bbox_min_lat": 50.0, "bbox_min_lon": 4.0,
                         "bbox_max_lat": 50.0, "bbox_max_lon": 4.0})
        park = self.root / "park.parquet"
        _write(park, park_schema, park_row)

        sport_types = {
            "canonical_osm_id": pa.int64(), "score_eligible": pa.bool_(),
            "specialized": pa.bool_(), "standalone": pa.bool_(),
            "sports": pa.list_(pa.string()), "direct_sports": pa.list_(pa.string()),
            "component_sports": pa.list_(pa.string()), "sport_count": pa.int64(),
            "component_count": pa.int64(), "is_commercial": pa.bool_(),
            "is_public_operator": pa.bool_(), "is_school_context": pa.bool_(),
            "geometry_wkb": pa.binary(), "representative_lat": pa.float64(),
            "representative_lon": pa.float64(), "entrance_lat": pa.float64(),
            "entrance_lon": pa.float64(), "bbox_min_lat": pa.float64(),
            "bbox_min_lon": pa.float64(), "bbox_max_lat": pa.float64(),
            "bbox_max_lon": pa.float64(),
        }
        sport_columns = readiness.ASSET_SPECS["sport_destinations"]["columns"]
        sport_schema = pa.schema([
            (name, sport_types.get(name, pa.string())) for name in sorted(sport_columns)
        ])
        sport_row = {name: None for name in sport_columns}
        sport_row.update({"sport_id": "sport:1", "canonical_osm_id": 1,
                          "score_eligible": True, "specialized": False,
                          "standalone": False, "sports": [], "direct_sports": [],
                          "component_sports": [], "sport_count": 0,
                          "component_count": 0, "is_commercial": False,
                          "is_public_operator": False, "is_school_context": False,
                          "geometry_wkb": b"x", "representative_lat": 50.0,
                          "representative_lon": 4.0, "bbox_min_lat": 50.0,
                          "bbox_min_lon": 4.0, "bbox_max_lat": 50.0,
                          "bbox_max_lon": 4.0})
        sport = self.root / "sport.parquet"
        _write(sport, sport_schema, sport_row)
        return {
            "poi_nodes": str(nodes), "poi_polygons": str(polygons),
            "transit_service_stops": str(stops),
            "transit_service_summary": str(summary), "rail_service": str(rail),
            "park_destinations": str(park), "sport_destinations": str(sport),
        }

    def _server_globals(self, paths):
        return {
            "NODES_PATH": paths["poi_nodes"],
            "POLYS_PATH": paths["poi_polygons"],
            "TRANSIT_STOPS_PATH": paths["transit_service_stops"],
            "TRANSIT_SUMMARY_PATH": paths["transit_service_summary"],
            "RAIL_SERVICE_PATH": paths["rail_service"],
            "PARK_PATH": paths["park_destinations"],
            "SPORT_PATH": paths["sport_destinations"],
        }

    def _analyze_payload(self):
        return {"address": "Fixture", "lat": 50.0, "lon": 4.0,
                "radius": 2500, "topn": 20, "lang": "en"}

    def _assert_unready(self, paths):
        readiness.reset_runtime_readiness_cache()
        with (patch.multiple(server, **self._server_globals(paths)),
              self.assertLogs(server.LOGGER, level="ERROR")):
            health = self.client.get("/api/health")
            analyze = self.client.post("/api/analyze", json=self._analyze_payload())
        for response in (health, analyze):
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.content_type, "application/json")
            body = response.get_data(as_text=True)
            self.assertNotIn(str(self.root), body)
            self.assertNotIn("Traceback", body)
        payload = analyze.get_json()
        self.assertEqual(payload["error"]["code"], "service_unavailable")
        self.assertNotIn("scores", payload)

    def test_all_valid_assets_make_health_and_analysis_ready(self):
        result = {"categories": [{"key": "market", "count": 1}], "overall": 1.0}
        with (patch.multiple(server, **self._server_globals(self.paths)),
              patch.object(server, "_run_preview_analysis", return_value=result)):
            health = self.client.get("/api/health")
            analyze = self.client.post("/api/analyze", json=self._analyze_payload())
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.get_json(), {
            "status": "ok", "ready": True,
            "components": {"poi": "ok", "transit": "ok",
                           "park": "ok", "sport": "ok"},
        })
        self.assertEqual(analyze.status_code, 200)
        self.assertEqual(analyze.get_json(), result)

    def test_missing_generic_node_fails_closed(self):
        paths = dict(self.paths); paths["poi_nodes"] = str(self.root / "missing-node.parquet")
        self._assert_unready(paths)

    def test_missing_generic_polygon_fails_closed(self):
        paths = dict(self.paths); paths["poi_polygons"] = str(self.root / "missing-poly.parquet")
        self._assert_unready(paths)

    def test_corrupt_generic_parquet_fails_closed(self):
        corrupt = self.root / "corrupt-poi.parquet"; corrupt.write_bytes(b"not parquet")
        paths = dict(self.paths); paths["poi_nodes"] = str(corrupt)
        self._assert_unready(paths)

    def test_incompatible_essential_column_type_fails_closed(self):
        source = pq.read_table(self.paths["poi_nodes"]).to_pylist()[0]
        source["lat"] = "50.0"
        fields = []
        for field in pq.read_schema(self.paths["poi_nodes"]):
            fields.append((field.name, pa.string() if field.name == "lat" else field.type))
        invalid_path = self.root / "wrong-type-poi.parquet"
        _write(invalid_path, pa.schema(fields), source)
        paths = dict(self.paths); paths["poi_nodes"] = str(invalid_path)
        self._assert_unready(paths)

    def test_each_missing_transit_component_fails_closed(self):
        for asset in ("transit_service_stops", "transit_service_summary", "rail_service"):
            with self.subTest(asset=asset):
                paths = dict(self.paths); paths[asset] = str(self.root / f"missing-{asset}.parquet")
                self._assert_unready(paths)

    def test_corrupt_transit_cache_fails_closed(self):
        corrupt = self.root / "corrupt-transit.parquet"; corrupt.write_bytes(b"not parquet")
        paths = dict(self.paths); paths["transit_service_stops"] = str(corrupt)
        self._assert_unready(paths)

    def test_missing_or_schema_invalid_park_fails_closed(self):
        missing = dict(self.paths); missing["park_destinations"] = str(self.root / "missing-park.parquet")
        self._assert_unready(missing)
        invalid_path = self.root / "invalid-park.parquet"
        _write(invalid_path, pa.schema([("park_id", pa.string())]), {"park_id": "park:1"})
        invalid = dict(self.paths); invalid["park_destinations"] = str(invalid_path)
        self._assert_unready(invalid)

    def test_missing_or_schema_invalid_sport_fails_closed(self):
        missing = dict(self.paths); missing["sport_destinations"] = str(self.root / "missing-sport.parquet")
        self._assert_unready(missing)
        invalid_path = self.root / "invalid-sport.parquet"
        _write(invalid_path, pa.schema([("sport_id", pa.string())]), {"sport_id": "sport:1"})
        invalid = dict(self.paths); invalid["sport_destinations"] = str(invalid_path)
        self._assert_unready(invalid)

    def test_empty_required_dataset_fails_closed(self):
        empty = self.root / "empty.parquet"
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), empty)
        paths = dict(self.paths); paths["poi_nodes"] = str(empty)
        self._assert_unready(paths)

    def test_build_only_inputs_are_not_runtime_requirements(self):
        self.assertEqual(set(readiness.ASSET_SPECS), {
            "poi_nodes", "poi_polygons", "transit_service_stops",
            "transit_service_summary", "rail_service", "park_destinations",
            "sport_destinations",
        })

    def test_readiness_is_cached_and_can_be_explicitly_reset(self):
        with patch.object(readiness, "_validate_asset",
                          wraps=readiness._validate_asset) as validate:
            readiness.check_runtime_readiness(self.paths)
            readiness.check_runtime_readiness(self.paths)
            self.assertEqual(validate.call_count, len(readiness.ASSET_SPECS))
            readiness.reset_runtime_readiness_cache()
            readiness.check_runtime_readiness(self.paths)
            self.assertEqual(validate.call_count, 2 * len(readiness.ASSET_SPECS))


if __name__ == "__main__":
    unittest.main()

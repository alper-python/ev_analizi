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
finally:
    sys.path.pop(0)


TEST_LAT, TEST_LON = 50.0, 4.0
EARTH_RADIUS_M = 6371000.0

POI_SCHEMA = pa.schema([
    ("name", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()),
    ("healthcare", pa.string()), ("railway", pa.string()),
    ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()),
    ("landuse", pa.string()), ("sport", pa.string()),
    ("school_level", pa.string()), ("isced_level", pa.string()),
    ("cat", pa.string()),
])
LOCAL_STOP_SCHEMA = pa.schema([
    ("logical_stop_id", pa.string()), ("operator", pa.string()),
    ("gtfs_stop_id", pa.string()), ("lat", pa.float64()),
    ("lon", pa.float64()),
])
LOCAL_SUMMARY_SCHEMA = pa.schema([
    ("logical_stop_id", pa.string()), ("display_name", pa.string()),
    ("operators", pa.list_(pa.string())), ("modes", pa.list_(pa.string())),
    ("weekday_departures", pa.float64()),
    ("saturday_departures", pa.float64()),
    ("sunday_departures", pa.float64()),
    ("seven_day_average", pa.float64()),
])
RAIL_SCHEMA = pa.schema([
    ("logical_station_id", pa.string()), ("uic_code", pa.string()),
    ("station_name", pa.string()), ("lat", pa.float64()),
    ("lon", pa.float64()), ("weekday_departures", pa.float64()),
    ("saturday_departures", pa.float64()),
    ("sunday_departures", pa.float64()),
    ("seven_day_average", pa.float64()),
])


def north(distance_m):
    return TEST_LAT + math.degrees(distance_m / EARTH_RADIUS_M)


def poi(name, distance_m):
    return {
        "name": name, "lat": north(distance_m), "lon": TEST_LON,
        "amenity": None, "shop": None, "healthcare": None,
        "railway": None, "highway": "bus_stop",
        "public_transport": "platform", "leisure": None,
        "boundary": None, "landuse": None, "sport": None,
        "school_level": None, "isced_level": None, "cat": "transit",
    }


def member(logical_id, member_id, distance_m):
    return {"logical_stop_id": logical_id, "operator": "Operator",
            "gtfs_stop_id": member_id, "lat": north(distance_m),
            "lon": TEST_LON}


def summary(logical_id, service, name=None):
    return {
        "logical_stop_id": logical_id, "display_name": name or logical_id,
        "operators": ["Operator"], "modes": ["BUS"],
        "weekday_departures": service, "saturday_departures": service,
        "sunday_departures": service, "seven_day_average": service,
    }


def station(logical_id, distance_m, service, name=None):
    return {
        "logical_station_id": logical_id,
        "uic_code": logical_id.removeprefix("uic:"),
        "station_name": name or logical_id, "lat": north(distance_m),
        "lon": TEST_LON, "weekday_departures": service,
        "saturday_departures": service, "sunday_departures": service,
        "seven_day_average": service,
    }


class TransitParquetIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.nodes = root / "be_poi.parquet"
        cls.stops = root / "be_transit_service_stops.parquet"
        cls.summary = root / "be_transit_service_summary.parquet"
        cls.rail = root / "be_rail_service.parquet"
        pq.write_table(pa.Table.from_pylist([
            poi("Display 500", 500), poi("Display 1500", 1500),
            poi("Display 3000", 3000)], schema=POI_SCHEMA), cls.nodes)
        pq.write_table(pa.Table.from_pylist([
            member("multi", "far", 800), member("multi", "near", 200),
            member("useful", "useful", 400),
            member("boundary", "boundary", 1000),
            member("outside", "outside", 1000.1),
            member("zero", "zero", 50),
            member("tie-z", "tie-z", 300), member("tie-a", "tie-a", 300),
        ], schema=LOCAL_STOP_SCHEMA), cls.stops)
        pq.write_table(pa.Table.from_pylist([
            summary("multi", 45), summary("useful", 90),
            summary("boundary", 90), summary("outside", 90),
            summary("zero", 0), summary("tie-z", 30), summary("tie-a", 30),
        ], schema=LOCAL_SUMMARY_SCHEMA), cls.summary)
        pq.write_table(pa.Table.from_pylist([
            station("uic:weak", 500, 20, "Weak Rail"),
            station("uic:strong", 2500, 350, "Strong Rail"),
            station("uic:boundary", 7500, 350, "Boundary Rail"),
            station("uic:outside", 7500.1, 350, "Outside Rail"),
            station("uic:tie-z", 3000, 100, "Tie Z"),
            station("uic:tie-a", 3000, 100, "Tie A"),
        ], schema=RAIL_SCHEMA), cls.rail)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def analyze(self, radius=2500, stops=True, rail=True):
        with duckdb.connect() as con:
            return app.analyze_transit(
                con, str(self.nodes), None,
                str(self.stops) if stops else None,
                str(self.summary) if stops else None,
                str(self.rail) if rail else None,
                TEST_LAT, TEST_LON, radius, 20)

    def test_local_uses_minimum_preserved_member_distance_and_summary_join(self):
        with duckdb.connect() as con:
            rows = app.query_transit_local_candidates(
                con, str(self.stops), str(self.summary), TEST_LAT, TEST_LON)
        multi = next(row for row in rows if row["logical_stop_id"] == "multi")
        self.assertAlmostEqual(multi["distance_m"], 200, places=5)
        self.assertEqual(multi["seven_day_average"], 45)
        self.assertAlmostEqual(multi["lat"], north(200))

    def test_exact_local_radius_prunes_outside_and_boundary_scores_zero(self):
        with duckdb.connect() as con:
            rows = app.query_transit_local_candidates(
                con, str(self.stops), str(self.summary), TEST_LAT, TEST_LON)
        identifiers = {row["logical_stop_id"] for row in rows}
        self.assertIn("boundary", identifiers)
        self.assertNotIn("outside", identifiers)

    def test_exact_rail_radius_prunes_outside_and_includes_boundary(self):
        with duckdb.connect() as con:
            rows = app.query_transit_rail_candidates(
                con, str(self.rail), TEST_LAT, TEST_LON)
        identifiers = {row["logical_station_id"] for row in rows}
        self.assertIn("uic:boundary", identifiers)
        self.assertNotIn("uic:outside", identifiers)

    def test_better_utility_wins_over_nearer_weak_candidates(self):
        _frame, _score, _count, _nearest, result = self.analyze()
        self.assertEqual(result["best_local_logical_stop_id"], "useful")
        self.assertEqual(result["best_rail_logical_station_id"], "uic:strong")

    def test_queried_candidates_keep_deterministic_ties_and_distance_tradeoff(self):
        with duckdb.connect() as con:
            local_rows = app.query_transit_local_candidates(
                con, str(self.stops), str(self.summary), TEST_LAT, TEST_LON)
            rail_rows = app.query_transit_rail_candidates(
                con, str(self.rail), TEST_LAT, TEST_LON)
        tied_local = [row for row in local_rows
                      if row["logical_stop_id"] in {"tie-a", "tie-z"}]
        tied_rail = [row for row in rail_rows
                     if row["logical_station_id"] in {"uic:tie-a", "uic:tie-z"}]
        self.assertEqual(
            app.transit_score_components(tied_local, [])["best_local_logical_stop_id"],
            "tie-a")
        self.assertEqual(
            app.transit_score_components([], tied_rail)["best_rail_logical_station_id"],
            "uic:tie-a")
        weak_vs_farther = [row for row in rail_rows if row["logical_station_id"]
                           in {"uic:weak", "uic:tie-a"}]
        self.assertEqual(
            app.transit_score_components([], weak_vs_farther)["best_rail_logical_station_id"],
            "uic:weak")

    def test_zero_service_local_does_not_contribute(self):
        with duckdb.connect() as con:
            rows = app.query_transit_local_candidates(
                con, str(self.stops), str(self.summary), TEST_LAT, TEST_LON)
        zero = next(row for row in rows if row["logical_stop_id"] == "zero")
        self.assertEqual(app.transit_score_components([zero], [])["score"], 0.0)

    def test_local_only_rail_only_and_neither(self):
        self.assertGreater(self.analyze(rail=False)[4]["local_access_points"], 0)
        self.assertEqual(self.analyze(rail=False)[4]["rail_access_points"], 0)
        self.assertEqual(self.analyze(stops=False)[4]["local_access_points"], 0)
        self.assertGreater(self.analyze(stops=False)[4]["rail_access_points"], 0)
        self.assertEqual(self.analyze(stops=False, rail=False)[1], 0.0)

    def test_display_radius_changes_only_legacy_display_rows(self):
        results = [self.analyze(radius) for radius in (1000, 2500, 5000)]
        components = [result[4] for result in results]
        invariant = (
            "score", "local_access_points", "rail_access_points",
            "best_local_logical_stop_id", "best_local_distance_m",
            "best_rail_logical_station_id", "best_rail_distance_m",
        )
        for field in invariant:
            self.assertEqual([item[field] for item in components],
                             [components[0][field]] * 3)
        self.assertEqual([result[2] for result in results], [1, 2, 3])

    def test_public_analyze_uses_dedicated_path_and_exposes_breakdown(self):
        with mock.patch.object(
                app, "calc_category_score", wraps=app.calc_category_score) as generic:
            result = app.analyze(
                lat=TEST_LAT, lon=TEST_LON, radius=2500, topn=20,
                nodes_path=str(self.nodes), polys_path=None,
                transit_stops_path=str(self.stops),
                transit_summary_path=str(self.summary),
                rail_service_path=str(self.rail))
        self.assertEqual(result["scores"]["Ulaşım"],
                         result["breakdowns"]["transit"]["public_score"])
        self.assertFalse(any(call.args[0] == "transit"
                             for call in generic.call_args_list))

    def test_generic_transit_scoring_fails_explicitly(self):
        with self.assertRaisesRegex(ValueError, "dedicated Transit Score V1"):
            app.calc_category_score("transit", 10, 50)


if __name__ == "__main__":
    unittest.main()

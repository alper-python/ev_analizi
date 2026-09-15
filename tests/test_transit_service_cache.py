from datetime import date
from pathlib import Path
import csv
import json
import sys
import tempfile
import unittest
import zipfile

import duckdb
import pyarrow.parquet as pq


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import build_transit_service_cache as service
finally:
    sys.path.pop(0)


def stop(operator, identifier, name, north_m=0, east_m=0, modes=("BUS",),
         parent=None):
    lat = 50.0 + north_m / 111195.0
    lon = 4.0 + east_m / (111195.0 * 0.6428)
    return service.Stop(
        operator, identifier, name, service.normalize_name(name), lat, lon,
        frozenset(modes), identifier + "-code", parent)


class NameAndGroupingTests(unittest.TestCase):
    def test_nfkc_casefold_whitespace_preserves_accents(self):
        self.assertEqual(service.normalize_name("  DE\u00a0BROUCKÈRE  "),
                         "de brouckère")
        self.assertNotEqual(service.normalize_name("Brouckère"),
                            service.normalize_name("Brouckere"))

    def test_delijn_exact_name_inside_100m_groups(self):
        groups = service.group_operator_stops(
            "delijn", [stop("delijn", "a", "Town Stop"),
                       stop("delijn", "b", "TOWN  STOP", 99)])
        self.assertEqual([len(group.members) for group in groups], [2])

    def test_delijn_beyond_100m_does_not_group(self):
        groups = service.group_operator_stops(
            "delijn", [stop("delijn", "a", "Town Stop"),
                       stop("delijn", "b", "Town Stop", 100.5)])
        self.assertEqual(sorted(len(group.members) for group in groups), [1, 1])

    def test_complete_link_prevents_transitive_chain(self):
        clusters = service.complete_link_clusters([
            stop("delijn", "a", "Chain", 0),
            stop("delijn", "b", "Chain", 70),
            stop("delijn", "c", "Chain", 140),
        ], 100)
        self.assertEqual(sorted(len(cluster) for cluster in clusters), [1, 2])

    def test_stib_parent_station_is_authoritative(self):
        members = [stop("stib", "a", "Platform A", parent="station"),
                   stop("stib", "b", "Platform B", 30, parent="station")]
        groups = service.group_operator_stops("stib", members, {
            "station": {"name": "Central", "lat": 50.0, "lon": 4.0}})
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].display_name, "Central")
        self.assertEqual(groups[0].grouping_method, "parent_station")

    def test_stib_exact_name_cluster_absorbs_into_parent_within_150m(self):
        members = [stop("stib", "child", "Central", parent="station"),
                   stop("stib", "surface", "CENTRAL", 149)]
        groups = service.group_operator_stops("stib", members, {
            "station": {"name": "Central", "lat": 50.0, "lon": 4.0}})
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0].members), 2)
        self.assertIn("absorption_150m", groups[0].grouping_method)

    def test_stib_fallback_uses_75m(self):
        groups = service.group_operator_stops("stib", [
            stop("stib", "a", "Surface", 0), stop("stib", "b", "Surface", 74)])
        self.assertEqual(len(groups), 1)
        groups = service.group_operator_stops("stib", [
            stop("stib", "a", "Surface", 0), stop("stib", "b", "Surface", 76)])
        self.assertEqual(len(groups), 2)

    def test_tec_parent_and_50m_fallback(self):
        parented = service.group_operator_stops("tec", [
            stop("tec", "a", "A", parent="p"), stop("tec", "b", "B", parent="p")
        ], {"p": {"name": "TEC Station", "lat": 50.0, "lon": 4.0}})
        self.assertEqual(len(parented), 1)
        fallback = service.group_operator_stops("tec", [
            stop("tec", "c", "Road", 0), stop("tec", "d", "Road", 49)])
        self.assertEqual(len(fallback), 1)
        outside = service.group_operator_stops("tec", [
            stop("tec", "c", "Road", 0), stop("tec", "d", "Road", 51)])
        self.assertEqual(len(outside), 2)

    def test_cross_operator_exact_name_inside_30m_merges(self):
        groups = []
        for operator, north in (("delijn", 0), ("tec", 29)):
            groups.extend(service.group_operator_stops(
                operator, [stop(operator, operator, "Boundary", north)]))
        merged, count = service.merge_cross_operator(groups)
        self.assertEqual(count, 1)
        self.assertEqual(len(merged), 1)
        self.assertEqual(set(merged[0].operators), {"delijn", "tec"})

    def test_cross_operator_different_name_same_coordinate_does_not_merge(self):
        groups = (service.group_operator_stops(
            "delijn", [stop("delijn", "a", "Town Hall")])
            + service.group_operator_stops(
                "tec", [stop("tec", "b", "Church")]))
        merged, count = service.merge_cross_operator(groups)
        self.assertEqual(count, 0)
        self.assertEqual(len(merged), 2)

    def test_incompatible_modes_do_not_fallback_merge(self):
        groups = service.group_operator_stops("stib", [
            stop("stib", "a", "Shared", modes=("BUS",)),
            stop("stib", "b", "Shared", 1, modes=("METRO",)),
        ])
        self.assertEqual(len(groups), 2)

    def test_unnamed_stops_are_not_treated_as_an_exact_name_match(self):
        same_operator = service.group_operator_stops("delijn", [
            stop("delijn", "a", ""), stop("delijn", "b", "", 1),
        ])
        self.assertEqual(len(same_operator), 2)
        cross_operator = (same_operator + service.group_operator_stops(
            "tec", [stop("tec", "c", "", 1)]))
        merged, count = service.merge_cross_operator(cross_operator)
        self.assertEqual(count, 0)
        self.assertEqual(len(merged), 3)

    def test_logical_ids_are_deterministic_across_input_order(self):
        values = [stop("delijn", "b", "Stable", 20),
                  stop("delijn", "a", "Stable")]
        forward = service.group_operator_stops("delijn", values)[0].provisional_id
        reverse = service.group_operator_stops("delijn", list(reversed(values)))[0].provisional_id
        self.assertEqual(forward, reverse)


class TimetableMathTests(unittest.TestCase):
    def test_zero_service_dates_are_included_in_medians_and_d(self):
        start, end = date(2026, 1, 5), date(2026, 1, 18)
        counts = {date(2026, 1, 5): 10, date(2026, 1, 10): 4,
                  date(2026, 1, 11): 2}
        w, sa, su, average = service.representative_values(counts, start, end)
        self.assertEqual((w, sa, su), (0.0, 2.0, 1.0))
        self.assertEqual(average, 3 / 7)

    def test_local_validity_overlap_and_audited_interval(self):
        validities = {
            "delijn": (date(2026, 9, 9), date(2026, 11, 18)),
            "stib": (date(2026, 8, 31), date(2026, 9, 27)),
            "tec": (date(2026, 9, 8), date(2026, 11, 11)),
        }
        self.assertEqual(service.select_local_window(
            validities, date(2026, 9, 10)),
            (date(2026, 9, 9), date(2026, 9, 27)))

    def test_explicit_window_outside_validity_fails(self):
        validities = {key: (date(2026, 1, 1), date(2026, 2, 28))
                      for key in ("delijn", "stib", "tec")}
        with self.assertRaisesRegex(ValueError, "outside common"):
            service.select_local_window(
                validities, date(2026, 1, 5), date(2025, 12, 20),
                date(2026, 1, 20))

    def test_too_short_common_window_fails(self):
        validities = {key: (date(2026, 1, 5), date(2026, 1, 11))
                      for key in ("delijn", "stib", "tec")}
        with self.assertRaisesRegex(ValueError, "too small"):
            service.select_local_window(validities, date(2026, 1, 5))

    def test_default_rail_window_is_six_complete_weeks(self):
        self.assertEqual(service.select_rail_window(
            (date(2026, 7, 9), date(2026, 12, 12)), date(2026, 9, 10)),
            (date(2026, 9, 14), date(2026, 10, 25)))


def _write_csv(archive, filename, fields, rows):
    import io
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    archive.writestr(filename, output.getvalue())


def make_feed(path, operator, route_type, stops, trips, stop_times,
              version="fixture-v1"):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        _write_csv(archive, "feed_info.txt",
                   ["feed_publisher_name", "feed_publisher_url", "feed_lang",
                    "feed_start_date", "feed_end_date", "feed_version"], [{
                        "feed_publisher_name": operator,
                        "feed_publisher_url": "https://example.invalid",
                        "feed_lang": "en", "feed_start_date": "20260101",
                        "feed_end_date": "20260228", "feed_version": version,
                    }])
        _write_csv(archive, "stops.txt",
                   ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon",
                    "location_type", "parent_station"], stops)
        route_ids = sorted({trip.get("route_id", "R") for trip in trips})
        _write_csv(archive, "routes.txt", ["route_id", "route_type"], [
            {"route_id": route_id,
             "route_type": ("3" if route_id == "REPLACEMENT" else route_type)}
            for route_id in route_ids])
        _write_csv(archive, "trips.txt",
                   ["route_id", "service_id", "trip_id"], trips)
        _write_csv(archive, "stop_times.txt",
                   ["trip_id", "departure_time", "stop_id", "stop_sequence",
                    "pickup_type"], stop_times)
        _write_csv(archive, "calendar.txt",
                   ["service_id", "monday", "tuesday", "wednesday", "thursday",
                    "friday", "saturday", "sunday", "start_date", "end_date"], [{
                        "service_id": "DAILY", "monday": "1", "tuesday": "1",
                        "wednesday": "1", "thursday": "1", "friday": "1",
                        "saturday": "1", "sunday": "1", "start_date": "20260101",
                        "end_date": "20260228",
                    }])
        _write_csv(archive, "calendar_dates.txt",
                   ["service_id", "date", "exception_type"], [])


def read_provenance(path):
    metadata = pq.ParquetFile(path).schema_arrow.metadata
    payload = metadata[service.PROVENANCE_METADATA_KEY.encode("utf-8")]
    return json.loads(payload.decode("utf-8"))


class DownloadProvenanceTests(unittest.TestCase):
    class Response:
        def __init__(self, headers):
            self.headers = headers

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            self.chunk_size = chunk_size
            return iter((b"fixture",))

    class Session:
        def __init__(self, headers):
            self.response = DownloadProvenanceTests.Response(headers)

        def get(self, url, stream, timeout):
            self.request = (url, stream, timeout)
            return self.response

    def test_valid_last_modified_etag_and_retrieval_are_separate(self):
        with tempfile.TemporaryDirectory(prefix="transit-download-test-") as root:
            session = self.Session({
                "Last-Modified": "Mon, 14 Sep 2026 05:09:24 +0200",
                "ETag": 'W/"fixture-etag"',
                "Date": "Tue, 15 Sep 2026 12:00:00 GMT",
            })
            provenance = service._download(
                "https://example.invalid/feed.zip", Path(root) / "feed.zip",
                session)
        self.assertEqual(provenance["dataset_updated_at"],
                         "2026-09-14T03:09:24Z")
        self.assertEqual(provenance["etag"], 'W/"fixture-etag"')
        self.assertRegex(provenance["retrieved_at"],
                         r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        self.assertNotEqual(provenance["retrieved_at"],
                            provenance["dataset_updated_at"])

    def test_missing_or_malformed_last_modified_has_no_fallback(self):
        for headers in (
            {"Date": "Tue, 15 Sep 2026 12:00:00 GMT"},
            {"Last-Modified": "not-a-date",
             "Date": "Tue, 15 Sep 2026 12:00:00 GMT"},
        ):
            with self.subTest(headers=headers):
                with tempfile.TemporaryDirectory(
                        prefix="transit-download-test-") as root:
                    provenance = service._download(
                        "https://example.invalid/feed.zip",
                        Path(root) / "feed.zip", self.Session(headers))
                self.assertIsNone(provenance["dataset_updated_at"])
                self.assertIsNone(provenance["etag"])
                self.assertIsNotNone(provenance["retrieved_at"])


class SyntheticBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="transit-service-test-")
        cls.root = Path(cls.temp.name)
        cls.feeds = {}
        common_stops = [
            {"stop_id": "D1", "stop_code": "D1", "stop_name": "Boundary",
             "stop_lat": "50.0", "stop_lon": "4.0", "location_type": "0",
             "parent_station": ""},
            {"stop_id": "D2", "stop_code": "D2", "stop_name": "BOUNDARY",
             "stop_lat": "50.00009", "stop_lon": "4.0", "location_type": "0",
             "parent_station": ""},
        ]
        local_trips = [
            {"route_id": "R", "service_id": "DAILY", "trip_id": "OUT"},
            {"route_id": "R", "service_id": "DAILY", "trip_id": "BACK"},
            {"route_id": "R", "service_id": "DAILY", "trip_id": "NOBOARD"},
            {"route_id": "R", "service_id": "DAILY", "trip_id": "LATE"},
        ]
        local_times = [
            {"trip_id": "OUT", "departure_time": "06:00:00", "stop_id": "D1",
             "stop_sequence": "1", "pickup_type": ""},
            {"trip_id": "OUT", "departure_time": "06:01:00", "stop_id": "D2",
             "stop_sequence": "2", "pickup_type": ""},
            {"trip_id": "BACK", "departure_time": "21:59:00", "stop_id": "D2",
             "stop_sequence": "1", "pickup_type": "0"},
            {"trip_id": "NOBOARD", "departure_time": "07:00:00", "stop_id": "D1",
             "stop_sequence": "1", "pickup_type": "1"},
            {"trip_id": "LATE", "departure_time": "22:00:00", "stop_id": "D1",
             "stop_sequence": "1", "pickup_type": "0"},
        ]
        path = cls.root / "delijn.zip"
        make_feed(path, "De Lijn", "3", common_stops, local_trips, local_times)
        cls.feeds["delijn"] = path

        path = cls.root / "stib.zip"
        make_feed(path, "STIB", "1", [
            {"stop_id": "S0", "stop_code": "", "stop_name": "Metro",
             "stop_lat": "50.01", "stop_lon": "4.0", "location_type": "1",
             "parent_station": ""},
            {"stop_id": "S1", "stop_code": "", "stop_name": "Metro platform",
             "stop_lat": "50.01001", "stop_lon": "4.0", "location_type": "0",
             "parent_station": "S0"},
        ], [{"route_id": "R", "service_id": "DAILY", "trip_id": "S-TRIP"}], [{
            "trip_id": "S-TRIP", "departure_time": "06:30:00", "stop_id": "S1",
            "stop_sequence": "1", "pickup_type": "0",
        }])
        cls.feeds["stib"] = path

        path = cls.root / "tec.zip"
        make_feed(path, "TEC", "3", [
            {"stop_id": "T0", "stop_code": "", "stop_name": "Boundary",
             "stop_lat": "50.00018", "stop_lon": "4.0", "location_type": "1",
             "parent_station": ""},
            {"stop_id": "T1", "stop_code": "", "stop_name": "Boundary",
             "stop_lat": "50.00018", "stop_lon": "4.0", "location_type": "0",
             "parent_station": "T0"},
        ], [{"route_id": "R", "service_id": "DAILY", "trip_id": "T-TRIP"}], [{
            "trip_id": "T-TRIP", "departure_time": "08:00:00", "stop_id": "T1",
            "stop_sequence": "1", "pickup_type": "0",
        }])
        cls.feeds["tec"] = path

        path = cls.root / "sncb.zip"
        make_feed(path, "SNCB", "2", [
            {"stop_id": "gs:nmbssncb:S8833209", "stop_code": "",
             "stop_name": "Aarschot", "stop_lat": "50.984", "stop_lon": "4.837",
             "location_type": "1", "parent_station": ""},
            {"stop_id": "platform-1", "stop_code": "", "stop_name": "Aarschot",
             "stop_lat": "50.9841", "stop_lon": "4.8371", "location_type": "0",
             "parent_station": "gs:nmbssncb:S8833209"},
        ], [
            {"route_id": "R", "service_id": "DAILY", "trip_id": "TRAIN"},
            {"route_id": "REPLACEMENT", "service_id": "DAILY",
             "trip_id": "REPLACEMENT-BUS"},
        ], [
            {"trip_id": "TRAIN", "departure_time": "09:00:00",
             "stop_id": "platform-1", "stop_sequence": "1", "pickup_type": "0"},
            {"trip_id": "REPLACEMENT-BUS", "departure_time": "10:00:00",
             "stop_id": "platform-1", "stop_sequence": "1", "pickup_type": "0"},
        ])
        cls.feeds["sncb"] = path

        cls.out1, cls.out2 = cls.root / "out1", cls.root / "out2"
        kwargs = dict(
            reference_date=date(2026, 1, 5),
            local_start=date(2026, 1, 5), local_end=date(2026, 1, 18),
            rail_start=date(2026, 1, 5), rail_end=date(2026, 2, 1))
        cls.report1 = service.build_service_caches(cls.feeds, cls.out1, **kwargs)
        cls.report2 = service.build_service_caches(cls.feeds, cls.out2, **kwargs)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_modes_0_1_3_are_local_and_rail_2_is_separate(self):
        self.assertEqual(service.LOCAL_ROUTE_MODES,
                         {"0": "TRAM", "1": "METRO", "3": "BUS"})

    def test_blank_pickup_0600_and_distinct_grouped_trip_semantics(self):
        summary = pq.read_table(
            self.out1 / "be_transit_service_summary.parquet").to_pylist()
        boundary = next(row for row in summary if row["normalized_name"] == "boundary")
        self.assertEqual(boundary["operators"], ["De Lijn", "TEC"])
        # OUT appears at both De Lijn members but counts once; BACK and TEC add one.
        # NOBOARD and exactly 22:00 are excluded.
        self.assertEqual(boundary["weekday_departures"], 3.0)
        self.assertEqual(boundary["saturday_departures"], 3.0)
        self.assertEqual(boundary["sunday_departures"], 3.0)
        self.assertEqual(boundary["seven_day_average"], 3.0)

    def test_parent_grouping_and_member_coordinates_are_preserved(self):
        members = pq.read_table(
            self.out1 / "be_transit_service_stops.parquet").to_pylist()
        stib = next(row for row in members if row["operator"] == "STIB/MIVB")
        self.assertEqual(stib["authoritative_parent_id"], "S0")
        self.assertAlmostEqual(stib["lat"], 50.01001)
        self.assertEqual(stib["grouping_method"], "parent_station")

    def test_cross_operator_merge_count(self):
        self.assertEqual(self.report1["cross_operator_merge_count"], 1)

    def test_rail_parent_uic_and_replacement_bus_exclusion(self):
        rows = pq.read_table(self.out1 / "be_rail_service.parquet").to_pylist()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["logical_station_id"], "uic:8833209")
        self.assertEqual(row["authoritative_station_id"],
                         "gs:nmbssncb:S8833209")
        self.assertEqual(row["child_platform_count"], 1)
        self.assertEqual(row["weekday_departures"], 1.0)
        self.assertEqual(row["seven_day_average"], 1.0)

    def test_repeat_build_has_identical_logical_rows(self):
        for filename in (
                "be_transit_service_stops.parquet",
                "be_transit_service_summary.parquet", "be_rail_service.parquet"):
            left = pq.read_table(self.out1 / filename).to_pylist()
            right = pq.read_table(self.out2 / filename).to_pylist()
            self.assertEqual(left, right)

    def test_report_records_explicit_windows_and_versions(self):
        self.assertEqual(self.report1["local_window"],
                         ("2026-01-05", "2026-01-18"))
        self.assertEqual(self.report1["rail_window"],
                         ("2026-01-05", "2026-02-01"))
        self.assertEqual(self.report1["feeds"]["sncb"]["version"], "fixture-v1")

    def test_local_override_report_has_explicit_unknown_http_provenance(self):
        for key in ("delijn", "stib", "tec", "sncb"):
            feed = self.report1["feeds"][key]
            self.assertFalse(feed["downloaded"])
            self.assertIsNone(feed["dataset_updated_at"])
            self.assertIsNone(feed["retrieved_at"])
            self.assertIsNone(feed["etag"])
            self.assertEqual(feed["source_url"], service.OFFICIAL_GTFS_URLS[key])

    def test_summary_metadata_contains_exactly_three_local_sources(self):
        document = read_provenance(
            self.out1 / "be_transit_service_summary.parquet")
        self.assertEqual(document["schema_version"], 1)
        self.assertEqual(set(document["sources"]), {"delijn", "stib", "tec"})
        self.assertEqual(
            {source["operator"] for source in document["sources"].values()},
            {"De Lijn", "STIB/MIVB", "TEC"})
        for source in document["sources"].values():
            self.assertFalse(source["downloaded"])
            self.assertIsNone(source["dataset_updated_at"])
            self.assertIsNone(source["retrieved_at"])
            self.assertIsNone(source["etag"])

    def test_rail_metadata_contains_only_sncb(self):
        document = read_provenance(self.out1 / "be_rail_service.parquet")
        self.assertEqual(document["schema_version"], 1)
        self.assertEqual(set(document["sources"]), {"sncb"})
        self.assertEqual(document["sources"]["sncb"]["operator"], "SNCB/NMBS")

    def test_parquet_row_schemas_and_stops_metadata_are_unchanged(self):
        expected = {
            "be_transit_service_stops.parquet": service.LOCAL_STOPS_SCHEMA,
            "be_transit_service_summary.parquet": service.LOCAL_SUMMARY_SCHEMA,
            "be_rail_service.parquet": service.RAIL_SCHEMA,
        }
        for filename, schema in expected.items():
            actual = pq.ParquetFile(self.out1 / filename).schema_arrow
            self.assertEqual(actual.remove_metadata(), schema)
        stops_metadata = pq.ParquetFile(
            self.out1 / "be_transit_service_stops.parquet").schema_arrow.metadata
        self.assertNotIn(service.PROVENANCE_METADATA_KEY.encode("utf-8"),
                         stops_metadata or {})


class CalendarExceptionsTests(unittest.TestCase):
    def test_calendar_activation_addition_and_removal(self):
        connection = duckdb.connect()
        connection.execute("""
            CREATE TABLE calendar(service_id VARCHAR, monday VARCHAR,
                tuesday VARCHAR, wednesday VARCHAR, thursday VARCHAR,
                friday VARCHAR, saturday VARCHAR, sunday VARCHAR,
                start_date VARCHAR, end_date VARCHAR)
        """)
        connection.execute("INSERT INTO calendar VALUES "
                           "('BASE','1','1','1','1','1','0','0','20260101','20260131'),"
                           "('EXTRA','0','0','0','0','0','0','0','20260101','20260131')")
        connection.execute("CREATE TABLE calendar_dates(service_id VARCHAR, date VARCHAR, exception_type VARCHAR)")
        connection.execute("INSERT INTO calendar_dates VALUES "
                           "('BASE','20260105','2'),('EXTRA','20260110','1')")
        service._create_active_services(
            connection, date(2026, 1, 5), date(2026, 1, 18))
        rows = set(connection.execute(
            "SELECT service_date, service_id FROM active_services").fetchall())
        self.assertNotIn((date(2026, 1, 5), "BASE"), rows)
        self.assertIn((date(2026, 1, 6), "BASE"), rows)
        self.assertIn((date(2026, 1, 10), "EXTRA"), rows)
        connection.close()


if __name__ == "__main__":
    unittest.main()

import csv
from datetime import date, timedelta
import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'belgium-location'))
try:
    import build_nl_transit_service_cache as nl
    import build_transit_service_cache as be
finally:
    sys.path.pop(0)


def fixture(path, reverse=False):
    tables = {
        'feed_info': [['feed_start_date', 'feed_end_date', 'feed_version'],
                      ['20260916', '20261212', '9583']],
        'agency': [['agency_id', 'agency_name'], ['A', 'Operator A'], ['B', 'Operator B']],
        'stops': [['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'stop_code', 'parent_station', 'location_type'],
                  ['P', 'Central', '52', '5', '', '', '1'],
                  ['b', 'Bay B', '52', '5', '', 'P', '0'],
                  ['c', 'Bay C', '52.001', '5', '', 'P', '0'],
                  ['m', 'Metro', '52.01', '5', '', '', '0'],
                  ['t', 'Tram', '52.02', '5', '', '', '0'],
                  ['stoparea:18184', 'Hengelo', '52.03', '5', 'hgl', '', '1'],
                  ['r1', 'Platform 1', '52.03', '5', '', 'stoparea:18184', '0'],
                  ['r2', 'Platform 2', '52.0301', '5', '', 'stoparea:18184', '0']],
        'routes': [['route_id', 'agency_id', 'route_type'],
                   ['busA', 'A', '3'], ['busB', 'B', '3'], ['metro', 'A', '1'],
                   ['tram', 'B', '0'], ['railA', 'A', '2'], ['railB', 'B', '2']],
        'trips': [['route_id', 'service_id', 'trip_id']],
        'stop_times': [['trip_id', 'stop_id', 'departure_time', 'pickup_type']],
        'calendar_dates': [['service_id', 'date', 'exception_type']],
    }
    for route, stop in [('busA', 'b'), ('busA', 'c'), ('busB', 'b'),
                        ('metro', 'm'), ('tram', 't'), ('railA', 'r1'), ('railB', 'r2')]:
        trip = route + stop
        tables['trips'].append([route, 'daily', trip])
        tables['stop_times'].append([trip, stop, '12:00:00', '0'])
    for offset in range(88):
        day = date(2026, 9, 16) + timedelta(days=offset)
        tables['calendar_dates'].append(['daily', day.strftime('%Y%m%d'), '1'])
    # A trip visiting two bays at one logical location is still one departure.
    tables['stop_times'].append(['busAb', 'c', '12:01:00', '0'])
    with zipfile.ZipFile(path, 'w') as archive:
        for table, rows in tables.items():
            stream = io.StringIO(newline='')
            csv.writer(stream).writerows([rows[0]] + (list(reversed(rows[1:])) if reverse else rows[1:]))
            archive.writestr(table + '.txt', stream.getvalue())


class NetherlandsCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.zip = cls.root / 'nl.zip'
        fixture(cls.zip)
        nl.build_service_caches(cls.zip, cls.root / 'out')
        cls.stops = pq.read_table(cls.root / 'out/nl_transit_service_stops.parquet').to_pylist()
        cls.summary = pq.read_table(cls.root / 'out/nl_transit_service_summary.parquet').to_pylist()
        cls.rail = pq.read_table(cls.root / 'out/nl_rail_service.parquet').to_pylist()

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_calendar_dates_only_preserves_frequency_and_version(self):
        for row in self.summary:
            self.assertGreater(row['weekday_departures'], 0)
            self.assertEqual(row['weekday_departures'], row['saturday_departures'])
            self.assertEqual(row['sunday_departures'], row['weekday_departures'])
            self.assertEqual((row['active_hour_start'], row['active_hour_end']), (6, 22))
        self.assertTrue(all(row['feed_version'] == '9583' for row in self.stops + self.rail))

    def test_agencies_are_public_labels_not_one_feed_operator(self):
        self.assertEqual({s['operator'] for s in self.stops}, {'Operator A', 'Operator B'})
        central = next(s for s in self.summary if s['display_name'] == 'Central')
        self.assertEqual(central['operators'], ['Operator A', 'Operator B'])
        self.assertEqual(central['weekday_departures'], 3)

    def test_all_local_modes(self):
        self.assertEqual({m for s in self.stops for m in s['modes']}, {'BUS', 'METRO', 'TRAM'})

    def test_parent_hierarchy_overrides_different_names_and_distance(self):
        members = [s for s in self.stops if s['gtfs_stop_id'] in ('b', 'c')]
        self.assertEqual(len({s['logical_stop_id'] for s in members}), 1)
        self.assertTrue(all(s['authoritative_parent_id'] == 'P' for s in members))

    def test_rail_all_agencies_parent_identity_no_fabricated_uic(self):
        self.assertEqual(len(self.rail), 1)
        row = self.rail[0]
        self.assertEqual(row['authoritative_station_id'], 'stoparea:18184')
        self.assertIsNone(row['uic_code'])
        self.assertTrue(row['logical_station_id'].startswith('gtfs-nl-rail:'))
        self.assertEqual(row['child_platform_count'], 2)
        self.assertEqual(row['weekday_departures'], 2)

    def test_exact_schemas(self):
        for name, schema in [('nl_transit_service_stops', be.LOCAL_STOPS_SCHEMA),
                             ('nl_transit_service_summary', be.LOCAL_SUMMARY_SCHEMA),
                             ('nl_rail_service', be.RAIL_SCHEMA)]:
            self.assertEqual(pq.read_schema(self.root / f'out/{name}.parquet'), schema)

    def test_deterministic_output_when_input_reordered(self):
        other = self.root / 'reversed.zip'
        fixture(other, reverse=True)
        nl.build_service_caches(other, self.root / 'again')
        for path in (self.root / 'out').glob('*.parquet'):
            self.assertEqual(pq.read_table(path).to_pylist(),
                             pq.read_table(self.root / 'again' / path.name).to_pylist())

    def test_calendar_removal_supported(self):
        with duckdb.connect() as connection:
            connection.execute('CREATE TABLE calendar(service_id VARCHAR, monday VARCHAR, tuesday VARCHAR, wednesday VARCHAR, thursday VARCHAR, friday VARCHAR, saturday VARCHAR, sunday VARCHAR, start_date VARCHAR, end_date VARCHAR)')
            connection.execute("INSERT INTO calendar VALUES ('x','1','1','1','1','1','1','1','20260916','20261212')")
            connection.execute('CREATE TABLE calendar_dates(service_id VARCHAR, date VARCHAR, exception_type VARCHAR)')
            connection.execute("INSERT INTO calendar_dates VALUES ('x','20260916','2'),('y','20260916','1')")
            be._create_active_services(connection, date(2026, 9, 16), date(2026, 9, 16))
            self.assertEqual(connection.execute('SELECT service_id FROM active_services').fetchall(), [('y',)])

    def test_standard_csv_quotes_preserve_embedded_comma_in_stop_times(self):
        with tempfile.TemporaryDirectory() as temporary:
            files = nl._extract(self.zip, Path(temporary))
            files['stop_times.txt'].write_text(
                'trip_id,stop_id,departure_time,pickup_type,stop_headsign\n'
                'busAb,b,12:00:00,0,"Wijk aan Zee, Dorpsduinen"\n'
                'busAc,c,12:01:00,0,"Town ""Centre"", Bay"\n',
                encoding='utf-8')
            with duckdb.connect() as connection:
                nl._load_gtfs_tables(connection, files)
                self.assertEqual(len(connection.execute(
                    "PRAGMA table_info('stop_times')").fetchall()), 5)
                self.assertEqual(connection.execute(
                    'SELECT stop_headsign FROM stop_times ORDER BY trip_id').fetchall(),
                    [('Wijk aan Zee, Dorpsduinen',), ('Town "Centre", Bay',)])

    def test_unparented_fallback_exact_name_mode_distance_and_no_chain(self):
        with duckdb.connect() as connection:
            files = nl._extract(self.zip, self.root)
            be._load_gtfs_tables(connection, files)
            connection.execute("CREATE TABLE agency(agency_id VARCHAR, agency_name VARCHAR)")
            connection.execute("INSERT INTO agency VALUES ('A','Operator A'),('B','Operator B')")
            connection.execute("UPDATE stops SET parent_station = NULL WHERE stop_id IN ('b','c')")
            connection.execute("UPDATE stops SET stop_name = 'Exact Name', stop_lat = '52', stop_lon = '5' WHERE stop_id IN ('b','c','m','t')")
            groups, _ = nl._local_groups(connection, be._station_rows(connection))
            a_bus = [g for g in groups if g.operator_key == 'A' and g.modes == {'BUS'}]
            self.assertEqual(len(a_bus), 1)
            self.assertEqual(len(a_bus[0].members), 2)
            self.assertEqual(len([g for g in groups if g.operator_key == 'A']), 2)
            connection.execute("UPDATE stops SET stop_name = 'Different' WHERE stop_id = 'c'")
            groups, _ = nl._local_groups(connection, be._station_rows(connection))
            self.assertEqual(len([g for g in groups if g.operator_key == 'A' and g.modes == {'BUS'}]), 2)
        members = [be.Stop('A', str(i), 'Exact', 'exact', 52 + i * 20 / 111195,
                           5, frozenset({'BUS'})) for i in range(3)]
        clusters = be.complete_link_clusters(members, 30)
        self.assertEqual(sorted(map(len, clusters)), [1, 2])

    def test_unknown_agency_fails_instead_of_mislabeling(self):
        with duckdb.connect() as connection:
            files = nl._extract(self.zip, self.root)
            be._load_gtfs_tables(connection, files)
            connection.execute("CREATE TABLE agency(agency_id VARCHAR, agency_name VARCHAR)")
            connection.execute("INSERT INTO agency VALUES ('A','Operator A')")
            with self.assertRaisesRegex(ValueError, 'unknown agencies'):
                nl._local_groups(connection, be._station_rows(connection))


if __name__ == '__main__':
    unittest.main()

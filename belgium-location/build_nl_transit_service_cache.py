"""Build Netherlands service caches independently of the Belgian pipeline."""

import argparse
from collections import defaultdict
from pathlib import Path
import re
import shutil
import tempfile
import zipfile

import duckdb
import build_transit_service_cache as be


CALENDAR_HEADER = ("service_id,monday,tuesday,wednesday,thursday,friday,"
                   "saturday,sunday,start_date,end_date\n")


def _extract(zip_path, target):
    files = {}
    with zipfile.ZipFile(zip_path) as archive:
        names = {Path(name).name.lower(): name for name in archive.namelist()}
        for filename in (*be.REQUIRED_FILES, "agency.txt"):
            path = target / filename
            if filename not in names:
                headers = {"calendar.txt": CALENDAR_HEADER,
                           "calendar_dates.txt": "service_id,date,exception_type\n"}
                if filename not in headers:
                    raise ValueError(f"Missing required {filename}")
                path.write_text(headers[filename], encoding="utf-8")
            else:
                with archive.open(names[filename]) as source, path.open("wb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
            files[filename] = path
    return files


def _load_gtfs_tables(connection, files):
    """Read standard GTFS CSV without relying on quote/delimiter detection."""
    for filename in (*be.REQUIRED_FILES, 'agency.txt'):
        if filename == 'feed_info.txt':
            continue
        table = filename.removesuffix('.txt')
        connection.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM "
            f"read_csv_auto('{be._sql_path(files[filename])}', "
            "delim=',', quote='\"', escape='\"', header=true, "
            "all_varchar=true, sample_size=20480, null_padding=true)")
    columns = {row[1] for row in connection.execute("PRAGMA table_info('stops')").fetchall()}
    for name in ('stop_code', 'parent_station', 'location_type'):
        if name not in columns:
            connection.execute(f'ALTER TABLE stops ADD COLUMN {name} VARCHAR')
    columns = {row[1] for row in connection.execute("PRAGMA table_info('stop_times')").fetchall()}
    if 'pickup_type' not in columns:
        connection.execute('ALTER TABLE stop_times ADD COLUMN pickup_type VARCHAR')


def _local_groups(connection, rows):
    labels = dict(connection.execute(
        "SELECT agency_id, agency_name FROM agency").fetchall())
    if any(not key or not str(label or '').strip() for key, label in labels.items()):
        raise ValueError("Agencies require nonempty IDs and public names")
    missing = connection.execute("""
        SELECT DISTINCT agency_id FROM routes
        WHERE agency_id IS NULL OR agency_id NOT IN (SELECT agency_id FROM agency)
    """).fetchall()
    if missing:
        raise ValueError(f"Routes reference unknown agencies: {missing}")
    modes = defaultdict(lambda: defaultdict(set))
    for agency, stop_id, route_type in connection.execute("""
        SELECT DISTINCT agency_id, stop_id, route_type
        FROM stop_times JOIN trips USING (trip_id) JOIN routes USING (route_id)
        WHERE route_type IN ('0', '1', '3')
    """).fetchall():
        modes[agency][stop_id].add(be.LOCAL_ROUTE_MODES[route_type])
    groups = []
    for agency in sorted(modes):
        parented, fallback = defaultdict(list), defaultdict(list)
        for stop_id, stop_modes in sorted(modes[agency].items()):
            row = rows.get(stop_id)
            if not row:
                continue
            stop = be.Stop(agency, stop_id, row['name'], be.normalize_name(row['name']),
                           row['lat'], row['lon'], frozenset(stop_modes),
                           row['stop_code'], row['parent_station'])
            parent = rows.get(stop.parent_station)
            if parent and parent['location_type'] == '1' and stop.parent_station != stop_id:
                parented[stop.parent_station].append(stop)
            else:
                fallback[stop.normalized_name].append(stop)
        for parent_id, members in sorted(parented.items()):
            name = be._preferred_name(members, rows[parent_id]['name'])
            groups.append(be.LogicalGroup(agency, members, name, be.normalize_name(name),
                                          'parent_station', parent_id))
        for name, members in sorted(fallback.items(), key=lambda item: item[0] or ''):
            clusters = be.complete_link_clusters(members, 30) if name else [[s] for s in members]
            for cluster in clusters:
                groups.append(be.LogicalGroup(agency, cluster, be._preferred_name(cluster),
                                              name, 'exact_name_complete_link_30m'))
    for group in groups:
        group.provisional_id = be._stable_id('gtfs-nl-local',
                                             [s.identity for s in group.members])
    return groups, labels


def _local_daily(connection, groups, start, end):
    """Count each scheduled trip once per logical location, across its agencies."""
    import pyarrow as pa
    mapping = [{'agency_id': s.operator_key, 'stop_id': s.stop_id,
                'group_id': g.logical_stop_id} for g in groups for s in g.members]
    connection.register('nl_group_map', pa.Table.from_pylist(mapping, schema=pa.schema([
        ('agency_id', pa.string()), ('stop_id', pa.string()), ('group_id', pa.string())])))
    be._create_active_services(connection, start, end)
    try:
        values = connection.execute("""
            WITH group_trip AS (
                SELECT DISTINCT m.group_id, t.trip_id, t.service_id
                FROM stop_times st JOIN trips t USING (trip_id)
                JOIN routes r USING (route_id)
                JOIN nl_group_map m ON m.stop_id = st.stop_id AND m.agency_id = r.agency_id
                WHERE r.route_type IN ('0', '1', '3')
                  AND coalesce(nullif(trim(st.pickup_type), ''), '0') = '0'
                  AND try_cast(split_part(st.departure_time, ':', 1) AS INTEGER) >= 6
                  AND try_cast(split_part(st.departure_time, ':', 1) AS INTEGER) < 22
            ) SELECT group_id, service_date, count(*) FROM group_trip
              JOIN active_services USING (service_id) GROUP BY 1, 2
        """).fetchall()
    finally:
        connection.unregister('nl_group_map')
    daily = defaultdict(dict)
    for group_id, day, count in values:
        daily[group_id][day] = count
    return daily


def _rail(connection, rows, metadata, window):
    by_station = defaultdict(list)
    for (stop_id,) in connection.execute("""
        SELECT DISTINCT stop_id FROM stop_times JOIN trips USING (trip_id)
        JOIN routes USING (route_id) WHERE route_type = '2'
    """).fetchall():
        if stop_id not in rows:
            continue
        parent = rows[stop_id]['parent_station']
        station_id = parent if parent in rows and rows[parent]['location_type'] == '1' else stop_id
        by_station[station_id].append(stop_id)
    groups = []
    for station_id, stop_ids in sorted(by_station.items()):
        members = [be.Stop('nl', s, rows[s]['name'], be.normalize_name(rows[s]['name']),
                           rows[s]['lat'], rows[s]['lon'], frozenset({'RAIL'}))
                   for s in sorted(stop_ids)]
        logical_id = be._stable_id('gtfs-nl-rail', [station_id])
        groups.append(be.LogicalGroup('nl', members, rows[station_id]['name'], None,
                                      'parent_station', station_id, logical_id, logical_id))
    daily = be._daily_for_groups(connection, groups, *window, ('2',))
    result = []
    for group in groups:
        w, sa, su, avg = be.representative_values(daily.get(group.provisional_id, {}), *window)
        if avg <= 0:
            continue
        station = rows[group.authoritative_parent_id]
        # Only an explicit numeric stop_code qualifies; never derive UIC from an ID.
        code = str(station['stop_code'] or '').strip()
        result.append(dict(logical_station_id=group.logical_stop_id,
                           authoritative_station_id=group.authoritative_parent_id,
                           uic_code=code if re.fullmatch(r'\d{7,9}', code) else None,
                           station_name=group.display_name, lat=station['lat'], lon=station['lon'],
                           child_platform_count=len(group.members), weekday_departures=w,
                           saturday_departures=sa, sunday_departures=su, seven_day_average=avg,
                           window_start=window[0], window_end=window[1],
                           active_hour_start=6, active_hour_end=22, feed_version=metadata['version']))
    return result


def build_service_caches(zip_path, output_dir):
    metadata = be._feed_metadata(zip_path)
    # Anchor to feed validity, not today's date, for reproducible builds.
    reference = metadata['validity'][0]
    local_window = be.select_local_window({'nl': metadata['validity']}, reference)
    rail_window = be.select_rail_window(metadata['validity'], reference)
    with tempfile.TemporaryDirectory(prefix='nl-gtfs-') as temporary:
        root = Path(temporary)
        files = _extract(zip_path, root)
        with duckdb.connect(str(root / 'feed.duckdb')) as connection:
            _load_gtfs_tables(connection, files)
            rows = be._station_rows(connection)
            groups, labels = _local_groups(connection, rows)
            # Filter inactive operator groups before cross-agency consolidation.
            for group in groups:
                group.logical_stop_id = group.provisional_id
            daily = _local_daily(connection, groups, *local_window)
            groups = [g for g in groups if be.representative_values(
                daily.get(g.logical_stop_id, {}), *local_window)[3] > 0]
            source = {s.identity: g for g in groups for s in g.members}
            final, _ = be.merge_cross_operator(groups)
            for group in final:
                group.logical_stop_id = be._stable_id('gtfs-nl-local', [s.identity for s in group.members])
            daily = _local_daily(connection, final, *local_window)
            rail = _rail(connection, rows, metadata, rail_window)
        stops, summaries = [], []
        for group in final:
            w, sa, su, avg = be.representative_values(daily.get(group.logical_stop_id, {}), *local_window)
            summaries.append(dict(logical_stop_id=group.logical_stop_id, display_name=group.display_name,
                                  normalized_name=group.normalized_name,
                                  operators=sorted({labels[k] for k in group.operators}), modes=sorted(group.modes),
                                  member_count=len(group.members), weekday_departures=w, saturday_departures=sa,
                                  sunday_departures=su, seven_day_average=avg, window_start=local_window[0],
                                  window_end=local_window[1], active_hour_start=6, active_hour_end=22,
                                  source_feed_versions=sorted({f'{labels[k]}={metadata["version"]}' for k in group.operators})))
            for member in group.members:
                original = source[member.identity]
                stops.append(dict(logical_stop_id=group.logical_stop_id, operator=labels[member.operator_key],
                                  gtfs_stop_id=member.stop_id, stop_code=member.stop_code,
                                  parent_station=member.parent_station, authoritative_parent_id=original.authoritative_parent_id,
                                  name=member.name, normalized_name=member.normalized_name,
                                  lat=member.lat, lon=member.lon, modes=sorted(member.modes),
                                  grouping_method=original.grouping_method + ('+cross_operator_exact_name_complete_link_30m'
                                      if len(group.operators) > 1 else ''), feed_version=metadata['version']))
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, values, schema, key in (
        ('nl_transit_service_stops', stops, be.LOCAL_STOPS_SCHEMA, lambda r: (r['logical_stop_id'], r['operator'], r['gtfs_stop_id'])),
        ('nl_transit_service_summary', summaries, be.LOCAL_SUMMARY_SCHEMA, lambda r: r['logical_stop_id']),
        ('nl_rail_service', rail, be.RAIL_SCHEMA, lambda r: r['logical_station_id'])):
        counts[name] = be._write_parquet(sorted(values, key=key), schema, output / f'{name}.parquet')
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gtfs', type=Path, default=Path('data/nl-gtfs.zip'))
    parser.add_argument('--output-dir', type=Path, default=Path('cache'))
    args = parser.parse_args()
    print(build_service_caches(args.gtfs, args.output_dir))


if __name__ == '__main__':
    main()

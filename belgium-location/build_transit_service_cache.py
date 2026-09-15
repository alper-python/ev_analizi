"""Build scheduled-service metadata caches from official Belgian static GTFS.

This module deliberately does not calculate Transit Score V1.  It converts
De Lijn, STIB/MIVB and TEC timetables into user-facing local access locations,
and SNCB/NMBS into a separate station service cache.  The existing OSM
Transit Cache remains an independent infrastructure layer.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import statistics
import tempfile
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from io import TextIOWrapper
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests


OFFICIAL_GTFS_URLS = {
    "delijn": "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/delijn/static",
    "stib": "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/stibmivb/static",
    "tec": "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/tec/static",
    "sncb": "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/nmbssncb/static",
}
OPERATOR_LABELS = {
    "delijn": "De Lijn", "stib": "STIB/MIVB", "tec": "TEC",
    "sncb": "SNCB/NMBS",
}
SOURCE_PROVIDER = "Belgian Mobility Open Data Portal"
PROVENANCE_METADATA_KEY = "ev_analizi.transit_source_provenance_v1"
LOCAL_ROUTE_MODES = {"0": "TRAM", "1": "METRO", "3": "BUS"}
LOCAL_THRESHOLDS_M = {"delijn": 100.0, "stib": 75.0, "tec": 50.0}
STIB_PARENT_ABSORPTION_M = 150.0
CROSS_OPERATOR_DISTANCE_M = 30.0
LOCAL_WINDOW_MAX_DAYS = 42
LOCAL_WINDOW_MIN_DAYS = 14
RAIL_WINDOW_DAYS = 42
ACTIVE_HOUR_START = 6
ACTIVE_HOUR_END = 22
EARTH_RADIUS_M = 6371000.0
REQUIRED_FILES = (
    "feed_info.txt", "stops.txt", "routes.txt", "trips.txt",
    "stop_times.txt", "calendar.txt", "calendar_dates.txt",
)


LOCAL_STOPS_SCHEMA = pa.schema([
    ("logical_stop_id", pa.string()),
    ("operator", pa.string()),
    ("gtfs_stop_id", pa.string()),
    ("stop_code", pa.string()),
    ("parent_station", pa.string()),
    ("authoritative_parent_id", pa.string()),
    ("name", pa.string()),
    ("normalized_name", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("modes", pa.list_(pa.string())),
    ("grouping_method", pa.string()),
    ("feed_version", pa.string()),
])
LOCAL_SUMMARY_SCHEMA = pa.schema([
    ("logical_stop_id", pa.string()),
    ("display_name", pa.string()),
    ("normalized_name", pa.string()),
    ("operators", pa.list_(pa.string())),
    ("modes", pa.list_(pa.string())),
    ("member_count", pa.int32()),
    ("weekday_departures", pa.float64()),
    ("saturday_departures", pa.float64()),
    ("sunday_departures", pa.float64()),
    ("seven_day_average", pa.float64()),
    ("window_start", pa.date32()),
    ("window_end", pa.date32()),
    ("active_hour_start", pa.int8()),
    ("active_hour_end", pa.int8()),
    ("source_feed_versions", pa.list_(pa.string())),
])
RAIL_SCHEMA = pa.schema([
    ("logical_station_id", pa.string()),
    ("authoritative_station_id", pa.string()),
    ("uic_code", pa.string()),
    ("station_name", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("child_platform_count", pa.int32()),
    ("weekday_departures", pa.float64()),
    ("saturday_departures", pa.float64()),
    ("sunday_departures", pa.float64()),
    ("seven_day_average", pa.float64()),
    ("window_start", pa.date32()),
    ("window_end", pa.date32()),
    ("active_hour_start", pa.int8()),
    ("active_hour_end", pa.int8()),
    ("feed_version", pa.string()),
])


def normalize_name(value):
    """Normalize exact text without translating, stripping accents or fuzzing."""
    if value is None:
        return None
    normalized = re.sub(
        r"\s+", " ", unicodedata.normalize("NFKC", str(value)).casefold()
    ).strip()
    return normalized or None


def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dlat = p2 - p1
    dlon = math.radians(float(lon2) - float(lon1))
    value = (math.sin(dlat / 2) ** 2
             + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(max(0.0, min(1.0, value))))


def _stable_id(prefix, member_keys):
    payload = "\n".join(sorted(str(value) for value in member_keys))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _parse_gtfs_date(value):
    text = str(value or "").strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError(f"Invalid GTFS date: {value!r}")
    return datetime.strptime(text, "%Y%m%d").date()


def _dates(start, end):
    return [start + timedelta(days=offset)
            for offset in range((end - start).days + 1)]


def _validate_sample(start, end, label, minimum_days=LOCAL_WINDOW_MIN_DAYS):
    if start > end:
        raise ValueError(f"{label} window start is after its end")
    days = _dates(start, end)
    weekday = sum(day.weekday() < 5 for day in days)
    saturday = sum(day.weekday() == 5 for day in days)
    sunday = sum(day.weekday() == 6 for day in days)
    if len(days) < minimum_days or weekday < 10 or saturday < 2 or sunday < 2:
        raise ValueError(
            f"{label} overlap is too small for a defensible sample: "
            f"{len(days)} days, {weekday} weekdays, {saturday} Saturdays, "
            f"{sunday} Sundays")
    return start, end


def select_local_window(validities, reference_date, explicit_start=None,
                        explicit_end=None):
    """Choose at most six weeks inside the common local-feed validity."""
    common_start = max(value[0] for value in validities.values())
    common_end = min(value[1] for value in validities.values())
    if explicit_start is not None or explicit_end is not None:
        if explicit_start is None or explicit_end is None:
            raise ValueError("Both local window overrides are required")
        start, end = explicit_start, explicit_end
        if start < common_start or end > common_end:
            raise ValueError("Explicit local window is outside common feed validity")
    elif (common_end - common_start).days + 1 <= LOCAL_WINDOW_MAX_DAYS:
        start, end = common_start, common_end
    else:
        preferred = reference_date - timedelta(days=7)
        latest_start = common_end - timedelta(days=LOCAL_WINDOW_MAX_DAYS - 1)
        start = min(max(preferred, common_start), latest_start)
        end = start + timedelta(days=LOCAL_WINDOW_MAX_DAYS - 1)
    return _validate_sample(start, end, "Local timetable")


def _next_monday(value):
    return value + timedelta(days=(-value.weekday()) % 7)


def select_rail_window(validity, reference_date, explicit_start=None,
                       explicit_end=None):
    """Prefer six complete Monday-Sunday weeks within SNCB validity."""
    valid_start, valid_end = validity
    if explicit_start is not None or explicit_end is not None:
        if explicit_start is None or explicit_end is None:
            raise ValueError("Both rail window overrides are required")
        start, end = explicit_start, explicit_end
        if start < valid_start or end > valid_end:
            raise ValueError("Explicit rail window is outside SNCB feed validity")
    else:
        earliest_monday = _next_monday(valid_start)
        latest_monday = valid_end - timedelta(days=RAIL_WINDOW_DAYS - 1)
        latest_monday -= timedelta(days=latest_monday.weekday())
        if latest_monday < earliest_monday:
            raise ValueError("SNCB validity cannot supply six complete weeks")
        start = min(max(_next_monday(reference_date), earliest_monday), latest_monday)
        end = start + timedelta(days=RAIL_WINDOW_DAYS - 1)
    return _validate_sample(start, end, "Rail timetable", minimum_days=28)


@dataclass(frozen=True)
class Stop:
    operator_key: str
    stop_id: str
    name: str
    normalized_name: str | None
    lat: float
    lon: float
    modes: frozenset[str]
    stop_code: str | None = None
    parent_station: str | None = None

    @property
    def identity(self):
        return f"{self.operator_key}:{self.stop_id}"


@dataclass
class LogicalGroup:
    operator_key: str | None
    members: list[Stop]
    display_name: str
    normalized_name: str | None
    grouping_method: str
    authoritative_parent_id: str | None = None
    provisional_id: str | None = None
    logical_stop_id: str | None = None

    @property
    def modes(self):
        return frozenset(mode for member in self.members for mode in member.modes)

    @property
    def operators(self):
        return tuple(sorted({member.operator_key for member in self.members}))


def _compatible(left, right):
    return bool(set(left.modes) & set(right.modes))


def complete_link_clusters(stops, threshold_m):
    """Cluster exact-name, mode-compatible stops without transitive chaining."""
    clusters = []
    for stop in sorted(stops, key=lambda item: item.identity):
        candidates = []
        for index, cluster in enumerate(clusters):
            if not all(_compatible(stop, member) for member in cluster):
                continue
            distances = [haversine_m(stop.lat, stop.lon, member.lat, member.lon)
                         for member in cluster]
            if max(distances) <= threshold_m:
                candidates.append((max(distances),
                                   tuple(item.identity for item in cluster), index))
        if candidates:
            clusters[min(candidates)[2]].append(stop)
        else:
            clusters.append([stop])
    return [sorted(cluster, key=lambda item: item.identity) for cluster in clusters]


def _preferred_name(members, explicit=None):
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    values = [member.name.strip() for member in members if member.name.strip()]
    return min(values, key=lambda value: (normalize_name(value) or "", value)) if values else "Unnamed"


def group_operator_stops(operator_key, stops, station_rows=None):
    """Apply calibrated hierarchy and fallback rules for one local operator."""
    station_rows = station_rows or {}
    by_parent = defaultdict(list)
    unparented = []
    for stop in stops:
        if operator_key != "delijn" and stop.parent_station in station_rows:
            by_parent[stop.parent_station].append(stop)
        else:
            unparented.append(stop)

    groups = []
    for parent_id in sorted(by_parent):
        parent = station_rows[parent_id]
        members = sorted(by_parent[parent_id], key=lambda item: item.identity)
        groups.append(LogicalGroup(
            operator_key, members, _preferred_name(members, parent["name"]),
            normalize_name(parent["name"]), "parent_station", parent_id))

    fallback_clusters = []
    by_name = defaultdict(list)
    for stop in unparented:
        by_name[stop.normalized_name].append(stop)
    for normalized in sorted(by_name, key=lambda value: value or ""):
        named_stops = by_name[normalized]
        clusters = ([[item] for item in sorted(
            named_stops, key=lambda stop: stop.identity)]
            if normalized is None else complete_link_clusters(
                named_stops, LOCAL_THRESHOLDS_M[operator_key]))
        for cluster in clusters:
            fallback_clusters.append(LogicalGroup(
                operator_key, cluster, _preferred_name(cluster), normalized,
                f"exact_name_complete_link_{int(LOCAL_THRESHOLDS_M[operator_key])}m"))

    if operator_key == "stib":
        parents_by_name = defaultdict(list)
        for group in groups:
            parents_by_name[group.normalized_name].append(group)
        remaining = []
        for fallback in fallback_clusters:
            candidates = []
            for parent in parents_by_name.get(fallback.normalized_name, []):
                station = station_rows[parent.authoritative_parent_id]
                distance = min(haversine_m(
                    member.lat, member.lon, station["lat"], station["lon"])
                    for member in fallback.members)
                if distance <= STIB_PARENT_ABSORPTION_M:
                    candidates.append((distance, parent.authoritative_parent_id, parent))
            if candidates:
                target = min(candidates)[2]
                target.members = sorted(target.members + fallback.members,
                                        key=lambda item: item.identity)
                target.grouping_method = "parent_station+exact_name_absorption_150m"
            else:
                remaining.append(fallback)
        fallback_clusters = remaining

    groups.extend(fallback_clusters)
    for group in groups:
        group.provisional_id = _stable_id(
            f"gtfs-{operator_key}", [member.identity for member in group.members])
    return sorted(groups, key=lambda group: group.provisional_id)


def _group_distance(left, right):
    return min(haversine_m(a.lat, a.lon, b.lat, b.lon)
               for a in left.members for b in right.members)


def merge_cross_operator(groups):
    """Merge only exact-name compatible operator locations within 30 metres."""
    by_name = defaultdict(list)
    for group in groups:
        by_name[group.normalized_name].append(group)
    final_groups = []
    merge_count = 0
    for normalized in sorted(by_name, key=lambda value: value or ""):
        if normalized is None:
            for group in sorted(
                    by_name[normalized], key=lambda item: item.provisional_id):
                group.logical_stop_id = _stable_id(
                    "gtfs-local", [member.identity for member in group.members])
                final_groups.append(group)
            continue
        clusters = []
        for group in sorted(by_name[normalized], key=lambda item: item.provisional_id):
            candidates = []
            for index, cluster in enumerate(clusters):
                if group.operator_key in {item.operator_key for item in cluster}:
                    continue
                if not all(group.modes & item.modes for item in cluster):
                    continue
                distances = [_group_distance(group, item) for item in cluster]
                if max(distances) <= CROSS_OPERATOR_DISTANCE_M:
                    candidates.append((max(distances),
                                       tuple(item.provisional_id for item in cluster), index))
            if candidates:
                clusters[min(candidates)[2]].append(group)
            else:
                clusters.append([group])
        for cluster in clusters:
            members = sorted(
                (member for group in cluster for member in group.members),
                key=lambda item: item.identity)
            methods = sorted({group.grouping_method for group in cluster})
            if len(cluster) > 1:
                methods.append("cross_operator_exact_name_complete_link_30m")
                merge_count += 1
            final = LogicalGroup(
                None, members,
                min((group.display_name for group in cluster),
                    key=lambda value: (normalize_name(value) or "", value)),
                normalized,
                "+".join(methods),
                provisional_id="+".join(group.provisional_id for group in cluster))
            final.logical_stop_id = _stable_id(
                "gtfs-local", [member.identity for member in members])
            final_groups.append(final)
    return sorted(final_groups, key=lambda group: group.logical_stop_id), merge_count


def representative_values(daily_counts, window_start, window_end):
    """Return weekday/Saturday/Sunday medians, explicitly including zeros."""
    populations = {"weekday": [], "saturday": [], "sunday": []}
    for service_date in _dates(window_start, window_end):
        kind = ("weekday" if service_date.weekday() < 5 else
                "saturday" if service_date.weekday() == 5 else "sunday")
        populations[kind].append(int(daily_counts.get(service_date, 0)))
    values = tuple(float(statistics.median(populations[kind]))
                   for kind in ("weekday", "saturday", "sunday"))
    return values + ((5 * values[0] + values[1] + values[2]) / 7.0,)


def _read_zip_rows(zip_path, filename):
    with zipfile.ZipFile(zip_path) as archive:
        try:
            raw = archive.open(filename)
        except KeyError:
            return []
        with raw, TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
            return list(csv.DictReader(stream))


def _feed_metadata(zip_path):
    rows = _read_zip_rows(zip_path, "feed_info.txt")
    info = rows[0] if rows else {}
    start_value, end_value = info.get("feed_start_date"), info.get("feed_end_date")
    if not start_value or not end_value:
        calendar = _read_zip_rows(zip_path, "calendar.txt")
        if not calendar:
            raise ValueError(f"Cannot determine feed validity for {zip_path}")
        start_value = min(row["start_date"] for row in calendar)
        end_value = max(row["end_date"] for row in calendar)
    return {
        "version": str(info.get("feed_version") or "unknown").strip(),
        "validity": (_parse_gtfs_date(start_value), _parse_gtfs_date(end_value)),
    }


def _utc_iso(value):
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z")


def _parse_last_modified(value):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    return _utc_iso(parsed)


def _download(url, target, session=requests):
    response = session.get(url, stream=True, timeout=120)
    response.raise_for_status()
    retrieved_at = _utc_iso(datetime.now(timezone.utc))
    with target.open("wb") as output:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output.write(chunk)
    etag = str(response.headers.get("ETag") or "").strip() or None
    return {
        "dataset_updated_at": _parse_last_modified(
            response.headers.get("Last-Modified")),
        "retrieved_at": retrieved_at,
        "etag": etag,
    }


def _extract_required(zip_path, target):
    target.mkdir(parents=True, exist_ok=True)
    extracted = {}
    with zipfile.ZipFile(zip_path) as archive:
        names = {Path(name).name.lower(): name for name in archive.namelist()}
        for filename in REQUIRED_FILES:
            source = names.get(filename.lower())
            if source is None:
                if filename == "calendar_dates.txt":
                    path = target / filename
                    path.write_text("service_id,date,exception_type\n", encoding="utf-8")
                    extracted[filename] = path
                    continue
                raise ValueError(f"{zip_path} is missing required {filename}")
            path = target / filename
            with archive.open(source) as input_file, path.open("wb") as output:
                shutil.copyfileobj(input_file, output, length=1024 * 1024)
            extracted[filename] = path
    return extracted


def _sql_path(path):
    return str(Path(path).resolve()).replace("'", "''")


def _load_gtfs_tables(connection, files):
    for filename in REQUIRED_FILES:
        if filename == "feed_info.txt":
            continue
        table = filename.removesuffix(".txt")
        connection.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM "
            f"read_csv_auto('{_sql_path(files[filename])}', header=true, "
            "all_varchar=true, sample_size=20480, null_padding=true)")
    columns = {row[1] for row in connection.execute("PRAGMA table_info('stops')").fetchall()}
    for name in ("stop_code", "parent_station", "location_type"):
        if name not in columns:
            connection.execute(f"ALTER TABLE stops ADD COLUMN {name} VARCHAR")
    stop_time_columns = {
        row[1] for row in connection.execute("PRAGMA table_info('stop_times')").fetchall()}
    if "pickup_type" not in stop_time_columns:
        connection.execute("ALTER TABLE stop_times ADD COLUMN pickup_type VARCHAR")


def _create_active_services(connection, start, end):
    connection.execute(
        "CREATE OR REPLACE TABLE selected_dates AS "
        "SELECT value::DATE AS service_date, "
        "extract(isodow FROM value)::INTEGER AS dow FROM "
        "generate_series(?::DATE, ?::DATE, INTERVAL 1 DAY) AS dates(value)",
        [start, end])
    connection.execute("""
        CREATE OR REPLACE TABLE active_services AS
        WITH base AS (
            SELECT dates.service_date, calendar.service_id
            FROM selected_dates dates JOIN calendar
              ON strptime(calendar.start_date, '%Y%m%d')::DATE <= dates.service_date
             AND strptime(calendar.end_date, '%Y%m%d')::DATE >= dates.service_date
            WHERE try_cast(CASE dates.dow
                WHEN 1 THEN calendar.monday WHEN 2 THEN calendar.tuesday
                WHEN 3 THEN calendar.wednesday WHEN 4 THEN calendar.thursday
                WHEN 5 THEN calendar.friday WHEN 6 THEN calendar.saturday
                ELSE calendar.sunday END AS INTEGER) = 1
        ), removed AS (
            SELECT dates.service_date, exceptions.service_id
            FROM selected_dates dates JOIN calendar_dates exceptions
              ON strptime(exceptions.date, '%Y%m%d')::DATE = dates.service_date
            WHERE exceptions.exception_type = '2'
        ), added AS (
            SELECT dates.service_date, exceptions.service_id
            FROM selected_dates dates JOIN calendar_dates exceptions
              ON strptime(exceptions.date, '%Y%m%d')::DATE = dates.service_date
            WHERE exceptions.exception_type = '1'
        )
        (SELECT * FROM base EXCEPT SELECT * FROM removed)
        UNION SELECT * FROM added
    """)


def _station_rows(connection):
    rows = connection.execute("""
        SELECT stop_id, stop_name, try_cast(stop_lat AS DOUBLE),
               try_cast(stop_lon AS DOUBLE), nullif(trim(stop_code), ''),
               nullif(trim(parent_station), ''), coalesce(location_type, '')
        FROM stops
    """).fetchall()
    result = {}
    for stop_id, name, lat, lon, code, parent, location_type in rows:
        if lat is None or lon is None:
            continue
        result[str(stop_id)] = {
            "stop_id": str(stop_id), "name": str(name or "").strip(),
            "lat": float(lat), "lon": float(lon), "stop_code": code,
            "parent_station": parent, "location_type": str(location_type or ""),
        }
    return result


def _local_stop_modes(connection):
    rows = connection.execute("""
        SELECT DISTINCT stop_times.stop_id, routes.route_type
        FROM stop_times JOIN trips USING (trip_id) JOIN routes USING (route_id)
        WHERE routes.route_type IN ('0', '1', '3')
    """).fetchall()
    modes = defaultdict(set)
    for stop_id, route_type in rows:
        modes[str(stop_id)].add(LOCAL_ROUTE_MODES[str(route_type)])
    return modes


def _operator_groups(connection, operator_key):
    rows = _station_rows(connection)
    modes = _local_stop_modes(connection)
    stops = []
    for stop_id in sorted(modes):
        row = rows.get(stop_id)
        if not row:
            continue
        stops.append(Stop(
            operator_key, stop_id, row["name"], normalize_name(row["name"]),
            row["lat"], row["lon"], frozenset(modes[stop_id]),
            row["stop_code"], row["parent_station"]))
    stations = {
        stop_id: row for stop_id, row in rows.items()
        if row["location_type"] == "1"
    }
    return group_operator_stops(operator_key, stops, stations), len(rows)


def _daily_for_groups(connection, groups, start, end, route_types):
    mapping = []
    for group in groups:
        for member in group.members:
            mapping.append({"stop_id": member.stop_id,
                            "provisional_id": group.provisional_id})
    map_table = pa.Table.from_pylist(mapping, schema=pa.schema([
        ("stop_id", pa.string()), ("provisional_id", pa.string())]))
    connection.register("stop_group_map", map_table)
    _create_active_services(connection, start, end)
    types = ",".join(f"'{value}'" for value in route_types)
    rows = connection.execute(f"""
        WITH group_trip AS (
            SELECT DISTINCT stop_group_map.provisional_id, stop_times.trip_id
            FROM stop_times
            JOIN stop_group_map USING (stop_id)
            JOIN trips USING (trip_id)
            JOIN routes USING (route_id)
            WHERE routes.route_type IN ({types})
              AND coalesce(nullif(trim(stop_times.pickup_type), ''), '0') = '0'
              AND try_cast(split_part(stop_times.departure_time, ':', 1) AS INTEGER) >= {ACTIVE_HOUR_START}
              AND try_cast(split_part(stop_times.departure_time, ':', 1) AS INTEGER) < {ACTIVE_HOUR_END}
        )
        SELECT group_trip.provisional_id, active_services.service_date,
               count(*)::INTEGER AS departures
        FROM group_trip JOIN trips USING (trip_id)
        JOIN active_services USING (service_id)
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    connection.unregister("stop_group_map")
    daily = defaultdict(dict)
    for group_id, service_date, departures in rows:
        daily[group_id][service_date] = int(departures)
    return daily


def _derive_uic(station_id, stop_code=None):
    for value in (stop_code, station_id):
        match = re.search(r"(?:^|\D)(\d{7,9})(?:\D|$)", str(value or ""))
        if match:
            return match.group(1)
    return None


def _process_rail(connection, metadata, start, end):
    rows = _station_rows(connection)
    rail_stop_ids = {
        str(value[0]) for value in connection.execute("""
            SELECT DISTINCT stop_times.stop_id FROM stop_times
            JOIN trips USING (trip_id) JOIN routes USING (route_id)
            WHERE routes.route_type = '2'
        """).fetchall()
    }
    by_station = defaultdict(list)
    for stop_id in sorted(rail_stop_ids):
        row = rows.get(stop_id)
        if not row:
            continue
        parent = row["parent_station"]
        station_id = parent if parent in rows else stop_id
        by_station[station_id].append(stop_id)

    groups = []
    for station_id in sorted(by_station):
        station = rows[station_id]
        member_ids = sorted(by_station[station_id])
        uic = _derive_uic(station_id, station["stop_code"])
        logical_id = f"uic:{uic}" if uic else _stable_id(
            "gtfs-rail", [f"sncb:{value}" for value in member_ids])
        members = [Stop(
            "sncb", value, rows[value]["name"], normalize_name(rows[value]["name"]),
            rows[value]["lat"], rows[value]["lon"], frozenset({"RAIL"}),
            rows[value]["stop_code"], rows[value]["parent_station"])
            for value in member_ids]
        group = LogicalGroup(
            "sncb", members, station["name"], normalize_name(station["name"]),
            "parent_station" if station_id != member_ids[0] else "station_stop",
            station_id, logical_id, logical_id)
        groups.append(group)

    daily = _daily_for_groups(connection, groups, start, end, ("2",))
    output = []
    for group in sorted(groups, key=lambda value: value.logical_stop_id):
        w, sa, su, average = representative_values(
            daily.get(group.provisional_id, {}), start, end)
        if average <= 0:
            continue
        station = rows[group.authoritative_parent_id]
        output.append({
            "logical_station_id": group.logical_stop_id,
            "authoritative_station_id": group.authoritative_parent_id,
            "uic_code": _derive_uic(group.authoritative_parent_id,
                                     station["stop_code"]),
            "station_name": group.display_name,
            "lat": station["lat"], "lon": station["lon"],
            "child_platform_count": len(group.members),
            "weekday_departures": w, "saturday_departures": sa,
            "sunday_departures": su, "seven_day_average": average,
            "window_start": start, "window_end": end,
            "active_hour_start": ACTIVE_HOUR_START,
            "active_hour_end": ACTIVE_HOUR_END,
            "feed_version": metadata["version"],
        })
    return output, len(rows)


def _provenance_metadata(sources):
    document = {"schema_version": 1, "sources": sources}
    payload = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return {PROVENANCE_METADATA_KEY.encode("utf-8"): payload}


def _write_parquet(rows, schema, path, file_metadata=None):
    table = pa.Table.from_pylist(rows, schema=schema)
    if file_metadata:
        metadata = dict(table.schema.metadata or {})
        metadata.update(file_metadata)
        table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path, compression="zstd", use_dictionary=True)
    return table.num_rows, path.stat().st_size


def build_service_caches(gtfs_paths, output_dir, reference_date=None,
                         local_start=None, local_end=None, rail_start=None,
                         rail_end=None, download_session=requests):
    """Build all three deterministic service Parquets and return a report."""
    started = time.monotonic()
    reference_date = reference_date or date.today()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report = {"feeds": {}, "source_stop_counts": {},
              "operator_logical_counts": {}}

    with tempfile.TemporaryDirectory(prefix="ev-transit-service-") as temp_name:
        temp_root = Path(temp_name)
        prepared = {}
        for key in ("delijn", "stib", "tec", "sncb"):
            configured = gtfs_paths.get(key)
            if configured:
                zip_path = Path(configured).resolve()
                if not zip_path.is_file():
                    raise FileNotFoundError(f"Missing {key} GTFS ZIP: {zip_path}")
                downloaded = False
                download_provenance = {
                    "dataset_updated_at": None,
                    "retrieved_at": None,
                    "etag": None,
                }
            else:
                zip_path = temp_root / f"{key}.zip"
                download_provenance = _download(
                    OFFICIAL_GTFS_URLS[key], zip_path, download_session)
                downloaded = True
            metadata = _feed_metadata(zip_path)
            files = _extract_required(zip_path, temp_root / key)
            provenance = {
                "operator": OPERATOR_LABELS[key],
                "source_provider": SOURCE_PROVIDER,
                "source_url": OFFICIAL_GTFS_URLS[key],
                "feed_version": metadata["version"],
                "dataset_updated_at": download_provenance["dataset_updated_at"],
                "retrieved_at": download_provenance["retrieved_at"],
                "etag": download_provenance["etag"],
                "downloaded": downloaded,
            }
            prepared[key] = {
                "zip": zip_path, "files": files, "metadata": metadata,
                "provenance": provenance,
            }
            report["feeds"][key] = {
                "version": metadata["version"],
                "validity_start": metadata["validity"][0].isoformat(),
                "validity_end": metadata["validity"][1].isoformat(),
                "zip_size": zip_path.stat().st_size,
                "downloaded": downloaded,
                "dataset_updated_at": provenance["dataset_updated_at"],
                "retrieved_at": provenance["retrieved_at"],
                "etag": provenance["etag"],
                "source_url": provenance["source_url"],
            }

        local_validities = {
            key: prepared[key]["metadata"]["validity"]
            for key in ("delijn", "stib", "tec")
        }
        local_window = select_local_window(
            local_validities, reference_date, local_start, local_end)
        rail_window = select_rail_window(
            prepared["sncb"]["metadata"]["validity"], reference_date,
            rail_start, rail_end)
        report["local_window"] = tuple(value.isoformat() for value in local_window)
        report["rail_window"] = tuple(value.isoformat() for value in rail_window)

        all_groups = []
        operator_daily = {}
        for key in ("delijn", "stib", "tec"):
            database = temp_root / f"{key}.duckdb"
            with duckdb.connect(str(database)) as connection:
                connection.execute("PRAGMA disable_progress_bar")
                _load_gtfs_tables(connection, prepared[key]["files"])
                groups, source_count = _operator_groups(connection, key)
                daily = _daily_for_groups(
                    connection, groups, *local_window,
                    tuple(LOCAL_ROUTE_MODES))
            active_groups = [
                group for group in groups
                if representative_values(
                    daily.get(group.provisional_id, {}), *local_window)[3] > 0
            ]
            report["source_stop_counts"][key] = source_count
            report["operator_logical_counts"][key] = len(active_groups)
            all_groups.extend(active_groups)
            operator_daily.update(daily)

        final_groups, _merge_count = merge_cross_operator(all_groups)
        provisional_to_final = {}
        for final in final_groups:
            for provisional in final.provisional_id.split("+"):
                provisional_to_final[provisional] = final.logical_stop_id
        final_daily = defaultdict(lambda: defaultdict(int))
        for provisional, by_date in operator_daily.items():
            if provisional not in provisional_to_final:
                continue
            final_id = provisional_to_final[provisional]
            for service_date, count in by_date.items():
                final_daily[final_id][service_date] += count

        versions = {
            key: prepared[key]["metadata"]["version"]
            for key in ("delijn", "stib", "tec")
        }
        source_by_member = {
            member.identity: group
            for group in all_groups for member in group.members
        }
        member_rows, summary_rows = [], []
        for group in final_groups:
            w, sa, su, average = representative_values(
                final_daily.get(group.logical_stop_id, {}), *local_window)
            if average <= 0:
                continue
            operators = sorted({OPERATOR_LABELS[member.operator_key]
                                for member in group.members})
            modes = sorted(group.modes)
            source_versions = sorted(
                f"{OPERATOR_LABELS[key]}={versions[key]}"
                for key in group.operators)
            summary_rows.append({
                "logical_stop_id": group.logical_stop_id,
                "display_name": group.display_name,
                "normalized_name": group.normalized_name,
                "operators": operators, "modes": modes,
                "member_count": len(group.members),
                "weekday_departures": w, "saturday_departures": sa,
                "sunday_departures": su, "seven_day_average": average,
                "window_start": local_window[0], "window_end": local_window[1],
                "active_hour_start": ACTIVE_HOUR_START,
                "active_hour_end": ACTIVE_HOUR_END,
                "source_feed_versions": source_versions,
            })
            cross = len(group.operators) > 1
            for member in group.members:
                source_group = source_by_member[member.identity]
                method = source_group.grouping_method
                if cross:
                    method += "+cross_operator_exact_name_complete_link_30m"
                member_rows.append({
                    "logical_stop_id": group.logical_stop_id,
                    "operator": OPERATOR_LABELS[member.operator_key],
                    "gtfs_stop_id": member.stop_id,
                    "stop_code": member.stop_code,
                    "parent_station": member.parent_station,
                    "authoritative_parent_id": source_group.authoritative_parent_id,
                    "name": member.name,
                    "normalized_name": member.normalized_name,
                    "lat": member.lat, "lon": member.lon,
                    "modes": sorted(member.modes), "grouping_method": method,
                    "feed_version": versions[member.operator_key],
                })

        member_rows.sort(key=lambda row: (row["logical_stop_id"], row["operator"],
                                           row["gtfs_stop_id"]))
        summary_rows.sort(key=lambda row: row["logical_stop_id"])
        stops_path = output_dir / "be_transit_service_stops.parquet"
        summary_path = output_dir / "be_transit_service_summary.parquet"
        report["local_stops"] = _write_parquet(
            member_rows, LOCAL_STOPS_SCHEMA, stops_path)
        report["local_summary"] = _write_parquet(
            summary_rows, LOCAL_SUMMARY_SCHEMA, summary_path,
            _provenance_metadata({
                key: prepared[key]["provenance"]
                for key in ("delijn", "stib", "tec")
            }))
        report["cross_operator_merge_count"] = sum(
            len(group.operators) > 1 for group in final_groups
            if representative_values(
                final_daily.get(group.logical_stop_id, {}), *local_window)[3] > 0)
        report["final_local_logical_count"] = len(summary_rows)

        database = temp_root / "sncb.duckdb"
        with duckdb.connect(str(database)) as connection:
            connection.execute("PRAGMA disable_progress_bar")
            _load_gtfs_tables(connection, prepared["sncb"]["files"])
            rail_rows, source_count = _process_rail(
                connection, prepared["sncb"]["metadata"], *rail_window)
        report["source_stop_counts"]["sncb"] = source_count
        rail_path = output_dir / "be_rail_service.parquet"
        report["rail"] = _write_parquet(
            rail_rows, RAIL_SCHEMA, rail_path,
            _provenance_metadata({"sncb": prepared["sncb"]["provenance"]}))
        report["rail_station_count"] = len(rail_rows)

    report["duration_seconds"] = round(time.monotonic() - started, 3)
    return report


def _optional_date(value):
    return _parse_gtfs_date(value) if value else None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("delijn", "stib", "tec", "sncb"):
        parser.add_argument(f"--{key}-gtfs", type=Path)
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parent / "cache")
    parser.add_argument("--reference-date", type=_parse_gtfs_date,
                        default=date.today())
    parser.add_argument("--local-start", type=_parse_gtfs_date)
    parser.add_argument("--local-end", type=_parse_gtfs_date)
    parser.add_argument("--rail-start", type=_parse_gtfs_date)
    parser.add_argument("--rail-end", type=_parse_gtfs_date)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    paths = {key: getattr(args, f"{key}_gtfs")
             for key in ("delijn", "stib", "tec", "sncb")}
    report = build_service_caches(
        paths, args.output_dir, args.reference_date,
        args.local_start, args.local_end, args.rail_start, args.rail_end)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

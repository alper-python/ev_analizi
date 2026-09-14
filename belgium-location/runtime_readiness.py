"""Cheap, centralized validation for production runtime scoring assets."""

from __future__ import annotations

import copy
from pathlib import Path
import threading

import pyarrow as pa
import pyarrow.parquet as pq


_GENERIC_COLUMNS = {
    "cat", "name", "lat", "lon", "amenity", "shop", "healthcare",
    "railway", "highway", "public_transport", "leisure", "boundary",
    "landuse", "sport", "school_level", "isced_level",
}

ASSET_SPECS = {
    "poi_nodes": {
        "component": "poi",
        "columns": _GENERIC_COLUMNS | {"id"},
        "types": {"id": "integer", "cat": "string", "lat": "floating",
                  "lon": "floating"},
    },
    "poi_polygons": {
        "component": "poi",
        "columns": _GENERIC_COLUMNS | {"uid", "brand"},
        "types": {"uid": "string", "cat": "string", "lat": "floating",
                  "lon": "floating"},
    },
    "transit_service_stops": {
        "component": "transit",
        "columns": {"logical_stop_id", "operator", "gtfs_stop_id", "lat", "lon"},
        "types": {"logical_stop_id": "string", "operator": "string",
                  "gtfs_stop_id": "string", "lat": "floating", "lon": "floating"},
    },
    "transit_service_summary": {
        "component": "transit",
        "columns": {"logical_stop_id", "display_name", "operators", "modes",
                    "weekday_departures", "saturday_departures",
                    "sunday_departures", "seven_day_average"},
        "types": {"logical_stop_id": "string", "display_name": "string",
                  "operators": "list", "modes": "list",
                  "weekday_departures": "numeric", "saturday_departures": "numeric",
                  "sunday_departures": "numeric", "seven_day_average": "numeric"},
    },
    "rail_service": {
        "component": "transit",
        "columns": {"logical_station_id", "station_name", "uic_code", "lat", "lon",
                    "weekday_departures", "saturday_departures",
                    "sunday_departures", "seven_day_average"},
        "types": {"logical_station_id": "string", "station_name": "string",
                  "uic_code": "string", "lat": "floating", "lon": "floating",
                  "weekday_departures": "numeric", "saturday_departures": "numeric",
                  "sunday_departures": "numeric", "seven_day_average": "numeric"},
    },
    "park_destinations": {
        "component": "park",
        "columns": {
            "park_id", "osm_type", "osm_id", "name", "display_name",
            "display_name_source", "park_class", "eligibility_tier",
            "counts_as_primary", "counts_as_choice", "counts_as_secondary",
            "parent_park_id", "strong_identity_type", "strong_identity_value",
            "access", "area_m2", "size_factor", "final_confidence",
            "secondary_class", "secondary_type_weight", "secondary_confidence",
            "geometry_wkb", "representative_lat", "representative_lon",
            "bbox_min_lat", "bbox_min_lon", "bbox_max_lat", "bbox_max_lon",
        },
        "types": {"park_id": "string", "geometry_wkb": "binary",
                  "counts_as_primary": "boolean", "counts_as_choice": "boolean",
                  "counts_as_secondary": "boolean", "representative_lat": "floating",
                  "representative_lon": "floating", "bbox_min_lat": "floating",
                  "bbox_min_lon": "floating", "bbox_max_lat": "floating",
                  "bbox_max_lon": "floating"},
    },
    "sport_destinations": {
        "component": "sport",
        "columns": {
            "sport_id", "canonical_osm_type", "canonical_osm_id", "name",
            "display_name", "display_name_source", "facility_class",
            "score_eligible", "eligibility_reason", "specialized", "standalone",
            "sports", "direct_sports", "component_sports", "sport_count",
            "component_count", "access_raw", "access_class", "access_evidence",
            "fee", "membership", "operator", "opening_hours", "club",
            "is_commercial", "is_public_operator", "is_school_context",
            "indoor", "covered", "building", "wikidata", "wikipedia",
            "geometry_wkb", "representative_lat", "representative_lon",
            "entrance_lat", "entrance_lon", "entrance_osm_key",
            "canonicalization_method", "bbox_min_lat", "bbox_min_lon",
            "bbox_max_lat", "bbox_max_lon",
        },
        "types": {"sport_id": "string", "geometry_wkb": "binary",
                  "score_eligible": "boolean", "sports": "list",
                  "direct_sports": "list", "component_sports": "list",
                  "representative_lat": "floating", "representative_lon": "floating",
                  "bbox_min_lat": "floating", "bbox_min_lon": "floating",
                  "bbox_max_lat": "floating", "bbox_max_lon": "floating"},
    },
}

COMPONENTS = ("poi", "transit", "park", "sport")
_cache_lock = threading.Lock()
_cached_key = None
_cached_status = None


def _type_matches(data_type, expected):
    checks = {
        "binary": lambda value: pa.types.is_binary(value) or pa.types.is_large_binary(value),
        "boolean": pa.types.is_boolean,
        "floating": pa.types.is_floating,
        "integer": pa.types.is_integer,
        "list": lambda value: pa.types.is_list(value) or pa.types.is_large_list(value),
        "numeric": lambda value: pa.types.is_integer(value) or pa.types.is_floating(value),
        "string": lambda value: pa.types.is_string(value) or pa.types.is_large_string(value),
    }
    return checks[expected](data_type)


def _validate_asset(path_value, spec):
    if not path_value:
        return {"ready": False, "status": "missing", "detail": "no path configured"}
    path = Path(path_value)
    if not path.is_file():
        return {"ready": False, "status": "missing", "detail": str(path)}
    try:
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows <= 0:
            return {"ready": False, "status": "empty", "detail": str(path)}
        schema = parquet.schema_arrow
        missing = sorted(set(spec["columns"]) - set(schema.names))
        if missing:
            return {"ready": False, "status": "schema_invalid",
                    "detail": f"{path}: missing columns {', '.join(missing)}"}
        wrong_types = []
        for column, expected in spec.get("types", {}).items():
            actual = schema.field(column).type
            if not _type_matches(actual, expected):
                wrong_types.append(f"{column}={actual} (expected {expected})")
        if wrong_types:
            return {"ready": False, "status": "schema_invalid",
                    "detail": f"{path}: incompatible types {', '.join(wrong_types)}"}
    except Exception as exc:
        return {"ready": False, "status": "unreadable",
                "detail": f"{path}: {type(exc).__name__}: {exc}"}
    return {"ready": True, "status": "ok", "detail": str(path)}


def reset_runtime_readiness_cache():
    """Forget the in-process result after runtime files or test fixtures change."""
    global _cached_key, _cached_status
    with _cache_lock:
        _cached_key = None
        _cached_status = None


def check_runtime_readiness(asset_paths, force=False):
    """Validate Parquet footers/schemas once and return structured readiness."""
    global _cached_key, _cached_status
    key = tuple((name, str(asset_paths.get(name) or "")) for name in ASSET_SPECS)
    with _cache_lock:
        if not force and key == _cached_key and _cached_status is not None:
            return copy.deepcopy(_cached_status)

    assets = {
        name: _validate_asset(asset_paths.get(name), spec)
        for name, spec in ASSET_SPECS.items()
    }
    components = {}
    for component in COMPONENTS:
        names = [name for name, spec in ASSET_SPECS.items()
                 if spec["component"] == component]
        ready = all(assets[name]["ready"] for name in names)
        components[component] = {
            "ready": ready,
            "status": "ok" if ready else "unavailable",
            "assets": {name: assets[name] for name in names},
        }
    status = {
        "ready": all(component["ready"] for component in components.values()),
        "components": components,
    }
    with _cache_lock:
        _cached_key = key
        _cached_status = copy.deepcopy(status)
    return status

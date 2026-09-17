"""Local-only Flask preview for the current analysis implementation."""

from datetime import datetime
import json
import logging
import math
import os
from ipaddress import ip_address
from pathlib import Path
import tempfile
import threading
import time
from urllib.parse import urlparse

import duckdb
from flask import Flask, jsonify, request, send_from_directory
from flask_limiter import Limiter
from flask_limiter.errors import RateLimitExceeded
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from shapely.geometry import Point

from app_duckdb import (CATS, DEFAULT_RADIUS_M, TOP_N, analyze as analyze_location,
                        analyze_health, analyze_market, analyze_park,
                        analyze_school, analyze_sport, geocode,
                        geocode_with_country, query_category,
                        query_market_candidates,
                        query_school_candidates)
from market_scoring import (MARKET_SCORING_RADIUS_M, MARKET_TYPE_WEIGHTS,
                            deduplicate_market_pois, market_type)
from school_scoring import (SCHOOL_SCORING_RADIUS_M, deduplicate_school_pois,
                            school_score_components)
from runtime_readiness import check_runtime_readiness


BASE_DIR = Path(__file__).resolve().parent
LOGGER = logging.getLogger(__name__)
DEMO_LAT, DEMO_LON = 50.876182, 4.680335
SUPPORTED_LANGS = ("tr", "en", "nl")
MAX_ADDRESS_LENGTH = 300
TRANSIT_PROVENANCE_METADATA_KEY = b"ev_analizi.transit_source_provenance_v1"
TRANSIT_SOURCE_PROVIDER = "Belgian Mobility Open Data Portal"
TRANSIT_SOURCE_PORTAL_URL = "https://data.belgianmobility.io/"
TRANSIT_SOURCE_IDENTITIES = (
    ("delijn", "De Lijn", "local"),
    ("stib", "STIB/MIVB", "local"),
    ("tec", "TEC", "local"),
    ("sncb", "SNCB/NMBS", "rail"),
)
LABELS = {
    "tr": {"school": "Okul", "market": "Market", "health": "Sağlık", "transit": "Ulaşım", "park": "Park", "sport": "Spor"},
    "en": {"school": "School", "market": "Groceries", "health": "Health", "transit": "Transit", "park": "Park", "sport": "Sports"},
    "nl": {"school": "School", "market": "Supermarkt", "health": "Gezondheid", "transit": "Openbaar vervoer", "park": "Park", "sport": "Sport"},
}
DEMO_NOTICES = {
    "tr": "DEMO VERİSİ — sonuçlar yalnızca arayüz testi için sentetiktir.",
    "en": "DEMO DATA — results are synthetic and only for interface testing.",
    "nl": "DEMOGEGEVENS — resultaten zijn synthetisch en alleen voor interfacetests.",
}
AUTOCOMPLETE_UNAVAILABLE = {
    "tr": "Adres önerileri kullanılamıyor. Adresi yazıp analiz edebilir veya koordinat girebilirsiniz.",
    "en": "Address suggestions are unavailable. You can still type an address or enter coordinates.",
    "nl": "Adressuggesties zijn niet beschikbaar. U kunt nog steeds een adres typen of coördinaten invoeren.",
}


class AddressSuggestionProvider:
    """Small provider boundary so autocomplete can be replaced or tested independently."""

    def suggest(self, query, lang, limit=6):
        raise NotImplementedError


class GeoapifyAddressSuggestionProvider(AddressSuggestionProvider):
    ENDPOINT = "https://api.geoapify.com/v1/geocode/autocomplete"

    def __init__(self, api_key, http_get=requests.get):
        self.api_key = api_key
        self.http_get = http_get

    def suggest(self, query, lang, limit=6):
        response = self.http_get(
            self.ENDPOINT,
            params={
                "text": query,
                "apiKey": self.api_key,
                "filter": "countrycode:be,nl",
                "lang": lang,
                "limit": min(int(limit), 6),
                "format": "json",
            },
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Geoapify response must be a JSON object")
        items = payload.get("results")
        if items is None:
            features = payload.get("features", [])
            if not isinstance(features, list):
                raise ValueError("Geoapify features must be a list")
            items = [feature.get("properties") for feature in features
                     if isinstance(feature, dict)]
        if not isinstance(items, list):
            raise ValueError("Geoapify results must be a list")

        suggestions = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                label = str(item.get("formatted") or item.get("address_line1") or "").strip()
                lat, lon = float(item["lat"]), float(item["lon"])
            except (KeyError, TypeError, ValueError):
                continue
            if not label:
                continue
            country_code = str(item.get("country_code") or "").strip().lower()
            if country_code not in COUNTRY_DATASETS:
                continue

            suggestions.append({
                "label": label,
                "lat": lat,
                "lon": lon,
                "country_code": country_code,
                "result_type": str(item.get("result_type") or "unknown"),
            })
            if len(suggestions) >= limit:
                break
        return suggestions


# Test/extension seam. Normal requests construct the provider from the current
# environment so a key configured after process startup is not frozen at import.
ADDRESS_PROVIDER = None


def _current_address_provider():
    """Resolve a late-configured key without exposing it or freezing startup state."""
    if ADDRESS_PROVIDER is not None:
        return ADDRESS_PROVIDER
    api_key = os.environ.get("GEOAPIFY_API_KEY", "").strip()
    return GeoapifyAddressSuggestionProvider(api_key) if api_key else None


def _json_error(code, message, status):
    """Return the stable JSON error envelope used by the preview APIs."""
    return jsonify({"error": {"code": code, "message": message}}), status


def _request_number(value):
    """Convert an API number while explicitly excluding JSON booleans."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("number must be finite")
    return number


def _request_integer(value):
    """Convert an integer parameter without accepting booleans or fractions."""
    number = _request_number(value)
    if not number.is_integer():
        raise ValueError("integer required")
    return int(number)


def _client_rate_limit_key():
    """Use Render/Cloudflare's validated client address without trusting XFF."""
    forwarded = request.headers.get("CF-Connecting-IP", "").strip()
    candidates = (forwarded, request.remote_addr)
    for candidate in candidates:
        try:
            return str(ip_address(candidate))
        except (TypeError, ValueError):
            continue
    return "unknown-client"


def _request_is_https():
    """Recognize direct TLS or Render's single forwarded scheme value."""
    return (request.is_secure
            or request.headers.get("X-Forwarded-Proto", "").strip().casefold() == "https")


def _existing_cache(env_name, filename):
    configured = os.environ.get(env_name)
    if configured:
        return str(Path(configured).expanduser().resolve())
    candidates = [
        BASE_DIR / "cache" / filename,
        BASE_DIR.parent / "cache" / filename,
    ]
    return next((str(path.resolve()) for path in candidates if path.is_file()),
                str(candidates[0].resolve()))


def _offset(distance_m, bearing_degrees):
    angle = math.radians(bearing_degrees)
    north = distance_m * math.cos(angle)
    east = distance_m * math.sin(angle)
    lat = DEMO_LAT + math.degrees(north / 6371000.0)
    lon = DEMO_LON + math.degrees(east / (6371000.0 * math.cos(math.radians(DEMO_LAT))))
    return lat, lon


NODE_SCHEMA = pa.schema([
    ("id", pa.int64()), ("cat", pa.string()), ("name", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()), ("landuse", pa.string()),
    ("sport", pa.string()), ("school_level", pa.string()), ("isced_level", pa.string()),
])
POLY_SCHEMA = pa.schema([
    ("uid", pa.string()), ("cat", pa.string()), ("name", pa.string()), ("brand", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()), ("landuse", pa.string()),
    ("sport", pa.string()), ("school_level", pa.string()), ("isced_level", pa.string()),
])


def _node(identifier, category, name, distance, bearing, **tags):
    lat, lon = _offset(distance, bearing)
    row = {name: None for name in NODE_SCHEMA.names}
    row.update({"id": identifier, "cat": category, "name": name, "lat": lat, "lon": lon})
    row.update(tags)
    return row


def _poly(identifier, category, name, distance, bearing, brand=None, **tags):
    lat, lon = _offset(distance, bearing)
    row = {name: None for name in POLY_SCHEMA.names}
    row.update({"uid": identifier, "cat": category, "name": name, "brand": brand,
                "lat": lat, "lon": lon})
    row.update(tags)
    return row


def _create_demo_caches():
    temp_dir = tempfile.TemporaryDirectory(prefix="ev-analizi-preview-")
    target = Path(temp_dir.name)
    nodes_path, polys_path = target / "nodes.parquet", target / "polys.parquet"
    parks_path = target / "be_park_destinations.parquet"
    sports_path = target / "be_sport_destinations.parquet"
    nodes = [
        _node(1, "market", "Delhaize Demo", 300, 20, shop="supermarket"),
        _node(2, "market", "Carrefour Express Demo", 100, 120, shop="convenience"),
        _node(3, "market", "Colruyt Demo", 520, 220, shop="supermarket"),
        _node(4, "market", "Night Shop Demo", 1800, 300, shop="convenience"),
        _node(5, "market", "Demo Marketplace", 80, 45, amenity="marketplace"),
        _node(6, "school", "Heverlee Demo School", 420, 55, amenity="school"),
        _node(7, "school", "Demo Kindergarten", 900, 160, amenity="kindergarten"),
        _node(8, "health", "Demo Pharmacy", 480, 260, amenity="pharmacy"),
        _node(9, "transit", "Demo Bus Stop", 180, 350, highway="bus_stop"),
        _node(10, "transit", "Demo Station", 760, 80, railway="station"),
        _node(11, "sport", "Demo Fitness", 700, 190, leisure="fitness_centre"),
    ]
    polygons = [
        _poly("p1", "market", "colruyt demo", 545, 220, brand="Colruyt",
              shop="supermarket"),
        _poly("p2", "health", "Demo Medical Centre", 1100, 135,
              amenity="hospital"),
        _poly("p3", "park", "Demo Neighbourhood Park", 600, 10,
              leisure="park"),
        _poly("p4", "sport", "Demo Sports Centre", 1300, 240,
              leisure="sports_centre"),
    ]
    pq.write_table(pa.Table.from_pylist(nodes, schema=NODE_SCHEMA), nodes_path)
    pq.write_table(pa.Table.from_pylist(polygons, schema=POLY_SCHEMA), polys_path)
    park_lat, park_lon = _offset(600, 10)
    park_geometry = Point(park_lon, park_lat).buffer(0.0002)
    min_lon, min_lat, max_lon, max_lat = park_geometry.bounds
    pq.write_table(pa.Table.from_pylist([{
        "park_id": "park:demo:1", "osm_type": "way", "osm_id": 1,
        "name": "Demo Neighbourhood Park",
        "display_name": "Demo Neighbourhood Park",
        "display_name_source": "canonical", "park_class": "park",
        "eligibility_tier": "primary", "counts_as_primary": True,
        "counts_as_choice": True, "counts_as_secondary": False,
        "parent_park_id": None, "strong_identity_type": "osm",
        "strong_identity_value": "way:1", "access": "yes",
        "area_m2": 15000.0, "size_factor": 0.9,
        "final_confidence": 1.0, "secondary_class": None,
        "secondary_type_weight": None, "secondary_confidence": None,
        "geometry_wkb": park_geometry.wkb,
        "representative_lat": park_lat, "representative_lon": park_lon,
        "bbox_min_lat": min_lat, "bbox_min_lon": min_lon,
        "bbox_max_lat": max_lat, "bbox_max_lon": max_lon,
    }]), parks_path)
    sport_rows = []
    for sport_id, name, distance, bearing, facility_class in (
            ("sport:demo:1", "Demo Fitness", 700, 190, "fitness_gym"),
            ("sport:demo:2", "Demo Sports Centre", 1300, 240,
             "general_sports_centre")):
        sport_lat, sport_lon = _offset(distance, bearing)
        geometry = Point(sport_lon, sport_lat)
        min_lon, min_lat, max_lon, max_lat = geometry.bounds
        sport_rows.append({
            "sport_id": sport_id, "canonical_osm_type": "node",
            "canonical_osm_id": int(sport_id.rsplit(":", 1)[1]),
            "name": name, "display_name": name,
            "display_name_source": "canonical_name",
            "facility_class": facility_class, "score_eligible": True,
            "eligibility_reason": "eligible", "specialized": False,
            "standalone": False, "sports": [], "direct_sports": [],
            "component_sports": [], "sport_count": 0,
            "component_count": 0, "access_raw": None,
            "access_class": "missing", "access_evidence": "demo",
            "fee": None, "membership": None, "operator": None,
            "opening_hours": None, "club": None,
            "is_commercial": facility_class == "fitness_gym",
            "is_public_operator": False, "is_school_context": False,
            "indoor": None, "covered": None, "building": None,
            "wikidata": None, "wikipedia": None,
            "geometry_wkb": geometry.wkb,
            "representative_lat": sport_lat,
            "representative_lon": sport_lon,
            "entrance_lat": None, "entrance_lon": None,
            "entrance_osm_key": None,
            "bbox_min_lat": min_lat, "bbox_min_lon": min_lon,
            "bbox_max_lat": max_lat, "bbox_max_lon": max_lon,
            "canonicalization_method": "canonical_source",
        })
    pq.write_table(pa.Table.from_pylist(sport_rows), sports_path)
    return temp_dir, str(nodes_path), str(polys_path)


NODES_PATH = _existing_cache("POI_NODES", "be_poi.parquet")
POLYS_PATH = _existing_cache("POI_POLYS", "be_poi_poly.parquet")
PARK_PATH = _existing_cache("PARK_DESTINATIONS", "be_park_destinations.parquet")
SPORT_PATH = _existing_cache("SPORT_DESTINATIONS", "be_sport_destinations.parquet")
TRANSIT_STOPS_PATH = _existing_cache(
    "TRANSIT_SERVICE_STOPS", "be_transit_service_stops.parquet")
TRANSIT_SUMMARY_PATH = _existing_cache(
    "TRANSIT_SERVICE_SUMMARY", "be_transit_service_summary.parquet")
RAIL_SERVICE_PATH = _existing_cache("RAIL_SERVICE", "be_rail_service.parquet")
COUNTRY_DATASETS = {
    "be": {
        "poi_nodes": NODES_PATH,
        "poi_polygons": POLYS_PATH,
        "park_destinations": PARK_PATH,
        "sport_destinations": SPORT_PATH,
        "transit_service_stops": TRANSIT_STOPS_PATH,
        "transit_service_summary": TRANSIT_SUMMARY_PATH,
        "rail_service": RAIL_SERVICE_PATH,
    },
    "nl": {
        "poi_nodes": str(BASE_DIR / "cache" / "nl_poi.parquet"),
        "poi_polygons": str(BASE_DIR / "cache" / "nl_poi_poly.parquet"),
        "park_destinations": str(BASE_DIR / "cache" / "nl_park_destinations.parquet"),
        "sport_destinations": str(BASE_DIR / "cache" / "nl_sport_destinations.parquet"),
        "transit_service_stops": str(BASE_DIR / "cache" / "nl_transit_service_stops.parquet"),
        "transit_service_summary": str(BASE_DIR / "cache" / "nl_transit_service_summary.parquet"),
        "rail_service": str(BASE_DIR / "cache" / "nl_rail_service.parquet"),
    },
}
# Demo caches remain available as explicit test fixtures, but production runtime
# selection never falls back to them when required Belgium data is unavailable.
DATA_MODE = "real"


app = Flask(__name__, static_folder="static", static_url_path="")
# One process and one instance use cheap in-memory limits for V1. A future
# multi-worker or multi-instance deployment must use shared storage such as Redis.
limiter = Limiter(
    key_func=_client_rate_limit_key,
    app=app,
    default_limits=[],
    storage_uri="memory://",
    headers_enabled=True,
)
_result_cache = {}
_cache_lock = threading.Lock()


@app.after_request
def _add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = (
        "geolocation=(), camera=(), microphone=()")
    if _request_is_https():
        response.headers["Strict-Transport-Security"] = "max-age=15552000"
    return response


@app.errorhandler(RateLimitExceeded)
def _rate_limit_error(_error):
    return _json_error(
        "rate_limited", "Too many requests. Please try again shortly.", 429)


def _runtime_asset_paths(country_code="be"):
    return COUNTRY_DATASETS[country_code]


def _runtime_readiness(force=False, country_code="be"):
    """Single server boundary for cached runtime-data readiness."""
    return check_runtime_readiness(_runtime_asset_paths(country_code), force=force)


def _optional_public_string(value, maximum=300):
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text if text and len(text) <= maximum else None


def _public_source_url(value):
    text = _optional_public_string(value, maximum=1000)
    if text is None:
        return None
    parsed = urlparse(text)
    return text if parsed.scheme in {"http", "https"} and parsed.netloc else None


def _public_dataset_timestamp(value):
    text = _optional_public_string(value, maximum=100)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return text if parsed.tzinfo is not None else None


def _read_transit_provenance(path, source_group):
    """Read optional builder metadata without affecting runtime readiness."""
    try:
        metadata = pq.ParquetFile(path).schema_arrow.metadata or {}
        raw = metadata.get(TRANSIT_PROVENANCE_METADATA_KEY)
        if raw is None:
            return {}
        document = json.loads(raw.decode("utf-8"))
        if (not isinstance(document, dict)
                or document.get("schema_version") != 1
                or not isinstance(document.get("sources"), dict)):
            return {}
        return document["sources"]
    except Exception as exc:
        LOGGER.warning(
            "Transit source provenance unavailable: group=%s error=%s",
            source_group, type(exc).__name__)
        return {}


def _public_transit_sources():
    source_documents = {
        "local": _read_transit_provenance(TRANSIT_SUMMARY_PATH, "local"),
        "rail": _read_transit_provenance(RAIL_SERVICE_PATH, "rail"),
    }
    result = []
    for key, fallback_operator, source_group in TRANSIT_SOURCE_IDENTITIES:
        raw = source_documents[source_group].get(key, {})
        if not isinstance(raw, dict):
            raw = {}
        result.append({
            "key": key,
            "operator": (_optional_public_string(raw.get("operator"), 100)
                         or fallback_operator),
            "source_provider": (
                _optional_public_string(raw.get("source_provider"), 200)
                or TRANSIT_SOURCE_PROVIDER),
            "source_url": (_public_source_url(raw.get("source_url"))
                           or TRANSIT_SOURCE_PORTAL_URL),
            "feed_version": _optional_public_string(
                raw.get("feed_version"), 200),
            "dataset_updated_at": _public_dataset_timestamp(
                raw.get("dataset_updated_at")),
        })
    return result


def _log_unready_runtime(readiness):
    for component, component_status in readiness["components"].items():
        for asset, asset_status in component_status["assets"].items():
            if not asset_status["ready"]:
                LOGGER.error(
                    "Runtime data unavailable: component=%s asset=%s status=%s detail=%s",
                    component, asset, asset_status["status"], asset_status["detail"])


def _value(value):
    return None if pd.isnull(value) else value


def _valid_map_anchor(row):
    """Return a safe optional presentation coordinate pair."""
    try:
        lat = float(_value(row.get("map_lat")))
        lon = float(_value(row.get("map_lon")))
    except (TypeError, ValueError):
        return None
    if (not math.isfinite(lat) or not math.isfinite(lon)
            or not -90.0 <= lat <= 90.0
            or not -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def _transit_display_type(row):
    """Classify display-only Transit infrastructure from preserved OSM tags."""
    def tag(column):
        value = _value(row.get(column))
        return str(value).strip().casefold() if value is not None else ""

    amenity = tag("amenity")
    railway = tag("railway")
    highway = tag("highway")
    public_transport = tag("public_transport")

    if amenity == "bus_station":
        return "bus_station"
    if railway == "station":
        return "rail_station"
    if railway == "halt":
        return "rail_halt"
    if railway == "tram_stop":
        return "tram_stop"
    if highway == "bus_stop":
        return "bus_stop"
    if public_transport == "platform" or railway == "platform" or highway == "platform":
        return "transit_platform"
    if public_transport == "stop_position":
        return "transit_stop"
    return "transit_point"


def _market_score_breakdown(con, lat, lon, score, dataset=None):
    """Describe the existing Market score without changing its calculation."""
    dataset = dataset if dataset is not None else _runtime_asset_paths()
    scoring_rows = deduplicate_market_pois(query_market_candidates(
        con, dataset["poi_nodes"], dataset["poi_polygons"], lat, lon,
        MARKET_SCORING_RADIUS_M))
    effective_count = sum(
        MARKET_TYPE_WEIGHTS.get(market_type(row), 0.0) for row in scoring_rows)
    choice_points = min(effective_count, 3.0)
    proximity_points = max(0.0, min(7.0, float(score) - choice_points))
    return {
        "proximity_points": proximity_points,
        "proximity_max": 7.0,
        "choice_points": choice_points,
        "choice_max": 3.0,
        "effective_count": effective_count,
        "effective_count_saturation": 3.0,
        "final_score": float(score),
        "final_max": 10.0,
    }


def _school_score_breakdown(con, lat, lon, dataset=None):
    """Expose centrally calculated School Score V1 components to the UI."""
    dataset = dataset if dataset is not None else _runtime_asset_paths()
    scoring_rows = deduplicate_school_pois(query_school_candidates(
        con, dataset["poi_nodes"], dataset["poi_polygons"], lat, lon,
        SCHOOL_SCORING_RADIUS_M))
    components = school_score_components(scoring_rows)
    return {
        "proximity_points": components["proximity_points"],
        "choice_points": components["choice_points"],
        "nearest_core_school_m": components["nearest_school_distance"],
    }


def _category_payload(con, category, lat, lon, radius, topn, score,
                      dedicated_breakdown=None, dataset=None):
    dataset = dataset if dataset is not None else _runtime_asset_paths()
    nodes_path, polys_path = dataset["poi_nodes"], dataset["poi_polygons"]
    if category == "market":
        frame, _market_score, count, nearest = analyze_market(
            con, nodes_path, polys_path, lat, lon, radius, topn)
        score_breakdown = _market_score_breakdown(con, lat, lon, score, dataset)
    elif category == "school":
        frame, _school_score, count, nearest = analyze_school(
            con, nodes_path, polys_path, lat, lon, radius, topn)
        score_breakdown = _school_score_breakdown(con, lat, lon, dataset)
    elif category == "health":
        frame, _health_score, count, nearest, components = analyze_health(
            con, nodes_path, polys_path, lat, lon, radius, topn)
        score_breakdown = {
            "clinical_proximity_points": components["clinical_proximity_points"],
            "choice_points": components["choice_points"],
            "hospital_points": components["hospital_points"],
            "nearest_clinical_m": components["nearest_clinical_m"],
            "nearest_hospital_m": components["nearest_hospital_m"],
        }
    elif category == "park":
        frame, _park_score, count, nearest, _components = analyze_park(
            con, dataset["park_destinations"], lat, lon, radius, topn)
        score_breakdown = dedicated_breakdown
    elif category == "sport":
        frame, _sport_score, count, nearest, _components = analyze_sport(
            con, dataset["sport_destinations"], lat, lon, radius, topn)
        score_breakdown = dedicated_breakdown
    else:
        frame = query_category(con, nodes_path, polys_path, category, lat, lon, radius, topn)
        count = int(frame.iloc[0]["n_total"]) if not frame.empty else 0
        nearest = float(frame.iloc[0]["d_min"]) if not frame.empty else None
        score_breakdown = dedicated_breakdown if category == "transit" else None
    items = []
    for _, row in frame.iterrows():
        poi_type = next((_value(row.get(column)) for column in
                         ("shop", "amenity", "healthcare", "railway", "highway",
                          "public_transport", "leisure", "sport")
                         if _value(row.get(column))), None)
        if category == "park":
            poi_type = _value(row.get("park_class"))
        elif category == "sport":
            poi_type = _value(row.get("facility_class"))
        item_name = (_value(row.get("display_name"))
                     if category in {"park", "sport"}
                     else None)
        item = {
            "name": item_name or _value(row["name"]) or _value(row["brand"]) or "Unnamed",
            "brand": _value(row["brand"]),
            "type": poi_type,
            "lat": float(row["lat"]), "lon": float(row["lon"]),
            "straight_m": int(round(row["d_lin"])),
            "walk_m": int(round(row["walk_m"])), "walk_min": int(round(row["walk_s"] / 60)),
            "drive_m": int(round(row["drive_m"])), "drive_min": int(round(row["drive_s"] / 60)),
        }
        if category in {"park", "sport"}:
            map_anchor = _valid_map_anchor(row)
            if map_anchor is not None:
                item["map_lat"], item["map_lon"] = map_anchor
        if category == "transit":
            item["display_type"] = _transit_display_type(row)
        elif category == "park":
            item.update({
                "park_id": _value(row.get("park_id")),
                "display_name": _value(row.get("display_name")),
                "park_class": _value(row.get("park_class")),
                "eligibility_tier": _value(row.get("eligibility_tier")),
                "area_m2": (_value(row.get("area_m2"))),
                "parent_park_id": _value(row.get("parent_park_id")),
                "counts_as_primary": bool(row.get("counts_as_primary")),
                "counts_as_secondary": bool(row.get("counts_as_secondary")),
            })
        elif category == "sport":
            item.update({
                "sport_id": _value(row.get("sport_id")),
                "display_name": _value(row.get("display_name")),
                "facility_class": _value(row.get("facility_class")),
                "distance_m": float(row["d_lin"]),
                "distance_method": _value(row.get("distance_method")),
                "score_eligible": bool(row.get("score_eligible")),
                "sports": list(row.get("sports")),
                "sport_count": int(row.get("sport_count") or 0),
                "access_class": _value(row.get("access_class")),
            })
        items.append(item)
    return {"key": category, "score": score, "count": count,
            "nearest_m": int(round(nearest)) if nearest is not None else None,
            "has_hospital": bool(frame.iloc[0]["has_hospital_any"]) if category == "health" and not frame.empty else None,
            "score_breakdown": score_breakdown,
            "items": items}


def _run_preview_analysis(lat, lon, display_address, radius, topn, lang,
                          country_code="be"):
    dataset = COUNTRY_DATASETS[country_code]
    key = (country_code, round(lat, 6), round(lon, 6), display_address, radius, topn, lang, DATA_MODE)
    with _cache_lock:
        cached = _result_cache.get(key)
    if cached:
        return cached

    result = analyze_location(lat=lat, lon=lon, radius=radius, topn=topn,
                              nodes_path=dataset["poi_nodes"],
                              polys_path=dataset["poi_polygons"],
                              transit_stops_path=dataset["transit_service_stops"],
                              transit_summary_path=dataset["transit_service_summary"],
                              rail_service_path=dataset["rail_service"],
                              park_cache_path=dataset["park_destinations"],
                              sport_cache_path=dataset["sport_destinations"])
    with duckdb.connect() as con:
        categories = [
            _category_payload(con, category, lat, lon, radius, topn,
                              result["scores"][CATS[category]["label"]],
                              result.get("breakdowns", {}).get(category)
                              if category in {"transit", "park", "sport"}
                              else None, dataset=dataset)
            for category in CATS
        ]
    payload = {"display_address": display_address, "lat": lat, "lon": lon,
               "country_code": country_code,
               "radius": radius, "lang": lang, "overall": result["overall"],
               "categories": categories, "data_mode": DATA_MODE,
               "data_notice": DEMO_NOTICES[lang] if DATA_MODE == "demo" else None}
    with _cache_lock:
        _result_cache[key] = payload
    return payload


@app.get("/api/health")
def health():
    readiness = _runtime_readiness()
    public_components = {
        component: status["status"]
        for component, status in readiness["components"].items()
    }
    if readiness["ready"]:
        return jsonify({"status": "ok", "ready": True,
                        "components": public_components})
    _log_unready_runtime(readiness)
    return jsonify({"status": "unavailable", "ready": False,
                    "components": public_components}), 503


@app.get("/api/data-sources")
def data_sources_api():
    return jsonify({"transit": _public_transit_sources()})


@app.get("/api/address-suggestions")
@limiter.limit("60 per minute")
def address_suggestions_api():
    try:
        lang = str(request.args.get("lang", "en")).lower()
        if lang not in SUPPORTED_LANGS:
            return _json_error("invalid_request", "Unsupported language.", 400)
        query = str(request.args.get("q", "")).strip()
        provider = _current_address_provider()
        if len(query) < 3:
            return jsonify({"available": provider is not None, "suggestions": []})
        if provider is None:
            return jsonify({"available": False, "suggestions": [],
                            "message": AUTOCOMPLETE_UNAVAILABLE[lang]})
        try:
            suggestions = provider.suggest(query, lang, limit=6)
        except (requests.RequestException, ValueError) as exc:
            LOGGER.warning("Address autocomplete upstream failure (%s)",
                           type(exc).__name__)
            return jsonify({"available": False, "suggestions": [],
                            "message": AUTOCOMPLETE_UNAVAILABLE[lang]})
        return jsonify({"available": True, "suggestions": suggestions})
    except Exception:
        LOGGER.exception("Unexpected address autocomplete failure")
        return _json_error(
            "internal_error", "Address suggestions are temporarily unavailable.", 500)


@app.post("/api/analyze")
@limiter.limit("30 per minute")
def analyze_api():
    try:
        return _analyze_api_response()
    except FileNotFoundError:
        LOGGER.exception("Required analysis data is unavailable")
        return _json_error(
            "service_unavailable",
            "Required analysis data is temporarily unavailable.", 503)
    except Exception:
        LOGGER.exception("Unexpected analysis failure")
        return _json_error(
            "internal_error", "The analysis could not be completed.", 500)


def _analyze_api_response():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _json_error("invalid_request", "A JSON object is required.", 400)

    country_code_explicit = "country_code" in body and body["country_code"] is not None
    country_code = body.get("country_code", "be")
    if not isinstance(country_code, str):
        return _json_error("invalid_request", "Unsupported country.", 400)
    country_code = country_code.strip().lower()
    if country_code not in COUNTRY_DATASETS:
        return _json_error("invalid_request", "Unsupported country.", 400)

    lang_value = body.get("lang", "en")
    if not isinstance(lang_value, str):
        return _json_error("invalid_request", "Unsupported language.", 400)
    lang = lang_value.lower()
    if lang not in SUPPORTED_LANGS:
        return _json_error("invalid_request", "Unsupported language.", 400)
    try:
        radius = _request_integer(body.get("radius", DEFAULT_RADIUS_M))
        topn = _request_integer(body.get("topn", TOP_N))
    except (TypeError, ValueError):
        return _json_error("invalid_request", "Invalid radius or topn.", 400)
    if radius not in (1000, 2500, 5000) or not (1 <= topn <= 20):
        return _json_error(
            "invalid_request", "Preview radius/topn is out of range.", 400)

    has_address = "address" in body and body["address"] is not None
    if has_address and not isinstance(body["address"], str):
        return _json_error("invalid_request", "Address must be a string.", 400)
    address = body.get("address", "").strip() if has_address else ""
    if has_address and not address:
        return _json_error("invalid_request", "Address must not be blank.", 400)
    if len(address) > MAX_ADDRESS_LENGTH:
        return _json_error(
            "invalid_request",
            f"Address must be at most {MAX_ADDRESS_LENGTH} characters.", 400)

    has_lat = "lat" in body and body["lat"] is not None
    has_lon = "lon" in body and body["lon"] is not None
    if has_lat != has_lon:
        return _json_error(
            "invalid_request", "Latitude and longitude must be provided together.", 400)

    if has_lat:
        try:
            lat, lon = _request_number(body["lat"]), _request_number(body["lon"])
        except (TypeError, ValueError):
            return _json_error("invalid_request", "Invalid coordinates.", 400)
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            return _json_error("invalid_request", "Coordinates are out of range.", 400)
        display_address = address or f"({lat:.6f}, {lon:.6f})"
    elif not address:
        return _json_error(
            "invalid_request", "Provide an address or coordinates.", 400)

    if not has_lat:
        if DATA_MODE == "demo":
            lat, lon, display_address = DEMO_LAT, DEMO_LON, f"{address} — DEMO"
        else:
            try:
                lat, lon, display_address, geocoded_country = geocode_with_country(address)
            except RuntimeError:
                return _json_error(
                    "address_not_found", "Address could not be geocoded.", 400)

            if not country_code_explicit:
                if geocoded_country not in COUNTRY_DATASETS:
                    return _json_error(
                        "invalid_request", "Unsupported country.", 400)
                country_code = geocoded_country

    readiness = _runtime_readiness(country_code=country_code)
    if not readiness["ready"]:
        _log_unready_runtime(readiness)
        return _json_error(
            "service_unavailable",
            "Required analysis data is temporarily unavailable.", 503)

    return jsonify(_run_preview_analysis(
        lat, lon, display_address, radius, topn, lang, country_code=country_code))


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    startup_readiness = _runtime_readiness(force=True)
    print(f"[preview] runtime ready: {startup_readiness['ready']}")
    if not startup_readiness["ready"]:
        _log_unready_runtime(startup_readiness)
    app.run(host="127.0.0.1", port=5000, debug=False)

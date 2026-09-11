# app_duckdb.py — node cache (varsa) + polygon cache (varsa) ile hızlı POI analizi + PUANLAMA
import os, math, argparse
import duckdb, pandas as pd, folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from health_scoring import (HEALTH_HOSPITAL_RADIUS_M, HEALTH_LOCAL_RADIUS_M,
                            deduplicate_health_local_pois,
                            health_score_components)
from market_scoring import (MARKET_SCORING_RADIUS_M, calc_market_score,
                            deduplicate_market_pois, market_type)
from school_scoring import (SCHOOL_SCORING_RADIUS_M, calc_school_score,
                            deduplicate_school_pois)
from transit_scoring import (LOCAL_SCORING_RADIUS_M, RADIUS_EPSILON_M,
                             RAIL_SCORING_RADIUS_M, transit_score_components)

# ========== KULLANICI AYARLANABİLİR PARAMETRELER ==========

TOP_N = 5
DEFAULT_RADIUS_M = 2500

# Yürüyüş/araç yaklaşımı (yaklaşık, yol dolaşıklığı dahil)
WALK_SPEED_KPH, DRIVE_SPEED_KPH = 4.8, 35.0
WALK_CIRCUITY, DRIVE_CIRCUITY = 1.25, 1.40

# Kategori isimleri, etiketleri ve renkleri
CATS = {
    "school": {"label": "Okul",   "color": "blue"},
    "market": {"label": "Market", "color": "orange"},
    "health": {"label": "Sağlık", "color": "red"},
    "transit":{"label": "Ulaşım", "color": "purple"},
    "park":   {"label": "Park",   "color": "green"},
    "sport":  {"label": "Spor",   "color": "cadetblue"},
}

# Genel puan ağırlıkları (toplamı = 1.0)
OVERALL_WEIGHTS = {
    "market": 0.25,
    "school": 0.25,
    "health": 0.20,
    "transit":0.15,
    "park":   0.10,
    "sport":  0.05,
}

# Kategori içi puanlama konfigürasyonu:
# D0: yakınlık doygunluk mesafesi (m)
# w_prox + w_count = 10 (puanların dağılımı)
# Nsat: sayıda doygunluğa ulaşılacak değer
SCORING = {
    "park":   {"D0": 1200, "w_prox": 6.0, "w_count": 4.0, "Nsat": 3},
    "sport":  {"D0": 1500, "w_prox": 5.0, "w_count": 5.0, "Nsat": 3},
}

# ===========================================================

def fmt_meters(m): 
    return "" if m is None else (f"{m/1000:.2f} km" if m>=1000 else f"{int(round(m))} m")

def fmt_seconds(s):
    if s is None: return ""
    m = int(round(s/60))
    return f"{m} dk" if m<60 else (f"{m//60} sa {m%60} dk" if m%60 else f"{m//60} sa")

def meters_to_deg_latlon(lat, r_m):
    dlat = r_m/111320.0
    dlon = r_m/(111320.0*max(0.1, math.cos(math.radians(lat))))
    return dlat, dlon

def geocode(address):
    geocoder = Nominatim(user_agent="be-poi-cache/1.3")
    rl = RateLimiter(geocoder.geocode, min_delay_seconds=1.0)
    loc = rl(address)
    if not loc: raise RuntimeError("Adres geocode edilemedi.")
    return loc.latitude, loc.longitude, loc.address

# Alt-skor (tür ağırlıkları) — sıralama için; puanlamadan bağımsız
SCORES = {
    "transit": """
        (CASE WHEN railway='station' THEN 100 ELSE 0 END) +
        (CASE WHEN railway='halt' THEN 90 ELSE 0 END) +
        (CASE WHEN amenity='bus_station' THEN 80 ELSE 0 END) +
        (CASE WHEN railway='tram_stop' OR railway='subway_entrance' THEN 70 ELSE 0 END) +
        (CASE WHEN highway='bus_stop' THEN 50 ELSE 0 END) +
        (CASE WHEN public_transport IS NOT NULL THEN 40 ELSE 0 END)
    """,
    "park": """
        (CASE WHEN leisure='park' THEN 100 ELSE 0 END) +
        (CASE WHEN leisure='garden' THEN 80 ELSE 0 END) +
        (CASE WHEN leisure='nature_reserve' OR boundary='national_park' THEN 80 ELSE 0 END) +
        (CASE WHEN leisure='recreation_ground' THEN 60 ELSE 0 END) +
        (CASE WHEN leisure='playground' THEN 50 ELSE 0 END) +
        (CASE WHEN landuse='grass' THEN 20 ELSE 0 END) +
        (CASE WHEN boundary='national_park' THEN 15 ELSE 0 END)
    """,
    "sport": """
        (CASE WHEN leisure='fitness_centre' OR amenity='gym' THEN 90 ELSE 0 END) +
        (CASE WHEN leisure='sports_centre' THEN 80 ELSE 0 END) +
        (CASE WHEN sport IS NOT NULL THEN 20 ELSE 0 END)
    """,
}

def query_market_candidates(con, nodes_path, polys_path, lat, lon, radius_m):
    """Return supermarket/convenience rows for Market's isolated pipeline."""
    # This bounding box must contain the full Haversine circle. The shared
    # 111320 m/degree approximation is slightly too narrow north/south and can
    # discard valid Market POIs just inside the requested radius.
    dlat = math.degrees(radius_m / 6371000.0)
    dlon = dlat / max(0.1, math.cos(math.radians(lat)))
    lat_min, lat_max = lat - dlat, lat + dlat
    lon_min, lon_max = lon - dlon, lon + dlon

    parts = []
    if nodes_path:
        parts.append(
            f"SELECT name, NULL AS brand, lat, lon, amenity, shop, healthcare, 'node' AS source "
            f"FROM read_parquet('{nodes_path}') WHERE cat='market' "
            f"AND shop IN ('supermarket', 'convenience') "
            f"AND amenity IS DISTINCT FROM 'marketplace'"
        )
    if polys_path:
        parts.append(
            f"SELECT name, brand, lat, lon, amenity, shop, healthcare, 'polygon' AS source "
            f"FROM read_parquet('{polys_path}') WHERE cat='market' "
            f"AND shop IN ('supermarket', 'convenience') "
            f"AND amenity IS DISTINCT FROM 'marketplace'"
        )
    if not parts:
        return []

    base_src = " UNION ALL ".join(parts)
    q = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
            cos(radians({lat}))*cos(radians(lat))*
            sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
          )
        ) AS d_lin
      FROM base
    )
    SELECT * FROM dist WHERE d_lin <= {radius_m}
    """
    return con.execute(q).df().to_dict("records")

def analyze_market(con, nodes_path, polys_path, lat, lon, display_radius_m, topn):
    """Compute a fixed-radius Market score and a display-radius result list."""
    scoring_rows = deduplicate_market_pois(query_market_candidates(
        con, nodes_path, polys_path, lat, lon, MARKET_SCORING_RADIUS_M))
    market_score = calc_market_score(scoring_rows)

    if display_radius_m == MARKET_SCORING_RADIUS_M:
        display_rows = scoring_rows
    else:
        display_rows = deduplicate_market_pois(query_market_candidates(
            con, nodes_path, polys_path, lat, lon, display_radius_m))

    type_rank = {"supermarket": 10, "convenience": 8}
    display_rows.sort(key=lambda p: (-type_rank[market_type(p)], float(p["d_lin"])))
    n_total = len(display_rows)
    d_min = min((float(p["d_lin"]) for p in display_rows), default=None)

    output = []
    for poi in display_rows[:max(0, topn)]:
        row = dict(poi)
        distance = float(row["d_lin"])
        row.update({
            "score": type_rank[market_type(row)],
            "d_min": d_min,
            "n_total": n_total,
            "has_hospital_any": 0,
            "walk_m": distance * WALK_CIRCUITY,
            "drive_m": distance * DRIVE_CIRCUITY,
            "walk_s": (distance * WALK_CIRCUITY) / (WALK_SPEED_KPH * 1000/3600),
            "drive_s": (distance * DRIVE_CIRCUITY) / (DRIVE_SPEED_KPH * 1000/3600),
        })
        output.append(row)

    columns = ["name", "brand", "amenity", "shop", "healthcare", "lat", "lon",
               "score", "d_lin", "d_min", "n_total", "has_hospital_any",
               "walk_m", "walk_s", "drive_m", "drive_s"]
    return pd.DataFrame(output, columns=columns), market_score, n_total, d_min

def school_bounding_box(lat, lon, radius_m):
    """Return a conservative spherical bounding box for a radius in metres."""
    angular_radius = float(radius_m) / 6371000.0
    lat_radians = math.radians(float(lat))
    lat_delta = math.degrees(angular_radius)
    lat_min = max(-90.0, float(lat) - lat_delta)
    lat_max = min(90.0, float(lat) + lat_delta)

    if lat_min <= -90.0 or lat_max >= 90.0:
        lon_delta = 180.0
    else:
        ratio = math.sin(angular_radius) / max(1e-15, math.cos(lat_radians))
        lon_delta = 180.0 if ratio >= 1.0 else math.degrees(math.asin(ratio))

    return (
        math.nextafter(lat_min, -math.inf),
        math.nextafter(lat_max, math.inf),
        math.nextafter(float(lon) - lon_delta, -math.inf),
        math.nextafter(float(lon) + lon_delta, math.inf),
    )

def query_school_candidates(con, nodes_path, polys_path, lat, lon, radius_m):
    """Return explicit school/kindergarten rows for School's isolated pipeline."""
    lat_min, lat_max, lon_min, lon_max = school_bounding_box(
        lat, lon, radius_m)

    parts = []
    if nodes_path:
        parts.append(
            f"SELECT name, NULL AS brand, lat, lon, amenity, shop, healthcare, "
            f"school_level, isced_level, 'node' AS source "
            f"FROM read_parquet('{nodes_path}') WHERE cat='school' "
            f"AND amenity IN ('school', 'kindergarten')"
        )
    if polys_path:
        parts.append(
            f"SELECT name, brand, lat, lon, amenity, shop, healthcare, "
            f"school_level, isced_level, 'polygon' AS source "
            f"FROM read_parquet('{polys_path}') WHERE cat='school' "
            f"AND amenity IN ('school', 'kindergarten')"
        )
    if not parts:
        return []

    base_src = " UNION ALL ".join(parts)
    q = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
            cos(radians({lat}))*cos(radians(lat))*
            sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
          )
        ) AS d_lin
      FROM base
    )
    SELECT * FROM dist WHERE d_lin <= {radius_m}
    """
    return con.execute(q).df().to_dict("records")

def analyze_school(con, nodes_path, polys_path, lat, lon, display_radius_m, topn):
    """Compute a fixed-radius School score and display-radius result list."""
    scoring_rows = deduplicate_school_pois(query_school_candidates(
        con, nodes_path, polys_path, lat, lon, SCHOOL_SCORING_RADIUS_M))
    school_score = calc_school_score(scoring_rows)

    if display_radius_m == SCHOOL_SCORING_RADIUS_M:
        display_rows = scoring_rows
    else:
        display_rows = deduplicate_school_pois(query_school_candidates(
            con, nodes_path, polys_path, lat, lon, display_radius_m))

    display_rows.sort(key=lambda p: (
        float(p["d_lin"]), str(p.get("name") or "").casefold(),
        str(p.get("amenity") or ""), str(p.get("source") or ""),
        float(p["lat"]), float(p["lon"])))
    n_total = len(display_rows)
    d_min = min((float(p["d_lin"]) for p in display_rows), default=None)

    output = []
    for poi in display_rows[:max(0, topn)]:
        row = dict(poi)
        distance = float(row["d_lin"])
        row.update({
            "score": 10 if row.get("amenity") == "school" else 4,
            "d_min": d_min,
            "n_total": n_total,
            "has_hospital_any": 0,
            "walk_m": distance * WALK_CIRCUITY,
            "drive_m": distance * DRIVE_CIRCUITY,
            "walk_s": (distance * WALK_CIRCUITY) / (WALK_SPEED_KPH * 1000/3600),
            "drive_s": (distance * DRIVE_CIRCUITY) / (DRIVE_SPEED_KPH * 1000/3600),
        })
        output.append(row)

    columns = ["name", "brand", "amenity", "shop", "healthcare", "lat", "lon",
               "school_level", "isced_level", "score", "d_lin", "d_min", "n_total",
               "has_hospital_any", "walk_m", "walk_s", "drive_m", "drive_s"]
    return pd.DataFrame(output, columns=columns), school_score, n_total, d_min

def health_bounding_box(lat, lon, radius_m):
    """Return a conservative spherical bounding box for a Health radius."""
    numeric_margin_degrees = 1e-8
    angular_radius = float(radius_m) / 6371000.0
    lat_radians = math.radians(float(lat))
    lat_delta = math.degrees(angular_radius)
    lat_min = max(-90.0, float(lat) - lat_delta)
    lat_max = min(90.0, float(lat) + lat_delta)

    if lat_min <= -90.0 or lat_max >= 90.0:
        lon_delta = 180.0
    else:
        ratio = math.sin(angular_radius) / max(1e-15, math.cos(lat_radians))
        lon_delta = 180.0 if ratio >= 1.0 else math.degrees(math.asin(ratio))

    return (
        math.nextafter(lat_min - numeric_margin_degrees, -math.inf),
        math.nextafter(lat_max + numeric_margin_degrees, math.inf),
        math.nextafter(float(lon) - lon_delta - numeric_margin_degrees, -math.inf),
        math.nextafter(float(lon) + lon_delta + numeric_margin_degrees, math.inf),
    )

def query_health_candidates(con, nodes_path, polys_path, lat, lon, radius_m):
    """Return every displayable Health row inside an exact radial query."""
    distance_tolerance_m = 1e-6
    lat_min, lat_max, lon_min, lon_max = health_bounding_box(
        lat, lon, radius_m)
    parts = []
    if nodes_path:
        parts.append(
            f"SELECT name, NULL AS brand, lat, lon, amenity, shop, healthcare, "
            f"'node' AS source FROM read_parquet('{nodes_path}') WHERE cat='health'"
        )
    if polys_path:
        parts.append(
            f"SELECT name, brand, lat, lon, amenity, shop, healthcare, "
            f"'polygon' AS source FROM read_parquet('{polys_path}') WHERE cat='health'"
        )
    if not parts:
        return []

    base_src = " UNION ALL ".join(parts)
    query = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
            cos(radians({lat}))*cos(radians(lat))*
            sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
          )
        ) AS d_lin
      FROM base
    )
    SELECT * FROM dist WHERE d_lin <= {float(radius_m) + distance_tolerance_m}
    """
    return con.execute(query).df().to_dict("records")

def _health_display_rank(poi):
    amenity = poi.get("amenity")
    healthcare = poi.get("healthcare")
    return (
        (100 if amenity == "hospital" or healthcare == "hospital" else 0)
        + (80 if amenity == "clinic" or healthcare == "clinic" else 0)
        + (60 if amenity == "doctors" or healthcare == "doctor" else 0)
        + (55 if amenity == "dentist" or healthcare == "dentist" else 0)
        + (50 if healthcare == "physiotherapist" else 0)
        + (40 if amenity == "pharmacy" else 0)
        + (30 if pd.notnull(healthcare) else 0)
    )

def analyze_health(con, nodes_path, polys_path, lat, lon, display_radius_m, topn):
    """Compute fixed-radius Health V1 and a display-radius result list.

    ``n_total`` is the raw number of all displayable Health cache rows inside
    the caller's display radius, before top-N. ``d_min`` is the nearest such
    displayed-category row and is not necessarily a scoring clinical POI.
    """
    local_candidates = query_health_candidates(
        con, nodes_path, polys_path, lat, lon, HEALTH_LOCAL_RADIUS_M)
    local_scoring_rows = deduplicate_health_local_pois(local_candidates)
    hospital_candidates = query_health_candidates(
        con, nodes_path, polys_path, lat, lon, HEALTH_HOSPITAL_RADIUS_M)
    components = health_score_components(local_scoring_rows, hospital_candidates)

    if display_radius_m == HEALTH_LOCAL_RADIUS_M:
        display_rows = local_candidates
    else:
        display_rows = query_health_candidates(
            con, nodes_path, polys_path, lat, lon, display_radius_m)
    display_rows.sort(key=lambda poi: (
        float(poi["d_lin"]), str(poi.get("name") or "").casefold(),
        str(poi.get("source") or ""), float(poi["lat"]), float(poi["lon"]),
        str(poi.get("amenity") or ""), str(poi.get("healthcare") or "")))

    n_total = len(display_rows)
    d_min = min((float(poi["d_lin"]) for poi in display_rows), default=None)
    has_hospital = any(
        poi.get("amenity") == "hospital" or poi.get("healthcare") == "hospital"
        for poi in display_rows)
    output = []
    for poi in display_rows[:max(0, topn)]:
        row = dict(poi)
        distance = float(row["d_lin"])
        row.update({
            "score": _health_display_rank(row),
            "d_min": d_min,
            "n_total": n_total,
            "has_hospital_any": int(has_hospital),
            "walk_m": distance * WALK_CIRCUITY,
            "drive_m": distance * DRIVE_CIRCUITY,
            "walk_s": (distance * WALK_CIRCUITY) / (WALK_SPEED_KPH * 1000/3600),
            "drive_s": (distance * DRIVE_CIRCUITY) / (DRIVE_SPEED_KPH * 1000/3600),
        })
        output.append(row)

    columns = ["name", "brand", "amenity", "shop", "healthcare", "lat", "lon",
               "source", "score", "d_lin", "d_min", "n_total",
               "has_hospital_any", "walk_m", "walk_s", "drive_m", "drive_s"]
    frame = pd.DataFrame(output, columns=columns)
    return frame, components["score"], n_total, d_min, components


def transit_bounding_box(lat, lon, radius_m):
    """Return a conservative spherical bbox for Transit candidate pruning."""
    angular_radius = float(radius_m) / 6371000.0
    latitude = float(lat)
    lat_radians = math.radians(latitude)
    lat_delta = math.degrees(angular_radius)
    lat_min = max(-90.0, latitude - lat_delta)
    lat_max = min(90.0, latitude + lat_delta)
    if lat_min <= -90.0 or lat_max >= 90.0:
        lon_delta = 180.0
    else:
        ratio = math.sin(angular_radius) / max(1e-15, math.cos(lat_radians))
        lon_delta = 180.0 if ratio >= 1.0 else math.degrees(math.asin(ratio))
    return (
        math.nextafter(lat_min, -math.inf),
        math.nextafter(lat_max, math.inf),
        math.nextafter(float(lon) - lon_delta, -math.inf),
        math.nextafter(float(lon) + lon_delta, math.inf),
    )


def _duckdb_path(path):
    return str(os.path.abspath(os.fspath(path))).replace("'", "''")


def query_transit_local_candidates(con, stops_path, summary_path, lat, lon):
    """Join fixed-radius service metadata to each logical stop's nearest member."""
    lat_min, lat_max, lon_min, lon_max = transit_bounding_box(
        lat, lon, LOCAL_SCORING_RADIUS_M)
    query = f"""
    WITH member_bbox AS (
      SELECT logical_stop_id, operator, gtfs_stop_id, lat, lon,
        2*6371000*asin(sqrt(least(1.0, greatest(0.0,
          sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
          cos(radians({lat}))*cos(radians(lat))*
          sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
        )))) AS distance_m
      FROM read_parquet('{_duckdb_path(stops_path)}')
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ), nearest_member AS (
      SELECT *, row_number() OVER (
        PARTITION BY logical_stop_id
        ORDER BY distance_m, operator, gtfs_stop_id) AS member_rank
      FROM member_bbox
      WHERE distance_m <= {LOCAL_SCORING_RADIUS_M + RADIUS_EPSILON_M}
    )
    SELECT summary.logical_stop_id, summary.display_name AS name,
           nearest.lat, nearest.lon, nearest.distance_m,
           summary.weekday_departures, summary.saturday_departures,
           summary.sunday_departures, summary.seven_day_average,
           summary.operators, summary.modes
    FROM nearest_member nearest
    JOIN read_parquet('{_duckdb_path(summary_path)}') summary
      USING (logical_stop_id)
    WHERE nearest.member_rank = 1
    """
    records = con.execute(query).df().to_dict("records")
    for record in records:
        record["operators"] = list(record["operators"])
        record["modes"] = list(record["modes"])
    return records


def query_transit_rail_candidates(con, rail_path, lat, lon):
    """Return exact-distance SNCB station candidates inside the fixed radius."""
    lat_min, lat_max, lon_min, lon_max = transit_bounding_box(
        lat, lon, RAIL_SCORING_RADIUS_M)
    query = f"""
    WITH station_bbox AS (
      SELECT logical_station_id, station_name AS name, uic_code, lat, lon,
             weekday_departures, saturday_departures, sunday_departures,
             seven_day_average,
        2*6371000*asin(sqrt(least(1.0, greatest(0.0,
          sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
          cos(radians({lat}))*cos(radians(lat))*
          sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
        )))) AS distance_m
      FROM read_parquet('{_duckdb_path(rail_path)}')
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    )
    SELECT * FROM station_bbox
    WHERE distance_m <= {RAIL_SCORING_RADIUS_M + RADIUS_EPSILON_M}
    """
    return con.execute(query).df().to_dict("records")


def resolve_transit_cache_paths(nodes_path, polys_path, stops_path=None,
                                summary_path=None, rail_path=None):
    """Resolve service caches beside the configured POI caches by default."""
    source_path = nodes_path or polys_path
    cache_dir = os.path.dirname(os.path.abspath(source_path)) if source_path else None

    def resolve(explicit, filename):
        candidate = explicit
        if candidate is None and cache_dir:
            candidate = os.path.join(cache_dir, filename)
        return candidate if candidate and os.path.isfile(candidate) else None

    return (
        resolve(stops_path, "be_transit_service_stops.parquet"),
        resolve(summary_path, "be_transit_service_summary.parquet"),
        resolve(rail_path, "be_rail_service.parquet"),
    )


def analyze_transit(con, nodes_path, polys_path, service_stops_path,
                    service_summary_path, rail_service_path, lat, lon,
                    display_radius_m, topn):
    """Compute fixed-radius Transit V1 and legacy radius-driven display rows."""
    local_candidates = []
    if service_stops_path and service_summary_path:
        local_candidates = query_transit_local_candidates(
            con, service_stops_path, service_summary_path, lat, lon)
    rail_candidates = []
    if rail_service_path:
        rail_candidates = query_transit_rail_candidates(
            con, rail_service_path, lat, lon)
    components = transit_score_components(local_candidates, rail_candidates)

    # The existing OSM list remains display-only compatibility data. Its raw
    # count and nearest infrastructure distance never enter Transit Score V1.
    display = query_category(
        con, nodes_path, polys_path, "transit", lat, lon,
        display_radius_m, topn)
    n_total = int(display.iloc[0]["n_total"]) if not display.empty else 0
    d_min = (float(display.iloc[0]["d_min"])
             if not display.empty and pd.notnull(display.iloc[0]["d_min"])
             else None)
    return display, components["score"], n_total, d_min, components

def query_category(con, nodes_path, polys_path, cat, lat, lon, radius_m, topn):
    if cat == "market":
        raise ValueError("Market uses the dedicated analyze_market() Market Score V1 path.")
    if cat == "school":
        raise ValueError("School uses the dedicated analyze_school() School Score V1 path.")
    if cat == "health":
        raise ValueError("Health uses the dedicated analyze_health() Health Score V1 path.")
    dlat, dlon = meters_to_deg_latlon(lat, radius_m)
    lat_min, lat_max = lat - dlat, lat + dlat
    lon_min, lon_max = lon - dlon, lon + dlon
    score_sql = SCORES[cat]

    parts = []
    if nodes_path:
        # node cache’de brand kolonu yok → NULL AS brand
        parts.append(
            f"SELECT name, NULL AS brand, lat, lon, amenity, shop, healthcare, railway, highway, public_transport, leisure, boundary, landuse, sport, school_level, isced_level "
            f"FROM read_parquet('{nodes_path}') WHERE cat='{cat}'"
        )
    if polys_path:
        parts.append(
            f"SELECT name, brand, lat, lon, amenity, shop, healthcare, railway, highway, public_transport, leisure, boundary, landuse, sport, school_level, isced_level "
            f"FROM read_parquet('{polys_path}') WHERE cat='{cat}'"
        )
    if not parts:
        return pd.DataFrame()

    base_src = " UNION ALL ".join(parts)

    # not: 'is_hospital' ve window fonksiyonları ile n_total, d_min ve has_hospital_any de getiriyoruz
    transit_display_columns = (
        ", railway, highway, public_transport" if cat == "transit" else "")
    q = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
            cos(radians({lat}))*cos(radians(lat))*
            sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
          )
        ) AS d_lin
      FROM base
    ),
    scored AS (
      SELECT *,
        {score_sql} AS score,
        (CASE WHEN amenity='hospital' OR healthcare='hospital' THEN 1 ELSE 0 END) AS is_hospital
      FROM dist
      WHERE d_lin <= {radius_m}
    ),
    ranked AS (
      SELECT *,
        COUNT(*) OVER () AS n_total,
        MIN(d_lin) OVER () AS d_min,
        MAX(is_hospital) OVER () AS has_hospital_any,
        d_lin*{WALK_CIRCUITY} AS walk_m,
        d_lin*{DRIVE_CIRCUITY} AS drive_m,
        (d_lin*{WALK_CIRCUITY}) / ({WALK_SPEED_KPH} * 1000/3600) AS walk_s,
        (d_lin*{DRIVE_CIRCUITY}) / ({DRIVE_SPEED_KPH} * 1000/3600) AS drive_s,
        ROW_NUMBER() OVER (ORDER BY score DESC, d_lin ASC) AS rn_all
      FROM scored
    )
    SELECT name, brand, amenity, shop, healthcare{transit_display_columns},
           lat, lon, score, d_lin, d_min, n_total, has_hospital_any,
           walk_m, walk_s, drive_m, drive_s
    FROM ranked
    WHERE rn_all <= {topn}
    """
    return con.execute(q).df()

def calc_category_score(cat, n_total:int, d_min:float, has_hospital:bool=False) -> float:
    if cat == "market":
        raise ValueError("Market uses calc_market_score() in the dedicated Market Score V1 path.")
    if cat == "school":
        raise ValueError("School uses calc_school_score() in the dedicated School Score V1 path.")
    if cat == "health":
        raise ValueError("Health uses health_score_components() in the dedicated Health Score V1 path.")
    if cat == "transit":
        raise ValueError("Transit uses transit_score_components() in the dedicated Transit Score V1 path.")
    # Kategoriye göre konfig
    cfg = SCORING[cat]
    D0 = cfg["D0"]
    w_prox = cfg["w_prox"]
    w_count = cfg["w_count"]
    Nsat = cfg["Nsat"]

    # yakınlık puanı (0..w_prox)
    if d_min is None or n_total == 0:
        prox_pts = 0.0
    else:
        prox_norm = max(0.0, 1.0 - min(d_min, D0)/D0)  # 0..1
        prox_pts = prox_norm * w_prox

    # sayıya bağlı puan (0..w_count)
    count_norm = min(n_total, Nsat) / Nsat  # 0..1
    count_pts = count_norm * w_count

    score = prox_pts + count_pts

    # 0..10 aralığına kırp
    return max(0.0, min(10.0, score))

def main():
    ap = argparse.ArgumentParser(description="Adres çevresinde hızlı POI analizi (node+polygon cache, puanlama).")
    ap.add_argument("--address", type=str)
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--radius", type=int, default=DEFAULT_RADIUS_M)
    ap.add_argument("--topn", type=int, default=TOP_N)
    ap.add_argument("--nodes", type=str, default="./cache/be_poi.parquet")
    ap.add_argument("--polys", type=str, default="./cache/be_poi_poly.parquet")
    args = ap.parse_args()

    # konum
    if args.address:
        lat, lon, disp = geocode(args.address)
    elif args.lat is not None and args.lon is not None:
        lat, lon, disp = args.lat, args.lon, f"({args.lat:.6f}, {args.lon:.6f})"
    else:
        raise SystemExit("Adres veya (lat,lon) verin.")

    # kaynaklar (fallback)
    nodes_path = args.nodes if (args.nodes and os.path.exists(args.nodes)) else None
    polys_path = args.polys if (args.polys and os.path.exists(args.polys)) else None
    if not nodes_path and not polys_path:
        raise SystemExit(f"Ne node ne polygon cache bulundu.\n  nodes arg: {args.nodes}\n  polys arg: {args.polys}\nLütfen cache dosyalarını üretin.")

    print(f"Adres: {disp}  (lat={lat:.6f}, lon={lon:.6f})")
    if nodes_path and not polys_path:
        print("[INFO] Sadece NODE cache bulunuyor.")
    elif polys_path and not nodes_path:
        print("[INFO] Sadece POLYGON cache bulunuyor (node yok).")
    else:
        print("[INFO] Node + Polygon birlikte kullanılacak.")

    con = duckdb.connect()
    all_rows=[]
    cat_scores={}
    summary_rows=[]  # kategori scorecard için
    transit_paths = resolve_transit_cache_paths(nodes_path, polys_path)

    for cat in CATS.keys():
        dedicated_score = None
        if cat == "market":
            df, dedicated_score, _, _ = analyze_market(
                con, nodes_path, polys_path, lat, lon, args.radius, args.topn)
        elif cat == "school":
            df, dedicated_score, _, _ = analyze_school(
                con, nodes_path, polys_path, lat, lon, args.radius, args.topn)
        elif cat == "health":
            df, dedicated_score, _, _, _ = analyze_health(
                con, nodes_path, polys_path, lat, lon, args.radius, args.topn)
        elif cat == "transit":
            df, dedicated_score, _, _, _ = analyze_transit(
                con, nodes_path, polys_path, *transit_paths,
                lat, lon, args.radius, args.topn)
        else:
            df = query_category(con, nodes_path, polys_path, cat, lat, lon, args.radius, args.topn)
        label = CATS[cat]["label"]
        if df.empty:
            print(f"\n— {label} (sonuç yok)")
            score = dedicated_score if dedicated_score is not None else 0.0
            cat_scores[cat] = score
            summary_rows.append((label, score, 0, None, False))
            continue

        # Konsola TOP-N
        print(f"\n— {label} (TOP {len(df)})")
        for _, r in df.iterrows():
            print(f"{label:<8} | {str(r['name'])[:48]:<48} | "
                  f"Yürüme: {fmt_meters(r['walk_m']):>8}, {fmt_seconds(r['walk_s']):>8} | "
                  f"Araba: {fmt_meters(r['drive_m']):>8}, {fmt_seconds(r['drive_s']):>8}")

        # Puanlama verileri (CTE window'dan aynı değerler tüm satırlarda aynı)
        n_total = int(df.iloc[0]["n_total"])
        d_min   = float(df.iloc[0]["d_min"]) if pd.notnull(df.iloc[0]["d_min"]) else None
        has_hospital = bool(df.iloc[0]["has_hospital_any"]) if "has_hospital_any" in df.columns else False

        score = dedicated_score if dedicated_score is not None else calc_category_score(cat, n_total, d_min, has_hospital)
        cat_scores[cat] = score
        summary_rows.append((label, score, n_total, d_min, has_hospital))

        # harita için
        df["cat"]=cat
        all_rows.append(df)

        # özet satırı
        dmin_txt = fmt_meters(d_min) if d_min is not None else "-"
        if cat == "health":
            hosp_txt = " (hastane: var)" if has_hospital else " (hastane: yok)"
        else:
            hosp_txt = ""
        print(f"   ⇒ Puan: {score:.1f}/10  | n={n_total}  | en yakın={dmin_txt}{hosp_txt}")

    # Genel puan (ağırlıklı)
    total_weight = sum(OVERALL_WEIGHTS.get(cat, 0.0) for cat in CATS.keys())
    overall = 0.0
    for cat in CATS.keys():
        w = OVERALL_WEIGHTS.get(cat, 0.0)
        overall += w * cat_scores.get(cat, 0.0)
    # Toplam ağırlık 1.0 varsayımıyla; yine de emniyet:
    if total_weight > 0:
        overall = overall / 1.0  # zaten ağırlıklar 1.0 topluyor
    print("\n=== Kategori Puanları ===")
    for label, score, n_total, d_min, has_hospital in summary_rows:
        print(f"{label:<8}: {score:>4.1f}/10")
    print(f"\n*** GENEL PUAN: {overall:.1f}/10 ***")

    # Harita
    if not all_rows:
        return
    big = pd.concat(all_rows, ignore_index=True)

    m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker([lat, lon], popup=f"Adres: {disp}", tooltip="Adres",
                  icon=folium.Icon(color="black", icon="home")).add_to(m)
    for _, r in big.iterrows():
        label = CATS[r["cat"]]["label"]
        popup = (f"{label}: {r['name']}<br>"
                 f"Yürüme: {fmt_meters(r['walk_m'])}, {fmt_seconds(r['walk_s'])}<br>"
                 f"Araba: {fmt_meters(r['drive_m'])}, {fmt_seconds(r['drive_s'])}")
        folium.Marker([float(r["lat"]), float(r["lon"])],
                      popup=popup, tooltip=f"{label}: {r['name']}",
                      icon=folium.Icon(color=CATS[r["cat"]]["color"])).add_to(m)

    # Legend
    entries = "".join(
        f'<div style="display:flex;align-items:center;margin:2px 0;">'
        f'<span style="display:inline-block;width:12px;height:12px;background:{meta["color"]};margin-right:6px;border:1px solid #333;"></span>'
        f'{meta["label"]}</div>' for meta in CATS.values()
    )
    legend_html = (
        f'<div style="position:fixed;bottom:10px;left:10px;z-index:9999;background:#fff;padding:8px 10px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;">'
        f'<div style="font-weight:600;margin-bottom:4px;">Legenda</div>{entries}</div>'
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    # Scorecard overlay (kategori puanları + genel)
    score_items = "".join(
        f'<div style="display:flex;justify-content:space-between;"><span>{label}</span>'
        f'<span>{score:0.1f}/10</span></div>'
        for (label, score, *_rest) in summary_rows
    )
    score_html = (
        f'<div style="position:fixed;top:10px;right:10px;z-index:9999;background:#fff;padding:10px 12px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;min-width:200px;">'
        f'<div style="font-weight:700;margin-bottom:6px;">Puanlama</div>'
        f'{score_items}'
        f'<hr style="margin:6px 0;border:none;border-top:1px solid #ddd;" />'
        f'<div style="display:flex;justify-content:space-between;font-weight:700;">'
        f'<span>Genel</span><span>{overall:0.1f}/10</span>'
        f'</div>'
        f'</div>'
    )
    m.get_root().html.add_child(folium.Element(score_html))

    out = os.path.abspath("map.html")
    m.save(out)
    print(f"\nHarita kaydedildi: {out}")

def analyze(address=None, lat=None, lon=None, radius=DEFAULT_RADIUS_M, topn=TOP_N,
            nodes_path="./cache/be_poi.parquet", polys_path="./cache/be_poi_poly.parquet",
            transit_stops_path=None, transit_summary_path=None,
            rail_service_path=None):
    # konum
    if address:
        lat, lon, disp = geocode(address)
    elif lat is not None and lon is not None:
        disp = f"({lat:.6f}, {lon:.6f})"
    else:
        raise ValueError("address veya (lat,lon) verin.")

    nodes_ok = nodes_path and os.path.exists(nodes_path)
    polys_ok = polys_path and os.path.exists(polys_path)
    if not nodes_ok and not polys_ok:
        raise FileNotFoundError("Ne node ne polygon cache bulundu.")

    con = duckdb.connect()
    all_rows = []
    cat_scores = {}
    summary_rows = []
    breakdowns = {}
    transit_paths = resolve_transit_cache_paths(
        nodes_path if nodes_ok else None, polys_path if polys_ok else None,
        transit_stops_path, transit_summary_path, rail_service_path)

    for cat in CATS.keys():
        dedicated_score = None
        dedicated_components = None
        if cat == "market":
            df, dedicated_score, _, _ = analyze_market(
                con, nodes_path if nodes_ok else None,
                polys_path if polys_ok else None,
                lat, lon, radius, topn)
        elif cat == "school":
            df, dedicated_score, _, _ = analyze_school(
                con, nodes_path if nodes_ok else None,
                polys_path if polys_ok else None,
                lat, lon, radius, topn)
        elif cat == "health":
            df, dedicated_score, _, _, _ = analyze_health(
                con, nodes_path if nodes_ok else None,
                polys_path if polys_ok else None,
                lat, lon, radius, topn)
        elif cat == "transit":
            df, dedicated_score, _, _, dedicated_components = analyze_transit(
                con, nodes_path if nodes_ok else None,
                polys_path if polys_ok else None, *transit_paths,
                lat, lon, radius, topn)
            breakdowns[cat] = dedicated_components
        else:
            df = query_category(con, nodes_path if nodes_ok else None,
                                polys_path if polys_ok else None,
                                cat, lat, lon, radius, topn)
        label = CATS[cat]["label"]
        if df.empty:
            score = dedicated_score if dedicated_score is not None else 0.0
            cat_scores[cat] = score
            summary_rows.append((label, score, 0, None, False))
            continue

        n_total = int(df.iloc[0]["n_total"])
        d_min   = float(df.iloc[0]["d_min"]) if pd.notnull(df.iloc[0]["d_min"]) else None
        has_hospital = bool(df.iloc[0]["has_hospital_any"]) if "has_hospital_any" in df.columns else False
        score = dedicated_score if dedicated_score is not None else calc_category_score(cat, n_total, d_min, has_hospital)
        cat_scores[cat] = score
        summary_rows.append((label, score, n_total, d_min, has_hospital))

        df["cat"] = cat
        all_rows.append(df)

    # genel puan (ağırlıklı)
    overall = 0.0
    for cat in CATS.keys():
        overall += OVERALL_WEIGHTS.get(cat, 0.0) * cat_scores.get(cat, 0.0)

    # harita
    m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker([lat, lon], popup=f"Adres: {disp}", tooltip="Adres",
                  icon=folium.Icon(color="black", icon="home")).add_to(m)

    big = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    if not big.empty:
        for _, r in big.iterrows():
            label = CATS[r["cat"]]["label"]
            popup = (f"{label}: {r['name']}<br>"
                     f"Yürüme: {fmt_meters(r['walk_m'])}, {fmt_seconds(r['walk_s'])}<br>"
                     f"Araba: {fmt_meters(r['drive_m'])}, {fmt_seconds(r['drive_s'])}")
            folium.Marker([float(r["lat"]), float(r["lon"])],
                          popup=popup, tooltip=f"{label}: {r['name']}",
                          icon=folium.Icon(color=CATS[r["cat"]]["color"])).add_to(m)

    # Legend + Scorecard (harita üstü overlay)
    entries = "".join(
        f'<div style="display:flex;align-items:center;margin:2px 0;">'
        f'<span style="display:inline-block;width:12px;height:12px;background:{meta["color"]};margin-right:6px;border:1px solid #333;"></span>'
        f'{meta["label"]}</div>' for meta in CATS.values()
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;bottom:10px;left:10px;z-index:9999;background:#fff;padding:8px 10px;border:1px solid #999;border-radius:6px;font-size:13px;">'
        f'<div style="font-weight:600;margin-bottom:4px;">Legenda</div>{entries}</div>'
    ))
    score_items = "".join(
        f'<div style="display:flex;justify-content:space-between;"><span>{label}</span>'
        f'<span>{cat_scores.get(slug,0.0):0.1f}/10</span></div>'
        for slug,label in [(k, CATS[k]["label"]) for k in CATS.keys()]
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;top:10px;right:10px;z-index:9999;background:#fff;padding:10px 12px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;min-width:200px;">'
        f'<div style="font-weight:700;margin-bottom:6px;">Puanlama</div>'
        f'{score_items}'
        f'</div>'
    ))
    map_html = m.get_root().render()

    # tablo verileri
    results_by_cat = {}
    if not big.empty:
        for cat in CATS.keys():
            sub = big[big["cat"] == cat].copy()
            if sub.empty:
                results_by_cat[cat] = []
                continue
            rows = []
            for _, r in sub.iterrows():
                item = {
                    "name": r["name"],
                    "walk_m": fmt_meters(r["walk_m"]),
                    "walk_s": fmt_seconds(r["walk_s"]),
                    "drive_m": fmt_meters(r["drive_m"]),
                    "drive_s": fmt_seconds(r["drive_s"]),
                }
                if cat == "school":
                    item["type"] = r["amenity"]
                rows.append(item)
            results_by_cat[cat] = rows
    else:
        for cat in CATS.keys():
            results_by_cat[cat] = []

    cat_scores_pretty = {CATS[c]["label"]: float(f"{cat_scores.get(c,0.0):.1f}") for c in CATS.keys()}
    return {
        "display_address": disp,
        "lat": lat, "lon": lon,
        "radius": radius,
        "map_html": map_html,
        "scores": cat_scores_pretty,
        "overall": float(f"{(sum(OVERALL_WEIGHTS.get(c,0.0)*cat_scores.get(c,0.0) for c in CATS.keys())):.1f}"),
        "results": results_by_cat,
        "breakdowns": breakdowns,
    }


if __name__ == "__main__":
    main()

"""Pure distance and numeric logic for Sport Score V1."""

from __future__ import annotations

import math

from geographiclib.geodesic import Geodesic
from shapely import wkb
from shapely.geometry import Point
from shapely.ops import nearest_points


SPORT_SCORING_RADIUS_M = 3000.0
SPORT_FULL_CREDIT_DISTANCE_M = 400.0
SPORT_BEST_MAX_POINTS = 7.5
SPORT_CHOICE_MAX_POINTS = 2.5
SPORT_CHOICE_SATURATION = 2.2
SPORT_CHOICE_WEIGHTS = (1.0, 0.70, 0.50, 0.35, 0.25)
DISTANCE_METHOD_SUMMARY = (
    "validated entrance, then canonical polygon boundary or node, "
    "then representative point"
)

FACILITY_FACTORS = {
    "multi_sport_centre": 1.00,
    "general_sports_centre": 0.90,
    "sports_hall": 0.95,
    "fitness_gym": 0.95,
    "swimming": 1.00,
    "stadium": 0.80,
    "standalone_local": 0.60,
    "specialized": 0.45,
    "standalone_specialized": 0.35,
}


def _finite(value, default=None):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _optional(value):
    try:
        return None if value is None or bool(math.isnan(value)) else value
    except (TypeError, ValueError):
        return value


def _string_list(value):
    if value is None:
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    return [str(item) for item in value]


def _geodesic_distance_m(lat1, lon1, lat2, lon2):
    values = [_finite(value) for value in (lat1, lon1, lat2, lon2)]
    if any(value is None for value in values):
        return math.inf
    return float(Geodesic.WGS84.Inverse(*values)["s12"])


def sport_distance_factor(distance_m):
    """Give full credit through 400 m, then decline to zero at 3 km."""
    distance = _finite(distance_m, math.inf)
    if distance < 0.0:
        distance = 0.0
    if distance <= SPORT_FULL_CREDIT_DISTANCE_M:
        return 1.0
    if distance >= SPORT_SCORING_RADIUS_M:
        return 0.0
    factor = 1.0 - (
        (distance - SPORT_FULL_CREDIT_DISTANCE_M)
        / (SPORT_SCORING_RADIUS_M - SPORT_FULL_CREDIT_DISTANCE_M)
    )
    return max(0.0, min(1.0, factor))


def facility_factor(facility_class):
    return FACILITY_FACTORS.get(str(facility_class or ""), 0.0)


def confidence_factor(row):
    """Derive the locked confidence factor from explicit cache evidence."""
    if not bool(row.get("score_eligible")):
        return 0.0
    access = str(row.get("access_class") or "").strip().casefold()
    reason = str(row.get("eligibility_reason") or "").strip().casefold()
    if access in {"private", "no"}:
        return 0.0
    if reason.startswith("excluded_"):
        return 0.0
    if access == "customers" and not bool(row.get("is_commercial")):
        return 0.0
    if bool(row.get("is_school_context")) and access != "positive_general":
        return 0.0
    if access == "positive_general":
        return 1.0
    if bool(row.get("is_public_operator")):
        return 1.0
    if (bool(row.get("is_commercial"))
            and row.get("facility_class") == "fitness_gym"):
        return 0.95
    if access == "members":
        return 0.70
    if access == "permit_limited":
        return 0.65
    if access in {"private", "no", "customers"}:
        return 0.0
    if bool(row.get("standalone")):
        has_identity = bool(
            str(row.get("name") or "").strip()
            or str(row.get("operator") or "").strip()
            or bool(row.get("is_public_operator"))
        )
        return 0.55 if has_identity else 0.0
    if bool(row.get("specialized")) and access == "missing":
        return 0.60
    if access == "missing":
        return 0.85
    return 0.85


def destination_distance_m(query_lat, query_lon, row):
    """Return ``(distance, method)`` using the locked evidence hierarchy."""
    entrance_lat = _finite(row.get("entrance_lat"))
    entrance_lon = _finite(row.get("entrance_lon"))
    if entrance_lat is not None and entrance_lon is not None:
        return (_geodesic_distance_m(
            query_lat, query_lon, entrance_lat, entrance_lon),
                "validated_entrance")

    geometry_wkb = row.get("geometry_wkb")
    if geometry_wkb is not None:
        try:
            geometry = wkb.loads(bytes(geometry_wkb))
            query = Point(float(query_lon), float(query_lat))
            if not geometry.is_empty and geometry.is_valid:
                if geometry.geom_type == "Point":
                    return (_geodesic_distance_m(
                        query_lat, query_lon, geometry.y, geometry.x),
                            "canonical_node")
                if geometry.covers(query):
                    return 0.0, "canonical_polygon_boundary"
                nearest = nearest_points(query, geometry)[1]
                return (_geodesic_distance_m(
                    query_lat, query_lon, nearest.y, nearest.x),
                        "canonical_polygon_boundary")
        except Exception:
            pass

    representative_lat = _finite(row.get("representative_lat"))
    representative_lon = _finite(row.get("representative_lon"))
    if representative_lat is not None and representative_lon is not None:
        return (_geodesic_distance_m(
            query_lat, query_lon, representative_lat, representative_lon),
                "representative_point")

    component_geometries = row.get("owned_component_geometry_wkb") or []
    best = None
    for component_wkb in component_geometries:
        component_row = {"geometry_wkb": component_wkb}
        distance, _method = destination_distance_m(
            query_lat, query_lon, component_row)
        if math.isfinite(distance) and (best is None or distance < best):
            best = distance
    if best is not None:
        return best, "owned_component_geometry"
    return math.inf, "unavailable"


def destination_utility(row):
    return (sport_distance_factor(row.get("distance_m"))
            * facility_factor(row.get("facility_class"))
            * confidence_factor(row))


def _destination_summary(row, utility):
    return {
        "sport_id": _optional(row.get("sport_id")),
        "canonical_osm_type": _optional(row.get("canonical_osm_type")),
        "canonical_osm_id": _optional(row.get("canonical_osm_id")),
        "display_name": _optional(row.get("display_name")),
        "facility_class": _optional(row.get("facility_class")),
        "distance_m": float(row["distance_m"]),
        "distance_method": _optional(row.get("distance_method")),
        "facility_factor": facility_factor(row.get("facility_class")),
        "confidence_factor": confidence_factor(row),
        "distance_factor": sport_distance_factor(row.get("distance_m")),
        "utility": utility,
        "sports": _string_list(row.get("sports")),
        "sport_count": int(row.get("sport_count") or 0),
        "access_class": _optional(row.get("access_class")),
        "is_commercial": bool(row.get("is_commercial")),
        "is_school_context": bool(row.get("is_school_context")),
    }


def sport_score_components(candidates):
    """Calculate the authoritative Sport Score V1 breakdown."""
    ranked = []
    seen = set()
    for candidate in candidates:
        row = dict(candidate)
        sport_id = str(row.get("sport_id") or "")
        distance = _finite(row.get("distance_m"), math.inf)
        if (not sport_id or sport_id in seen or distance < 0.0
                or distance >= SPORT_SCORING_RADIUS_M
                or not bool(row.get("score_eligible"))):
            continue
        seen.add(sport_id)
        utility = destination_utility(row)
        if utility <= 0.0:
            continue
        ranked.append((row, utility))
    ranked.sort(key=lambda item: (
        -item[1], float(item[0]["distance_m"]),
        str(item[0].get("sport_id") or "")))

    winner_row = ranked[0][0] if ranked else None
    winner_utility = ranked[0][1] if ranked else 0.0
    best_points = SPORT_BEST_MAX_POINTS * winner_utility

    alternative_entries = []
    for (row, utility), weight in zip(ranked[1:6], SPORT_CHOICE_WEIGHTS):
        summary = _destination_summary(row, utility)
        summary.update({
            "weight": weight,
            "weighted_utility": utility * weight,
        })
        alternative_entries.append(summary)
    choice_raw = sum(
        item["weighted_utility"] for item in alternative_entries)
    choice_points = SPORT_CHOICE_MAX_POINTS * min(
        1.0, choice_raw / SPORT_CHOICE_SATURATION)
    score_precise = max(0.0, min(10.0, best_points + choice_points))
    public_score = round(score_precise, 1)

    winner = (_destination_summary(winner_row, winner_utility)
              if winner_row is not None else None)
    return {
        "scoring_radius_m": int(SPORT_SCORING_RADIUS_M),
        "distance_method": DISTANCE_METHOD_SUMMARY,
        "score_precise": score_precise,
        "score": public_score,
        "score_public": public_score,
        "best": {
            "points": best_points,
            "max_points": SPORT_BEST_MAX_POINTS,
            "winner": winner,
        },
        "choice": {
            "points": choice_points,
            "max_points": SPORT_CHOICE_MAX_POINTS,
            "choice_raw": choice_raw,
            "saturation": SPORT_CHOICE_SATURATION,
            "considered_count": max(0, len(ranked) - 1),
            "alternatives_used": len(alternative_entries),
            "weights": list(SPORT_CHOICE_WEIGHTS),
            "alternatives": alternative_entries,
        },
    }

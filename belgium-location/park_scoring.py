"""Pure scoring and geometry-distance logic for Park Score V1."""

from __future__ import annotations

import math

from geographiclib.geodesic import Geodesic
from shapely import wkb
from shapely.geometry import Point
from shapely.ops import nearest_points


PARK_SCORING_RADIUS_M = 2500.0
PARK_FULL_CREDIT_DISTANCE_M = 250.0
PARK_PRIMARY_MAX_POINTS = 7.5
PARK_CHOICE_MAX_POINTS = 2.0
PARK_SECONDARY_MAX_POINTS = 0.5
DISTANCE_METHOD = "geometry_boundary_or_point_straight_line"
EARTH_RADIUS_M = 6_371_008.8


def _finite_nonnegative(value, default=0.0):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) and number >= 0.0 else default


def park_distance_factor(distance_m):
    """Return full credit through 250 m, then decay to zero at 2.5 km."""
    distance = _finite_nonnegative(distance_m, math.inf)
    if distance <= PARK_FULL_CREDIT_DISTANCE_M:
        return 1.0
    if distance >= PARK_SCORING_RADIUS_M:
        return 0.0
    decay_span = PARK_SCORING_RADIUS_M - PARK_FULL_CREDIT_DISTANCE_M
    return max(0.0, min(
        1.0, 1.0 - (distance - PARK_FULL_CREDIT_DISTANCE_M) / decay_span))


def geometry_distance_m(lat, lon, geometry_wkb):
    """Return the calibrated WGS84 distance to a cached point/footprint."""
    geometry = wkb.loads(bytes(geometry_wkb))
    query = Point(float(lon), float(lat))
    if geometry.covers(query):
        return 0.0
    if geometry.geom_type == "Point":
        lat1 = math.radians(float(lat))
        lat2 = math.radians(float(geometry.y))
        delta_lat = lat2 - lat1
        delta_lon = math.radians(float(geometry.x) - float(lon))
        haversine = (math.sin(delta_lat / 2.0) ** 2
                     + math.cos(lat1) * math.cos(lat2)
                     * math.sin(delta_lon / 2.0) ** 2)
        return 2.0 * EARTH_RADIUS_M * math.asin(
            min(1.0, math.sqrt(haversine)))
    nearest = nearest_points(query, geometry)[1]
    return float(Geodesic.WGS84.Inverse(
        float(lat), float(lon), float(nearest.y), float(nearest.x))["s12"])


def _identity(row):
    return (str(row.get("strong_identity_type") or ""),
            str(row.get("strong_identity_value") or ""))


def _optional(value):
    """Convert dataframe missing values to JSON-safe ``None`` values."""
    try:
        return None if value is None or bool(math.isnan(value)) else value
    except (TypeError, ValueError):
        return value


def _primary_utility(row):
    return (park_distance_factor(row["distance_m"])
            * _finite_nonnegative(row.get("size_factor"))
            * _finite_nonnegative(row.get("final_confidence")))


def _secondary_utility(row):
    return (park_distance_factor(row["distance_m"])
            * _finite_nonnegative(row.get("secondary_type_weight"))
            * _finite_nonnegative(row.get("secondary_confidence")))


def _winner_contract(row, utility):
    if row is None:
        return None
    return {
        "park_id": _optional(row.get("park_id")),
        "display_name": _optional(row.get("display_name")),
        "name": _optional(row.get("name")),
        "park_class": _optional(row.get("park_class")),
        "eligibility_tier": _optional(row.get("eligibility_tier")),
        "distance_m": float(row["distance_m"]),
        "area_m2": _optional(row.get("area_m2")),
        "size_factor": _optional(row.get("size_factor")),
        "final_confidence": _optional(row.get("final_confidence")),
        "utility": utility,
        "access": _optional(row.get("access")),
        "strong_identity_type": _optional(row.get("strong_identity_type")),
        "strong_identity_value": _optional(row.get("strong_identity_value")),
    }


def park_score_components(candidates):
    """Calculate the authoritative Park Score V1 breakdown."""
    inside = [dict(row) for row in candidates
              if _finite_nonnegative(row.get("distance_m"), math.inf)
              <= PARK_SCORING_RADIUS_M]

    primary = []
    for row in inside:
        if row.get("counts_as_primary"):
            utility = _primary_utility(row)
            primary.append((row, utility))
    winner_row = winner_utility = None
    if primary:
        winner_row, winner_utility = min(primary, key=lambda item: (
            -item[1], float(item[0]["distance_m"]),
            str(item[0].get("park_id") or "")))

    winner_identity = _identity(winner_row) if winner_row else None
    identity_utilities = {}
    for row, utility in primary:
        if not row.get("counts_as_choice"):
            continue
        identity = _identity(row)
        identity_utilities[identity] = max(
            identity_utilities.get(identity, 0.0), utility)
    alternatives = {
        identity: utility for identity, utility in identity_utilities.items()
        if identity != winner_identity
    }
    alternative_sum = sum(alternatives.values())

    secondary_utilities = [
        _secondary_utility(row) for row in inside
        if row.get("counts_as_secondary")
    ]
    secondary_sum = sum(secondary_utilities)

    primary_points = PARK_PRIMARY_MAX_POINTS * (winner_utility or 0.0)
    choice_points = PARK_CHOICE_MAX_POINTS * min(1.0, alternative_sum / 2.0)
    secondary_points = PARK_SECONDARY_MAX_POINTS * min(1.0, secondary_sum / 2.0)
    precise = max(0.0, min(
        10.0, primary_points + choice_points + secondary_points))

    return {
        "scoring_radius_m": int(PARK_SCORING_RADIUS_M),
        "distance_method": DISTANCE_METHOD,
        "score_precise": precise,
        "score_public": round(precise, 1),
        "primary_points": primary_points,
        "choice_points": choice_points,
        "secondary_points": secondary_points,
        "primary_candidate_count": len(primary),
        "choice_identity_count": len(identity_utilities),
        "secondary_candidate_count": len(secondary_utilities),
        "winner": _winner_contract(winner_row, winner_utility),
        "choice": {
            "effective_alternative_utility_sum": alternative_sum,
            "distinct_alternative_identity_count": len(alternatives),
        },
        "secondary": {
            "effective_utility_sum": secondary_sum,
            "contributing_candidate_count": len(secondary_utilities),
        },
    }

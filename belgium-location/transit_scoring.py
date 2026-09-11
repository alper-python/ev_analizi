"""Pure mathematics for the service-aware Belgium Transit Score V1."""

from __future__ import annotations

import math


LOCAL_SCORING_RADIUS_M = 1000.0
LOCAL_FULL_CREDIT_DISTANCE_M = 250.0
RAIL_SCORING_RADIUS_M = 7500.0
LOCAL_SERVICE_SATURATION = 90.0
RAIL_SERVICE_SATURATION = 350.0
LOCAL_MAX_POINTS = 6.0
RAIL_MAX_POINTS = 4.0
UTILITY_TIE_PRECISION = 12
RADIUS_EPSILON_M = 1e-6


def _nonnegative(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) and number > 0.0 else 0.0


def local_service_factor(seven_day_average):
    """Return the capped linear regular-local-service factor."""
    return min(1.0, _nonnegative(seven_day_average) /
               LOCAL_SERVICE_SATURATION)


def rail_service_factor(seven_day_average):
    """Return the capped logarithmic rail-service factor."""
    service = _nonnegative(seven_day_average)
    return min(1.0, math.log1p(service) /
               math.log1p(RAIL_SERVICE_SATURATION))


def distance_factor(distance_m, radius_m):
    """Return a linear distance decay, including the exact zero boundary."""
    distance = _nonnegative(distance_m)
    radius = float(radius_m)
    if radius <= 0.0:
        raise ValueError("Scoring radius must be positive")
    return max(0.0, 1.0 - distance / radius)


def local_distance_factor(distance_m):
    """Return full credit through 250 m, then decay to zero at 1 km."""
    distance = _nonnegative(distance_m)
    if distance <= LOCAL_FULL_CREDIT_DISTANCE_M:
        return 1.0
    decay_span = LOCAL_SCORING_RADIUS_M - LOCAL_FULL_CREDIT_DISTANCE_M
    return max(0.0, min(
        1.0, 1.0 - (distance - LOCAL_FULL_CREDIT_DISTANCE_M) / decay_span))


def local_utility(seven_day_average, distance_m):
    return (local_service_factor(seven_day_average)
            * local_distance_factor(distance_m))


def rail_utility(seven_day_average, distance_m):
    return (rail_service_factor(seven_day_average)
            * distance_factor(distance_m, RAIL_SCORING_RADIUS_M))


def _ranked_candidate(candidate, utility_function, radius_m, id_field):
    distance = float(candidate["distance_m"])
    if (not math.isfinite(distance) or distance < 0.0
            or distance > radius_m + RADIUS_EPSILON_M):
        return None
    service = _nonnegative(candidate.get("seven_day_average"))
    utility = utility_function(service, distance)
    candidate_distance_factor = (
        local_distance_factor(distance)
        if radius_m == LOCAL_SCORING_RADIUS_M
        else distance_factor(distance, radius_m))
    enriched = dict(candidate)
    enriched.update({
        "distance_m": distance,
        "seven_day_average": service,
        "service_factor": (local_service_factor(service)
                           if radius_m == LOCAL_SCORING_RADIUS_M
                           else rail_service_factor(service)),
        "distance_factor": candidate_distance_factor,
        "utility": utility,
    })
    # Utilities that differ below floating-point noise use the documented
    # distance/service/stable-ID tie break rather than input iteration order.
    rank = (-round(utility, UTILITY_TIE_PRECISION), distance, -service,
            str(candidate.get(id_field) or ""))
    return rank, enriched


def select_best_local(candidates):
    ranked = [value for candidate in candidates
              if (value := _ranked_candidate(
                  candidate, local_utility, LOCAL_SCORING_RADIUS_M,
                  "logical_stop_id")) is not None]
    return min(ranked, key=lambda value: value[0])[1] if ranked else None


def select_best_rail(candidates):
    ranked = [value for candidate in candidates
              if (value := _ranked_candidate(
                  candidate, rail_utility, RAIL_SCORING_RADIUS_M,
                  "logical_station_id")) is not None]
    return min(ranked, key=lambda value: value[0])[1] if ranked else None


def _winner_value(winner, field):
    return None if winner is None else winner.get(field)


def transit_score_components(local_candidates, rail_candidates):
    """Select one local stop and one station and return the full V1 contract."""
    local = select_best_local(local_candidates)
    rail = select_best_rail(rail_candidates)
    local_points = LOCAL_MAX_POINTS * (local["utility"] if local else 0.0)
    rail_points = RAIL_MAX_POINTS * (rail["utility"] if rail else 0.0)
    score = max(0.0, min(10.0, local_points + rail_points))

    return {
        "local_access_points": local_points,
        "rail_access_points": rail_points,
        "local_scoring_radius_m": int(LOCAL_SCORING_RADIUS_M),
        "rail_scoring_radius_m": int(RAIL_SCORING_RADIUS_M),
        "best_local_logical_stop_id": _winner_value(local, "logical_stop_id"),
        "best_local_name": _winner_value(local, "name"),
        "best_local_distance_m": _winner_value(local, "distance_m"),
        "best_local_weekday_departures": _winner_value(local, "weekday_departures"),
        "best_local_saturday_departures": _winner_value(local, "saturday_departures"),
        "best_local_sunday_departures": _winner_value(local, "sunday_departures"),
        "best_local_seven_day_average": _winner_value(local, "seven_day_average"),
        "best_local_service_factor": _winner_value(local, "service_factor"),
        "best_local_distance_factor": _winner_value(local, "distance_factor"),
        "best_local_utility": _winner_value(local, "utility"),
        "best_local_operators": _winner_value(local, "operators"),
        "best_local_modes": _winner_value(local, "modes"),
        "best_rail_logical_station_id": _winner_value(rail, "logical_station_id"),
        "best_rail_name": _winner_value(rail, "name"),
        "best_rail_uic_code": _winner_value(rail, "uic_code"),
        "best_rail_distance_m": _winner_value(rail, "distance_m"),
        "best_rail_weekday_departures": _winner_value(rail, "weekday_departures"),
        "best_rail_saturday_departures": _winner_value(rail, "saturday_departures"),
        "best_rail_sunday_departures": _winner_value(rail, "sunday_departures"),
        "best_rail_seven_day_average": _winner_value(rail, "seven_day_average"),
        "best_rail_service_factor": _winner_value(rail, "service_factor"),
        "best_rail_distance_factor": _winner_value(rail, "distance_factor"),
        "best_rail_utility": _winner_value(rail, "utility"),
        "score": score,
        "public_score": round(score, 1),
    }

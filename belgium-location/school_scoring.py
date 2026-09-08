"""Dependency-free School Score V1 classification, deduplication and scoring."""

import math
import re
import unicodedata


SCHOOL_SCORING_RADIUS_M = 2500
SCHOOL_NAMED_DEDUP_DISTANCE_M = 75
SCHOOL_UNNAMED_DEDUP_DISTANCE_M = 10
SCHOOL_TYPE_WEIGHTS = {"school": 1.0, "kindergarten": 0.4}


def haversine_m(lat1, lon1, lat2, lon2):
    """Straight-line distance in metres between latitude/longitude points."""
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dlat = p2 - p1
    dlon = math.radians(float(lon2) - float(lon1))
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _is_missing(value):
    if value is None:
        return True
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def normalize_school_name(name):
    """Normalize Unicode, case and whitespace while preserving exact wording."""
    if _is_missing(name):
        return None
    normalized = unicodedata.normalize("NFKC", str(name)).casefold().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized or None


def school_type(poi):
    """Return the explicitly included School V1 amenity type, if any."""
    amenity = poi.get("amenity")
    return amenity if amenity in SCHOOL_TYPE_WEIGHTS else None


def _within_threshold(distance, threshold):
    """Compare metre distances with only floating-point boundary tolerance."""
    return distance <= threshold or math.isclose(
        distance, threshold, rel_tol=1e-12, abs_tol=1e-9)


def _stable_row_key(row):
    intrinsic_fields = tuple(sorted(
        (str(key), type(value).__name__, repr(value))
        for key, value in row.items()
    ))
    return (
        school_type(row) or "",
        normalize_school_name(row.get("name")) or "",
        float(row["lat"]),
        float(row["lon"]),
        str(row.get("source") or ""),
        str(row.get("name") or ""),
        str(row.get("brand") or ""),
        str(row.get("school_level") or ""),
        str(row.get("isced_level") or ""),
        float(row.get("d_lin", math.inf)),
        intrinsic_fields,
    )


def _merge_group(rows, indexes, threshold_m):
    """Create deterministic complete-link clusters within one exact group.

    Requiring a new member to be within the threshold of every existing member
    avoids transitive chain merges in which the endpoints are too far apart.
    """
    clusters = []
    for index in sorted(indexes, key=lambda i: _stable_row_key(rows[i])):
        for cluster in clusters:
            if all(_within_threshold(haversine_m(
                    rows[index]["lat"], rows[index]["lon"],
                    rows[member]["lat"], rows[member]["lon"]), threshold_m)
                   for member in cluster):
                cluster.append(index)
                break
        else:
            clusters.append([index])
    return clusters


def _spatial_cell(row, cell_size_m):
    """Return an Earth-centred Cartesian cell safe for neighbor searches."""
    lat = math.radians(float(row["lat"]))
    lon = math.radians(float(row["lon"]))
    radius = 6371000.0
    x = radius * math.cos(lat) * math.cos(lon)
    y = radius * math.cos(lat) * math.sin(lon)
    z = radius * math.sin(lat)
    return tuple(math.floor(axis / cell_size_m) for axis in (x, y, z))


def _merge_unnamed_group(rows, indexes):
    """Complete-link cluster unnamed rows using a small spatial index."""
    clusters = []
    leader_cells = {}
    for index in sorted(indexes, key=lambda i: _stable_row_key(rows[i])):
        x_cell, y_cell, z_cell = _spatial_cell(
            rows[index], SCHOOL_UNNAMED_DEDUP_DISTANCE_M)
        candidates = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    candidates.update(leader_cells.get(
                        (x_cell + dx, y_cell + dy, z_cell + dz), []))
        for cluster_index in sorted(candidates):
            cluster = clusters[cluster_index]
            if all(_within_threshold(haversine_m(
                    rows[index]["lat"], rows[index]["lon"],
                    rows[member]["lat"], rows[member]["lon"]),
                    SCHOOL_UNNAMED_DEDUP_DISTANCE_M) for member in cluster):
                cluster.append(index)
                break
        else:
            cluster_index = len(clusters)
            clusters.append([index])
            leader_cells.setdefault((x_cell, y_cell, z_cell), []).append(cluster_index)
    return clusters


def deduplicate_school_pois(pois):
    """Deduplicate included School V1 POIs within exact amenity/name groups."""
    rows = [dict(poi) for poi in pois if school_type(poi)]
    named_groups = {}
    unnamed_groups = {}
    for index, row in enumerate(rows):
        amenity = school_type(row)
        name = normalize_school_name(row.get("name"))
        groups = named_groups if name else unnamed_groups
        groups.setdefault((amenity, name), []).append(index)

    clusters = []
    for key in sorted(named_groups):
        clusters.extend(_merge_group(
            rows, named_groups[key], SCHOOL_NAMED_DEDUP_DISTANCE_M))
    for key in sorted(unnamed_groups, key=lambda value: value[0]):
        clusters.extend(_merge_unnamed_group(rows, unnamed_groups[key]))

    result = []
    for cluster in clusters:
        closest = min(cluster, key=lambda i: (
            float(rows[i].get("d_lin", math.inf)), _stable_row_key(rows[i])))
        result.append(dict(rows[closest]))
    return result


def school_score_components(pois):
    """Return School Score V1 components for already deduplicated POIs."""
    nearest_school_distance = None
    effective_choice = 0.0
    scoring_population_size = 0

    for poi in pois:
        amenity = school_type(poi)
        distance = float(poi.get("d_lin", math.inf))
        if amenity is None or distance < 0 or distance > SCHOOL_SCORING_RADIUS_M:
            continue
        scoring_population_size += 1
        distance_factor = max(0.0, 1.0 - distance / SCHOOL_SCORING_RADIUS_M)
        effective_choice += SCHOOL_TYPE_WEIGHTS[amenity] * distance_factor
        if amenity == "school" and (
                nearest_school_distance is None or distance < nearest_school_distance):
            nearest_school_distance = distance

    proximity_points = 0.0
    if nearest_school_distance is not None:
        proximity_points = 7.0 * max(
            0.0, 1.0 - nearest_school_distance / SCHOOL_SCORING_RADIUS_M)
    choice_points = min(effective_choice, 3.0)
    score = max(0.0, min(10.0, proximity_points + choice_points))
    return {
        "proximity_points": proximity_points,
        "choice_points": choice_points,
        "effective_choice": effective_choice,
        "score": score,
        "nearest_school_distance": nearest_school_distance,
        "scoring_population_size": scoring_population_size,
    }


def calc_school_score(pois):
    """Calculate School Score V1 for already deduplicated School POIs."""
    return school_score_components(pois)["score"]

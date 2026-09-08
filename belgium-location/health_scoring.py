"""Dependency-free Health Score V1 classification, deduplication and scoring."""

import math
import re
import unicodedata


HEALTH_LOCAL_RADIUS_M = 2500
HEALTH_HOSPITAL_RADIUS_M = 20000
HEALTH_NAMED_DEDUP_DISTANCE_M = 75
HEALTH_UNNAMED_DEDUP_DISTANCE_M = 10
HEALTH_LOCAL_TYPE_WEIGHTS = {"clinical_care": 1.0, "pharmacy": 0.4}


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


def _tag(value):
    return None if _is_missing(value) or not str(value).strip() else str(value).strip()


def normalize_health_name(name):
    """Normalize Unicode, case and whitespace while preserving exact wording."""
    if _is_missing(name):
        return None
    normalized = unicodedata.normalize("NFKC", str(name)).casefold().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized or None


def health_local_class(poi):
    """Return the Health V1 local scoring class, if the row qualifies."""
    amenity = _tag(poi.get("amenity"))
    healthcare = _tag(poi.get("healthcare"))
    if amenity == "hospital" or healthcare == "hospital":
        return None
    if amenity == "pharmacy" or healthcare == "pharmacy":
        return "pharmacy"
    if amenity in {"doctors", "clinic"} or healthcare in {"doctor", "clinic"}:
        return "clinical_care"
    return None


def health_local_subtype(poi):
    """Return a conservative subtype for unnamed duplicate matching."""
    local_class = health_local_class(poi)
    if local_class == "pharmacy":
        return "pharmacy"
    if local_class != "clinical_care":
        return None
    amenity = _tag(poi.get("amenity"))
    healthcare = _tag(poi.get("healthcare"))
    if amenity == "clinic" or healthcare == "clinic":
        return "clinic"
    return "doctor"


def qualifies_hospital(poi):
    """Apply the deliberately narrow Health V1 hospital qualification rule."""
    amenity = _tag(poi.get("amenity"))
    healthcare = _tag(poi.get("healthcare"))
    return healthcare == "hospital" or (amenity == "hospital" and healthcare is None)


def _within_threshold(distance, threshold):
    return distance <= threshold or math.isclose(
        distance, threshold, rel_tol=1e-12, abs_tol=1e-9)


def _stable_row_key(row):
    intrinsic_fields = tuple(sorted(
        (str(key), type(value).__name__, repr(value))
        for key, value in row.items()
    ))
    return (
        health_local_class(row) or "",
        health_local_subtype(row) or "",
        normalize_health_name(row.get("name")) or "",
        float(row["lat"]),
        float(row["lon"]),
        str(row.get("source") or ""),
        str(row.get("name") or ""),
        float(row.get("d_lin", math.inf)),
        intrinsic_fields,
    )


def _merge_group(rows, indexes, threshold_m):
    """Build deterministic complete-link clusters for one exact group."""
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
    """Complete-link cluster unnamed rows using neighboring spatial cells."""
    clusters = []
    leader_cells = {}
    for index in sorted(indexes, key=lambda i: _stable_row_key(rows[i])):
        cell = _spatial_cell(rows[index], HEALTH_UNNAMED_DEDUP_DISTANCE_M)
        candidates = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    neighbor = (cell[0] + dx, cell[1] + dy, cell[2] + dz)
                    candidates.update(leader_cells.get(neighbor, []))
        for cluster_index in sorted(candidates):
            cluster = clusters[cluster_index]
            if all(_within_threshold(haversine_m(
                    rows[index]["lat"], rows[index]["lon"],
                    rows[member]["lat"], rows[member]["lon"]),
                    HEALTH_UNNAMED_DEDUP_DISTANCE_M) for member in cluster):
                cluster.append(index)
                break
        else:
            cluster_index = len(clusters)
            clusters.append([index])
            leader_cells.setdefault(cell, []).append(cluster_index)
    return clusters


def deduplicate_health_local_pois(pois):
    """Deduplicate only clinical-care and pharmacy scoring populations."""
    rows = [dict(poi) for poi in pois if health_local_class(poi)]
    named_groups = {}
    unnamed_groups = {}
    for index, row in enumerate(rows):
        local_class = health_local_class(row)
        name = normalize_health_name(row.get("name"))
        if name:
            named_groups.setdefault((local_class, name), []).append(index)
        else:
            unnamed_groups.setdefault(health_local_subtype(row), []).append(index)

    clusters = []
    for key in sorted(named_groups):
        clusters.extend(_merge_group(
            rows, named_groups[key], HEALTH_NAMED_DEDUP_DISTANCE_M))
    for key in sorted(unnamed_groups):
        clusters.extend(_merge_unnamed_group(rows, unnamed_groups[key]))

    result = []
    for cluster in clusters:
        closest = min(cluster, key=lambda i: (
            float(rows[i].get("d_lin", math.inf)), _stable_row_key(rows[i])))
        result.append(dict(rows[closest]))
    return result


def health_score_components(local_pois, hospital_pois):
    """Return Health Score V1 components for local and hospital populations."""
    nearest_clinical = None
    effective_choice = 0.0
    clinical_count = 0
    pharmacy_count = 0

    for poi in local_pois:
        local_class = health_local_class(poi)
        distance = float(poi.get("d_lin", math.inf))
        if (local_class is None or distance < 0
                or not _within_threshold(distance, HEALTH_LOCAL_RADIUS_M)):
            continue
        distance_factor = max(0.0, 1.0 - distance / HEALTH_LOCAL_RADIUS_M)
        effective_choice += HEALTH_LOCAL_TYPE_WEIGHTS[local_class] * distance_factor
        if local_class == "clinical_care":
            clinical_count += 1
            if nearest_clinical is None or distance < nearest_clinical:
                nearest_clinical = distance
        else:
            pharmacy_count += 1

    clinical_proximity_points = 0.0
    if nearest_clinical is not None:
        clinical_proximity_points = 5.0 * max(
            0.0, 1.0 - nearest_clinical / HEALTH_LOCAL_RADIUS_M)
    choice_points = min(effective_choice, 3.0)

    qualifying_hospitals = [
        float(poi.get("d_lin", math.inf)) for poi in hospital_pois
        if qualifies_hospital(poi)
        and 0 <= float(poi.get("d_lin", math.inf))
        and _within_threshold(
            float(poi.get("d_lin", math.inf)), HEALTH_HOSPITAL_RADIUS_M)
    ]
    nearest_hospital = min(qualifying_hospitals, default=None)
    hospital_points = 0.0
    if nearest_hospital is not None:
        hospital_points = 2.0 * max(
            0.0, 1.0 - nearest_hospital / HEALTH_HOSPITAL_RADIUS_M)

    score = max(0.0, min(
        10.0, clinical_proximity_points + choice_points + hospital_points))
    return {
        "clinical_proximity_points": clinical_proximity_points,
        "choice_points": choice_points,
        "hospital_points": hospital_points,
        "effective_choice": effective_choice,
        "score": score,
        "nearest_clinical_m": nearest_clinical,
        "nearest_hospital_m": nearest_hospital,
        "clinical_count": clinical_count,
        "pharmacy_count": pharmacy_count,
    }


def calc_health_score(local_pois, hospital_pois):
    """Calculate Health Score V1 from already deduplicated local POIs."""
    return health_score_components(local_pois, hospital_pois)["score"]

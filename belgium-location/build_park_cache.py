"""Build the canonical Park Destination Cache V1 from Belgian OSM data.

The cache contains eligibility and hierarchy metadata only.  It deliberately
does not calculate an address score or depend on the generic POI caches.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import tempfile

from geographiclib.geodesic import Geodesic
from shapely import wkb
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon
from shapely.strtree import STRtree
from shapely.validation import make_valid


BUILDER_VERSION = "park-cache-v1"
POSITIVE_ACCESS = frozenset({"yes", "permissive", "public", "designated"})
EXCLUDED_ACCESS = frozenset({"private", "no", "customers"})
PARENT_CLASSES = frozenset(
    {"park", "nature_reserve", "national_park", "protected_area"})
CONDITIONAL_CLASSES = frozenset(
    {"garden", "leisure_recreation", "landuse_recreation", "village_green",
     "protected_area"})
SECONDARY_CLASSES = frozenset({"playground", "dog_park"})
SECONDARY_WEIGHTS = {"playground": 1.0, "dog_park": 0.6}
BASE_CONFIDENCE = {
    "park": 0.93,
    "garden": 0.78,
    "leisure_recreation": 0.80,
    "landuse_recreation": 0.75,
    "village_green": 0.65,
    "nature_reserve": 0.70,
    "national_park": 0.58,
    "protected_area": 0.60,
}
MISSING_ACCESS_CAP = {
    "nature_reserve": 0.50,
    "national_park": 0.35,
    "protected_area": 0.35,
}
OSM_TYPE_ORDER = {"node": 0, "way": 1, "relation": 2}
FILTER_TAGS = (
    ("leisure", "park"),
    ("leisure", "garden"),
    ("leisure", "recreation_ground"),
    ("leisure", "nature_reserve"),
    ("leisure", "playground"),
    ("leisure", "dog_park"),
    ("landuse", "village_green"),
    ("landuse", "recreation_ground"),
    ("boundary", "national_park"),
    ("boundary", "protected_area"),
)


def normalized_access(value):
    return str(value or "").strip().casefold()


def has_positive_access(tags):
    return normalized_access(tags.get("access")) in POSITIVE_ACCESS


def source_classes(tags):
    """Return every Park V1 source class present on an OSM object."""
    classes = []
    leisure = tags.get("leisure")
    landuse = tags.get("landuse")
    boundary = tags.get("boundary")
    if leisure == "park":
        classes.append("park")
    if leisure == "garden":
        classes.append("garden")
    if leisure == "recreation_ground":
        classes.append("leisure_recreation")
    if leisure == "nature_reserve":
        classes.append("nature_reserve")
    if leisure == "playground":
        classes.append("playground")
    if leisure == "dog_park":
        classes.append("dog_park")
    if landuse == "recreation_ground":
        classes.append("landuse_recreation")
    if landuse == "village_green":
        classes.append("village_green")
    if boundary == "national_park":
        classes.append("national_park")
    if boundary == "protected_area":
        classes.append("protected_area")
    return classes


def size_factor(area_m2):
    """Return the locked, diminishing-return polygon size factor."""
    if area_m2 is None:
        return None
    area_m2 = float(area_m2)
    if area_m2 < 500:
        return 0.45
    if area_m2 < 2_000:
        return 0.60
    if area_m2 < 10_000:
        return 0.75
    if area_m2 < 50_000:
        return 0.90
    return 1.0


def confidence_for(park_class, tags):
    """Return ``(base, final)`` using the locked operation order.

    Evidence bonuses are added to the base and clamped to one first.  A
    missing-positive-access reserve cap is then applied, which is intentionally
    different from capping the base before adding evidence bonuses.
    """
    if park_class not in BASE_CONFIDENCE:
        return None, None
    base = BASE_CONFIDENCE[park_class]
    value = base
    if has_positive_access(tags):
        value += 0.07
    if tags.get("operator"):
        value += 0.04
    if tags.get("opening_hours"):
        value += 0.03
    if tags.get("name"):
        value += 0.01
    value = min(1.0, value)
    if not has_positive_access(tags) and park_class in MISSING_ACCESS_CAP:
        value = min(value, MISSING_ACCESS_CAP[park_class])
    return base, value


def secondary_metadata(classes, tags):
    """Return the retained secondary class, weight and confidence."""
    candidates = [item for item in ("playground", "dog_park") if item in classes]
    if not candidates:
        return None, None, None
    if not (has_positive_access(tags) or tags.get("name")
            or tags.get("operator") or tags.get("opening_hours")):
        return None, None, None
    secondary_class = candidates[0]
    return (secondary_class, SECONDARY_WEIGHTS[secondary_class],
            0.95 if has_positive_access(tags) else 0.80)


def classify_polygon(classes, tags, area_m2):
    """Return ``(class, tier, reason)`` for an eligible polygon."""
    classes = set(classes)
    positive = has_positive_access(tags)
    named = bool(tags.get("name"))
    operated = bool(tags.get("operator"))
    hours = bool(tags.get("opening_hours"))
    area = float(area_m2 or 0)

    if "park" in classes:
        return "park", "primary", "leisure=park polygon"
    if "national_park" in classes:
        return "national_park", "primary", "boundary=national_park polygon"
    if "nature_reserve" in classes:
        return "nature_reserve", "primary", "leisure=nature_reserve polygon"
    if "garden" in classes and (
            (area >= 200 and positive)
            or (area >= 500 and named and (operated or hours))):
        return "garden", "conditional", "qualifying public/recreational garden"
    if "leisure_recreation" in classes and area >= 500 and named:
        return ("leisure_recreation", "conditional",
                "named leisure=recreation_ground with area >= 500 m2")
    if ("landuse_recreation" in classes and area >= 1_000
            and (named or positive)):
        return ("landuse_recreation", "conditional",
                "landuse=recreation_ground with area/evidence")
    if "village_green" in classes and area >= 500 and (named or positive):
        return ("village_green", "conditional",
                "landuse=village_green with area/evidence")
    if ("protected_area" in classes and area >= 10_000 and named
            and (positive or operated)):
        return ("protected_area", "conditional",
                "named protected area with area/access/operator evidence")
    return None, None, None


def _ring_geodesic_area(coordinates):
    polygon = Geodesic.WGS84.Polygon()
    for lon, lat in coordinates:
        polygon.AddPoint(float(lat), float(lon))
    return abs(polygon.Compute(False, True)[2])


def geodesic_area_m2(geometry):
    """Calculate polygon area using GeographicLib/WGS84, as calibrated."""
    if isinstance(geometry, Polygon):
        area = _ring_geodesic_area(geometry.exterior.coords)
        area -= sum(_ring_geodesic_area(ring.coords)
                    for ring in geometry.interiors)
        return max(0.0, area)
    if isinstance(geometry, MultiPolygon):
        return sum(geodesic_area_m2(part) for part in geometry.geoms)
    return None


def _polygonal_only(geometry):
    if isinstance(geometry, (Polygon, MultiPolygon)):
        return geometry
    if isinstance(geometry, GeometryCollection):
        polygons = [part for part in geometry.geoms
                    if isinstance(part, (Polygon, MultiPolygon))]
        flattened = []
        for item in polygons:
            flattened.extend(item.geoms if isinstance(item, MultiPolygon) else [item])
        if not flattened:
            return None
        return flattened[0] if len(flattened) == 1 else MultiPolygon(flattened)
    return None


def normalize_geometry(geometry, *, allow_point=False):
    """Return a conservative valid geometry and its original validity flag."""
    if geometry is None or geometry.is_empty:
        return None, False
    original_valid = bool(geometry.is_valid)
    if isinstance(geometry, Point):
        return (geometry, original_valid) if allow_point else (None, original_valid)
    if not isinstance(geometry, (Polygon, MultiPolygon)):
        return None, original_valid
    if not original_valid:
        try:
            geometry = _polygonal_only(make_valid(geometry))
        except Exception:
            return None, False
    if geometry is None or geometry.is_empty or not geometry.is_valid:
        return None, False
    return geometry, original_valid


def stable_park_id(osm_type, osm_id):
    return f"park:osm:{osm_type}:{int(osm_id)}"


def strong_identity(tags, canonical_osm_type, canonical_osm_id):
    if tags.get("wikidata"):
        return "wikidata", tags["wikidata"]
    if tags.get("wikipedia"):
        return "wikipedia", tags["wikipedia"]
    return "osm", f"{canonical_osm_type}:{int(canonical_osm_id)}"


def display_name_for(row):
    """Return conservative presentation metadata without affecting identity."""
    canonical_name = str(row.get("name") or "").strip()
    if canonical_name:
        return canonical_name, "canonical"
    absorbed_keys = set(row.get("absorbed_member_osm_keys") or ())
    absorbed_names = set()
    for representation in json.loads(row["source_representations_json"]):
        key = _key_text((representation["osm_type"], representation["osm_id"]))
        if key not in absorbed_keys:
            continue
        name = str((representation.get("tags") or {}).get("name") or "").strip()
        if name:
            absorbed_names.add(name)
    if len(absorbed_names) == 1:
        return absorbed_names.pop(), "absorbed_member"
    if absorbed_names:
        return None, "ambiguous"
    return None, "none"


def _key(feature):
    return str(feature["osm_type"]), int(feature["osm_id"])


def _key_text(key):
    return f"{key[0]}:{key[1]}"


def _source_precedence(classes):
    for value in (
            "park", "national_park", "nature_reserve", "garden",
            "leisure_recreation", "landuse_recreation", "village_green",
            "protected_area", "playground", "dog_park"):
        if value in classes:
            return value
    return None


def _make_row(feature, source_path, source_size):
    tags = dict(feature.get("tags") or {})
    access = normalized_access(tags.get("access"))
    if access in EXCLUDED_ACCESS:
        return None, "restricted_access"
    classes = source_classes(tags)
    if not classes:
        return None, "not_candidate"
    is_node = feature["osm_type"] == "node"
    geometry, original_valid = normalize_geometry(
        feature.get("geometry"), allow_point=is_node)
    if geometry is None:
        return None, "geometry_failure"

    if is_node:
        area = None
        primary_class = primary_tier = primary_reason = None
    else:
        area = feature.get("area_m2")
        if area is None:
            area = geodesic_area_m2(geometry)
        primary_class, primary_tier, primary_reason = classify_polygon(
            classes, tags, area)
    secondary_class, secondary_weight, secondary_confidence = secondary_metadata(
        classes, tags)
    if primary_class is None and secondary_class is None:
        return None, "eligibility_failure"

    park_class = primary_class or secondary_class or _source_precedence(classes)
    tier_parts = []
    if primary_tier == "primary":
        tier_parts.append("primary")
    elif primary_tier == "conditional":
        tier_parts.append("conditional")
    if secondary_class:
        tier_parts.append("secondary")
    eligibility_tier = "+".join(tier_parts)
    base_confidence, final_confidence = confidence_for(primary_class, tags)
    rp = geometry.representative_point()
    centroid = geometry.centroid
    min_lon, min_lat, max_lon, max_lat = geometry.bounds
    osm_type, osm_id = _key(feature)
    park_id = stable_park_id(osm_type, osm_id)
    reason_parts = [part for part in (primary_reason,
                    f"eligible {secondary_class}" if secondary_class else None) if part]
    return {
        "park_id": park_id,
        "osm_type": osm_type,
        "osm_id": osm_id,
        "canonical_osm_type": osm_type,
        "canonical_osm_id": osm_id,
        "parent_relation_id": None,
        "member_of_relation_ids": [],
        "parent_park_id": None,
        "attachment_method": None,
        "containment_ratio": None,
        "strong_identity_type": None,
        "strong_identity_value": None,
        "wikidata": tags.get("wikidata"),
        "wikipedia": tags.get("wikipedia"),
        "name": tags.get("name"),
        "display_name": None,
        "display_name_source": None,
        "park_class": park_class,
        "source_classes": sorted(classes),
        "eligibility_tier": eligibility_tier,
        "is_primary": primary_tier == "primary",
        "is_conditional": primary_tier == "conditional",
        "is_secondary": bool(secondary_class),
        "counts_as_primary": primary_class is not None,
        "counts_as_choice": False,
        "counts_as_secondary": secondary_class is not None,
        "eligibility_reason": "; ".join(reason_parts),
        "access": tags.get("access"),
        "has_positive_access": has_positive_access(tags),
        "operator": tags.get("operator"),
        "opening_hours": tags.get("opening_hours"),
        "area_m2": area,
        "geometry_wkb": geometry.wkb,
        "geometry_valid": bool(geometry.is_valid),
        "geometry_was_valid": original_valid,
        "centroid_lat": float(centroid.y),
        "centroid_lon": float(centroid.x),
        "representative_lat": float(rp.y),
        "representative_lon": float(rp.x),
        "bbox_min_lat": float(min_lat),
        "bbox_min_lon": float(min_lon),
        "bbox_max_lat": float(max_lat),
        "bbox_max_lon": float(max_lon),
        "leisure": tags.get("leisure"),
        "landuse": tags.get("landuse"),
        "boundary": tags.get("boundary"),
        "protect_class": tags.get("protect_class"),
        "size_factor": size_factor(area) if primary_class else None,
        "base_confidence": base_confidence,
        "final_confidence": final_confidence,
        "secondary_class": secondary_class,
        "secondary_type_weight": secondary_weight,
        "secondary_confidence": secondary_confidence,
        "absorbed_member_osm_keys": [],
        "source_osm_keys": [_key_text((osm_type, osm_id))],
        "source_representations_json": json.dumps([{
            "osm_type": osm_type, "osm_id": osm_id, "tags": tags,
        }], ensure_ascii=False, sort_keys=True),
        "source_path": str(source_path),
        "source_size_bytes": int(source_size),
        "builder_version": BUILDER_VERSION,
        "_geometry": geometry,
        "_tags": tags,
        "_primary_class": primary_class,
    }, None


def _relation_ownership(rows_by_key, relation_memberships):
    """Absorb duplicate tagged member representations into relation owners."""
    suppressed = set()
    for relation_key in sorted(relation_memberships,
                               key=lambda key: (key[1], key[0])):
        owner = rows_by_key.get(relation_key)
        if owner is None or not owner["counts_as_primary"]:
            continue
        for member_key, role in relation_memberships[relation_key]:
            member = rows_by_key.get(member_key)
            if member is None:
                continue
            member["member_of_relation_ids"].append(relation_key[1])
            same_representation = (
                role in {"", "outer"}
                and member["counts_as_primary"]
            )
            if not same_representation:
                continue
            suppressed.add(member_key)
            key_text = _key_text(member_key)
            owner["absorbed_member_osm_keys"].append(key_text)
            owner["source_osm_keys"].append(key_text)
            representations = json.loads(owner["source_representations_json"])
            representations.extend(json.loads(member["source_representations_json"]))
            owner["source_representations_json"] = json.dumps(
                representations, ensure_ascii=False, sort_keys=True)
            if not owner["wikidata"] and member["wikidata"]:
                owner["wikidata"] = member["wikidata"]
            if not owner["wikipedia"] and member["wikipedia"]:
                owner["wikipedia"] = member["wikipedia"]
    for key in suppressed:
        rows_by_key.pop(key, None)
    for row in rows_by_key.values():
        row["absorbed_member_osm_keys"] = sorted(set(
            row["absorbed_member_osm_keys"]))
        row["source_osm_keys"] = sorted(set(row["source_osm_keys"]))
        row["member_of_relation_ids"] = sorted(set(row["member_of_relation_ids"]))
    return len(suppressed)


def _attach_children(rows, relation_memberships, threshold=0.80):
    rows_by_key = {(_key(row)): row for row in rows}
    parents = [row for row in rows if row["counts_as_primary"]
               and row["park_class"] in PARENT_CLASSES]
    parent_keys = {_key(row) for row in parents}
    tree = STRtree([row["_geometry"] for row in parents]) if parents else None
    attached = 0

    for child in sorted(rows, key=lambda row: (
            OSM_TYPE_ORDER[row["osm_type"]], row["osm_id"])):
        is_conditional_child = child["is_conditional"]
        is_secondary_child = child["counts_as_secondary"]
        if not (is_conditional_child or is_secondary_child):
            continue
        child_key = _key(child)
        explicit = []
        for relation_key, members in relation_memberships.items():
            if relation_key not in parent_keys:
                continue
            for member_key, role in members:
                if member_key == child_key:
                    explicit.append((rows_by_key[relation_key], role))
                    break
        chosen = None
        ratio = None
        method = None
        if explicit:
            chosen = min((item[0] for item in explicit), key=lambda row: (
                float(row["area_m2"] or math.inf), row["park_id"]))
            ratio = 1.0
            method = "relation"
        elif tree is not None:
            candidates = []
            child_geometry = child["_geometry"]
            for index in tree.query(child_geometry):
                parent = parents[int(index)]
                if parent is child:
                    continue
                if isinstance(child_geometry, Point):
                    child_ratio = 1.0 if parent["_geometry"].covers(
                        child_geometry) else 0.0
                else:
                    denominator = float(geodesic_area_m2(child_geometry) or 0.0)
                    intersection = _polygonal_only(child_geometry.intersection(
                        parent["_geometry"]))
                    intersection_area = (geodesic_area_m2(intersection)
                                         if intersection is not None else 0.0)
                    child_ratio = (intersection_area / denominator
                                   if denominator else 0.0)
                if child_ratio >= threshold or math.isclose(
                        child_ratio, threshold, abs_tol=1e-12):
                    candidates.append((child_ratio, parent))
            if candidates:
                ratio, chosen = min(candidates, key=lambda item: (
                    -item[0], float(item[1]["area_m2"] or math.inf),
                    item[1]["park_id"]))
                method = "spatial"
        if chosen is None:
            continue
        child["parent_park_id"] = chosen["park_id"]
        child["attachment_method"] = method
        child["containment_ratio"] = ratio
        if method == "relation":
            child["parent_relation_id"] = chosen["osm_id"]
        if is_conditional_child:
            child["counts_as_primary"] = False
            child["eligibility_reason"] += "; attached conditional child"
        attached += 1
    return attached


def build_destination_rows(features, relation_memberships=None, *,
                           source_path="synthetic", source_size=0):
    """Build canonical cache rows from copied feature dictionaries.

    This pure entry point is also used by the synthetic regression tests.
    ``relation_memberships`` maps relation keys to ``[(member_key, role), ...]``.
    """
    relation_memberships = relation_memberships or {}
    diagnostics = Counter()
    rows_by_key = {}
    for feature in features:
        row, rejected = _make_row(feature, source_path, source_size)
        if row is None:
            diagnostics[rejected] += 1
            continue
        rows_by_key[_key(feature)] = row
    diagnostics["relation_member_suppressed"] = _relation_ownership(
        rows_by_key, relation_memberships)
    rows = list(rows_by_key.values())
    diagnostics["attached_child_count"] = _attach_children(
        rows, relation_memberships, 0.80)

    for row in rows:
        row["display_name"], row["display_name_source"] = display_name_for(row)
        identity_type, identity_value = strong_identity(
            {"wikidata": row["wikidata"], "wikipedia": row["wikipedia"]},
            row["canonical_osm_type"], row["canonical_osm_id"])
        row["strong_identity_type"] = identity_type
        row["strong_identity_value"] = identity_value
        row["counts_as_choice"] = bool(
            row["counts_as_primary"]
            and (row["name"] or identity_type in {"wikidata", "wikipedia"}))
        row["park_id"] = stable_park_id(
            row["canonical_osm_type"], row["canonical_osm_id"])

    rows.sort(key=lambda row: (
        OSM_TYPE_ORDER[row["osm_type"]], row["osm_id"]))
    diagnostics["row_count"] = len(rows)
    diagnostics["primary_count"] = sum(row["is_primary"] for row in rows)
    diagnostics["conditional_count"] = sum(row["is_conditional"] for row in rows)
    diagnostics["secondary_count"] = sum(row["is_secondary"] for row in rows)
    diagnostics["counts_as_primary"] = sum(
        row["counts_as_primary"] for row in rows)
    diagnostics["counts_as_choice"] = sum(
        row["counts_as_choice"] for row in rows)
    diagnostics["counts_as_secondary"] = sum(
        row["counts_as_secondary"] for row in rows)
    diagnostics["node_secondary_count"] = sum(
        row["osm_type"] == "node" and row["counts_as_secondary"] for row in rows)

    public_rows = []
    for row in rows:
        public_rows.append({key: value for key, value in row.items()
                            if not key.startswith("_")})
    return public_rows, dict(diagnostics)


def _tags_dict(osm_object):
    return {tag.k: tag.v for tag in osm_object.tags}


def collect_osm_features(input_path):
    """Collect candidate point/area geometries from an OSM PBF/XML file."""
    import osmium

    input_path = Path(input_path)
    handle, filtered_name = tempfile.mkstemp(
        prefix="park-cache-candidates-", suffix=".pbf")
    os.close(handle)
    os.unlink(filtered_name)
    filtered_path = Path(filtered_name)
    relation_memberships = defaultdict(list)
    candidate_keys = set()
    candidate_metadata = {}
    source_counts = Counter()
    try:
        tag_filter = osmium.filter.TagFilter(*FILTER_TAGS)
        with osmium.BackReferenceWriter(
                str(filtered_path), str(input_path), overwrite=True,
                remove_tags=False, relation_depth=1) as writer:
            for obj in osmium.FileProcessor(str(input_path)).with_filter(tag_filter):
                tags = _tags_dict(obj)
                if not source_classes(tags):
                    continue
                osm_type = ("node" if isinstance(obj, osmium.osm.Node)
                            else "way" if isinstance(obj, osmium.osm.Way)
                            else "relation")
                key = (osm_type, int(obj.id))
                candidate_keys.add(key)
                candidate_metadata[key] = {
                    "name": tags.get("name"),
                    "source_classes": source_classes(tags),
                    "type": tags.get("type"),
                    "area": normalized_access(tags.get("area")),
                    "closed": (bool(obj.is_closed())
                               if osm_type == "way" else None),
                }
                source_counts[f"source_{osm_type}_count"] += 1
                if osm_type == "relation":
                    for member in obj.members:
                        member_type = {"n": "node", "w": "way", "r": "relation"}[
                            member.type]
                        relation_memberships[key].append((
                            (member_type, int(member.ref)), member.role or ""))
                writer.add(obj)

        features = {}
        errors = set()
        area_keys = set()
        factory = osmium.geom.WKBFactory()

        class GeometryHandler(osmium.SimpleHandler):
            def node(self, node):
                tags = _tags_dict(node)
                if source_classes(tags) and node.location.valid():
                    features[("node", int(node.id))] = {
                        "osm_type": "node", "osm_id": int(node.id),
                        "tags": tags,
                        "geometry": Point(float(node.location.lon),
                                          float(node.location.lat)),
                    }

            def area(self, area):
                tags = _tags_dict(area)
                if not source_classes(tags):
                    return
                osm_type = "way" if area.from_way() else "relation"
                key = (osm_type, int(area.orig_id()))
                try:
                    geometry = wkb.loads(factory.create_multipolygon(area), hex=True)
                    geometry, _was_valid = normalize_geometry(geometry)
                    if geometry is None:
                        errors.add(key)
                        return
                    features[key] = {
                        "osm_type": osm_type, "osm_id": key[1],
                        "tags": tags, "geometry": geometry,
                        "area_m2": geodesic_area_m2(geometry),
                    }
                    area_keys.add(key)
                except Exception:
                    errors.add(key)

        GeometryHandler().apply_file(
            str(filtered_path), locations=True, idx="flex_mem")
        non_area_ways = {
            key for key in candidate_keys
            if key[0] == "way" and key not in area_keys
        }
        open_ways = {
            key for key in non_area_ways
            if not candidate_metadata[key]["closed"]
        }
        closed_area_no = {
            key for key in non_area_ways
            if candidate_metadata[key]["closed"]
            and candidate_metadata[key]["area"] == "no"
        }
        other_closed_non_area = non_area_ways - open_ways - closed_area_no
        missing_relations = {
            key for key in candidate_keys
            if key[0] == "relation" and key not in area_keys
        }
        failed_geometry_keys = errors | missing_relations | other_closed_non_area
        rejection_rows = []
        for key in sorted(failed_geometry_keys | open_ways | closed_area_no,
                          key=lambda item: (OSM_TYPE_ORDER[item[0]], item[1])):
            metadata = candidate_metadata[key]
            if key in open_ways:
                reason = "open_non_area_way"
            elif key in closed_area_no:
                reason = "closed_area_no"
            elif (key in missing_relations
                  and metadata["type"] == "site"):
                reason = "unsupported_site_relation"
            else:
                reason = "no_polygon_geometry"
            rejection_rows.append({
                "osm_type": key[0], "osm_id": key[1],
                "name": metadata["name"],
                "source_classes": metadata["source_classes"],
                "reason": reason,
            })
        diagnostics = {
            **source_counts,
            "source_candidate_count": len(candidate_keys),
            "geometry_failure_count": len(failed_geometry_keys),
            "excluded_non_area_ways": len(non_area_ways),
            "open_way_count": len(open_ways),
            "closed_area_no_count": len(closed_area_no),
            "geometry_rejection_count": len(rejection_rows),
            "geometry_rejections_json": json.dumps(
                rejection_rows, ensure_ascii=False, sort_keys=True),
            "filtered_pbf_size_bytes": filtered_path.stat().st_size,
        }
        return list(features.values()), dict(relation_memberships), diagnostics
    finally:
        filtered_path.unlink(missing_ok=True)


def park_schema():
    import pyarrow as pa
    return pa.schema([
        ("park_id", pa.string()),
        ("osm_type", pa.string()), ("osm_id", pa.int64()),
        ("canonical_osm_type", pa.string()), ("canonical_osm_id", pa.int64()),
        ("parent_relation_id", pa.int64()),
        ("member_of_relation_ids", pa.list_(pa.int64())),
        ("parent_park_id", pa.string()), ("attachment_method", pa.string()),
        ("containment_ratio", pa.float64()),
        ("strong_identity_type", pa.string()),
        ("strong_identity_value", pa.string()),
        ("wikidata", pa.string()), ("wikipedia", pa.string()),
        ("name", pa.string()), ("park_class", pa.string()),
        ("display_name", pa.string()), ("display_name_source", pa.string()),
        ("source_classes", pa.list_(pa.string())),
        ("eligibility_tier", pa.string()),
        ("is_primary", pa.bool_()), ("is_conditional", pa.bool_()),
        ("is_secondary", pa.bool_()), ("counts_as_primary", pa.bool_()),
        ("counts_as_choice", pa.bool_()), ("counts_as_secondary", pa.bool_()),
        ("eligibility_reason", pa.string()), ("access", pa.string()),
        ("has_positive_access", pa.bool_()), ("operator", pa.string()),
        ("opening_hours", pa.string()), ("area_m2", pa.float64()),
        ("geometry_wkb", pa.binary()), ("geometry_valid", pa.bool_()),
        ("geometry_was_valid", pa.bool_()),
        ("centroid_lat", pa.float64()), ("centroid_lon", pa.float64()),
        ("representative_lat", pa.float64()),
        ("representative_lon", pa.float64()),
        ("bbox_min_lat", pa.float64()), ("bbox_min_lon", pa.float64()),
        ("bbox_max_lat", pa.float64()), ("bbox_max_lon", pa.float64()),
        ("leisure", pa.string()), ("landuse", pa.string()),
        ("boundary", pa.string()), ("protect_class", pa.string()),
        ("size_factor", pa.float64()), ("base_confidence", pa.float64()),
        ("final_confidence", pa.float64()), ("secondary_class", pa.string()),
        ("secondary_type_weight", pa.float64()),
        ("secondary_confidence", pa.float64()),
        ("absorbed_member_osm_keys", pa.list_(pa.string())),
        ("source_osm_keys", pa.list_(pa.string())),
        ("source_representations_json", pa.string()),
        ("source_path", pa.string()), ("source_size_bytes", pa.int64()),
        ("builder_version", pa.string()),
    ])


def _write_atomic(rows, output_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = park_schema()
    table = pa.Table.from_pylist(
        [{name: row.get(name) for name in schema.names} for row in rows],
        schema=schema)
    handle, temporary = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent)
    os.close(handle)
    try:
        pq.write_table(table, temporary, compression="zstd", compression_level=6)
        os.replace(temporary, output_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def build_park_cache(input_path, output_path):
    input_path = Path(input_path)
    features, memberships, collection = collect_osm_features(input_path)
    rows, diagnostics = build_destination_rows(
        features, memberships, source_path=str(input_path.resolve()),
        source_size=input_path.stat().st_size)
    _write_atomic(rows, output_path)
    diagnostics.update(collection)
    diagnostics["output_size_bytes"] = Path(output_path).stat().st_size
    diagnostics["built_at_utc"] = datetime.now(timezone.utc).isoformat()
    return diagnostics


def main(argv=None):
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Build Park Destination Cache V1")
    parser.add_argument("--pbf", default=root / "data" / "belgium-latest.osm.pbf")
    parser.add_argument("--output", default=root / "cache" / "be_park_destinations.parquet")
    args = parser.parse_args(argv)
    diagnostics = build_park_cache(args.pbf, args.output)
    print("Park Destination Cache V1 built:")
    for key in sorted(diagnostics):
        print(f"  {key}: {diagnostics[key]}")
    print(f"  output: {Path(args.output).resolve()}")


if __name__ == "__main__":
    main()

"""Build the canonical Belgium Sport Destination Cache V1.

The cache describes independent destinations, component lineage, access and
context metadata.  It deliberately contains no address-specific score.
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
from shapely.ops import unary_union
from shapely.strtree import STRtree
from shapely.validation import make_valid


BUILDER_VERSION = "sport-destination-cache-v1"
OSM_TYPE_ORDER = {"relation": 0, "way": 1, "node": 2}
POSITIVE_ACCESS = frozenset({"yes", "permissive", "public", "designated"})
EXCLUDED_ACCESS = frozenset({"private", "no"})
LIMITED_ACCESS = frozenset({"permit", "limited", "restricted"})
FACILITY_LEISURE = frozenset({
    "sports_centre", "fitness_centre", "sports_hall", "stadium",
    "golf_course", "horse_riding",
})
COMPONENT_LEISURE = frozenset({
    "pitch", "track", "swimming_pool", "fitness_station",
})
SPECIALIZED_SPORTS = frozenset({
    "archery", "bmx", "boxing", "climbing", "equestrian", "golf",
    "horse_racing", "martial_arts", "motocross", "motor", "padel",
    "pilates", "shooting", "skateboard", "yoga",
})
EXCLUDED_SPORTS = frozenset({
    "dog_training", "dog_sport", "model_aerodrome", "rc_car",
})
INCIDENTAL_AMENITIES = frozenset({
    "bar", "cafe", "hotel", "pub", "restaurant",
})
PUBLIC_OPERATOR_MARKERS = (
    "commune", "gemeente", "municipal", "sportdienst", "stad ",
    "stad", "ville ",
)
DISPLAY_NAME_SOURCES = frozenset({
    "canonical_name", "absorbed_component", "operator", "brand", "none",
})


def normalized_text(value):
    return " ".join(str(value or "").strip().casefold().split())


def normalized_access(value):
    value = normalized_text(value)
    if not value:
        return "missing"
    if value in POSITIVE_ACCESS:
        return "positive_general"
    if value in EXCLUDED_ACCESS:
        return value
    if value == "customers":
        return "customers"
    if value == "members":
        return "members"
    if value in LIMITED_ACCESS:
        return "permit_limited"
    return "other"


def split_sports(value):
    """Return a deterministic normalized set without expanding ``multi``."""
    if not value:
        return set()
    normalized = str(value).replace(",", ";")
    return {normalized_text(item) for item in normalized.split(";")
            if normalized_text(item)}


def is_public_operator(value):
    value = normalized_text(value)
    return bool(value and any(marker in value for marker in PUBLIC_OPERATOR_MARKERS))


def source_kind(tags):
    """Classify a raw sport source before component enrichment."""
    leisure = normalized_text(tags.get("leisure"))
    amenity = normalized_text(tags.get("amenity"))
    sports = split_sports(tags.get("sport"))
    name = normalized_text(tags.get("name"))
    building = normalized_text(tags.get("building"))

    if leisure == "fitness_centre" or amenity == "gym":
        return "fitness_gym"
    if leisure == "sports_hall" or building == "sports_hall":
        return "sports_hall"
    if leisure == "stadium":
        return "stadium"
    if leisure == "golf_course":
        return "golf"
    if leisure == "horse_riding":
        return "equestrian"
    swimming_context = (
        "swimming" in sports
        and (leisure in FACILITY_LEISURE | {"swimming_area", "water_park"}
             or amenity in {"gym", "swimming_pool"}
             or building in {"sports_hall", "stadium"})
    )
    if swimming_context or (
            leisure in {"sports_centre", "sports_hall"}
            and any(word in name for word in
                    ("zwembad", "piscine", "swimming pool"))):
        return "swimming"
    if leisure == "sports_centre":
        return "sports_centre"
    if leisure in COMPONENT_LEISURE:
        return leisure
    if sports or normalized_text(tags.get("club")) == "sport":
        return "other_sport"
    return None


def is_school(tags):
    return normalized_text(tags.get("amenity")) == "school"


def is_entrance(tags):
    return bool(normalized_text(tags.get("entrance")))


def is_candidate(tags):
    return source_kind(tags) is not None


def final_facility_class(kind, sports, *, standalone):
    known = set(sports) - {"multi"}
    if standalone:
        if kind == "swimming_pool":
            return "swimming"
        if known and known <= SPECIALIZED_SPORTS:
            return "standalone_specialized"
        return "standalone_local"
    if kind == "fitness_gym":
        return "fitness_gym"
    if kind == "swimming":
        return "swimming"
    if kind == "sports_hall":
        return "sports_hall"
    if kind == "stadium":
        return "stadium"
    if kind in {"golf", "equestrian", "other_sport"}:
        return "specialized"
    if kind == "sports_centre":
        if known and known <= SPECIALIZED_SPORTS:
            return "specialized"
        if len(known) >= 2:
            return "multi_sport_centre"
        return "general_sports_centre"
    return "specialized"


def _ring_geodesic_area(coordinates):
    polygon = Geodesic.WGS84.Polygon()
    for lon, lat in coordinates:
        polygon.AddPoint(float(lat), float(lon))
    return abs(polygon.Compute(False, True)[2])


def geodesic_area_m2(geometry):
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
        parts = []
        for item in geometry.geoms:
            if isinstance(item, Polygon):
                parts.append(item)
            elif isinstance(item, MultiPolygon):
                parts.extend(item.geoms)
        if len(parts) == 1:
            return parts[0]
        if parts:
            return MultiPolygon(parts)
    return None


def normalize_geometry(geometry, *, allow_point=False):
    if geometry is None or geometry.is_empty:
        return None, False
    was_valid = bool(geometry.is_valid)
    if isinstance(geometry, Point):
        return (geometry, was_valid) if allow_point else (None, was_valid)
    if not isinstance(geometry, (Polygon, MultiPolygon)):
        return None, was_valid
    if not was_valid:
        try:
            geometry = _polygonal_only(make_valid(geometry))
        except Exception:
            return None, False
    if geometry is None or geometry.is_empty or not geometry.is_valid:
        return None, False
    return geometry, was_valid


def _key(feature):
    return str(feature["osm_type"]), int(feature["osm_id"])


def _key_text(key):
    return f"{key[0]}:{int(key[1])}"


def stable_sport_id(key):
    return f"sport:osm:{key[0]}:{int(key[1])}"


def _geometry_metadata(geometry):
    centroid = geometry.centroid
    representative = geometry.representative_point()
    min_lon, min_lat, max_lon, max_lat = geometry.bounds
    return {
        "geometry_wkb": geometry.wkb,
        "geometry_valid": bool(geometry.is_valid),
        "area_m2": geodesic_area_m2(geometry),
        "centroid_lat": float(centroid.y),
        "centroid_lon": float(centroid.x),
        "representative_lat": float(representative.y),
        "representative_lon": float(representative.x),
        "bbox_min_lat": float(min_lat),
        "bbox_min_lon": float(min_lon),
        "bbox_max_lat": float(max_lat),
        "bbox_max_lon": float(max_lon),
    }


def _strong_identity(tags):
    if tags.get("wikidata"):
        return "wikidata", normalized_text(tags["wikidata"])
    if tags.get("wikipedia"):
        return "wikipedia", normalized_text(tags["wikipedia"])
    return None


def _semantic_compatible(first, second):
    a, b = first["kind"], second["kind"]
    if a == b:
        return True
    broad = {"sports_centre", "sports_hall", "stadium", "swimming"}
    return a in broad and b in broad


def _choose_canonical(items):
    return min(items, key=lambda item: (
        OSM_TYPE_ORDER.get(item["osm_type"], 9),
        -(float(item["area_m2"] or 0.0)),
        int(item["osm_id"]),
    ))


def _add_absorption(owner, child, method, evidence, *, component):
    child_key = _key(child)
    owner["absorbed"].add(child_key)
    owner["source_keys"].update(child["source_keys"])
    owner["source_representations"].extend(child["source_representations"])
    owner["parent_relation_ids"].update(child["parent_relation_ids"])
    owner["relation_roles"].update(child["relation_roles"])
    owner["canonicalization_steps"].append({
        "source": _key_text(child_key), "method": method,
        "evidence": evidence,
    })
    if component:
        owner["component_keys"].add(child_key)
        owner["component_keys"].update(child["component_keys"])
        owner["component_sports"].update(
            child["direct_sports"] | child["component_sports"])
        owner["component_kinds"].append(child["kind"])
        owner["component_kinds"].extend(child["component_kinds"])
    else:
        # Representation/facility absorption must carry through components
        # already owned by the child.  Otherwise canonicalizing a contained
        # pool/hall after its courts were assigned would silently lose both
        # lineage and sport diversity.
        owner["component_keys"].update(child["component_keys"])
        owner["component_sports"].update(
            child["direct_sports"] | child["component_sports"])
        owner["component_kinds"].extend(child["component_kinds"])


def _base_record(feature, source_path, source_timestamp):
    tags = dict(feature.get("tags") or {})
    key = _key(feature)
    allow_point = key[0] == "node"
    geometry, was_valid = normalize_geometry(
        feature.get("geometry"), allow_point=allow_point)
    if geometry is None:
        return None
    metadata = _geometry_metadata(geometry)
    area_override = feature.get("area_m2")
    if area_override is not None and not isinstance(geometry, Point):
        metadata["area_m2"] = float(area_override)
    return {
        "osm_type": key[0], "osm_id": key[1], "tags": tags,
        "kind": source_kind(tags), "geometry": geometry,
        "geometry_was_valid": was_valid, "direct_sports": split_sports(
            tags.get("sport")), "component_sports": set(),
        "component_kinds": [], "component_keys": set(), "absorbed": set(),
        "rejected_component_keys": set(), "source_keys": {key},
        "source_representations": [{
            "osm_type": key[0], "osm_id": key[1], "tags": tags,
        }],
        "parent_relation_ids": set(), "relation_roles": set(),
        "canonicalization_steps": [], "school_context": False,
        "school_keys": set(), "entrance": None,
        "source_path": str(source_path),
        "source_timestamp": source_timestamp,
        **metadata,
    }


def _spatially_contains(parent, child):
    if isinstance(child["geometry"], Point):
        return parent["geometry"].covers(child["geometry"])
    point = child["geometry"].representative_point()
    return parent["geometry"].covers(point)


def _display_name(record):
    tags = record["tags"]
    if str(tags.get("name") or "").strip():
        return tags["name"].strip(), "canonical_name"
    names = {
        str(rep["tags"].get("name") or "").strip()
        for rep in record["source_representations"]
        if str(rep["tags"].get("name") or "").strip()
    }
    if len(names) == 1:
        return names.pop(), "absorbed_component"
    if str(tags.get("operator") or "").strip():
        return tags["operator"].strip(), "operator"
    if str(tags.get("brand") or "").strip():
        return tags["brand"].strip(), "brand"
    return None, "none"


def _access_decision(record, facility_class, standalone):
    tags = record["tags"]
    access_raw = tags.get("access")
    access_class = normalized_access(access_raw)
    public_operator = is_public_operator(tags.get("operator"))
    commercial = facility_class == "fitness_gym"
    sports = record["direct_sports"] | record["component_sports"]

    if access_class in {"private", "no"}:
        return False, "excluded_explicit_access", access_class, (
            f"access={normalized_text(access_raw)}"), commercial, public_operator
    if sports & EXCLUDED_SPORTS:
        return False, "excluded_animal_or_model_sport", access_class, (
            "excluded sport tag"), commercial, public_operator
    amenity = normalized_text(tags.get("amenity"))
    if amenity in INCIDENTAL_AMENITIES:
        return False, "excluded_incidental_business_sport", access_class, (
            f"amenity={amenity}"), commercial, public_operator
    if record["kind"] == "other_sport" and not (
            normalized_text(tags.get("club")) == "sport"
            or amenity in {"community_centre", "dojo"}
            or normalized_text(tags.get("leisure")) in {
                "sports_centre", "sports_hall"}
            or (normalized_text(tags.get("type")) == "site"
                and bool(tags.get("name") or tags.get("operator")))):
        return False, "excluded_unproven_sport_tag", access_class, (
            "sport tag without dedicated facility context"), commercial, public_operator
    if access_class == "customers" and not commercial:
        return False, "excluded_customer_only_context", access_class, (
            "customer-only non-fitness destination"), commercial, public_operator
    if record["school_context"] and access_class != "positive_general":
        return False, "excluded_school_without_general_access", access_class, (
            "school containment without positive access"), commercial, public_operator
    if standalone and record["kind"] == "swimming_pool":
        return False, "excluded_pool_without_canonical_facility", access_class, (
            "standalone pool polygon"), commercial, public_operator
    if standalone and not (
            access_class == "positive_general" or tags.get("name")
            or tags.get("operator") or public_operator):
        return False, "excluded_unnamed_unknown_standalone", access_class, (
            "no name, operator, public or positive-access evidence"), commercial, public_operator

    if access_class == "positive_general":
        evidence = "explicit general access"
    elif public_operator:
        evidence = "municipal/public operator"
    elif commercial:
        evidence = "recognized generally joinable fitness facility"
    elif access_class == "members":
        evidence = "member access"
    elif access_class == "permit_limited":
        evidence = "permit/limited access"
    else:
        evidence = "access missing/unknown; not interpreted as public"
    return True, "eligible", access_class, evidence, commercial, public_operator


def build_destination_rows(features, relation_memberships=None,
                           relation_tags=None, *,
                           source_path="synthetic", source_timestamp=None):
    """Canonicalize extracted features into deterministic cache rows."""
    relation_memberships = relation_memberships or {}
    relation_tags = relation_tags or {}
    diagnostics = Counter()
    records = {}
    school_records = []
    entrances = []
    for feature in features:
        tags = dict(feature.get("tags") or {})
        base = _base_record(feature, source_path, source_timestamp)
        if base is None:
            diagnostics["geometry_failure"] += 1
            continue
        if is_school(tags):
            if not isinstance(base["geometry"], Point):
                school_records.append(base)
            continue
        if is_entrance(tags) and feature["osm_type"] == "node":
            entrances.append(base)
            continue
        if not is_candidate(tags):
            continue
        records[_key(feature)] = base

    # Reconstruct a geometry-bearing canonical source for a tagged sport/site
    # relation that has no area callback of its own.  Only polygonal member
    # geometry is used; a node-only relation continues to fall back to a
    # geometry-bearing member rather than inventing a site footprint.
    for relation_key, tags in sorted(
            relation_tags.items(), key=lambda item: int(item[0][1])):
        relation_key = tuple(relation_key)
        if relation_key in records:
            continue
        if (normalized_text(tags.get("type")) != "site"
                or source_kind(tags) is None):
            continue
        member_polygons = []
        for member_key, _role in relation_memberships.get(relation_key, []):
            member = records.get(tuple(member_key))
            if member is not None and not isinstance(member["geometry"], Point):
                member_polygons.append(member["geometry"])
        if not member_polygons:
            continue
        geometry, _was_valid = normalize_geometry(unary_union(member_polygons))
        if geometry is None:
            diagnostics["site_relation_geometry_failure"] += 1
            continue
        relation = _base_record({
            "osm_type": "relation", "osm_id": int(relation_key[1]),
            "tags": dict(tags), "geometry": geometry,
        }, source_path, source_timestamp)
        if relation is not None:
            records[relation_key] = relation
            diagnostics["reconstructed_site_relations"] += 1

    # Preserve relation membership independently from semantic ownership.
    for relation_key, members in relation_memberships.items():
        relation = records.get(tuple(relation_key))
        for member_key, role in members:
            membership = (
                f"relation:{int(relation_key[1])}:member="
                f"{_key_text(tuple(member_key))}:role={role or ''}")
            if relation is not None:
                relation["relation_roles"].add(membership)
            member = records.get(tuple(member_key))
            if member is not None:
                member["parent_relation_ids"].add(int(relation_key[1]))
                member["relation_roles"].add(membership)

    suppressed = set()

    # A tagged multipolygon relation may replace its tagged outer geometry, but
    # this is representation deduplication rather than site ownership.
    for relation_key in sorted(relation_memberships,
                               key=lambda key: (int(key[1]), str(key[0]))):
        owner = records.get(tuple(relation_key))
        if owner is None or owner["osm_type"] != "relation":
            continue
        if normalized_text(owner["tags"].get("type")) != "multipolygon":
            continue
        for member_key, role in relation_memberships[relation_key]:
            child = records.get(tuple(member_key))
            if child is None or child is owner or role not in {"", "outer"}:
                continue
            same_kind = child["kind"] == owner["kind"]
            same_identity = (_strong_identity(child["tags"])
                             and _strong_identity(child["tags"])
                             == _strong_identity(owner["tags"]))
            if same_kind or same_identity:
                _add_absorption(owner, child, "multipolygon_representation",
                                f"outer member role={role!r}", component=False)
                suppressed.add(_key(child))

    # Explicit type=site ownership precedes identity and spatial inference.
    # Geometryless site relations are common, so their tags arrive separately
    # from the area-feature stream.  The chosen geometry-bearing destination
    # retains the relation identity, member roles and source tags.
    explicit_owner = {}
    for relation_key, members in sorted(
            relation_memberships.items(), key=lambda item: (
                OSM_TYPE_ORDER.get(item[0][0], 9), int(item[0][1]))):
        relation = records.get(tuple(relation_key))
        tags = dict(relation_tags.get(tuple(relation_key)) or
                    (relation["tags"] if relation else {}))
        if normalized_text(tags.get("type")) != "site":
            continue
        member_facilities = [records.get(tuple(key)) for key, _role in members]
        member_facilities = [
            row for row in member_facilities
            if row is not None and _key(row) not in suppressed
            and row["kind"] not in COMPONENT_LEISURE
            and row["kind"] is not None]
        if (relation is not None and _key(relation) not in suppressed
                and relation["kind"] not in COMPONENT_LEISURE):
            owner = relation
        elif member_facilities:
            owner = _choose_canonical(member_facilities)
        else:
            continue
        owner["parent_relation_ids"].add(int(relation_key[1]))
        if _key(owner) != tuple(relation_key):
            owner["source_keys"].add(tuple(relation_key))
            owner["source_representations"].append({
                "osm_type": "relation", "osm_id": int(relation_key[1]),
                "tags": tags,
            })
        for member_key, role in members:
            membership = (
                f"relation:{int(relation_key[1])}:member="
                f"{_key_text(tuple(member_key))}:role={role or ''}")
            owner["relation_roles"].add(membership)
            child = records.get(tuple(member_key))
            if child is None or child is owner or _key(child) in suppressed:
                continue
            if child["kind"] in COMPONENT_LEISURE:
                explicit_owner[_key(child)] = owner
                continue
            if child["kind"] is not None:
                _add_absorption(
                    owner, child, "site_relation",
                    f"explicit type=site member role={role!r}",
                    component=True)
                suppressed.add(_key(child))

    # Strong identity is accepted only with compatible semantics and geographic
    # sanity (intersection/containment or no more than ~100 m in latitude).
    identity_groups = defaultdict(list)
    for key, record in records.items():
        if key in suppressed or record["kind"] in COMPONENT_LEISURE:
            continue
        identity = _strong_identity(record["tags"])
        if identity:
            identity_groups[identity].append(record)
    for identity, group in identity_groups.items():
        pending = sorted(group, key=lambda row: (
            OSM_TYPE_ORDER.get(row["osm_type"], 9), row["osm_id"]))
        while pending:
            seed = pending.pop(0)
            cluster = [seed]
            remainder = []
            for candidate in pending:
                nearby = (seed["geometry"].intersects(candidate["geometry"])
                          or seed["geometry"].distance(candidate["geometry"])
                          <= 0.0015)
                if nearby and _semantic_compatible(seed, candidate):
                    cluster.append(candidate)
                else:
                    remainder.append(candidate)
            pending = remainder
            if len(cluster) < 2:
                continue
            owner = _choose_canonical(cluster)
            for child in cluster:
                if child is owner:
                    continue
                _add_absorption(owner, child, "strong_identity",
                                f"shared {identity[0]}={identity[1]}",
                                component=False)
                suppressed.add(_key(child))

    facilities = [record for key, record in records.items()
                  if key not in suppressed
                  and record["kind"] not in COMPONENT_LEISURE
                  and record["kind"] is not None]
    facility_keys = {_key(record) for record in facilities}
    polygon_facilities = [record for record in facilities
                          if not isinstance(record["geometry"], Point)]
    facility_tree = STRtree([row["geometry"] for row in polygon_facilities]) \
        if polygon_facilities else None

    # Components are assigned once: site relation first, then the smallest
    # containing canonical facility polygon.
    for key in sorted(records, key=lambda item: (
            OSM_TYPE_ORDER.get(item[0], 9), int(item[1]))):
        child = records[key]
        if key in suppressed or child["kind"] not in COMPONENT_LEISURE:
            continue
        owner = explicit_owner.get(key)
        method = "site_relation"
        evidence = "explicit type=site membership"
        if owner is None and facility_tree is not None:
            candidates = []
            point = child["geometry"].representative_point()
            for index in facility_tree.query(point, predicate="within"):
                candidate = polygon_facilities[int(index)]
                if _key(candidate) == key or _key(candidate) in suppressed:
                    continue
                candidates.append(candidate)
            if candidates:
                owner = min(candidates, key=lambda row: (
                    float(row["area_m2"] or math.inf), stable_sport_id(_key(row))))
                method = "facility_containment"
                evidence = "component representative point inside facility polygon"
        if owner is not None:
            _add_absorption(owner, child, method, evidence, component=True)
            suppressed.add(key)

    # Compatible nested node/polygon representations inside a sports-centre
    # envelope. Names support an already spatial match; they never initiate it.
    centre_parents = [row for row in facilities
                      if _key(row) not in suppressed
                      and row["kind"] == "sports_centre"
                      and not isinstance(row["geometry"], Point)]
    centre_tree = STRtree([row["geometry"] for row in centre_parents]) \
        if centre_parents else None
    if centre_tree:
        for key, child in sorted(records.items(), key=lambda item: (
                OSM_TYPE_ORDER.get(item[0][0], 9), item[0][1])):
            if key in suppressed or child in centre_parents:
                continue
            if child["kind"] in COMPONENT_LEISURE or child["kind"] is None:
                continue
            point = child["geometry"].representative_point()
            parents = [centre_parents[int(i)] for i in centre_tree.query(
                point, predicate="within")]
            parents = [row for row in parents if _key(row) != key]
            if not parents:
                continue
            parent = min(parents, key=lambda row: (
                float(row["area_m2"] or math.inf), stable_sport_id(_key(row))))
            parent_name = normalized_text(parent["tags"].get("name"))
            child_name = normalized_text(child["tags"].get("name"))
            exact = bool(parent_name and child_name and parent_name == child_name)
            contextual = bool(parent_name and child_name and parent_name in child_name)
            unnamed_internal = (not child_name and child["kind"] in
                                {"stadium", "sports_hall", "swimming"})
            strong = (_strong_identity(parent["tags"])
                      and _strong_identity(parent["tags"])
                      == _strong_identity(child["tags"]))
            if not (exact or contextual or unnamed_internal or strong):
                continue
            why = ("exact contained identity" if exact else
                   "contained contextual name" if contextual else
                   "shared strong identity" if strong else
                   "unnamed compatible internal facility")
            _add_absorption(parent, child, "compatible_containment", why,
                            component=True)
            suppressed.add(key)

    # School containment is context, never ownership or access propagation.
    if school_records:
        school_tree = STRtree([row["geometry"] for row in school_records])
        for key, record in records.items():
            if key in suppressed:
                continue
            point = record["geometry"].representative_point()
            parents = [school_records[int(i)] for i in school_tree.query(
                point, predicate="within")]
            if parents:
                record["school_context"] = True
                record["school_keys"].update(_key(row) for row in parents)

    # Validated entrances must lie on/inside a polygon and within three metres
    # (approximately 0.00004 degrees) of its boundary.
    if entrances:
        entrance_tree = STRtree([row["geometry"] for row in entrances])
        for key, record in records.items():
            if key in suppressed or isinstance(record["geometry"], Point):
                continue
            candidates = []
            for index in entrance_tree.query(record["geometry"]):
                entrance = entrances[int(index)]
                if not record["geometry"].covers(entrance["geometry"]):
                    continue
                boundary_distance = record["geometry"].boundary.distance(
                    entrance["geometry"])
                if boundary_distance <= 0.00004:
                    candidates.append((boundary_distance, entrance))
            if candidates:
                _distance, record["entrance"] = min(candidates, key=lambda item: (
                    item[0], item[1]["osm_id"]))

    rows = []
    for key, record in sorted(records.items(), key=lambda item: (
            OSM_TYPE_ORDER.get(item[0][0], 9), item[0][1])):
        if key in suppressed:
            continue
        kind = record["kind"]
        if kind is None:
            continue
        standalone = kind in COMPONENT_LEISURE
        sports = record["direct_sports"] | record["component_sports"]
        facility_class = final_facility_class(
            kind, sports, standalone=standalone)
        eligible, reason, access_class, access_evidence, commercial, public = (
            _access_decision(record, facility_class, standalone))
        display_name, display_source = _display_name(record)
        assert display_source in DISPLAY_NAME_SOURCES
        tags = record["tags"]
        entrance = record["entrance"]
        steps = record["canonicalization_steps"]
        canonicalization_method = (steps[-1]["method"] if steps
                                   else "canonical_source")
        canonicalization_evidence = json.dumps(
            steps, ensure_ascii=False, sort_keys=True)
        row = {
            "sport_id": stable_sport_id(key),
            "canonical_osm_type": key[0],
            "canonical_osm_id": key[1],
            "name": tags.get("name"),
            "display_name": display_name,
            "display_name_source": display_source,
            "facility_class": facility_class,
            "score_eligible": bool(eligible),
            "eligibility_reason": reason,
            "specialized": facility_class in {
                "specialized", "standalone_specialized"},
            "standalone": standalone,
            "sports": sorted(sports),
            "direct_sports": sorted(record["direct_sports"]),
            "component_sports": sorted(record["component_sports"]),
            "sport_count": len(sports - {"multi"}),
            "component_count": len(record["component_keys"]),
            "access_raw": tags.get("access"),
            "access_class": access_class,
            "access_evidence": access_evidence,
            "fee": tags.get("fee"),
            "membership": tags.get("membership"),
            "operator": tags.get("operator"),
            "opening_hours": tags.get("opening_hours"),
            "club": tags.get("club"),
            "is_commercial": bool(commercial),
            "is_public_operator": bool(public),
            "is_school_context": bool(record["school_context"]),
            "indoor": tags.get("indoor"),
            "covered": tags.get("covered"),
            "building": tags.get("building"),
            "wikidata": tags.get("wikidata"),
            "wikipedia": tags.get("wikipedia"),
            "geometry_wkb": record["geometry_wkb"],
            "geometry_valid": record["geometry_valid"],
            "area_m2": record["area_m2"],
            "centroid_lat": record["centroid_lat"],
            "centroid_lon": record["centroid_lon"],
            "representative_lat": record["representative_lat"],
            "representative_lon": record["representative_lon"],
            "entrance_lat": (float(entrance["geometry"].y)
                             if entrance else None),
            "entrance_lon": (float(entrance["geometry"].x)
                             if entrance else None),
            "entrance_osm_key": (_key_text(_key(entrance))
                                 if entrance else None),
            "bbox_min_lat": record["bbox_min_lat"],
            "bbox_min_lon": record["bbox_min_lon"],
            "bbox_max_lat": record["bbox_max_lat"],
            "bbox_max_lon": record["bbox_max_lon"],
            "canonicalization_method": canonicalization_method,
            "canonicalization_evidence": canonicalization_evidence,
            "source_osm_keys": sorted(_key_text(item)
                                      for item in record["source_keys"]),
            "absorbed_component_keys": sorted(
                _key_text(item) for item in record["component_keys"]),
            "rejected_component_keys": sorted(
                _key_text(item) for item in record["rejected_component_keys"]),
            "parent_relation_ids": sorted(record["parent_relation_ids"]),
            "relation_membership_roles": sorted(record["relation_roles"]),
            "source_tags": json.dumps(tags, ensure_ascii=False, sort_keys=True),
            "source_representations": json.dumps(
                record["source_representations"], ensure_ascii=False,
                sort_keys=True),
            "school_osm_keys": sorted(_key_text(item)
                                      for item in record["school_keys"]),
            "build_version": BUILDER_VERSION,
            "source_pbf_timestamp": source_timestamp,
        }
        rows.append(row)
        diagnostics[f"facility_class_{facility_class}"] += 1
        diagnostics["score_eligible" if eligible else "display_only"] += 1
        diagnostics[f"access_class_{access_class}"] += 1
        diagnostics[f"geometry_type_{record['geometry'].geom_type}"] += 1
        diagnostics[f"canonicalization_{canonicalization_method}"] += 1
        diagnostics["absorbed_components"] += row["component_count"]
        for component_kind in record["component_kinds"]:
            diagnostics[f"absorbed_{component_kind}"] += 1

    diagnostics["canonical_rows"] = len(rows)
    diagnostics["suppressed_sources"] = len(suppressed)
    diagnostics["school_source_count"] = len(school_records)
    diagnostics["entrance_source_count"] = len(entrances)
    return rows, dict(diagnostics)


def _tags_dict(obj):
    return {tag.k: tag.v for tag in obj.tags}


def collect_osm_features(input_path):
    """Read source candidates through one bounded filtered PBF extract."""
    import osmium

    input_path = Path(input_path)
    handle, filtered_name = tempfile.mkstemp(
        prefix="sport-cache-candidates-", suffix=".pbf")
    os.close(handle)
    os.unlink(filtered_name)
    filtered_path = Path(filtered_name)
    metadata = {}
    relation_tags = {}
    memberships = defaultdict(list)
    source_counts = Counter()
    try:
        with osmium.BackReferenceWriter(
                str(filtered_path), str(input_path), overwrite=True,
                remove_tags=False, relation_depth=1) as writer:
            processor = osmium.FileProcessor(str(input_path)).with_filter(
                osmium.filter.KeyFilter(
                    "leisure", "amenity", "sport", "club", "building",
                    "entrance", "site"))
            for obj in processor:
                tags = _tags_dict(obj)
                osm_type = ("node" if isinstance(obj, osmium.osm.Node)
                            else "way" if isinstance(obj, osmium.osm.Way)
                            else "relation")
                relevant = (is_candidate(tags) or is_school(tags)
                            or (osm_type == "node" and is_entrance(tags))
                            or (osm_type == "relation"
                                and normalized_text(tags.get("type")) == "site"
                                and (tags.get("sport") or tags.get("leisure")
                                     or normalized_text(tags.get("site"))
                                     in {"sport", "sports"})))
                if not relevant:
                    continue
                key = (osm_type, int(obj.id))
                metadata[key] = tags
                source_counts[f"source_{osm_type}"] += 1
                if osm_type == "relation":
                    relation_tags[key] = tags
                    memberships[key] = [
                        (({"n": "node", "w": "way", "r": "relation"}[
                            member.type], int(member.ref)), member.role or "")
                        for member in obj.members
                    ]
                writer.add(obj)

        features = {}
        factory = osmium.geom.WKBFactory()
        geometry_errors = set()

        class GeometryHandler(osmium.SimpleHandler):
            def node(self, node):
                key = ("node", int(node.id))
                if key in metadata and node.location.valid():
                    features[key] = {
                        "osm_type": "node", "osm_id": key[1],
                        "tags": _tags_dict(node),
                        "geometry": Point(float(node.location.lon),
                                          float(node.location.lat)),
                    }

            def area(self, area):
                key = (("way" if area.from_way() else "relation"),
                       int(area.orig_id()))
                if key not in metadata:
                    return
                try:
                    geometry = wkb.loads(
                        factory.create_multipolygon(area), hex=True)
                    geometry, _valid = normalize_geometry(geometry)
                    if geometry is None:
                        geometry_errors.add(key)
                        return
                    features[key] = {
                        "osm_type": key[0], "osm_id": key[1],
                        "tags": _tags_dict(area), "geometry": geometry,
                        "area_m2": geodesic_area_m2(geometry),
                    }
                except Exception:
                    geometry_errors.add(key)

        GeometryHandler().apply_file(
            str(filtered_path), locations=True, idx="flex_mem")
        source_counts["geometry_errors"] = len(geometry_errors)
        source_counts["filtered_extract_size_bytes"] = filtered_path.stat().st_size
        source_counts["candidate_source_count"] = len(metadata)
        source_counts["geometry_feature_count"] = len(features)
        return (list(features.values()), dict(memberships), relation_tags,
                dict(source_counts))
    finally:
        if filtered_path.exists():
            filtered_path.unlink()


def sport_schema():
    import pyarrow as pa

    return pa.schema([
        ("sport_id", pa.string()),
        ("canonical_osm_type", pa.string()),
        ("canonical_osm_id", pa.int64()),
        ("name", pa.string()), ("display_name", pa.string()),
        ("display_name_source", pa.string()),
        ("facility_class", pa.string()),
        ("score_eligible", pa.bool_()),
        ("eligibility_reason", pa.string()),
        ("specialized", pa.bool_()), ("standalone", pa.bool_()),
        ("sports", pa.list_(pa.string())),
        ("direct_sports", pa.list_(pa.string())),
        ("component_sports", pa.list_(pa.string())),
        ("sport_count", pa.int32()), ("component_count", pa.int32()),
        ("access_raw", pa.string()), ("access_class", pa.string()),
        ("access_evidence", pa.string()),
        ("fee", pa.string()), ("membership", pa.string()),
        ("operator", pa.string()), ("opening_hours", pa.string()),
        ("club", pa.string()), ("is_commercial", pa.bool_()),
        ("is_public_operator", pa.bool_()),
        ("is_school_context", pa.bool_()),
        ("indoor", pa.string()), ("covered", pa.string()),
        ("building", pa.string()), ("wikidata", pa.string()),
        ("wikipedia", pa.string()), ("geometry_wkb", pa.binary()),
        ("geometry_valid", pa.bool_()), ("area_m2", pa.float64()),
        ("centroid_lat", pa.float64()), ("centroid_lon", pa.float64()),
        ("representative_lat", pa.float64()),
        ("representative_lon", pa.float64()),
        ("entrance_lat", pa.float64()), ("entrance_lon", pa.float64()),
        ("entrance_osm_key", pa.string()),
        ("bbox_min_lat", pa.float64()), ("bbox_min_lon", pa.float64()),
        ("bbox_max_lat", pa.float64()), ("bbox_max_lon", pa.float64()),
        ("canonicalization_method", pa.string()),
        ("canonicalization_evidence", pa.string()),
        ("source_osm_keys", pa.list_(pa.string())),
        ("absorbed_component_keys", pa.list_(pa.string())),
        ("rejected_component_keys", pa.list_(pa.string())),
        ("parent_relation_ids", pa.list_(pa.int64())),
        ("relation_membership_roles", pa.list_(pa.string())),
        ("source_tags", pa.string()),
        ("source_representations", pa.string()),
        ("school_osm_keys", pa.list_(pa.string())),
        ("build_version", pa.string()),
        ("source_pbf_timestamp", pa.string()),
    ])


def _write_atomic(rows, output_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = sport_schema()
    table = pa.Table.from_pylist(
        [{name: row.get(name) for name in schema.names} for row in rows],
        schema=schema)
    handle, temporary = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp",
        dir=output_path.parent)
    os.close(handle)
    try:
        pq.write_table(table, temporary, compression="zstd",
                       compression_level=6)
        os.replace(temporary, output_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def validate_rows(rows):
    diagnostics = Counter()
    sport_ids = [row["sport_id"] for row in rows]
    canonical = [(row["canonical_osm_type"], row["canonical_osm_id"])
                 for row in rows]
    component_owners = Counter(
        key for row in rows for key in row["absorbed_component_keys"])
    diagnostics["duplicate_sport_ids"] = len(sport_ids) - len(set(sport_ids))
    diagnostics["duplicate_canonical_ids"] = len(canonical) - len(set(canonical))
    diagnostics["components_with_multiple_owners"] = sum(
        count > 1 for count in component_owners.values())
    diagnostics["invalid_geometry_rows"] = sum(
        not row["geometry_valid"] for row in rows)
    diagnostics["missing_canonical_ids"] = sum(
        not row["canonical_osm_type"] or row["canonical_osm_id"] is None
        for row in rows)
    diagnostics["impossible_bbox_rows"] = sum(
        row["bbox_min_lat"] > row["bbox_max_lat"]
        or row["bbox_min_lon"] > row["bbox_max_lon"] for row in rows)
    diagnostics["zero_area_polygons"] = sum(
        row["area_m2"] is not None and row["area_m2"] <= 0 for row in rows)
    return dict(diagnostics)


def build_sport_cache(input_path, output_path):
    started = datetime.now(timezone.utc)
    input_path = Path(input_path)
    source_timestamp = datetime.fromtimestamp(
        input_path.stat().st_mtime, timezone.utc).isoformat()
    features, memberships, relation_tags, collection = collect_osm_features(
        input_path)
    rows, diagnostics = build_destination_rows(
        features, memberships, relation_tags,
        source_path=str(input_path.resolve()),
        source_timestamp=source_timestamp)
    safety = validate_rows(rows)
    if any(safety.values()):
        raise RuntimeError(f"Sport cache safety validation failed: {safety}")
    _write_atomic(rows, output_path)
    diagnostics.update(collection)
    diagnostics.update(safety)
    diagnostics["output_size_bytes"] = Path(output_path).stat().st_size
    diagnostics["build_runtime_seconds"] = (
        datetime.now(timezone.utc) - started).total_seconds()
    diagnostics["built_at_utc"] = datetime.now(timezone.utc).isoformat()
    return diagnostics


def main(argv=None):
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Build Belgium Sport Destination Cache V1")
    parser.add_argument(
        "--pbf", default=root / "data" / "belgium-latest.osm.pbf")
    parser.add_argument(
        "--output", default=root / "cache" / "be_sport_destinations.parquet")
    args = parser.parse_args(argv)
    diagnostics = build_sport_cache(args.pbf, args.output)
    print("Sport Destination Cache V1 built:")
    for key in sorted(diagnostics):
        print(f"  {key}: {diagnostics[key]}")
    print(f"  output: {Path(args.output).resolve()}")


if __name__ == "__main__":
    main()

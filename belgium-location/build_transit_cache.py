"""Build relation-aware Transit Cache V1 Parquet files from an OSM extract.

This pipeline is deliberately independent from the generic POI caches.  Route
metadata is retained as enrichment only; nothing in this module computes a
location score.
"""

import argparse
import hashlib
import math
import os
from pathlib import Path
import re
import tempfile
import unicodedata
from collections import Counter, defaultdict


TRANSIT_NAMED_FALLBACK_DISTANCE_M = 75
TRANSIT_UNNAMED_FALLBACK_DISTANCE_M = 10
EARTH_RADIUS_M = 6371000.0

BROAD_MODES = ("BUS", "FERRY", "GENERIC", "LIGHT_RAIL", "METRO", "RAIL", "TRAM")
ROUTE_MODE_MAP = {
    "bus": "BUS",
    "ferry": "FERRY",
    "light_rail": "LIGHT_RAIL",
    "subway": "METRO",
    "train": "RAIL",
    "tram": "TRAM",
}
OSM_TYPE_ORDER = {"node": 0, "way": 1, "relation": 2}
TAG_FIELDS = (
    "name", "railway", "highway", "public_transport", "amenity", "station",
    "bus", "tram", "train", "subway", "light_rail", "ferry", "construction",
    "network", "operator", "ref", "local_ref", "uic_ref",
)
FILTER_KEYS = (
    "public_transport", "highway", "railway", "amenity", "station", "bus",
    "tram", "train", "subway", "light_rail", "ferry", "construction",
)


def normalize_name(value):
    """Conservatively normalize exact text without fuzzy matching."""
    if value is None:
        return None
    value = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(value)).casefold()).strip()
    return value or None


def haversine_m(lat1, lon1, lat2, lon2):
    """Return straight-line great-circle distance in metres."""
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dlat = p2 - p1
    dlon = math.radians(float(lon2) - float(lon1))
    value = (math.sin(dlat / 2) ** 2
             + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(max(0.0, min(1.0, value))))


def normalize_route_key(mode, network=None, operator=None, ref=None, name=None,
                        relation_id=None):
    """Return a conservative direction-independent logical route identity."""
    broad_mode = ROUTE_MODE_MAP.get(str(mode).casefold(), str(mode).upper())
    provider = normalize_name(network) or normalize_name(operator) or "unknown"
    normalized_ref = normalize_name(ref)
    if normalized_ref:
        identity = f"ref:{normalized_ref}"
    else:
        normalized_name = normalize_name(name)
        identity = (f"name:{normalized_name}" if normalized_name
                    else f"relation:{int(relation_id)}")
    return f"{broad_mode}|{provider}|{identity}"


def _tag_yes(tags, key):
    return normalize_name(tags.get(key)) in {"yes", "designated", "official"}


def derive_direct_modes(member):
    """Derive a set of broad modes from tags and normalized route modes."""
    tags = member.get("tags", member)
    route_modes = set(member.get("classification_modes",
                                 member.get("route_modes", ())))
    modes = set(route_modes)
    railway = normalize_name(tags.get("railway"))
    station = normalize_name(tags.get("station"))
    provider = " ".join(filter(None, (
        normalize_name(tags.get("network")), normalize_name(tags.get("operator")))))

    if (_tag_yes(tags, "bus") or tags.get("highway") == "bus_stop"
            or tags.get("amenity") == "bus_station"):
        modes.add("BUS")
    if _tag_yes(tags, "tram") or railway == "tram_stop":
        modes.add("TRAM")
    if _tag_yes(tags, "light_rail") or station == "light_rail":
        modes.add("LIGHT_RAIL")
    if (_tag_yes(tags, "subway") or station == "subway"
            or railway == "subway_entrance"):
        modes.add("METRO")
    if _tag_yes(tags, "ferry") or tags.get("amenity") == "ferry_terminal":
        modes.add("FERRY")
    if _tag_yes(tags, "train"):
        modes.add("RAIL")
    # railway=station alone is deliberately insufficient: Belgian subway and
    # light-rail stations also use it. UIC/provider evidence is heavy-rail
    # evidence, and railway=halt is accepted only with the same evidence or an
    # explicit train indication/route.
    if railway in {"station", "halt"} and (
            tags.get("uic_ref") or "nmbs" in provider or "sncb" in provider):
        modes.add("RAIL")

    return modes or {"GENERIC"}


def _is_station_level_rail_seed(member):
    """Return whether a member can seed a heavy-rail station identity.

    ``train=yes`` and inherited train-route modes remain useful mode evidence,
    but are deliberately insufficient on platforms, stop positions, buffer
    stops, construction objects, or other track-level infrastructure. Those
    objects can be absorbed by a structurally associated station access; they
    cannot independently become one.
    """
    tags = member.get("tags", member)
    railway = normalize_name(tags.get("railway"))
    public_transport = normalize_name(tags.get("public_transport"))
    construction = normalize_name(tags.get("construction"))
    station = normalize_name(tags.get("station"))
    if (station in {"subway", "light_rail"} or _tag_yes(tags, "subway")
            or _tag_yes(tags, "light_rail")):
        return False
    # A UIC reference is authoritative station identity even when a sparsely
    # mapped border station exposes it only on its passenger stop positions.
    # Explicit infrastructure/construction objects remain ineligible.
    if (tags.get("uic_ref") and railway not in
            {"buffer_stop", "construction", "track"}
            and construction not in {"platform", "stop_position"}):
        return True
    if (railway in {"platform", "stop", "buffer_stop", "construction", "track"}
            or (public_transport in {"platform", "stop_position"}
                and railway not in {"station", "halt"})
            or construction in {"platform", "stop_position"}):
        return False

    provider = " ".join(filter(None, (
        normalize_name(tags.get("network")), normalize_name(tags.get("operator")))))
    modes = set(member.get("classification_modes",
                           member.get("route_modes", ())))
    heavy_evidence = (
        _tag_yes(tags, "train")
        or "RAIL" in modes
        or "nmbs" in provider
        or "sncb" in provider
    )
    return bool(heavy_evidence and (
        railway in {"station", "halt"} or public_transport == "station"))


def has_heavy_rail_station_evidence(members):
    """Return whether members contain a genuine station-level rail seed."""
    return any(_is_station_level_rail_seed(member) for member in members)


def _object_key(member):
    return str(member["osm_type"]), int(member["osm_id"])


def _stable_member_key(member):
    return (OSM_TYPE_ORDER.get(member["osm_type"], 9), int(member["osm_id"]))


def _has_coordinate(member):
    return member.get("lat") is not None and member.get("lon") is not None


def _spatial_cell(lat, lon, cell_size_m):
    lat_r, lon_r = math.radians(float(lat)), math.radians(float(lon))
    xyz = (
        EARTH_RADIUS_M * math.cos(lat_r) * math.cos(lon_r),
        EARTH_RADIUS_M * math.cos(lat_r) * math.sin(lon_r),
        EARTH_RADIUS_M * math.sin(lat_r),
    )
    return tuple(math.floor(axis / cell_size_m) for axis in xyz)


def _within(distance, threshold):
    return distance <= threshold or math.isclose(
        distance, threshold, rel_tol=1e-12, abs_tol=1e-8)


def _compatible_modes(left, right):
    left = set(left) - {"GENERIC"}
    right = set(right) - {"GENERIC"}
    return not left or not right or bool(left & right)


def _cluster_complete_link(items, threshold_m, compatible):
    """Spatially indexed deterministic complete-link clustering."""
    ordered = sorted(items, key=lambda item: (
        float(item["lat"]), float(item["lon"]), item["stable_key"]))
    clusters = []
    leader_cells = defaultdict(list)
    for item in ordered:
        cell = _spatial_cell(item["lat"], item["lon"], threshold_m)
        candidates = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    candidates.update(leader_cells.get(
                        (cell[0] + dx, cell[1] + dy, cell[2] + dz), ()))
        accepted = None
        for index in sorted(candidates):
            cluster = clusters[index]
            if all(compatible(item, other) and _within(haversine_m(
                    item["lat"], item["lon"], other["lat"], other["lon"]),
                    threshold_m) for other in cluster):
                accepted = index
                break
        if accepted is None:
            accepted = len(clusters)
            clusters.append([item])
            leader_cells[cell].append(accepted)
        else:
            clusters[accepted].append(item)
    return clusters


def member_subtype(member):
    tags = member.get("tags", member)
    for field, value in (
        ("railway", "station"), ("railway", "halt"),
        ("railway", "tram_stop"), ("railway", "subway_entrance"),
        ("amenity", "bus_station"), ("highway", "bus_stop"),
        ("public_transport", "station"), ("public_transport", "platform"),
        ("railway", "platform"), ("public_transport", "stop_position"),
    ):
        if tags.get(field) == value:
            return f"{field}={value}"
    return "generic"


def representative_member(members):
    """Choose a deterministic passenger-access member-based medoid.

    Priority is: passenger platform/access node, station passenger node, other
    platform geometry, stop_position, then any remaining located member.
    Within the best priority, the actual member minimizing total distance to
    the other candidates is used, with OSM identity as the tie-breaker.
    """
    located = [member for member in members if _has_coordinate(member)]
    if not located:
        return None

    def priority(member):
        tags = member.get("tags", member)
        if (tags.get("highway") == "bus_stop"
                or (tags.get("public_transport") == "platform"
                    and member["osm_type"] == "node")):
            return 0
        if tags.get("public_transport") == "station" and member["osm_type"] == "node":
            return 1
        if (tags.get("public_transport") == "platform"
                or tags.get("railway") == "platform"
                or tags.get("highway") == "platform"):
            return 2
        if tags.get("public_transport") == "stop_position":
            return 3
        return 4

    best_priority = min(priority(member) for member in located)
    candidates = [member for member in located if priority(member) == best_priority]
    return min(candidates, key=lambda member: (
        sum(haversine_m(member["lat"], member["lon"], other["lat"], other["lon"])
            for other in candidates),
        _stable_member_key(member),
    ))


def _preferred_name(explicit_name, members):
    if explicit_name and normalize_name(explicit_name):
        return str(explicit_name).strip()
    names = [str(member.get("name") or member.get("tags", {}).get("name")).strip()
             for member in members
             if member.get("name") or member.get("tags", {}).get("name")]
    if not names:
        return None
    counts = Counter(normalize_name(name) for name in names)
    winning = min(counts, key=lambda name: (-counts[name], name))
    return min((name for name in names if normalize_name(name) == winning),
               key=lambda name: (name.casefold(), name))


def _hash_identity(prefix, values):
    payload = "|".join(str(value) for value in sorted(values))
    return f"{prefix}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _make_access(members, explicit_name=None, stop_area_ids=(), group_id=None,
                 source_hint=None, is_rail_station=None):
    unique = {_object_key(member): member for member in members}
    members = sorted(unique.values(), key=_stable_member_key)
    representative = representative_member(members)
    if representative is None:
        return None
    modes = set()
    for member in members:
        modes.update(derive_direct_modes(member))
    if len(modes) > 1:
        modes.discard("GENERIC")
    if is_rail_station is None:
        is_rail_station = has_heavy_rail_station_evidence(members)
    stop_area_ids = sorted(set(int(value) for value in stop_area_ids))
    uic_refs = sorted({str(member.get("uic_ref") or member.get("tags", {}).get("uic_ref"))
                       for member in members
                       if member.get("uic_ref") or member.get("tags", {}).get("uic_ref")})

    if group_id is not None:
        logical_id = f"stop_area_group:{int(group_id)}"
        identity_source = "stop_area_group"
    elif is_rail_station and uic_refs:
        logical_id = f"uic:{'+'.join(uic_refs)}"
        identity_source = "uic"
    elif len(stop_area_ids) == 1:
        logical_id = f"stop_area:{stop_area_ids[0]}"
        identity_source = "stop_area"
    elif len(stop_area_ids) > 1:
        logical_id = _hash_identity("fallback", [f"sa:{value}" for value in stop_area_ids])
        identity_source = "fallback"
    elif len(members) == 1:
        logical_id = f"osm:{members[0]['osm_type']}:{members[0]['osm_id']}"
        identity_source = "osm"
    else:
        logical_id = _hash_identity(
            "fallback", [f"{m['osm_type']}:{m['osm_id']}" for m in members])
        identity_source = source_hint or "fallback"

    route_keys = sorted({key for member in members for key in member.get("route_keys", ())})
    route_modes = sorted({mode for member in members for mode in member.get("route_modes", ())})
    route_refs = sorted({ref for member in members for ref in member.get("route_refs", ())},
                        key=lambda value: (value.casefold(), value))
    return {
        "logical_stop_id": logical_id,
        "identity_source": identity_source,
        "name": _preferred_name(explicit_name, members),
        "lat": float(representative["lat"]),
        "lon": float(representative["lon"]),
        "modes": sorted(modes),
        "is_rail_station": bool(is_rail_station),
        "stop_area_ids": stop_area_ids,
        "stop_area_group_id": int(group_id) if group_id is not None else None,
        "member_count": len(members),
        "member_osm_keys": [f"{m['osm_type']}:{m['osm_id']}" for m in members],
        "representative_osm_type": representative["osm_type"],
        "representative_osm_id": int(representative["osm_id"]),
        "route_count": len(route_keys),
        "route_modes": route_modes,
        "route_refs": route_refs,
    }


def _unit_from_stop_area(stop_area, members_by_key):
    members = [members_by_key[key] for key, _role in stop_area["members"]
               if key in members_by_key]
    representative = representative_member(members)
    if not members or representative is None:
        return None
    modes = set().union(*(derive_direct_modes(member) for member in members))
    modes.discard("GENERIC") if len(modes) > 1 else None
    return {
        "id": int(stop_area["id"]),
        "name": _preferred_name(stop_area.get("name"), members),
        "normalized_name": normalize_name(_preferred_name(stop_area.get("name"), members)),
        "lat": representative["lat"], "lon": representative["lon"],
        "modes": modes or {"GENERIC"},
        "is_rail": has_heavy_rail_station_evidence(members),
        "members": members,
        "stable_key": f"stop_area:{int(stop_area['id'])}",
    }


def _structural_stop_area_components(units, groups):
    """Merge relation units by explicit topology, not geographic chaining.

    Stop-area-group membership and a shared actual passenger member are
    authoritative structural evidence. Unlike the 75 m fallback below, this
    merge is intentionally transitive: relations connected through the same
    passenger geometry describe one ownership component. Shared-member edges
    still require compatible modes so unrelated modal locations are not joined.
    """
    units = sorted(units, key=lambda unit: unit["id"])
    if not units:
        return []
    index_by_id = {unit["id"]: index for index, unit in enumerate(units)}
    parents = list(range(len(units)))

    def find(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left, right):
        left, right = find(left), find(right)
        if left == right:
            return
        if left > right:
            left, right = right, left
        parents[right] = left

    for group in sorted(groups, key=lambda value: int(value["id"])):
        indexes = sorted(index_by_id[area_id]
                         for area_id in set(group.get("stop_area_ids", ()))
                         if area_id in index_by_id)
        for index in indexes[1:]:
            union(indexes[0], index)

    owners_by_member = defaultdict(list)
    for index, unit in enumerate(units):
        for member in unit["members"]:
            owners_by_member[_object_key(member)].append(index)
    for indexes in owners_by_member.values():
        indexes = sorted(set(indexes))
        for position, left in enumerate(indexes):
            for right in indexes[position + 1:]:
                if _compatible_modes(units[left]["modes"], units[right]["modes"]):
                    union(left, right)

    grouped = defaultdict(list)
    for index, unit in enumerate(units):
        grouped[find(index)].append(unit)
    group_by_area = defaultdict(list)
    for group in groups:
        for area_id in group.get("stop_area_ids", ()):
            group_by_area[int(area_id)].append(group)

    components = []
    for component_units in grouped.values():
        area_ids = sorted(unit["id"] for unit in component_units)
        component_groups = {
            int(group["id"]): group
            for area_id in area_ids for group in group_by_area.get(area_id, ())
        }
        winning_group = (component_groups[min(component_groups)]
                         if component_groups else None)
        members = [member for unit in component_units for member in unit["members"]]
        representative = representative_member(members)
        modes = set().union(*(unit["modes"] for unit in component_units))
        if len(modes) > 1:
            modes.discard("GENERIC")
        components.append({
            "area_ids": area_ids,
            "members": members,
            "name": (winning_group.get("name") if winning_group else
                     (component_units[0]["name"] if len(component_units) == 1 else None)),
            "normalized_name": normalize_name(
                winning_group.get("name") if winning_group else
                _preferred_name(None, members)),
            "lat": representative["lat"], "lon": representative["lon"],
            "modes": modes or {"GENERIC"},
            "is_rail": any(unit["is_rail"] for unit in component_units),
            "group_id": int(winning_group["id"]) if winning_group else None,
            "stable_key": "areas:" + ",".join(str(value) for value in area_ids),
        })
    return sorted(components, key=lambda component: component["stable_key"])


def build_logical_access(members, stop_areas=(), stop_area_groups=()):
    """Derive deterministic logical residential access rows from parsed data."""
    members_by_key = {_object_key(member): dict(member) for member in members}
    stop_areas_by_id = {int(area["id"]): dict(area) for area in stop_areas}
    groups = sorted((dict(group) for group in stop_area_groups),
                    key=lambda group: int(group["id"]))
    assigned_members = set()
    access_rows = []

    units = []
    for area_id in sorted(stop_areas_by_id):
        unit = _unit_from_stop_area(stop_areas_by_id[area_id], members_by_key)
        if unit:
            units.append(unit)

    # Structural relation evidence is resolved before geographic fallback.
    # This gives every shared member one logical relation-backed owner without
    # weakening complete-link's no-proximity-chain behavior.
    components = _structural_stop_area_components(units, groups)
    relation_clusters = []
    named_components = defaultdict(list)
    unnamed_components = []
    for component in components:
        if component["group_id"] is not None or component["is_rail"]:
            relation_clusters.append([component])
        elif component["normalized_name"]:
            named_components[component["normalized_name"]].append(component)
        else:
            unnamed_components.append(component)
    for name in sorted(named_components):
        relation_clusters.extend(_cluster_complete_link(
            named_components[name], TRANSIT_NAMED_FALLBACK_DISTANCE_M,
            lambda left, right: _compatible_modes(left["modes"], right["modes"])))
    relation_clusters.extend([[component] for component in sorted(
        unnamed_components, key=lambda component: component["stable_key"])])

    def relation_precedence(cluster):
        if any(component["group_id"] is not None for component in cluster):
            return 0
        if any(component["is_rail"] for component in cluster):
            return 1
        return 2

    # Group, station-level rail, and local relation accesses claim members in
    # that order. Compatible shared candidates are already one component; this
    # filter handles incompatible malformed relationships deterministically.
    for cluster in sorted(relation_clusters, key=lambda value: (
            relation_precedence(value),
            tuple(component["stable_key"] for component in value))):
        area_ids = sorted({area_id for component in cluster
                           for area_id in component["area_ids"]})
        group_ids = sorted({component["group_id"] for component in cluster
                            if component["group_id"] is not None})
        cluster_members = [member for component in cluster
                           for member in component["members"]
                           if _object_key(member) not in assigned_members]
        if not cluster_members:
            continue
        cluster_keys = {_object_key(member) for member in cluster_members}
        area_ids = [area_id for area_id in area_ids
                    if any(key in cluster_keys for key, _role in
                           stop_areas_by_id[area_id]["members"])]
        explicit_name = cluster[0]["name"] if len(cluster) == 1 else None
        row = _make_access(
            cluster_members, explicit_name, area_ids,
            group_ids[0] if group_ids else None,
            is_rail_station=any(component["is_rail"] for component in cluster))
        if row:
            access_rows.append(row)
            assigned_members.update(_object_key(member) for member in cluster_members)

    # Fallback grouping operates only on unowned members and therefore cannot
    # reclaim relation-owned station platforms or shared local-stop objects.
    raw = [member for key, member in members_by_key.items()
           if key not in assigned_members and _has_coordinate(member)]
    named_groups = defaultdict(list)
    unnamed_groups = defaultdict(list)
    for member in raw:
        member = dict(member)
        member["modes"] = derive_direct_modes(member)
        member["stable_key"] = f"{member['osm_type']}:{member['osm_id']}"
        normalized = normalize_name(member.get("name") or member.get("tags", {}).get("name"))
        if normalized:
            named_groups[normalized].append(member)
        else:
            unnamed_groups[(member_subtype(member), tuple(sorted(member["modes"])))].append(member)

    raw_clusters = []
    for name in sorted(named_groups):
        raw_clusters.extend(_cluster_complete_link(
            named_groups[name], TRANSIT_NAMED_FALLBACK_DISTANCE_M,
            lambda left, right: _compatible_modes(left["modes"], right["modes"])))
    for key in sorted(unnamed_groups):
        raw_clusters.extend(_cluster_complete_link(
            unnamed_groups[key], TRANSIT_UNNAMED_FALLBACK_DISTANCE_M,
            lambda _left, _right: True))
    for cluster in raw_clusters:
        row = _make_access(cluster)
        if row:
            access_rows.append(row)

    # UIC is the strongest rail identity. Coalesce disconnected authoritative
    # representations carrying the same UIC. Other collisions remain fatal.
    rows_by_id = defaultdict(list)
    for row in access_rows:
        rows_by_id[row["logical_stop_id"]].append(row)
    coalesced = []
    member_by_text_key = {
        f"{member['osm_type']}:{member['osm_id']}": member
        for member in members_by_key.values()
    }
    for logical_id in sorted(rows_by_id):
        duplicate_rows = rows_by_id[logical_id]
        if len(duplicate_rows) == 1:
            coalesced.append(duplicate_rows[0])
            continue
        if not logical_id.startswith("uic:"):
            raise ValueError(f"Logical stop identity collision: {logical_id}")
        combined_members = [member_by_text_key[key]
                            for row in duplicate_rows for key in row["member_osm_keys"]]
        combined_area_ids = [area_id for row in duplicate_rows
                             for area_id in row["stop_area_ids"]]
        merged = _make_access(
            combined_members, stop_area_ids=combined_area_ids,
            is_rail_station=True)
        if merged is None or merged["logical_stop_id"] != logical_id:
            raise ValueError(f"Unable to coalesce UIC identity: {logical_id}")
        coalesced.append(merged)
    access_rows = coalesced

    # A station-level object outside relation topology may still be a duplicate
    # station building/entrance. Absorb it only when exactly one authoritative
    # UIC station has the same normalized name within the existing conservative
    # 75 m named threshold. This is a direct comparison to the UIC access, never
    # a transitive proximity chain; unnamed or differently named rows remain.
    uic_rows = {row["logical_stop_id"]: row for row in access_rows
                if row["identity_source"] == "uic"}
    retained = []
    for row in sorted(access_rows, key=lambda value: value["logical_stop_id"]):
        normalized = normalize_name(row["name"])
        if (not row["is_rail_station"] or row["identity_source"] in
                {"uic", "stop_area_group"} or not normalized):
            retained.append(row)
            continue
        matches = [candidate for candidate in uic_rows.values()
                   if normalize_name(candidate["name"]) == normalized
                   and _within(haversine_m(
                       row["lat"], row["lon"], candidate["lat"], candidate["lon"]),
                       TRANSIT_NAMED_FALLBACK_DISTANCE_M)]
        if len(matches) != 1:
            retained.append(row)
            continue
        target = matches[0]
        combined_members = [member_by_text_key[key] for key in
                            target["member_osm_keys"] + row["member_osm_keys"]]
        merged = _make_access(
            combined_members, target["name"],
            target["stop_area_ids"] + row["stop_area_ids"],
            is_rail_station=True)
        if merged is None or merged["logical_stop_id"] != target["logical_stop_id"]:
            raise ValueError(
                f"Unable to absorb named station into {target['logical_stop_id']}")
        uic_rows[target["logical_stop_id"]] = merged
    access_rows = [row for row in retained if row["identity_source"] != "uic"]
    access_rows.extend(uic_rows.values())
    access_rows.sort(key=lambda row: row["logical_stop_id"])

    ids = [row["logical_stop_id"] for row in access_rows]
    if len(ids) != len(set(ids)):
        raise ValueError("Logical stop identity collision detected")
    owners = Counter(key for row in access_rows for key in row["member_osm_keys"])
    duplicate_owners = [key for key, count in owners.items() if count > 1]
    if duplicate_owners:
        raise ValueError(
            f"Transit members have multiple logical owners: {duplicate_owners[:5]}")
    return access_rows


def _relevant_by_tags(tags):
    return (
        tags.get("highway") == "bus_stop"
        or tags.get("amenity") in {"bus_station", "ferry_terminal"}
        or tags.get("public_transport") in {"platform", "stop_position", "station"}
        or tags.get("railway") in {
            "station", "halt", "tram_stop", "subway_entrance"}
        or any(_tag_yes(tags, key) for key in
               ("bus", "tram", "train", "subway", "light_rail", "ferry"))
    )


def _osmium_type(obj, osmium):
    if isinstance(obj, osmium.osm.Node):
        return "node"
    if isinstance(obj, osmium.osm.Way):
        return "way"
    return "relation"


def _copy_osm_object(obj, osmium):
    osm_type = _osmium_type(obj, osmium)
    tags = {key: value for key, value in obj.tags if key in TAG_FIELDS}
    record = {
        "osm_type": osm_type, "osm_id": int(obj.id),
        "name": tags.get("name"), "lat": None, "lon": None, "tags": tags,
        "child_keys": [],
    }
    if osm_type == "node" and obj.location.valid():
        record["lat"], record["lon"] = float(obj.location.lat), float(obj.location.lon)
    elif osm_type == "way":
        record["child_keys"] = [("node", int(node.ref)) for node in obj.nodes]
    else:
        record["child_keys"] = [
            ({"n": "node", "w": "way", "r": "relation"}[member.type], int(member.ref))
            for member in obj.members]
    return record


def _route_member_is_passenger(member):
    role = normalize_name(member.role) or ""
    return ("stop" in role or "platform" in role
            or (member.type == "n" and role in {"", "forward", "backward"}))


def collect_transit_data(input_path):
    """Read an OSM file in four streaming, C-filtered passes."""
    import osmium

    bits = osmium.osm.osm_entity_bits
    stop_areas = {}
    stop_area_groups = {}
    routes = {}
    required_keys = set()

    # Pass 1: relation topology and normalized route identities.
    processor = osmium.FileProcessor(input_path, entities=bits.RELATION).with_filter(
        osmium.filter.KeyFilter("public_transport", "route"))
    for relation in processor:
        tags = {key: value for key, value in relation.tags}
        public_transport = tags.get("public_transport")
        raw_members = [
            (({"n": "node", "w": "way", "r": "relation"}[member.type], int(member.ref)),
             member.role or "") for member in relation.members]
        if public_transport == "stop_area":
            stop_areas[int(relation.id)] = {
                "id": int(relation.id), "name": tags.get("name"), "members": raw_members}
            required_keys.update(key for key, _role in raw_members)
        elif public_transport == "stop_area_group":
            stop_area_groups[int(relation.id)] = {
                "id": int(relation.id), "name": tags.get("name"),
                "stop_area_ids": sorted({key[1] for key, _role in raw_members
                                         if key[0] == "relation"}),
            }
        route_mode = tags.get("route")
        if route_mode in ROUTE_MODE_MAP:
            route_key = normalize_route_key(
                route_mode, tags.get("network"), tags.get("operator"), tags.get("ref"),
                tags.get("name"), relation.id)
            passenger_members = []
            for member in relation.members:
                if _route_member_is_passenger(member):
                    key = ({"n": "node", "w": "way", "r": "relation"}[member.type],
                           int(member.ref))
                    passenger_members.append((key, member.role or ""))
                    required_keys.add(key)
            routes[int(relation.id)] = {
                "id": int(relation.id), "key": route_key,
                "mode": ROUTE_MODE_MAP[route_mode],
                "ref": tags.get("ref"), "members": passenger_members,
            }

    objects = {}
    passenger_keys = set()
    geometry_keys = set()
    # Pass 2: all independently relevant passenger objects, filtered in C.
    processor = osmium.FileProcessor(input_path).with_filter(
        osmium.filter.KeyFilter(*FILTER_KEYS))
    for obj in processor:
        copied = _copy_osm_object(obj, osmium)
        if _relevant_by_tags(copied["tags"]):
            key = _object_key(copied)
            objects[key] = copied
            passenger_keys.add(key)
            geometry_keys.update(copied["child_keys"])

    # Pass 3: weakly tagged objects proven relevant by relation membership,
    # plus children of already collected relation/way passenger objects.
    # Geometry vertex nodes are coordinate support, not passenger objects. They
    # are fetched in pass 4 and must never become standalone access rows.
    target_keys = required_keys | {key for key in geometry_keys if key[0] != "node"}
    if target_keys:
        target_ids = sorted({key[1] for key in target_keys})
        processor = osmium.FileProcessor(input_path).with_filter(osmium.filter.IdFilter(target_ids))
        for obj in processor:
            copied = _copy_osm_object(obj, osmium)
            key = _object_key(copied)
            if key in target_keys:
                objects[key] = copied
                if key in required_keys:
                    passenger_keys.add(key)

    # Pass 4: coordinates only for nodes used by collected passenger ways.
    way_node_ids = sorted({child[1] for obj in objects.values()
                           if obj["osm_type"] == "way" for child in obj["child_keys"]
                           if child[0] == "node"})
    coordinates = {}
    if way_node_ids:
        processor = osmium.FileProcessor(input_path, entities=bits.NODE).with_filter(
            osmium.filter.IdFilter(way_node_ids))
        for node in processor:
            if node.location.valid():
                coordinates[int(node.id)] = (float(node.location.lat), float(node.location.lon))
    for obj in objects.values():
        if obj["osm_type"] == "way":
            points = [coordinates[key[1]] for key in obj["child_keys"]
                      if key[0] == "node" and key[1] in coordinates]
            if points:
                obj["lat"] = sum(point[0] for point in points) / len(points)
                obj["lon"] = sum(point[1] for point in points) / len(points)
    # Relation members (for example, complex station platforms) receive a
    # representative based only on already collected direct passenger geometry.
    for _depth in range(2):
        for obj in objects.values():
            if obj["osm_type"] == "relation" and obj["lat"] is None:
                children = [objects[key] for key in obj["child_keys"]
                            if key in objects and _has_coordinate(objects[key])]
                if children:
                    obj["lat"] = sum(child["lat"] for child in children) / len(children)
                    obj["lon"] = sum(child["lon"] for child in children) / len(children)

    stop_area_memberships = defaultdict(set)
    for area in stop_areas.values():
        for key, role in area["members"]:
            stop_area_memberships[key].add((area["id"], role))
    area_groups = defaultdict(set)
    for group in stop_area_groups.values():
        for area_id in group["stop_area_ids"]:
            area_groups[area_id].add(group["id"])
    route_memberships = defaultdict(set)
    for route in routes.values():
        for key, role in route["members"]:
            route_memberships[key].add((
                route["key"], route["mode"], route.get("ref"), route["id"], role))

    members = []
    for key in sorted(passenger_keys,
                      key=lambda value: (OSM_TYPE_ORDER[value[0]], value[1])):
        obj = objects[key]
        memberships = sorted(stop_area_memberships.get(key, ()),
                             key=lambda value: (value[0], value[1]))
        route_values = sorted(route_memberships.get(key, ()),
                              key=lambda value: (value[3], value[4], value[0]))
        group_ids = sorted({group_id for area_id, _role in memberships
                            for group_id in area_groups.get(area_id, ())})
        row = {
            "osm_type": obj["osm_type"], "osm_id": obj["osm_id"],
            "name": obj["name"], "lat": obj["lat"], "lon": obj["lon"],
            **{field: obj["tags"].get(field) for field in TAG_FIELDS if field != "name"},
            "stop_area_ids": [area_id for area_id, _role in memberships],
            "stop_area_roles": [role for _area_id, role in memberships],
            "stop_area_group_ids": group_ids,
            "route_keys": sorted({value[0] for value in route_values}),
            "route_modes": sorted({value[1] for value in route_values}),
            "route_refs": sorted({value[2] for value in route_values if value[2]},
                                 key=lambda value: (value.casefold(), value)),
            "route_relation_ids": [value[3] for value in route_values],
            "route_roles": [value[4] for value in route_values],
        }
        row["tags"] = {field: row.get(field) for field in TAG_FIELDS if row.get(field) is not None}
        members.append(row)

    # Route and stop-area evidence is inherited only for classification. Raw
    # tag/route fields remain unchanged for auditability.
    members_by_key = {_object_key(member): member for member in members}
    for area in stop_areas.values():
        area_members = [members_by_key[key] for key, _role in area["members"]
                        if key in members_by_key]
        area_modes = set().union(*(derive_direct_modes(member) for member in area_members))
        area_modes.discard("GENERIC") if len(area_modes) > 1 else None
        for member in area_members:
            member["inherited_modes"] = sorted(
                set(member.get("inherited_modes", ())) | area_modes)
            # Access derivation consumes the inherited modes through a local
            # synthetic route-mode view without changing stored route metadata.
            member["tags"] = dict(member["tags"])
    for member in members:
        if member.get("inherited_modes"):
            original = set(member["route_modes"])
            member["classification_modes"] = sorted(
                original | set(member["inherited_modes"]))
        else:
            member["classification_modes"] = list(member["route_modes"])

    access = build_logical_access(
        members, list(stop_areas.values()), list(stop_area_groups.values()))
    return members, access, {
        "stop_area_count": len(stop_areas),
        "stop_area_group_count": len(stop_area_groups),
        "normalized_route_count": len({route["key"] for route in routes.values()}),
    }


def _schemas():
    import pyarrow as pa
    string_list = pa.list_(pa.string())
    int_list = pa.list_(pa.int64())
    members_schema = pa.schema([
        ("osm_type", pa.string()), ("osm_id", pa.int64()), ("name", pa.string()),
        ("lat", pa.float64()), ("lon", pa.float64()),
        *((field, pa.string()) for field in TAG_FIELDS if field != "name"),
        ("stop_area_ids", int_list), ("stop_area_roles", string_list),
        ("stop_area_group_ids", int_list), ("route_keys", string_list),
        ("route_modes", string_list), ("route_refs", string_list),
        ("route_relation_ids", int_list), ("route_roles", string_list),
    ])
    access_schema = pa.schema([
        ("logical_stop_id", pa.string()), ("identity_source", pa.string()),
        ("name", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()),
        ("modes", string_list), ("is_rail_station", pa.bool_()),
        ("stop_area_ids", int_list), ("stop_area_group_id", pa.int64()),
        ("member_count", pa.int32()), ("member_osm_keys", string_list),
        ("representative_osm_type", pa.string()),
        ("representative_osm_id", pa.int64()), ("route_count", pa.int32()),
        ("route_modes", string_list), ("route_refs", string_list),
    ])
    return members_schema, access_schema


def _write_atomic(rows, schema, output_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table_rows = [{name: row.get(name) for name in schema.names} for row in rows]
    table = pa.Table.from_pylist(table_rows, schema=schema)
    handle, temporary = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent)
    os.close(handle)
    try:
        pq.write_table(table, temporary, compression="zstd")
        os.replace(temporary, output_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def build_transit_caches(input_path, members_output, access_output):
    members, access, diagnostics = collect_transit_data(str(input_path))
    members_schema, access_schema = _schemas()
    _write_atomic(members, members_schema, members_output)
    _write_atomic(access, access_schema, access_output)
    return {**diagnostics, "member_count": len(members), "access_count": len(access)}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build relation-aware Transit Cache V1")
    parser.add_argument("--pbf", required=True, help="Input OSM PBF/XML file")
    parser.add_argument("--members-out", default="cache/be_transit_members.parquet")
    parser.add_argument("--access-out", default="cache/be_transit_access.parquet")
    args = parser.parse_args(argv)
    diagnostics = build_transit_caches(args.pbf, args.members_out, args.access_out)
    print("Transit Cache V1 built:")
    for key in sorted(diagnostics):
        print(f"  {key}: {diagnostics[key]}")
    print(f"  members: {Path(args.members_out).resolve()}")
    print(f"  access: {Path(args.access_out).resolve()}")


if __name__ == "__main__":
    main()

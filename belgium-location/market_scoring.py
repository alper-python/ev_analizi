"""Dependency-free Market Score V1 classification, deduplication and scoring."""

import math
import re
import unicodedata


MARKET_SCORING_RADIUS_M = 2500
MARKET_NAMED_DEDUP_DISTANCE_M = 75
MARKET_UNNAMED_DEDUP_DISTANCE_M = 10
MARKET_TYPE_WEIGHTS = {"supermarket": 1.0, "convenience": 0.6}


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
        return bool(value != value)  # NaN, including NumPy floating scalars
    except (TypeError, ValueError):
        return False


def normalize_market_name(name):
    """Normalize Unicode, case and whitespace without fuzzy matching."""
    if _is_missing(name):
        return None
    normalized = unicodedata.normalize("NFKC", str(name)).casefold().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized or None


def market_type(poi):
    """Classify a qualifying grocery; every marketplace-tagged POI is excluded."""
    if poi.get("amenity") == "marketplace":
        return None
    shop = poi.get("shop")
    return shop if shop in MARKET_TYPE_WEIGHTS else None


def _dedup_name(poi):
    """Return a genuine name candidate without assuming brand equals name.

    The current polygon cache may populate ``name`` from ``brand``. If both
    cached values are identical, provenance is ambiguous and V1 treats the
    polygon as unnamed. This deliberately prefers a possible duplicate false
    negative over merging separate businesses.
    """
    name = normalize_market_name(poi.get("name"))
    brand = normalize_market_name(poi.get("brand"))
    if poi.get("source") == "polygon" and brand and name == brand:
        return None
    return name


def _stable_row_key(row, index):
    return (
        market_type(row) or "",
        _dedup_name(row) or "",
        float(row["lat"]),
        float(row["lon"]),
        str(row.get("name") or ""),
        float(row.get("d_lin", math.inf)),
        index,
    )


def _maximum_cardinality_min_distance_pairs(rows, candidate_edges):
    """Return deterministic min-distance pairs among all maximum matchings.

    ``candidate_edges`` contains ``(node_index, polygon_index, distance)``.
    Successive shortest augmenting paths produce maximum cardinality, then
    minimum total separation. Stable row ordering and edge ordering resolve
    exact-cost ties deterministically.
    """
    if not candidate_edges:
        return []

    node_indexes = sorted({edge[0] for edge in candidate_edges},
                          key=lambda i: _stable_row_key(rows[i], i))
    polygon_indexes = sorted({edge[1] for edge in candidate_edges},
                             key=lambda i: _stable_row_key(rows[i], i))
    node_vertex = {index: pos + 1 for pos, index in enumerate(node_indexes)}
    polygon_vertex = {
        index: 1 + len(node_indexes) + pos for pos, index in enumerate(polygon_indexes)
    }
    source = 0
    sink = 1 + len(node_indexes) + len(polygon_indexes)
    graph = [[] for _ in range(sink + 1)]

    def add_edge(start, end, capacity, cost):
        forward = [end, len(graph[end]), capacity, float(cost)]
        reverse = [start, len(graph[start]), 0, -float(cost)]
        graph[start].append(forward)
        graph[end].append(reverse)
        return forward

    for index in node_indexes:
        add_edge(source, node_vertex[index], 1, 0)
    for index in polygon_indexes:
        add_edge(polygon_vertex[index], sink, 1, 0)

    match_edges = []
    for node_index, polygon_index, distance in sorted(
            candidate_edges,
            key=lambda edge: (edge[2], _stable_row_key(rows[edge[0]], edge[0]),
                              _stable_row_key(rows[edge[1]], edge[1]))):
        edge = add_edge(node_vertex[node_index], polygon_vertex[polygon_index], 1, distance)
        match_edges.append((node_index, polygon_index, edge))

    # Bellman-Ford supports negative-cost residual edges created by reassigning
    # an earlier pair. Unit capacities mean every successful path adds one pair.
    while True:
        distances = [math.inf] * len(graph)
        predecessor = [None] * len(graph)
        distances[source] = 0.0
        for _ in range(len(graph) - 1):
            changed = False
            for start, edges in enumerate(graph):
                if math.isinf(distances[start]):
                    continue
                for edge_index, edge in enumerate(edges):
                    end, _reverse, capacity, cost = edge
                    candidate = distances[start] + cost
                    if capacity and candidate < distances[end] - 1e-9:
                        distances[end] = candidate
                        predecessor[end] = (start, edge_index)
                        changed = True
            if not changed:
                break
        if predecessor[sink] is None:
            break

        vertex = sink
        while vertex != source:
            start, edge_index = predecessor[vertex]
            edge = graph[start][edge_index]
            edge[2] -= 1
            graph[vertex][edge[1]][2] += 1
            vertex = start

    return sorted(
        ((node_index, polygon_index) for node_index, polygon_index, edge in match_edges
         if edge[2] == 0),
        key=lambda pair: (_stable_row_key(rows[pair[0]], pair[0]),
                          _stable_row_key(rows[pair[1]], pair[1])),
    )


def _spatial_cell(row, cell_size_m):
    lat = float(row["lat"])
    y = lat * 111320.0
    x = float(row["lon"]) * 111320.0 * math.cos(math.radians(lat))
    return math.floor(x / cell_size_m), math.floor(y / cell_size_m)


def deduplicate_market_pois(pois):
    """Conservatively merge qualifying one-to-one node/polygon duplicates."""
    rows = [dict(poi) for poi in pois if market_type(poi)]
    named_nodes = {}
    named_polygons = {}
    unnamed_nodes = {}
    unnamed_polygon_buckets = {}

    for index, row in enumerate(rows):
        source = row.get("source")
        if source not in {"node", "polygon"}:
            continue
        grocery_type = market_type(row)
        name = _dedup_name(row)
        if name:
            groups = named_nodes if source == "node" else named_polygons
            groups.setdefault((grocery_type, name), []).append(index)
        elif source == "node":
            unnamed_nodes.setdefault(grocery_type, []).append(index)
        else:
            cell = _spatial_cell(row, MARKET_UNNAMED_DEDUP_DISTANCE_M)
            unnamed_polygon_buckets.setdefault((grocery_type, cell), []).append(index)

    candidates = []
    for group, node_indexes in named_nodes.items():
        for node_index in node_indexes:
            for polygon_index in named_polygons.get(group, []):
                separation = haversine_m(
                    rows[node_index]["lat"], rows[node_index]["lon"],
                    rows[polygon_index]["lat"], rows[polygon_index]["lon"])
                if separation <= MARKET_NAMED_DEDUP_DISTANCE_M:
                    candidates.append((node_index, polygon_index, separation))

    for grocery_type, node_indexes in unnamed_nodes.items():
        for node_index in node_indexes:
            x_cell, y_cell = _spatial_cell(rows[node_index], MARKET_UNNAMED_DEDUP_DISTANCE_M)
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for polygon_index in unnamed_polygon_buckets.get(
                            (grocery_type, (x_cell + dx, y_cell + dy)), []):
                        separation = haversine_m(
                            rows[node_index]["lat"], rows[node_index]["lon"],
                            rows[polygon_index]["lat"], rows[polygon_index]["lon"])
                        if separation <= MARKET_UNNAMED_DEDUP_DISTANCE_M:
                            candidates.append((node_index, polygon_index, separation))

    pairs = _maximum_cardinality_min_distance_pairs(rows, candidates)
    matched = {index for pair in pairs for index in pair}
    result = [
        dict(min((rows[node_index], rows[polygon_index]),
                 key=lambda poi: float(poi["d_lin"])))
        for node_index, polygon_index in pairs
    ]
    result.extend(dict(row) for index, row in enumerate(rows) if index not in matched)
    return result


def calc_market_score(pois):
    """Calculate Market Score V1 for already deduplicated grocery POIs."""
    effective_count = 0.0
    best_weighted_proximity = 0.0
    for poi in pois:
        grocery_type = market_type(poi)
        weight = MARKET_TYPE_WEIGHTS.get(grocery_type, 0.0)
        distance = float(poi.get("d_lin", math.inf))
        if weight <= 0 or distance < 0 or distance > MARKET_SCORING_RADIUS_M:
            continue
        distance_factor = max(0.0, 1.0 - distance / MARKET_SCORING_RADIUS_M)
        best_weighted_proximity = max(best_weighted_proximity, weight * distance_factor)
        effective_count += weight

    proximity_points = 7.0 * best_weighted_proximity
    count_points = min(effective_count, 3.0)
    return max(0.0, min(10.0, proximity_points + count_points))

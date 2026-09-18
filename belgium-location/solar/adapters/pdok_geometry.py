"""Extract local LoD building geometry from PDOK CityJSON.

PDOK building tiles use a parent Building object for identity / extent and
BuildingPart children for detailed 3D geometry. Solar Analysis selects the
best supported LoD from those BuildingPart objects and loads only the global
vertices referenced by the selected local geometry.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from numbers import Integral
from typing import Any, BinaryIO, Iterable

import ijson


PREFERRED_LODS = (
    "2.2",
    "1.3",
    "1.2",
)


@dataclass(frozen=True)
class CityJsonContext:
    reference_system: str | None
    scale: tuple[float, float, float]
    translate: tuple[float, float, float]


@dataclass(frozen=True)
class SelectedPartGeometry:
    parent_id: str
    part_id: str
    lod: str
    geometry_type: str
    boundaries: Any
    semantics: dict[str, Any] | None


def _to_float(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)

    return float(value)


def read_cityjson_context(
    transform_source: BinaryIO,
    metadata_source: BinaryIO,
) -> CityJsonContext:
    """Read the global transform and CRS metadata."""

    transform = next(
        ijson.items(
            transform_source,
            "transform",
        ),
        None,
    )

    if not isinstance(transform, dict):
        raise ValueError(
            "CityJSON transform object is missing."
        )

    scale = transform.get("scale")
    translate = transform.get("translate")

    if (
        not isinstance(scale, list)
        or len(scale) != 3
        or not isinstance(translate, list)
        or len(translate) != 3
    ):
        raise ValueError(
            "CityJSON transform must contain three-value "
            "scale and translate arrays."
        )

    metadata = next(
        ijson.items(
            metadata_source,
            "metadata",
        ),
        {},
    )

    return CityJsonContext(
        reference_system=(
            metadata.get("referenceSystem")
            if isinstance(metadata, dict)
            else None
        ),
        scale=tuple(
            _to_float(value)
            for value in scale
        ),
        translate=tuple(
            _to_float(value)
            for value in translate
        ),
    )


def choose_preferred_geometries(
    city_object: dict[str, Any],
    *,
    preferred_lods: tuple[str, ...] = PREFERRED_LODS,
) -> list[dict[str, Any]]:
    """Return every geometry at the best available preferred LoD."""

    geometries = city_object.get("geometry") or []

    for preferred in preferred_lods:
        matches = [
            geometry
            for geometry in geometries
            if str(geometry.get("lod")) == preferred
        ]

        if matches:
            return matches

    return []


def load_selected_parts(
    source: BinaryIO,
    *,
    child_to_parent: dict[str, str],
) -> list[SelectedPartGeometry]:
    """Load only BuildingPart objects requested by nearby parent Buildings."""

    wanted = set(child_to_parent)
    results: list[SelectedPartGeometry] = []

    if not wanted:
        return results

    for object_id, city_object in ijson.kvitems(
        source,
        "CityObjects",
    ):
        if object_id not in wanted:
            continue

        if city_object.get("type") != "BuildingPart":
            continue

        for geometry in choose_preferred_geometries(
            city_object
        ):
            results.append(
                SelectedPartGeometry(
                    parent_id=child_to_parent[object_id],
                    part_id=str(object_id),
                    lod=str(geometry.get("lod")),
                    geometry_type=str(
                        geometry.get("type", "")
                    ),
                    boundaries=geometry.get("boundaries"),
                    semantics=geometry.get("semantics"),
                )
            )

        if {
            item.part_id
            for item in results
        } >= wanted:
            break

    return results


def iter_vertex_indices(value: Any):
    """Yield every CityJSON vertex index inside nested boundaries."""

    if isinstance(value, Integral):
        yield int(value)
        return

    if isinstance(value, list):
        for item in value:
            yield from iter_vertex_indices(item)


def required_vertex_indices(
    geometries: Iterable[SelectedPartGeometry],
) -> set[int]:
    indices: set[int] = set()

    for geometry in geometries:
        indices.update(
            iter_vertex_indices(
                geometry.boundaries
            )
        )

    return indices


def transform_vertex(
    vertex: list[Any],
    *,
    context: CityJsonContext,
) -> tuple[float, float, float]:
    """Apply the CityJSON Transform Object to one integer vertex."""

    if len(vertex) != 3:
        raise ValueError(
            "A CityJSON vertex must contain exactly three values."
        )

    return tuple(
        _to_float(vertex[index])
        * context.scale[index]
        + context.translate[index]
        for index in range(3)
    )


def load_selected_vertices(
    source: BinaryIO,
    *,
    indices: set[int],
    context: CityJsonContext,
) -> dict[int, tuple[float, float, float]]:
    """Stream the global vertex array and retain only required vertices."""

    if not indices:
        return {}

    result: dict[int, tuple[float, float, float]] = {}

    for index, vertex in enumerate(
        ijson.items(
            source,
            "vertices.item",
        )
    ):
        if index in indices:
            result[index] = transform_vertex(
                vertex,
                context=context,
            )

            if len(result) == len(indices):
                break

    missing = indices.difference(result)

    if missing:
        raise ValueError(
            f"{len(missing)} referenced CityJSON vertices "
            "could not be found."
        )

    return result


def semantic_surface_counts(
    geometry: SelectedPartGeometry,
) -> Counter[str]:
    """Count primitive semantic surface assignments in one geometry."""

    semantics = geometry.semantics or {}
    definitions = semantics.get("surfaces") or []
    values = semantics.get("values")

    index_to_type = {
        index: str(surface.get("type", "Unknown"))
        for index, surface in enumerate(definitions)
    }

    counts: Counter[str] = Counter()

    def visit(value: Any) -> None:
        if value is None:
            return

        if isinstance(value, Integral):
            counts[
                index_to_type.get(
                    int(value),
                    "Unknown",
                )
            ] += 1
            return

        if isinstance(value, list):
            for item in value:
                visit(item)

    visit(values)

    return counts

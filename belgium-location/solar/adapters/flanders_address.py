"""Resolve Flemish addresses to authoritative building footprints.

Resolution flow:

postcode + street + house number
    -> Basisregisters Adres
    -> related realized Gebouweenheid
    -> Gebouw ObjectId
    -> Gebouwenregister OGC footprint
    -> realized unit count

Geometry is retained in Belgian Lambert 72 / EPSG:31370 because DHMV
elevation products use the same projected coordinate system.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from pyproj import Transformer


BASIS_BASE_URL = (
    "https://api.basisregisters.vlaanderen.be/v2/"
)

BUILDING_OGC_BASE_URL = (
    "https://geo.api.vlaanderen.be/"
    "Gebouwenregister/ogc/features/v1"
)

LAMBERT72_TO_WGS84 = Transformer.from_crs(
    31370,
    4326,
    always_xy=True,
)


@dataclass(frozen=True)
class FlandersBuilding:
    object_id: int
    status: str | None
    geometry_method: str | None
    realized_unit_count: int | None
    footprint_geojson: dict[str, Any]
    footprint_bounds: tuple[
        float,
        float,
        float,
        float,
    ]

    @property
    def is_shared_building(
        self,
    ) -> bool | None:
        if self.realized_unit_count is None:
            return None

        return (
            self.realized_unit_count
            > 1
        )


@dataclass(frozen=True)
class FlandersAddressResolution:
    address_object_id: int
    full_address: str
    street: str
    house_number: str
    postcode: str
    city: str | None
    status: str | None

    x_lambert72: float
    y_lambert72: float

    lat: float
    lon: float

    building_unit_ids: tuple[
        int,
        ...,
    ]

    buildings: tuple[
        FlandersBuilding,
        ...,
    ]


def _default_json_get(
    url: str,
) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": (
                "application/ld+json, "
                "application/geo+json, "
                "application/json"
            ),
            "User-Agent": (
                "DomiFrame-Solar/1.0"
            ),
        },
    )

    try:
        with urlopen(
            request,
            timeout=30,
        ) as response:
            return json.load(
                response
            )

    except HTTPError as exc:
        try:
            body = exc.read().decode(
                "utf-8",
                errors="replace",
            )
        except Exception:
            body = ""

        raise RuntimeError(
            "Flanders registry request failed: "
            f"HTTP {exc.code}\n"
            f"URL: {url}\n"
            f"Response: {body[:3000]}"
        ) from exc


def _absolute_basis_url(
    href: str,
) -> str:
    return urljoin(
        BASIS_BASE_URL,
        href,
    )


def _extract_object_id(
    value: dict[str, Any],
    *,
    context: str,
) -> int:
    candidates = [
        value.get(
            "objectId"
        ),
        (
            value.get(
                "identificator"
            )
            or {}
        ).get(
            "objectId"
        ),
    ]

    for candidate in candidates:
        if candidate is None:
            continue

        try:
            return int(
                candidate
            )
        except (
            TypeError,
            ValueError,
        ):
            pass

    raise LookupError(
        f"No usable ObjectId found for {context}. "
        f"Keys={sorted(value.keys())}"
    )


def _nested_text(
    value: Any,
    *keys: str,
) -> str | None:
    current = value

    for key in keys:
        if not isinstance(
            current,
            dict,
        ):
            return None

        current = current.get(
            key
        )

    if current is None:
        return None

    result = str(
        current
    ).strip()

    return (
        result
        if result
        else None
    )


def _extract_gml_point(
    value: dict[str, Any],
) -> tuple[
    float,
    float,
]:
    gml = _nested_text(
        value,
        "adresPositie",
        "geometrie",
        "gml",
    )

    if not gml:
        raise LookupError(
            "Address detail contains no GML position."
        )

    root = ET.fromstring(
        gml
    )

    pos = None

    for element in root.iter():
        if element.tag.endswith(
            "pos"
        ):
            pos = element
            break

    if (
        pos is None
        or not pos.text
    ):
        raise LookupError(
            "Address GML contains no gml:pos."
        )

    parts = (
        pos.text.strip().split()
    )

    if len(
        parts
    ) < 2:
        raise LookupError(
            "Address GML point contains fewer than "
            "two coordinates."
        )

    return (
        float(
            parts[0]
        ),
        float(
            parts[1]
        ),
    )


def _coordinate_pairs(
    value: Any,
):
    if (
        isinstance(
            value,
            list,
        )
        and len(value) >= 2
        and isinstance(
            value[0],
            (int, float)
        )
        and isinstance(
            value[1],
            (int, float)
        )
    ):
        yield (
            float(
                value[0]
            ),
            float(
                value[1]
            ),
        )

        return

    if isinstance(
        value,
        list,
    ):
        for child in value:
            yield from _coordinate_pairs(
                child
            )


def _geometry_bounds(
    geometry: dict[str, Any],
) -> tuple[
    float,
    float,
    float,
    float,
]:
    pairs = list(
        _coordinate_pairs(
            geometry.get(
                "coordinates"
            )
        )
    )

    if not pairs:
        raise LookupError(
            "Building geometry contains no coordinates."
        )

    xs = [
        item[0]
        for item in pairs
    ]

    ys = [
        item[1]
        for item in pairs
    ]

    return (
        min(
            xs
        ),
        min(
            ys
        ),
        max(
            xs
        ),
        max(
            ys
        ),
    )


def _load_building_feature(
    building_id: int,
    *,
    json_get: Callable[
        [str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    params = {
        "f": "application/geo+json",
        "limit": 5,
        "filter-lang": "cql2-text",
        "filter": (
            f"ObjectId = {building_id}"
        ),
        "crs": (
            "http://www.opengis.net/"
            "def/crs/EPSG/0/31370"
        ),
    }

    url = (
        BUILDING_OGC_BASE_URL
        + "/collections/Gebouw/items?"
        + urlencode(
            params
        )
    )

    payload = json_get(
        url
    )

    features = (
        payload.get(
            "features"
        )
        or []
    )

    exact = [
        feature
        for feature in features
        if str(
            (
                feature.get(
                    "properties"
                )
                or {}
            ).get(
                "ObjectId"
            )
        )
        == str(
            building_id
        )
    ]

    if not exact:
        raise LookupError(
            "Official building footprint not found for "
            f"ObjectId={building_id}."
        )

    if len(
        exact
    ) > 1:
        raise LookupError(
            "Multiple official building footprints found for "
            f"ObjectId={building_id}."
        )

    return exact[
        0
    ]


def _realized_unit_count(
    building_id: int,
    *,
    json_get: Callable[
        [str],
        dict[str, Any],
    ],
) -> int:
    url = (
        BASIS_BASE_URL
        + "gebouweenheden?"
        + urlencode(
            {
                "gebouwObjectId": (
                    building_id
                ),
                "status": (
                    "gerealiseerd"
                ),
                "limit": 500,
            }
        )
    )

    payload = json_get(
        url
    )

    return len(
        payload.get(
            "gebouweenheden"
        )
        or []
    )


def resolve_flanders_address(
    *,
    postcode: str | int,
    street: str,
    house_number: str | int,
    json_get: Callable[
        [str],
        dict[str, Any],
    ] = _default_json_get,
) -> FlandersAddressResolution:
    """Resolve one active Flemish address to official building geometry."""

    postcode_text = str(
        postcode
    ).strip()

    street_text = str(
        street
    ).strip()

    house_text = str(
        house_number
    ).strip()

    if not postcode_text:
        raise ValueError(
            "postcode is required."
        )

    if not street_text:
        raise ValueError(
            "street is required."
        )

    if not house_text:
        raise ValueError(
            "house_number is required."
        )

    search_url = (
        BASIS_BASE_URL
        + "adressen?"
        + urlencode(
            {
                "postcode": (
                    postcode_text
                ),
                "straatnaam": (
                    street_text
                ),
                "huisnummer": (
                    house_text
                ),
                "limit": 20,
            }
        )
    )

    payload = json_get(
        search_url
    )

    candidates = (
        payload.get(
            "adressen"
        )
        or []
    )

    exact = [
        item
        for item in candidates
        if (
            str(
                item.get(
                    "huisnummer"
                )
                or ""
            ).strip()
            == house_text
            and str(
                item.get(
                    "adresStatus"
                )
                or ""
            ).lower()
            == "ingebruik"
        )
    ]

    if not exact:
        raise LookupError(
            "No active exact Flemish address found for "
            f"{street_text} {house_text}, {postcode_text}."
        )

    if len(
        exact
    ) > 1:
        raise LookupError(
            "Address search is ambiguous. "
            "Sub-address/bus-number handling is not implemented yet. "
            f"Matches={len(exact)}"
        )

    address_item = exact[
        0
    ]

    address_object_id = (
        _extract_object_id(
            address_item,
            context=(
                "address search result"
            ),
        )
    )

    detail_href = (
        address_item.get(
            "detail"
        )
        or (
            BASIS_BASE_URL
            + "adressen/"
            + str(
                address_object_id
            )
        )
    )

    detail = json_get(
        _absolute_basis_url(
            detail_href
        )
    )

    x, y = _extract_gml_point(
        detail
    )

    lon, lat = (
        LAMBERT72_TO_WGS84.transform(
            x,
            y,
        )
    )

    links = (
        detail.get(
            "_links"
        )
        or {}
    )

    units_href = _nested_text(
        links,
        "gebouweenheden",
        "href",
    )

    if units_href:
        units_url = (
            _absolute_basis_url(
                units_href
            )
        )
    else:
        units_url = (
            BASIS_BASE_URL
            + "gebouweenheden?"
            + urlencode(
                {
                    "adresObjectId": (
                        address_object_id
                    ),
                    "limit": 100,
                }
            )
        )

    units_payload = json_get(
        units_url
    )

    unit_items = (
        units_payload.get(
            "gebouweenheden"
        )
        or []
    )

    realized_unit_ids = []

    building_ids = set()

    for item in unit_items:
        unit_status = str(
            item.get(
                "gebouweenheidStatus"
            )
            or ""
        ).lower()

        if (
            unit_status
            and unit_status
            != "gerealiseerd"
        ):
            continue

        unit_id = _extract_object_id(
            item,
            context=(
                "building unit search result"
            ),
        )

        detail_href = item.get(
            "detail"
        )

        if not detail_href:
            detail_href = (
                BASIS_BASE_URL
                + "gebouweenheden/"
                + str(
                    unit_id
                )
            )

        unit_detail = json_get(
            _absolute_basis_url(
                detail_href
            )
        )

        detail_status = str(
            unit_detail.get(
                "gebouweenheidStatus"
            )
            or ""
        ).lower()

        if (
            detail_status
            and detail_status
            != "gerealiseerd"
        ):
            continue

        building = (
            unit_detail.get(
                "gebouw"
            )
            or {}
        )

        building_object_id = (
            building.get(
                "objectId"
            )
        )

        if building_object_id is None:
            raise LookupError(
                "Realized building unit has no related Gebouw ObjectId: "
                f"{unit_id}"
            )

        realized_unit_ids.append(
            unit_id
        )

        building_ids.add(
            int(
                building_object_id
            )
        )

    if not realized_unit_ids:
        raise LookupError(
            "Address has no realized building units."
        )

    if not building_ids:
        raise LookupError(
            "Address building units have no related buildings."
        )

    buildings = []

    for building_id in sorted(
        building_ids
    ):
        feature = _load_building_feature(
            building_id,
            json_get=json_get,
        )

        properties = (
            feature.get(
                "properties"
            )
            or {}
        )

        geometry = (
            feature.get(
                "geometry"
            )
            or {}
        )

        if geometry.get(
            "type"
        ) not in {
            "Polygon",
            "MultiPolygon",
        }:
            raise LookupError(
                "Unsupported official building footprint geometry: "
                f"{geometry.get('type')!r}"
            )

        buildings.append(
            FlandersBuilding(
                object_id=building_id,
                status=(
                    str(
                        properties.get(
                            "GebouwStatus"
                        )
                    )
                    if properties.get(
                        "GebouwStatus"
                    )
                    is not None
                    else None
                ),
                geometry_method=(
                    str(
                        properties.get(
                            "GeometrieMethode"
                        )
                    )
                    if properties.get(
                        "GeometrieMethode"
                    )
                    is not None
                    else None
                ),
                realized_unit_count=(
                    _realized_unit_count(
                        building_id,
                        json_get=json_get,
                    )
                ),
                footprint_geojson=geometry,
                footprint_bounds=(
                    _geometry_bounds(
                        geometry
                    )
                ),
            )
        )

    city = _nested_text(
        detail,
        "gemeente",
        "gemeentenaam",
        "geografischeNaam",
        "spelling",
    )

    full_address = (
        _nested_text(
            detail,
            "volledigAdres",
            "geografischeNaam",
            "spelling",
        )
        or (
            f"{street_text} "
            f"{house_text}, "
            f"{postcode_text}"
        )
    )

    return FlandersAddressResolution(
        address_object_id=(
            address_object_id
        ),
        full_address=(
            full_address
        ),
        street=street_text,
        house_number=house_text,
        postcode=postcode_text,
        city=city,
        status=(
            str(
                detail.get(
                    "adresStatus"
                )
            )
            if detail.get(
                "adresStatus"
            )
            is not None
            else None
        ),
        x_lambert72=x,
        y_lambert72=y,
        lat=float(
            lat
        ),
        lon=float(
            lon
        ),
        building_unit_ids=tuple(
            sorted(
                set(
                    realized_unit_ids
                )
            )
        ),
        buildings=tuple(
            buildings
        ),
    )

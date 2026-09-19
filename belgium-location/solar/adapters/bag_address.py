"""Resolve Dutch addresses to authoritative BAG Pand identifiers.

Resolution flow:

postcode + house number
    -> PDOK Location API address search
    -> exact BAG address feature
    -> adresseerbaar_object_identificatie
    -> BAG verblijfsobject
    -> related BAG pand feature(s)
    -> NL.IMBAG.Pand.<identification>

The Location API is intentionally used for address search. The BAG OGC API
is then used for authoritative object relations.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


BAG_BASE_URL = (
    "https://api.pdok.nl/kadaster/bag/ogc/v2"
)

LOCATION_SEARCH_URL = (
    "https://api.pdok.nl/kadaster/location-api/v1/search"
)


@dataclass(frozen=True)
class BagPand:
    identification: str
    feature_href: str | None = None

    @property
    def cityjson_object_id(self) -> str:
        return (
            "NL.IMBAG.Pand."
            + self.identification
        )


@dataclass(frozen=True)
class BagAddressResolution:
    verblijfsobject_identification: str
    postcode: str
    house_number: int
    house_letter: str | None
    addition: str | None
    street: str | None
    city: str | None
    status: str | None
    lat: float | None
    lon: float | None
    panden: tuple[BagPand, ...]


def normalize_postcode(
    postcode: str,
) -> str:
    value = re.sub(
        r"\s+",
        "",
        postcode,
    ).upper()

    if not re.fullmatch(
        r"\d{4}[A-Z]{2}",
        value,
    ):
        raise ValueError(
            f"Invalid Dutch postcode: {postcode!r}"
        )

    return value


def _clean_optional(
    value: Any,
) -> str | None:
    if value is None:
        return None

    result = str(
        value
    ).strip()

    return (
        result
        if result
        else None
    )


def _default_json_get(
    url: str,
) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": (
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
            "PDOK request failed: "
            f"HTTP {exc.code}\n"
            f"URL: {url}\n"
            f"Response: {body[:2000]}"
        ) from exc


def _relation_hrefs(
    value: Any,
) -> list[str]:
    """Recursively collect HTTP href relations."""

    result: list[str] = []

    if isinstance(
        value,
        str,
    ):
        if value.startswith(
            "http"
        ):
            result.append(
                value
            )

    elif isinstance(
        value,
        dict,
    ):
        href = value.get(
            "href"
        )

        if (
            isinstance(
                href,
                str,
            )
            and href.startswith(
                "http"
            )
        ):
            result.append(
                href
            )

        for child in value.values():
            result.extend(
                _relation_hrefs(
                    child
                )
            )

    elif isinstance(
        value,
        list,
    ):
        for child in value:
            result.extend(
                _relation_hrefs(
                    child
                )
            )

    return list(
        dict.fromkeys(
            result
        )
    )


def _pand_relation_hrefs(
    feature: dict[str, Any],
) -> list[str]:
    """Find BAG Pand feature relations regardless of JSON representation.

    PDOK relation fields can be represented structurally as ``pand`` or
    rendered/flattened as ``pand.href`` depending on the representation.

    Search the complete feature and retain only actual Pand item links.
    """

    hrefs = _relation_hrefs(
        feature
    )

    return [
        href
        for href in hrefs
        if "/collections/pand/items/" in href
    ]


def _feature_point(
    feature: dict[str, Any],
) -> tuple[
    float | None,
    float | None,
]:
    geometry = (
        feature.get(
            "geometry"
        )
        or {}
    )

    if geometry.get(
        "type"
    ) != "Point":
        return (
            None,
            None,
        )

    coordinates = geometry.get(
        "coordinates"
    )

    if (
        not isinstance(
            coordinates,
            list,
        )
        or len(
            coordinates
        ) < 2
    ):
        return (
            None,
            None,
        )

    # GeoJSON / CRS84 coordinate order:
    # longitude, latitude.
    return (
        float(
            coordinates[1]
        ),
        float(
            coordinates[0]
        ),
    )


def _location_address_href(
    feature: dict[str, Any],
) -> str | None:
    """Find the source BAG address-feature href in a Location API result."""

    hrefs = _relation_hrefs(
        feature
    )

    for href in hrefs:
        if (
            "/collections/adres/items/"
            in href
        ):
            return href

    feature_id = feature.get(
        "id"
    )

    if feature_id:
        return (
            BAG_BASE_URL
            + "/collections/adres/items/"
            + quote(
                str(
                    feature_id
                ),
                safe="",
            )
            + "?f=json"
        )

    return None


def _address_matches(
    properties: dict[str, Any],
    *,
    postcode: str,
    house_number: int,
    house_letter: str | None,
    addition: str | None,
) -> bool:
    try:
        candidate_postcode = normalize_postcode(
            str(
                properties.get(
                    "postcode"
                )
                or ""
            )
        )
    except ValueError:
        return False

    if candidate_postcode != postcode:
        return False

    try:
        candidate_number = int(
            properties.get(
                "huisnummer"
            )
        )
    except (
        TypeError,
        ValueError,
    ):
        return False

    if candidate_number != house_number:
        return False

    candidate_letter = _clean_optional(
        properties.get(
            "huisletter"
        )
    )

    candidate_addition = _clean_optional(
        properties.get(
            "toevoeging"
        )
    )

    if house_letter is not None:
        if (
            (candidate_letter or "").lower()
            != house_letter.lower()
        ):
            return False

    if addition is not None:
        if (
            (candidate_addition or "").lower()
            != addition.lower()
        ):
            return False

    return True


def _build_search_text(
    *,
    postcode: str,
    house_number: int,
    house_letter: str | None,
    addition: str | None,
) -> str:
    house = str(
        house_number
    )

    if house_letter:
        house += house_letter

    if addition:
        house += f"-{addition}"

    return (
        f"{postcode} {house}"
    )


def _find_exact_address_feature(
    *,
    postcode: str,
    house_number: int,
    house_letter: str | None,
    addition: str | None,
    json_get: Callable[
        [str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    query = _build_search_text(
        postcode=postcode,
        house_number=house_number,
        house_letter=house_letter,
        addition=addition,
    )

    params = {
        "q": query,
        "adres[version]": 1,
        "limit": 20,
        "f": "json",
    }

    search_url = (
        LOCATION_SEARCH_URL
        + "?"
        + urlencode(
            params
        )
    )

    payload = json_get(
        search_url
    )

    features = (
        payload.get(
            "features"
        )
        or []
    )

    exact: list[
        dict[str, Any]
    ] = []

    for search_feature in features:
        href = _location_address_href(
            search_feature
        )

        if href is None:
            continue

        # Ensure JSON output even if the Location API supplied
        # a bare BAG item URL.
        if "?" not in href:
            href = (
                href
                + "?f=json"
            )

        address_feature = json_get(
            href
        )

        properties = (
            address_feature.get(
                "properties"
            )
            or {}
        )

        if _address_matches(
            properties,
            postcode=postcode,
            house_number=house_number,
            house_letter=house_letter,
            addition=addition,
        ):
            exact.append(
                address_feature
            )

    if not exact:
        raise LookupError(
            "PDOK Location API returned no exact BAG address for "
            f"{postcode} {house_number}."
        )

    # If the caller did not specify a suffix, avoid silently selecting
    # between multiple apartments/units sharing postcode + house number.
    if (
        len(exact) > 1
        and house_letter is None
        and addition is None
    ):
        suffixes = {
            (
                _clean_optional(
                    (
                        feature.get(
                            "properties"
                        )
                        or {}
                    ).get(
                        "huisletter"
                    )
                ),
                _clean_optional(
                    (
                        feature.get(
                            "properties"
                        )
                        or {}
                    ).get(
                        "toevoeging"
                    )
                ),
            )
            for feature in exact
        }

        if len(
            suffixes
        ) > 1:
            raise LookupError(
                "Address is ambiguous in BAG. "
                "Provide house_letter or addition."
            )

    return exact[0]


def _load_verblijfsobject(
    identification: str,
    *,
    json_get: Callable[
        [str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    """Fetch a VBO using the production BAG identification query."""

    params = {
        "identificatie": identification,
        "limit": 10,
        "f": "json",
    }

    url = (
        BAG_BASE_URL
        + "/collections/verblijfsobject/items?"
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
                "identificatie"
            )
            or ""
        )
        == identification
    ]

    if not exact:
        raise LookupError(
            "No BAG verblijfsobject found with identification "
            f"{identification}."
        )

    if len(
        exact
    ) > 1:
        raise LookupError(
            "Multiple BAG verblijfsobjects returned for identification "
            f"{identification}."
        )

    return exact[0]


def resolve_bag_address(
    *,
    postcode: str,
    house_number: int,
    house_letter: str | None = None,
    addition: str | None = None,
    json_get: Callable[
        [str],
        dict[str, Any],
    ] = _default_json_get,
) -> BagAddressResolution:
    """Resolve an exact Dutch address to authoritative BAG Pand IDs."""

    normalized_postcode = normalize_postcode(
        postcode
    )

    number = int(
        house_number
    )

    normalized_letter = _clean_optional(
        house_letter
    )

    normalized_addition = _clean_optional(
        addition
    )

    address_feature = _find_exact_address_feature(
        postcode=normalized_postcode,
        house_number=number,
        house_letter=normalized_letter,
        addition=normalized_addition,
        json_get=json_get,
    )

    address_properties = (
        address_feature.get(
            "properties"
        )
        or {}
    )

    object_type = str(
        address_properties.get(
            "adresseerbaar_object_type"
        )
        or ""
    )

    if object_type.lower() != "verblijfsobject":
        raise LookupError(
            "Resolved BAG address does not reference a verblijfsobject: "
            f"{object_type!r}."
        )

    vbo_identification = str(
        address_properties.get(
            "adresseerbaar_object_identificatie"
        )
        or ""
    )

    if not vbo_identification:
        raise LookupError(
            "Resolved BAG address has no "
            "adresseerbaar_object_identificatie."
        )

    vbo_feature = _load_verblijfsobject(
        vbo_identification,
        json_get=json_get,
    )

    vbo_properties = (
        vbo_feature.get(
            "properties"
        )
        or {}
    )

    # Do not depend on one specific JSON relation layout.
    #
    # PDOK exposes Pand relations as relation arrays, while different
    # representations may expose the href under structures such as
    # ``pand`` or ``pand.href``. Search the complete VBO feature and
    # retain only real Pand-item URLs.
    pand_hrefs = _pand_relation_hrefs(
        vbo_feature
    )

    if not pand_hrefs:
        raise LookupError(
            "BAG verblijfsobject has no detectable related pand. "
            f"VBO={vbo_identification}; "
            f"top_level_keys={sorted(vbo_feature.keys())}; "
            f"property_keys={sorted(vbo_properties.keys())}"
        )

    panden: list[
        BagPand
    ] = []

    for href in pand_hrefs:
        request_href = href

        if "?" not in request_href:
            request_href += (
                "?f=json"
            )

        pand_feature = json_get(
            request_href
        )

        pand_properties = (
            pand_feature.get(
                "properties"
            )
            or {}
        )

        identification = str(
            pand_properties.get(
                "identificatie"
            )
            or ""
        )

        if not identification:
            raise LookupError(
                "Related BAG pand has no identificatie."
            )

        panden.append(
            BagPand(
                identification=identification,
                feature_href=href,
            )
        )

    # Preserve relation order, but remove duplicates.
    unique_panden = tuple(
        {
            item.identification: item
            for item in panden
        }.values()
    )

    lat, lon = _feature_point(
        address_feature
    )

    return BagAddressResolution(
        verblijfsobject_identification=(
            vbo_identification
        ),
        postcode=normalized_postcode,
        house_number=number,
        house_letter=_clean_optional(
            address_properties.get(
                "huisletter"
            )
        ),
        addition=_clean_optional(
            address_properties.get(
                "toevoeging"
            )
        ),
        street=_clean_optional(
            address_properties.get(
                "openbare_ruimte_naam"
            )
        ),
        city=_clean_optional(
            address_properties.get(
                "woonplaats_naam"
            )
        ),
        status=_clean_optional(
            vbo_properties.get(
                "status"
            )
        ),
        lat=lat,
        lon=lon,
        panden=unique_panden,
    )

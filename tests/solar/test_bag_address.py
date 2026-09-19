import sys
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.adapters.bag_address import (  # noqa: E402
    normalize_postcode,
    resolve_bag_address,
)


class BagAddressTests(unittest.TestCase):
    def test_postcode_normalization(self):
        self.assertEqual(
            normalize_postcode(
                "3512 je"
            ),
            "3512JE",
        )

    def test_invalid_postcode_rejected(self):
        with self.assertRaises(
            ValueError
        ):
            normalize_postcode(
                "351 JE"
            )

    def test_location_api_to_bag_pand_chain(self):
        calls = []

        address_uuid = (
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )

        def fake_get(
            url,
        ):
            calls.append(
                url
            )

            if (
                "/location-api/v1/search?"
                in url
            ):
                query = parse_qs(
                    urlparse(
                        url
                    ).query
                )

                self.assertEqual(
                    query["q"][0],
                    "3512JE 22",
                )

                self.assertEqual(
                    query[
                        "adres[version]"
                    ][0],
                    "1",
                )

                return {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": address_uuid,
                            "properties": {
                                "display_name": (
                                    "Domplein 22, "
                                    "3512 JE Utrecht"
                                )
                            },
                        }
                    ],
                }

            if (
                f"/collections/adres/items/{address_uuid}"
                in url
            ):
                return {
                    "type": "Feature",
                    "id": address_uuid,
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            5.121537,
                            52.090435,
                        ],
                    },
                    "properties": {
                        "identificatie": (
                            "0344200000060477"
                        ),
                        "postcode": "3512JE",
                        "huisnummer": 22,
                        "huisletter": None,
                        "toevoeging": None,
                        "openbare_ruimte_naam": (
                            "Domplein"
                        ),
                        "woonplaats_naam": (
                            "Utrecht"
                        ),
                        "adresseerbaar_object_type": (
                            "Verblijfsobject"
                        ),
                        "adresseerbaar_object_identificatie": (
                            "0344010000060477"
                        ),
                    },
                }

            if (
                "/collections/verblijfsobject/items?"
                in url
            ):
                query = parse_qs(
                    urlparse(
                        url
                    ).query
                )

                self.assertEqual(
                    query[
                        "identificatie"
                    ][0],
                    "0344010000060477",
                )

                return {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {
                                "identificatie": (
                                    "0344010000060477"
                                ),
                                "status": (
                                    "Verblijfsobject in gebruik"
                                ),
                                # PDOK relation representations may expose
                                # this as a flattened pand.href field.
                                "pand.href": [
                                    (
                                        "https://example.test/"
                                        "collections/pand/items/"
                                        "domplein22"
                                    )
                                ],
                            },
                        }
                    ],
                }

            if (
                url.startswith(
                    "https://example.test/"
                    "collections/pand/items/"
                    "domplein22"
                )
            ):
                return {
                    "type": "Feature",
                    "properties": {
                        "identificatie": (
                            "0344100000024581"
                        )
                    },
                }

            raise AssertionError(
                f"Unexpected URL: {url}"
            )

        result = resolve_bag_address(
            postcode="3512 JE",
            house_number=22,
            json_get=fake_get,
        )

        self.assertEqual(
            result.postcode,
            "3512JE",
        )

        self.assertEqual(
            result.street,
            "Domplein",
        )

        self.assertEqual(
            result.city,
            "Utrecht",
        )

        self.assertEqual(
            result.verblijfsobject_identification,
            "0344010000060477",
        )

        self.assertAlmostEqual(
            result.lat,
            52.090435,
        )

        self.assertAlmostEqual(
            result.lon,
            5.121537,
        )

        self.assertEqual(
            len(
                result.panden
            ),
            1,
        )

        self.assertEqual(
            result.panden[
                0
            ].cityjson_object_id,
            (
                "NL.IMBAG.Pand."
                "0344100000024581"
            ),
        )

        self.assertEqual(
            len(
                calls
            ),
            4,
        )

    def test_non_verblijfsobject_address_is_rejected(self):
        address_uuid = (
            "11111111-2222-3333-4444-555555555555"
        )

        def fake_get(
            url,
        ):
            if (
                "/location-api/v1/search?"
                in url
            ):
                return {
                    "features": [
                        {
                            "id": address_uuid,
                        }
                    ],
                }

            if (
                f"/collections/adres/items/{address_uuid}"
                in url
            ):
                return {
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            5.0,
                            52.0,
                        ],
                    },
                    "properties": {
                        "postcode": "3512JE",
                        "huisnummer": 22,
                        "adresseerbaar_object_type": (
                            "Standplaats"
                        ),
                        "adresseerbaar_object_identificatie": (
                            "test"
                        ),
                    },
                }

            raise AssertionError(
                f"Unexpected URL: {url}"
            )

        with self.assertRaises(
            LookupError
        ):
            resolve_bag_address(
                postcode="3512JE",
                house_number=22,
                json_get=fake_get,
            )


if __name__ == "__main__":
    unittest.main()

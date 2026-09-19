import sys
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.adapters.flanders_address import (  # noqa: E402
    resolve_flanders_address,
)


class FlandersAddressTests(unittest.TestCase):
    def test_address_to_building_chain(self):
        calls = []

        def fake_get(
            url,
        ):
            calls.append(
                url
            )

            if "/v2/adressen?" in url:
                query = parse_qs(
                    urlparse(
                        url
                    ).query
                )

                self.assertEqual(
                    query[
                        "postcode"
                    ][0],
                    "3200",
                )

                self.assertEqual(
                    query[
                        "straatnaam"
                    ][0],
                    "Ten Drossaarde",
                )

                self.assertEqual(
                    query[
                        "huisnummer"
                    ][0],
                    "1",
                )

                return {
                    "adressen": [
                        {
                            "identificator": {
                                "objectId": (
                                    "2648372"
                                )
                            },
                            "detail": (
                                "https://api.basisregisters."
                                "vlaanderen.be/v2/"
                                "adressen/2648372"
                            ),
                            "huisnummer": "1",
                            "adresStatus": "inGebruik",
                        }
                    ]
                }

            if url.endswith(
                "/v2/adressen/2648372"
            ):
                return {
                    "gemeente": {
                        "gemeentenaam": {
                            "geografischeNaam": {
                                "spelling": (
                                    "Aarschot"
                                )
                            }
                        }
                    },
                    "volledigAdres": {
                        "geografischeNaam": {
                            "spelling": (
                                "Ten Drossaarde 1, "
                                "3200 Aarschot"
                            )
                        }
                    },
                    "adresStatus": "inGebruik",
                    "adresPositie": {
                        "geometrie": {
                            "gml": (
                                '<gml:Point '
                                'srsName="https://www.opengis.net/'
                                'def/crs/EPSG/0/31370" '
                                'xmlns:gml="http://www.opengis.net/'
                                'gml/3.2">'
                                '<gml:pos>'
                                '182808.97 186234.75'
                                '</gml:pos>'
                                '</gml:Point>'
                            )
                        }
                    },
                    "_links": {
                        "gebouweenheden": {
                            "href": (
                                "https://api.basisregisters."
                                "vlaanderen.be/v2/"
                                "gebouweenheden?"
                                "adresobjectid=2648372"
                            )
                        }
                    },
                }

            if (
                "gebouweenheden?"
                "adresobjectid=2648372"
                in url
            ):
                return {
                    "gebouweenheden": [
                        {
                            "identificator": {
                                "objectId": (
                                    "11076894"
                                )
                            },
                            "detail": (
                                "https://api.basisregisters."
                                "vlaanderen.be/v2/"
                                "gebouweenheden/11076894"
                            ),
                            "gebouweenheidStatus": (
                                "gerealiseerd"
                            ),
                        }
                    ]
                }

            if url.endswith(
                "/v2/gebouweenheden/11076894"
            ):
                return {
                    "gebouweenheidStatus": (
                        "gerealiseerd"
                    ),
                    "gebouw": {
                        "objectId": (
                            "11075298"
                        )
                    },
                }

            if (
                "/collections/Gebouw/items?"
                in url
            ):
                return {
                    "features": [
                        {
                            "properties": {
                                "ObjectId": (
                                    11075298
                                ),
                                "GeometrieMethode": (
                                    "IngemetenGRB"
                                ),
                                "GebouwStatus": (
                                    "Gerealiseerd"
                                ),
                            },
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [
                                            182795.429,
                                            186212.726,
                                        ],
                                        [
                                            182821.672,
                                            186212.726,
                                        ],
                                        [
                                            182821.672,
                                            186256.070,
                                        ],
                                        [
                                            182795.429,
                                            186256.070,
                                        ],
                                        [
                                            182795.429,
                                            186212.726,
                                        ],
                                    ]
                                ],
                            },
                        }
                    ]
                }

            if (
                "gebouweenheden?"
                "gebouwObjectId=11075298"
                in url
            ):
                return {
                    "gebouweenheden": [
                        {
                            "identificator": {
                                "objectId": (
                                    "11076894"
                                )
                            }
                        }
                    ]
                }

            raise AssertionError(
                f"Unexpected URL: {url}"
            )

        result = resolve_flanders_address(
            postcode=3200,
            street="Ten Drossaarde",
            house_number=1,
            json_get=fake_get,
        )

        self.assertEqual(
            result.address_object_id,
            2648372,
        )

        self.assertEqual(
            result.full_address,
            "Ten Drossaarde 1, 3200 Aarschot",
        )

        self.assertEqual(
            result.city,
            "Aarschot",
        )

        self.assertAlmostEqual(
            result.x_lambert72,
            182808.97,
        )

        self.assertAlmostEqual(
            result.y_lambert72,
            186234.75,
        )

        self.assertEqual(
            result.building_unit_ids,
            (
                11076894,
            ),
        )

        self.assertEqual(
            len(
                result.buildings
            ),
            1,
        )

        building = (
            result.buildings[
                0
            ]
        )

        self.assertEqual(
            building.object_id,
            11075298,
        )

        self.assertEqual(
            building.geometry_method,
            "IngemetenGRB",
        )

        self.assertEqual(
            building.realized_unit_count,
            1,
        )

        self.assertFalse(
            building.is_shared_building
        )

        self.assertEqual(
            building.footprint_bounds,
            (
                182795.429,
                186212.726,
                182821.672,
                186256.070,
            ),
        )

        self.assertEqual(
            len(
                calls
            ),
            6,
        )

    def test_shared_building_flag(self):
        from solar.adapters.flanders_address import (
            FlandersBuilding,
        )

        building = FlandersBuilding(
            object_id=1,
            status="Gerealiseerd",
            geometry_method="IngemetenGRB",
            realized_unit_count=4,
            footprint_geojson={
                "type": "Polygon",
                "coordinates": [],
            },
            footprint_bounds=(
                0.0,
                0.0,
                1.0,
                1.0,
            ),
        )

        self.assertTrue(
            building.is_shared_building
        )


if __name__ == "__main__":
    unittest.main()

import io
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.pdok_geometry import (  # noqa: E402
    choose_preferred_geometries,
    load_selected_parts,
    load_selected_vertices,
    read_cityjson_context,
    required_vertex_indices,
    semantic_surface_counts,
)


class PdokGeometryTests(unittest.TestCase):
    def test_lod_22_is_preferred(self):
        obj = {
            "geometry": [
                {"lod": "1.2"},
                {"lod": "2.2"},
                {"lod": "1.3"},
            ]
        }

        result = choose_preferred_geometries(obj)

        self.assertEqual(
            [str(item["lod"]) for item in result],
            ["2.2"],
        )

    def test_fallback_to_lod_13(self):
        obj = {
            "geometry": [
                {"lod": "1.2"},
                {"lod": "1.3"},
            ]
        }

        result = choose_preferred_geometries(obj)

        self.assertEqual(
            [str(item["lod"]) for item in result],
            ["1.3"],
        )

    def test_selected_vertices_are_transformed(self):
        payload = {
            "type": "CityJSON",
            "version": "2.0",
            "transform": {
                "scale": [0.001, 0.001, 0.001],
                "translate": [100.0, 200.0, 3.0],
            },
            "metadata": {
                "referenceSystem": (
                    "https://www.opengis.net/def/crs/"
                    "EPSG/0/7415"
                )
            },
            "CityObjects": {
                "part": {
                    "type": "BuildingPart",
                    "geometry": [
                        {
                            "type": "Solid",
                            "lod": "2.2",
                            "boundaries": [
                                [
                                    [[0, 1, 2]]
                                ]
                            ],
                            "semantics": {
                                "surfaces": [
                                    {"type": "RoofSurface"}
                                ],
                                "values": [[0]],
                            },
                        }
                    ],
                }
            },
            "vertices": [
                [0, 0, 0],
                [1000, 0, 0],
                [0, 1000, 2000],
            ],
        }

        encoded = json.dumps(payload).encode("utf-8")

        context = read_cityjson_context(
            io.BytesIO(encoded),
            io.BytesIO(encoded),
        )

        parts = load_selected_parts(
            io.BytesIO(encoded),
            child_to_parent={
                "part": "building",
            },
        )

        indices = required_vertex_indices(parts)

        vertices = load_selected_vertices(
            io.BytesIO(encoded),
            indices=indices,
            context=context,
        )

        self.assertEqual(
            context.reference_system,
            "https://www.opengis.net/def/crs/EPSG/0/7415",
        )

        self.assertEqual(
            indices,
            {0, 1, 2},
        )

        self.assertEqual(
            vertices[2],
            (100.0, 201.0, 5.0),
        )

        counts = semantic_surface_counts(
            parts[0]
        )

        self.assertEqual(
            counts["RoofSurface"],
            1,
        )


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.pdok_geometry import (  # noqa: E402
    SelectedPartGeometry,
)
from solar.geometry.cityjson_mesh import (  # noqa: E402
    DegenerateSurfaceError,
    build_parent_meshes,
    semantic_surfaces,
    triangulate_surface,
)


class CityJsonMeshTests(unittest.TestCase):
    def setUp(self):
        self.geometry = SelectedPartGeometry(
            parent_id="building-1",
            part_id="building-1-0",
            lod="2.2",
            geometry_type="Solid",
            boundaries=[
                [
                    [[0, 1, 2, 3]],
                    [[4, 5, 6, 7]],
                ]
            ],
            semantics={
                "surfaces": [
                    {"type": "RoofSurface"},
                    {"type": "WallSurface"},
                ],
                "values": [
                    [0, 1]
                ],
            },
        )

        self.vertices = {
            0: (0.0, 0.0, 3.0),
            1: (2.0, 0.0, 3.0),
            2: (2.0, 2.0, 3.0),
            3: (0.0, 2.0, 3.0),

            4: (0.0, 0.0, 0.0),
            5: (2.0, 0.0, 0.0),
            6: (2.0, 0.0, 3.0),
            7: (0.0, 0.0, 3.0),
        }

    def test_semantic_surfaces_are_preserved(self):
        surfaces = semantic_surfaces(
            self.geometry
        )

        self.assertEqual(
            [item.surface_type for item in surfaces],
            [
                "RoofSurface",
                "WallSurface",
            ],
        )

    def test_square_surface_triangulates(self):
        surface = semantic_surfaces(
            self.geometry
        )[0]

        mesh = triangulate_surface(
            surface,
            vertices=self.vertices,
        )

        self.assertEqual(
            len(mesh.faces),
            2,
        )

        self.assertAlmostEqual(
            float(mesh.area),
            4.0,
            places=6,
        )

    def test_local_origin_is_applied(self):
        surface = semantic_surfaces(
            self.geometry
        )[0]

        mesh = triangulate_surface(
            surface,
            vertices=self.vertices,
            local_origin=(
                100.0,
                200.0,
                0.0,
            ),
        )

        self.assertLess(
            float(mesh.vertices[:, 0].max()),
            0.0,
        )

    def test_parent_mesh_combines_surfaces(self):
        meshes = build_parent_meshes(
            [self.geometry],
            vertices=self.vertices,
            local_origin=(0.0, 0.0, 0.0),
        )

        self.assertIn(
            "building-1",
            meshes,
        )

        mesh = meshes["building-1"]

        self.assertGreater(
            len(mesh.faces),
            0,
        )

        self.assertTrue(
            np.isfinite(mesh.vertices).all()
        )



    def test_self_intersecting_projected_surface_can_be_repaired(self):
        geometry = SelectedPartGeometry(
            parent_id="building-invalid",
            part_id="building-invalid-0",
            lod="2.2",
            geometry_type="Solid",
            boundaries=[
                [
                    [[0, 1, 2, 3]]
                ]
            ],
            semantics={
                "surfaces": [
                    {"type": "RoofSurface"}
                ],
                "values": [
                    [0]
                ],
            },
        )

        # Self-intersecting ordering with non-zero signed area.
        # Unlike a perfectly symmetric bow-tie, this polygon still has a
        # stable plane normal, so it reaches the Shapely repair path that
        # mirrors the real PDOK failure.
        vertices = {
            0: (0.0, 0.0, 3.0),
            1: (3.0, 3.0, 3.0),
            2: (0.0, 3.0, 3.0),
            3: (2.0, 0.0, 3.0),
        }

        surface = semantic_surfaces(
            geometry
        )[0]

        mesh = triangulate_surface(
            surface,
            vertices=vertices,
        )

        self.assertGreater(
            len(mesh.faces),
            0,
        )

        self.assertTrue(
            np.isfinite(mesh.vertices).all()
        )



    def test_zero_area_surface_is_explicitly_degenerate(self):
        geometry = SelectedPartGeometry(
            parent_id="degenerate-building",
            part_id="degenerate-building-0",
            lod="2.2",
            geometry_type="Solid",
            boundaries=[
                [
                    [[0, 0, 1]]
                ]
            ],
            semantics={
                "surfaces": [
                    {"type": "WallSurface"}
                ],
                "values": [
                    [0]
                ],
            },
        )

        vertices = {
            0: (0.0, 0.0, 0.0),
            1: (0.0, 0.0, 2.0),
        }

        surface = semantic_surfaces(
            geometry
        )[0]

        with self.assertRaises(
            DegenerateSurfaceError
        ):
            triangulate_surface(
                surface,
                vertices=vertices,
            )

    def test_parent_mesh_builder_skips_only_degenerate_surface(self):
        valid_geometry = SelectedPartGeometry(
            parent_id="mixed-building",
            part_id="mixed-building-0",
            lod="2.2",
            geometry_type="Solid",
            boundaries=[
                [
                    [[0, 1, 2, 3]],
                    [[4, 4, 5]],
                ]
            ],
            semantics={
                "surfaces": [
                    {"type": "RoofSurface"},
                    {"type": "WallSurface"},
                ],
                "values": [
                    [0, 1]
                ],
            },
        )

        vertices = {
            0: (0.0, 0.0, 3.0),
            1: (2.0, 0.0, 3.0),
            2: (2.0, 2.0, 3.0),
            3: (0.0, 2.0, 3.0),

            4: (0.0, 0.0, 0.0),
            5: (0.0, 0.0, 2.0),
        }

        meshes = build_parent_meshes(
            [valid_geometry],
            vertices=vertices,
            local_origin=(0.0, 0.0, 0.0),
        )

        self.assertIn(
            "mixed-building",
            meshes,
        )

        self.assertAlmostEqual(
            float(meshes["mixed-building"].area),
            4.0,
            places=6,
        )



    def test_triangulation_preserves_source_surface_normal(self):
        geometry = SelectedPartGeometry(
            parent_id="orientation-building",
            part_id="orientation-building-0",
            lod="2.2",
            geometry_type="Solid",
            boundaries=[
                [
                    [[0, 1, 2, 3]]
                ]
            ],
            semantics={
                "surfaces": [
                    {"type": "RoofSurface"}
                ],
                "values": [
                    [0]
                ],
            },
        )

        # Counter-clockwise XY ordering represents an upward-facing roof.
        vertices = {
            0: (0.0, 0.0, 3.0),
            1: (2.0, 0.0, 3.0),
            2: (2.0, 2.0, 3.0),
            3: (0.0, 2.0, 3.0),
        }

        surface = semantic_surfaces(
            geometry
        )[0]

        mesh = triangulate_surface(
            surface,
            vertices=vertices,
        )

        weighted_normal = (
            mesh.face_normals
            * mesh.area_faces[:, None]
        ).sum(axis=0)

        weighted_normal /= np.linalg.norm(
            weighted_normal
        )

        self.assertGreater(
            float(weighted_normal[2]),
            0.99,
        )


if __name__ == "__main__":
    unittest.main()

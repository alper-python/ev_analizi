import sys
import unittest
from pathlib import Path

import trimesh


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.aggregation.surface_groups import (  # noqa: E402
    area_weighted_percentage,
    compass_sector,
    group_surfaces,
)
from solar.geometry.real_surfaces import (  # noqa: E402
    AnalyzableSurface,
    normal_to_orientation,
)


def fake_surface(
    surface_id,
    surface_type,
    area,
    azimuth,
):
    return AnalyzableSurface(
        surface_id=surface_id,
        parent_id="building",
        part_id="building-0",
        surface_type=surface_type,
        area_m2=area,
        azimuth_deg=azimuth,
        tilt_deg=90.0,
        normal=(0.0, 0.0, 1.0),
        mesh=trimesh.Trimesh(),
    )


class SurfaceGroupingTests(unittest.TestCase):
    def test_compass_sectors(self):
        self.assertEqual(compass_sector(0.0), "N")
        self.assertEqual(compass_sector(44.0), "NE")
        self.assertEqual(compass_sector(75.0), "E")
        self.assertEqual(compass_sector(162.0), "S")
        self.assertEqual(compass_sector(251.0), "W")
        self.assertEqual(compass_sector(342.0), "N")
        self.assertEqual(compass_sector(None), "FLAT")

    def test_nearly_flat_surface_has_no_user_facing_azimuth(self):
        azimuth, tilt = normal_to_orientation(
            [0.0053, 0.0327, 0.9995]
        )

        self.assertLess(
            tilt,
            5.0,
        )

        self.assertIsNone(
            azimuth
        )

    def test_area_weighted_percentage(self):
        result = area_weighted_percentage(
            [
                (80.0, 100.0),
                (20.0, 0.0),
            ]
        )

        self.assertAlmostEqual(
            result,
            80.0,
        )

    def test_surfaces_group_by_type_and_direction(self):
        surfaces = [
            fake_surface(
                "south-1",
                "WallSurface",
                100.0,
                162.0,
            ),
            fake_surface(
                "south-2",
                "WallSurface",
                20.0,
                170.0,
            ),
            fake_surface(
                "north",
                "WallSurface",
                80.0,
                342.0,
            ),
        ]

        groups = group_surfaces(
            surfaces
        )

        south = next(
            group
            for group in groups
            if group.direction == "S"
        )

        self.assertEqual(
            len(south.surfaces),
            2,
        )

        self.assertAlmostEqual(
            south.total_area_m2,
            120.0,
        )


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

import trimesh


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.geometry.facade_exposure import (  # noqa: E402
    FacadeExposureClass,
    FacadeExposureReport,
    classify_attachment,
    remove_party_walls,
)
from solar.geometry.real_surfaces import (  # noqa: E402
    AnalyzableSurface,
)


def fake_surface(
    surface_id,
    surface_type="WallSurface",
):
    return AnalyzableSurface(
        surface_id=surface_id,
        parent_id="building",
        part_id="building-0",
        surface_type=surface_type,
        area_m2=10.0,
        azimuth_deg=180.0,
        tilt_deg=90.0,
        normal=(0.0, -1.0, 0.0),
        mesh=trimesh.Trimesh(),
    )


def report(
    surface_id,
    classification,
):
    return FacadeExposureReport(
        surface_id=surface_id,
        classification=classification,
        sample_count=30,
        near_zero_fraction=0.0,
        no_hit_fraction=1.0,
        median_clearance_m=None,
        min_clearance_m=None,
    )


class FacadeExposureTests(unittest.TestCase):
    def test_zero_attachment_is_exposed(self):
        self.assertEqual(
            classify_attachment(
                0.0
            ),
            FacadeExposureClass.EXPOSED,
        )

    def test_small_attachment_is_partial(self):
        self.assertEqual(
            classify_attachment(
                0.10
            ),
            FacadeExposureClass.PARTIALLY_ATTACHED,
        )

    def test_eighty_percent_attachment_is_party_wall(self):
        self.assertEqual(
            classify_attachment(
                0.80
            ),
            FacadeExposureClass.PARTY_WALL,
        )

    def test_remove_party_walls_preserves_exposed_and_roof(self):
        exposed = fake_surface(
            "wall-exposed"
        )

        party = fake_surface(
            "wall-party"
        )

        roof = fake_surface(
            "roof",
            surface_type="RoofSurface",
        )

        reports = {
            "wall-exposed": report(
                "wall-exposed",
                FacadeExposureClass.EXPOSED,
            ),
            "wall-party": report(
                "wall-party",
                FacadeExposureClass.PARTY_WALL,
            ),
        }

        result = remove_party_walls(
            [
                exposed,
                party,
                roof,
            ],
            reports,
        )

        self.assertEqual(
            [
                item.surface_id
                for item in result
            ],
            [
                "wall-exposed",
                "roof",
            ],
        )

    def test_partial_wall_is_retained(self):
        partial = fake_surface(
            "wall-partial"
        )

        reports = {
            "wall-partial": report(
                "wall-partial",
                FacadeExposureClass.PARTIALLY_ATTACHED,
            ),
        }

        result = remove_party_walls(
            [partial],
            reports,
        )

        self.assertEqual(
            len(result),
            1,
        )


if __name__ == "__main__":
    unittest.main()

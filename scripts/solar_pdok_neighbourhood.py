"""Inspect buildings near the Utrecht test point inside a cached PDOK tile."""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.pdok_cityjson import (
    nearby_buildings_from_stream,
    wgs84_to_rd,
)


LAT = 52.0907
LON = 5.1214
RADIUS_M = 75.0

ARCHIVE = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
    / "buildings_2025_136000_454000.zip"
)


def main() -> None:
    print(
        "DomiFrame Solar — PDOK neighbourhood inspection"
    )
    print()

    x, y = wgs84_to_rd(
        lat=LAT,
        lon=LON,
    )

    print(f"WGS84: {LAT:.6f}, {LON:.6f}")
    print(f"RD New: X={x:.3f}, Y={y:.3f}")
    print(f"Radius: {RADIUS_M:.1f} m")
    print()

    if not ARCHIVE.exists():
        raise SystemExit(
            f"Cached PDOK archive not found: {ARCHIVE}"
        )

    with ZipFile(ARCHIVE) as archive:
        candidates = [
            info.filename
            for info in archive.infolist()
            if info.filename.lower().endswith(
                ".city.json"
            )
        ]

        if len(candidates) != 1:
            raise SystemExit(
                "Expected exactly one CityJSON file "
                f"but found {len(candidates)}."
            )

        with archive.open(
            candidates[0]
        ) as source:
            buildings = nearby_buildings_from_stream(
                source,
                target_x=x,
                target_y=y,
                radius_m=RADIUS_M,
            )

    print(
        f"Buildings within {RADIUS_M:.0f} m: "
        f"{len(buildings)}"
    )
    print()

    for index, building in enumerate(
        buildings[:20],
        start=1,
    ):
        marker = (
            "  <-- nearest / target candidate"
            if index == 1
            else ""
        )

        print(
            f"{index:>2}. "
            f"{building.object_id}"
            f"{marker}"
        )

        print(
            f"    distance: "
            f"{building.distance_m:.2f} m"
        )

        print(
            f"    BAG id:   "
            f"{building.identification}"
        )

        print(
            f"    year:     "
            f"{building.construction_year}"
        )

        print(
            f"    LoD:      "
            f"{', '.join(building.geometry_lods) or 'unknown'}"
        )

        print(
            f"    geometry: "
            f"{', '.join(building.geometry_types) or 'unknown'}"
        )

        print(
            f"    pointcloud: "
            f"{building.pointcloud_source} "
            f"({building.pointcloud_year})"
        )

        print(
            f"    pc unusable: "
            f"{building.pointcloud_unusable}"
        )

        print(
            "    extent:   "
            + ", ".join(
                f"{value:.2f}"
                for value in building.extent
            )
        )

        print()

    if buildings:
        print(
            "IMPORTANT: the nearest building is only a "
            "prototype target candidate."
        )
        print(
            "Production target-building identification "
            "must not rely only on nearest distance."
        )


if __name__ == "__main__":
    main()

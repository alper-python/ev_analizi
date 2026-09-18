"""Inspect PDOK 3D building downloads covering a test location."""

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.adapters.netherlands import discover_building_tiles


LAT = 52.0907
LON = 5.1214


def main():
    print("DomiFrame Solar — PDOK 3D discovery")
    print()
    print(f"Location: {LAT}, {LON}")
    print()

    tiles = discover_building_tiles(
        lat=LAT,
        lon=LON,
        radius_m=25.0,
    )

    if not tiles:
        print("No PDOK building tile found.")
        return

    print(f"Newest matching tile(s): {len(tiles)}")
    print()

    for tile in tiles:
        print(f"Feature ID:   {tile.feature_id}")
        print(f"Sheet:        {tile.sheet_id}")
        print(f"Imagery year: {tile.imagery_year}")
        print(f"Start:        {tile.start_date}")
        print(f"End:          {tile.end_date}")
        print(f"Download:     {tile.download_url}")
        print()


if __name__ == "__main__":
    main()

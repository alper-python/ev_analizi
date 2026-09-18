"""Inspect a real daily solar path using pvlib."""

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.position.solar_position import solar_positions


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"


def print_day(date: str) -> None:
    times = pd.date_range(
        f"{date} 00:00",
        periods=24,
        freq="1h",
        tz=TZ,
    )

    positions = solar_positions(
        times,
        lat=LAT,
        lon=LON,
    )

    print()
    print(date)
    print("-" * 62)
    print("time   azimuth   elevation   apparent elevation")

    for timestamp, row in positions.iterrows():
        print(
            f"{timestamp:%H:%M}"
            f"   {row['azimuth']:7.2f}°"
            f"   {row['elevation']:9.2f}°"
            f"   {row['apparent_elevation']:9.2f}°"
        )


def main():
    print("DomiFrame Solar — Utrecht seasonal position check")

    print_day("2026-06-21")
    print_day("2026-12-21")


if __name__ == "__main__":
    main()

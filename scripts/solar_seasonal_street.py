"""Real solar path + synthetic street obstruction demonstration."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import trimesh


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "belgium-location"))

from solar.geometry.exposure import hourly_surface_exposure
from solar.geometry.raycast import ShadowEngine
from solar.geometry.sampling import sample_vertical_facade
from solar.position.solar_position import solar_positions


LAT = 52.0907
LON = 5.1214
TZ = "Europe/Amsterdam"

SOUTH_FACING = np.array([0.0, -1.0, 0.0])


def positions_for(date: str):
    times = pd.date_range(
        f"{date} 00:00",
        periods=24,
        freq="1h",
        tz=TZ,
    )

    return solar_positions(
        times,
        lat=LAT,
        lon=LON,
    )


def analyze_day(date: str):
    facade = sample_vertical_facade(
        x_min=-2.0,
        x_max=2.0,
        y=0.0,
        z_min=0.5,
        z_max=3.0,
        horizontal_samples=20,
        vertical_samples=12,
    )

    opposite = trimesh.creation.box(
        extents=(20.0, 1.0, 8.0)
    )
    opposite.apply_translation(
        [0.0, -5.0, 4.0]
    )

    return hourly_surface_exposure(
        positions_for(date),
        sample_points=facade,
        surface_normal=SOUTH_FACING,
        theoretical_engine=ShadowEngine(),
        actual_engine=ShadowEngine([opposite]),
    )


def print_day(date: str):
    result = analyze_day(date)

    print()
    print(date)
    print("-" * 83)
    print(
        "time   azimuth   elevation   theoretical   actual   obstruction"
    )

    relevant = result[
        result["theoretical_sunlit_area_pct"] > 0
    ]

    for timestamp, row in relevant.iterrows():
        print(
            f"{timestamp:%H:%M}"
            f"   {row['sun_azimuth_deg']:7.2f}°"
            f"   {row['sun_elevation_deg']:8.2f}°"
            f"   {row['theoretical_sunlit_area_pct']:9.2f}%"
            f"   {row['actual_sunlit_area_pct']:7.2f}%"
            f"   {row['obstruction_impact_pct_points']:9.2f} pp"
        )

    theoretical_hours = (
        result["theoretical_sunlit_area_pct"].sum()
        / 100.0
    )

    actual_hours = (
        result["actual_sunlit_area_pct"].sum()
        / 100.0
    )

    print()
    print(
        "Sampled equivalent direct-sun exposure:"
    )
    print(
        f"  without opposite building: {theoretical_hours:.2f} h"
    )
    print(
        f"  with opposite building:    {actual_hours:.2f} h"
    )


def main():
    print(
        "DomiFrame Solar — real seasonal sun + synthetic street"
    )

    print_day("2026-06-21")
    print_day("2026-12-21")


if __name__ == "__main__":
    main()

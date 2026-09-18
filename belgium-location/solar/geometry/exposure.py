"""Hourly direct-sun exposure calculations for sampled surfaces."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .raycast import ShadowEngine


_REQUIRED_POSITION_COLUMNS = {
    "azimuth",
    "apparent_elevation",
}


def hourly_surface_exposure(
    positions: pd.DataFrame,
    *,
    sample_points: np.ndarray,
    surface_normal: np.ndarray,
    theoretical_engine: ShadowEngine | None = None,
    actual_engine: ShadowEngine | None = None,
) -> pd.DataFrame:
    """Calculate hourly direct-sun exposure for one physical surface.

    Two values are deliberately retained:

    theoretical_sunlit_area_pct
        Direct sun with no surrounding obstruction.

    actual_sunlit_area_pct
        Direct sun after scene obstacles are included.

    Their difference is local obstruction impact, not an energy-loss value.
    """

    missing = _REQUIRED_POSITION_COLUMNS.difference(positions.columns)

    if missing:
        raise ValueError(
            "positions is missing required columns: "
            + ", ".join(sorted(missing))
        )

    theoretical = theoretical_engine or ShadowEngine()
    actual = actual_engine or theoretical

    rows = []

    for timestamp, row in positions.iterrows():
        azimuth = float(row["azimuth"])
        elevation = float(row["apparent_elevation"])

        theoretical_pct = theoretical.sunlit_area_pct(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        actual_pct = actual.sunlit_area_pct(
            sample_points,
            surface_normal=surface_normal,
            sun_azimuth_deg=azimuth,
            sun_elevation_deg=elevation,
        )

        rows.append(
            {
                "timestamp": timestamp,
                "sun_azimuth_deg": azimuth,
                "sun_elevation_deg": elevation,
                "theoretical_sunlit_area_pct": theoretical_pct,
                "actual_sunlit_area_pct": actual_pct,
                "obstruction_impact_pct_points": (
                    theoretical_pct - actual_pct
                ),
            }
        )

    result = pd.DataFrame(rows)

    if result.empty:
        return result.set_index("timestamp")

    return result.set_index("timestamp")

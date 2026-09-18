"""Solar position and direction helpers.

Coordinate convention used throughout Solar Analysis:

    +X = East
    +Y = North
    +Z = Up

Solar azimuth:

    0 deg   = North
    90 deg  = East
    180 deg = South
    270 deg = West
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pvlib


def sun_direction(
    azimuth_deg: float,
    elevation_deg: float,
) -> np.ndarray:
    """Return a normalized vector pointing from a surface toward the sun."""

    azimuth = math.radians(float(azimuth_deg))
    elevation = math.radians(float(elevation_deg))

    cos_el = math.cos(elevation)

    direction = np.array(
        [
            cos_el * math.sin(azimuth),  # east
            cos_el * math.cos(azimuth),  # north
            math.sin(elevation),         # up
        ],
        dtype=float,
    )

    norm = np.linalg.norm(direction)

    if norm == 0:
        raise ValueError("Solar direction cannot have zero length.")

    return direction / norm


def solar_positions(
    times: pd.DatetimeIndex,
    *,
    lat: float,
    lon: float,
) -> pd.DataFrame:
    """Calculate solar position for timezone-aware timestamps.

    Returns both geometric elevation and apparent elevation. The latter
    includes atmospheric refraction and will generally be used for direct
    sunlight visibility near the horizon.
    """

    if not isinstance(times, pd.DatetimeIndex):
        raise TypeError("times must be a pandas.DatetimeIndex.")

    if times.tz is None:
        raise ValueError(
            "times must be timezone-aware; UTC or the property's local "
            "timezone must be supplied explicitly."
        )

    result = pvlib.solarposition.get_solarposition(
        time=times,
        latitude=float(lat),
        longitude=float(lon),
        method="nrel_numpy",
    )

    return result[
        [
            "azimuth",
            "elevation",
            "apparent_elevation",
            "zenith",
            "apparent_zenith",
        ]
    ].copy()


def hourly_solar_positions_for_year(
    *,
    year: int,
    lat: float,
    lon: float,
    timezone: str,
) -> pd.DataFrame:
    """Calculate one solar-position observation per elapsed hour in a year.

    The index is localized to the property's timezone. DST transitions are
    therefore represented correctly by pandas rather than by manually
    shifting local clock times.
    """

    start = pd.Timestamp(
        year=year,
        month=1,
        day=1,
        tz=timezone,
    )

    end = pd.Timestamp(
        year=year + 1,
        month=1,
        day=1,
        tz=timezone,
    )

    times = pd.date_range(
        start=start,
        end=end,
        freq="1h",
        inclusive="left",
    )

    return solar_positions(
        times,
        lat=lat,
        lon=lon,
    )

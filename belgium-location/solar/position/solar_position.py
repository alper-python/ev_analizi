"""Solar direction helpers.

Coordinate convention used throughout Solar Analysis:

    +X = East
    +Y = North
    +Z = Up

Solar azimuth follows the common meteorological / pvlib convention:

    0 deg   = North
    90 deg  = East
    180 deg = South
    270 deg = West
"""

from __future__ import annotations

import math

import numpy as np


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

"""Surface sampling helpers for Solar Analysis prototypes."""

from __future__ import annotations

import numpy as np


def sample_vertical_facade(
    *,
    x_min: float,
    x_max: float,
    y: float,
    z_min: float,
    z_max: float,
    horizontal_samples: int = 10,
    vertical_samples: int = 10,
) -> np.ndarray:
    """Sample a north/south-aligned vertical facade.

    This deliberately simple sampler is used by the synthetic M1 tests.
    General arbitrary-plane sampling will be introduced later.
    """

    if horizontal_samples < 1 or vertical_samples < 1:
        raise ValueError("Sample counts must be at least 1.")

    if x_max <= x_min:
        raise ValueError("x_max must be greater than x_min.")

    if z_max <= z_min:
        raise ValueError("z_max must be greater than z_min.")

    # Use cell centres rather than polygon edges.
    x_step = (x_max - x_min) / horizontal_samples
    z_step = (z_max - z_min) / vertical_samples

    xs = (
        np.arange(horizontal_samples, dtype=float) + 0.5
    ) * x_step + x_min

    zs = (
        np.arange(vertical_samples, dtype=float) + 0.5
    ) * z_step + z_min

    points = np.array(
        [
            [x, float(y), z]
            for z in zs
            for x in xs
        ],
        dtype=float,
    )

    return points

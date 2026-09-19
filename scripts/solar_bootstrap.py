"""Shared bootstrap for standalone Solar prototype scripts."""

from pathlib import Path
import sys


def bootstrap():
    root = Path(__file__).resolve().parents[1]

    solar_path = str(
        root / "belgium-location"
    )

    if solar_path not in sys.path:
        sys.path.insert(
            0,
            solar_path,
        )

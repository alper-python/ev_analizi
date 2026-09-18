"""DomiFrame Solar Analysis.

This package is intentionally independent from the Nearby Access analysis.
Shared location information may be passed in, but Solar owns its own
geometry, shadow, aggregation and energy-analysis pipeline.
"""

from .models import (
    ConfidenceLevel,
    HourlyExposure,
    LocationContext,
    SurfaceRef,
    SurfaceType,
)

__all__ = [
    "ConfidenceLevel",
    "HourlyExposure",
    "LocationContext",
    "SurfaceRef",
    "SurfaceType",
]

"""Core data contracts for Solar Analysis.

Keep these contracts independent from specific regional data providers.
PDOK, DHMV, UrbIS and SPW adapters must eventually normalize their source
data into these common models.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class SurfaceType(str, Enum):
    ROOF = "roof"
    FACADE = "facade"
    GARDEN = "garden"


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class LocationContext:
    """Location shared between DomiFrame analysis modules."""

    address: str
    lat: float
    lon: float
    country_code: str
    region: str | None = None


@dataclass(frozen=True)
class SurfaceRef:
    """A physical surface that can receive direct sunlight."""

    surface_id: str
    surface_type: SurfaceType
    azimuth_deg: float | None = None
    tilt_deg: float | None = None
    area_m2: float | None = None


@dataclass(frozen=True)
class HourlyExposure:
    """Raw hourly solar-access observation for one surface.

    sunlit_area_pct is intentionally the primary raw measurement.
    User-facing labels such as Strong / Moderate / Limited must be derived
    later by the interpretation layer rather than embedded here.
    """

    timestamp: datetime
    surface_id: str
    sun_azimuth_deg: float
    sun_elevation_deg: float
    sunlit_area_pct: float
    theoretical_sun: bool
    actual_sun: bool

"""Solar Analysis pipeline orchestration.

Implementation will be added incrementally after the geometry and
solar-position prototypes are validated.
"""

from .models import LocationContext


class SolarAnalysisPipeline:
    """Coordinates regional data, geometry, shadow and aggregation layers."""

    def analyze(self, location: LocationContext):
        raise NotImplementedError(
            "Solar pipeline is not implemented yet; the prototype will be "
            "built milestone by milestone."
        )

"""Live validation of Netherlands address -> BAG Pand resolution."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.adapters.bag_address import (
    resolve_bag_address,
)


POSTCODE = "3512JE"
HOUSE_NUMBER = 22

EXPECTED_CITYJSON_ID = (
    "NL.IMBAG.Pand.0344100000024581"
)


def main():
    print(
        "DomiFrame Solar — live BAG address resolution"
    )
    print("=" * 72)
    print()

    print(
        f"Address query: {POSTCODE} {HOUSE_NUMBER}"
    )

    print()
    print("Calling PDOK BAG...")

    resolution = resolve_bag_address(
        postcode=POSTCODE,
        house_number=HOUSE_NUMBER,
    )

    print()
    print("Resolved BAG address")
    print("-" * 72)

    print(
        f"Street:               {resolution.street}"
    )

    print(
        f"House number:         {resolution.house_number}"
    )

    print(
        f"Postcode:             {resolution.postcode}"
    )

    print(
        f"City:                 {resolution.city}"
    )

    print(
        f"Status:               {resolution.status}"
    )

    print(
        f"Verblijfsobject ID:   "
        f"{resolution.verblijfsobject_identification}"
    )

    print(
        f"Latitude:             {resolution.lat}"
    )

    print(
        f"Longitude:            {resolution.lon}"
    )

    print()
    print("Related BAG panden")
    print("-" * 72)

    if not resolution.panden:
        raise RuntimeError(
            "Address resolved but no BAG pand relation was returned."
        )

    resolved_ids = []

    for pand in resolution.panden:
        cityjson_id = (
            pand.cityjson_object_id
        )

        resolved_ids.append(
            cityjson_id
        )

        print(
            f"BAG Pand ID:          {pand.identification}"
        )

        print(
            f"PDOK CityJSON ID:     {cityjson_id}"
        )

        print(
            f"Feature href:         {pand.feature_href}"
        )

        print()

    print("=" * 72)
    print("Cross-check against existing Solar target")
    print("=" * 72)

    print(
        f"Expected: {EXPECTED_CITYJSON_ID}"
    )

    print(
        "Resolved: "
        + ", ".join(
            resolved_ids
        )
    )

    if (
        EXPECTED_CITYJSON_ID
        not in resolved_ids
    ):
        raise RuntimeError(
            "LIVE VALIDATION FAILED: "
            "address resolution did not return the existing "
            "PDOK Solar target building."
        )

    print()
    print(
        "PASS — exact address resolves to the same PDOK "
        "building used by the existing real-shadow pipeline."
    )


if __name__ == "__main__":
    main()

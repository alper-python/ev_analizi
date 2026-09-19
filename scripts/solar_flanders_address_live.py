"""Live validation of the Flanders address/building resolver."""

from __future__ import annotations

from solar_bootstrap import bootstrap

bootstrap()

from solar.adapters.flanders_address import (
    resolve_flanders_address,
)


def main():
    print(
        "DomiFrame Solar — live Flanders address resolution"
    )
    print("=" * 82)

    result = resolve_flanders_address(
        postcode=3200,
        street="Ten Drossaarde",
        house_number=1,
    )

    print()
    print("ADDRESS")
    print("-" * 82)

    print(
        f"Resolved:        {result.full_address}"
    )

    print(
        f"Address ObjectId:{result.address_object_id:>10}"
    )

    print(
        f"Lambert 72:      "
        f"{result.x_lambert72:.3f}, "
        f"{result.y_lambert72:.3f}"
    )

    print(
        f"WGS84:           "
        f"{result.lat:.8f}, "
        f"{result.lon:.8f}"
    )

    print(
        f"Building units:  "
        f"{result.building_unit_ids}"
    )

    print()
    print("BUILDINGS")
    print("-" * 82)

    for building in result.buildings:
        print(
            f"ObjectId:        {building.object_id}"
        )

        print(
            f"Status:          {building.status}"
        )

        print(
            f"Geometry method: {building.geometry_method}"
        )

        print(
            f"Realized units:  "
            f"{building.realized_unit_count}"
        )

        print(
            f"Shared building: "
            f"{building.is_shared_building}"
        )

        print(
            f"Geometry type:   "
            f"{building.footprint_geojson.get('type')}"
        )

        print(
            f"Bounds:          "
            f"{building.footprint_bounds}"
        )

        print()

    if len(
        result.buildings
    ) != 1:
        raise RuntimeError(
            "Expected one building for this reference case."
        )

    building = (
        result.buildings[
            0
        ]
    )

    if (
        building.object_id
        != 11075298
    ):
        raise RuntimeError(
            "Live resolver returned an unexpected "
            "reference building."
        )

    print(
        "PASS — exact Flemish address resolves to the "
        "same authoritative building footprint discovered "
        "by the diagnostic probe."
    )


if __name__ == "__main__":
    main()

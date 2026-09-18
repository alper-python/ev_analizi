# Solar Analysis Architecture

## High-level flow

Address / coordinates
-> LocationContext
-> regional data adapter
-> normalized SolarScene
-> solar-position engine
-> surface sampling
-> shadow / visibility engine
-> hourly exposure
-> monthly / seasonal / annual aggregation
-> interpretation
-> optional solar-energy layer

## Regional adapters

Initial regions:

- Netherlands: PDOK 3D + AHN
- Flanders: GRB + DHMV
- Brussels: UrbIS 3D
- Wallonia: SPW LiDAR / DSM

Provider-specific formats must not leak into the geometry or shadow engines.

## Primary raw measurement

For every analysis surface and every analyzed hour:

> What percentage of that surface has direct line-of-sight to the sun?

This supports roof, facade and garden analysis without forcing a subjective
single sunlight score.

## Shadow comparison

Where possible the engine should retain both:

- theoretical solar access without surrounding obstacles
- actual solar access with surrounding obstacles

The difference represents local obstruction / shading impact.

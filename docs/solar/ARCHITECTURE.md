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

## Direct-sun definition

A sampled point is considered directly sunlit only when all three conditions
are true:

1. the sun is above the local horizon;
2. the physical surface faces the sun;
3. the ray from the sample point toward the sun is not blocked by scene
   geometry.

Therefore unobstructed line-of-sight alone is not sufficient. A north-facing
facade, for example, must not be marked as directly sunlit when the sun is
behind the facade to the south.

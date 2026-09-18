# Solar Analysis Decisions

## Accepted

1. Solar Analysis is developed on `feature/solar-analysis-v1` and is not
   merged into `main` until the feature is complete.
2. Main priority: year-round sunlight relationship of the home.
3. PV potential is secondary and optional.
4. No single Solar Score in V1.
5. Primary surfaces: roof planes, main facades and garden.
6. Surrounding buildings and terrain should be included in shading.
7. Vegetation should be included where data quality permits.
8. Hourly resolution is sufficient for the initial annual model.
9. Main raw metric: percentage of each surface directly sunlit per hour.
10. User-facing presentation should emphasize seasons rather than four
    specific calendar dates.
11. Special dates may still be used internally for validation.
12. Weather/cloudiness is not part of geometric sun access.
13. Weather/irradiance belongs to the separate energy layer.
14. Address/coordinates may move between DomiFrame modules.
15. Entering a module must not automatically start an expensive analysis.
16. Data quality and source freshness must be visible through confidence
    metadata.
17. Performance optimization is not an initial prototype gate; correctness
    comes first.
18. Interpretation thresholds such as Strong / Moderate / Limited remain
    undecided until real validation distributions are available.

## Open

- Reliable automatic garden-boundary source by region.
- Final annual/seasonal visualization design.
- Final user-facing interpretation thresholds.
- Vegetation handling and seasonal foliage model.
- PV simulation UX.
- Production caching strategy.

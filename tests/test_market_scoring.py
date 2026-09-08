import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

import pandas as pd


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"

MARKET_SPEC = importlib.util.spec_from_file_location(
    "market_scoring", SOURCE_DIR / "market_scoring.py")
market = importlib.util.module_from_spec(MARKET_SPEC)
MARKET_SPEC.loader.exec_module(market)
SCHOOL_SPEC = importlib.util.spec_from_file_location(
    "school_scoring", SOURCE_DIR / "school_scoring.py")
school = importlib.util.module_from_spec(SCHOOL_SPEC)
SCHOOL_SPEC.loader.exec_module(school)


def _load_app_without_leaking_optional_stubs():
    """Load the app, stubbing only missing dependencies and restoring sys.modules."""
    stubs = {"market_scoring": market, "school_scoring": school}
    stubbed_names = []
    for module_name in ("duckdb", "folium"):
        if importlib.util.find_spec(module_name) is None:
            stubs[module_name] = types.ModuleType(module_name)
            stubbed_names.append(module_name)
    if importlib.util.find_spec("geopy") is None:
        geopy = types.ModuleType("geopy")
        geopy.__path__ = []
        geocoders = types.ModuleType("geopy.geocoders")
        geocoders.Nominatim = object
        extra = types.ModuleType("geopy.extra")
        extra.__path__ = []
        rate_limiter = types.ModuleType("geopy.extra.rate_limiter")
        rate_limiter.RateLimiter = object
        stubs.update({
            "geopy": geopy,
            "geopy.geocoders": geocoders,
            "geopy.extra": extra,
            "geopy.extra.rate_limiter": rate_limiter,
        })
        stubbed_names.extend(name for name in stubs if name.startswith("geopy"))

    app_spec = importlib.util.spec_from_file_location("market_app", SOURCE_DIR / "app_duckdb.py")
    loaded_app = importlib.util.module_from_spec(app_spec)
    with mock.patch.dict(sys.modules, stubs, clear=False):
        app_spec.loader.exec_module(loaded_app)
    return loaded_app, tuple(stubbed_names)


app, STUBBED_MODULE_NAMES = _load_app_without_leaking_optional_stubs()


def poi(shop, distance, name="Shop", source="node", lat=50.0, lon=4.0,
        amenity=None):
    return {
        "name": name,
        "brand": None,
        "lat": lat,
        "lon": lon,
        "amenity": amenity,
        "shop": shop,
        "healthcare": None,
        "source": source,
        "d_lin": float(distance),
    }


class MarketScoreTests(unittest.TestCase):
    def test_no_groceries_scores_zero(self):
        self.assertEqual(market.calc_market_score([]), 0.0)

    def test_supermarket_at_300m(self):
        expected = 7 * (1 - 300 / 2500) + 1.0
        self.assertAlmostEqual(market.calc_market_score([poi("supermarket", 300)]), expected)

    def test_convenience_at_same_distance_scores_lower(self):
        supermarket = market.calc_market_score([poi("supermarket", 300)])
        convenience = market.calc_market_score([poi("convenience", 300)])
        self.assertLess(convenience, supermarket)

    def test_three_nearby_convenience_stores_do_not_beat_strong_supermarket(self):
        conveniences = [poi("convenience", d, name=f"C{d}") for d in (100, 120, 140)]
        self.assertLess(market.calc_market_score(conveniences),
                        market.calc_market_score([poi("supermarket", 100)]))

    def test_mixed_effective_count_uses_type_weights(self):
        rows = [poi("supermarket", 2500), poi("convenience", 2500, name="C")]
        self.assertAlmostEqual(market.calc_market_score(rows), 1.6)

    def test_marketplace_is_ignored(self):
        baseline = market.calc_market_score([poi("supermarket", 500)])
        marketplace = poi(None, 0, name="Weekly market", amenity="marketplace")
        self.assertEqual(market.calc_market_score([marketplace]), 0.0)
        self.assertEqual(market.calc_market_score([poi("supermarket", 500), marketplace]), baseline)

    def test_dual_tagged_marketplaces_are_completely_excluded(self):
        for shop in ("supermarket", "convenience"):
            dual_tagged = poi(shop, 0, amenity="marketplace")
            self.assertIsNone(market.market_type(dual_tagged))
            self.assertEqual(market.deduplicate_market_pois([dual_tagged]), [])
            self.assertEqual(market.calc_market_score([dual_tagged]), 0.0)

    def test_candidate_query_excludes_marketplace_from_results(self):
        class Result:
            def df(self):
                return pd.DataFrame()

        class Connection:
            sql = None

            def execute(self, sql):
                self.sql = sql
                return Result()

        con = Connection()
        self.assertEqual(app.query_market_candidates(
            con, "nodes.parquet", "polys.parquet", 50.0, 4.0, 2500), [])
        self.assertIn("shop IN ('supermarket', 'convenience')", con.sql)
        self.assertIn("amenity IS DISTINCT FROM 'marketplace'", con.sql)

    def test_beyond_fixed_radius_is_ignored(self):
        self.assertEqual(market.calc_market_score([poi("supermarket", 2500.01)]), 0.0)

    def test_score_is_clamped_to_valid_range(self):
        many = [poi("supermarket", 0, name=f"S{i}") for i in range(20)]
        self.assertEqual(market.calc_market_score(many), 10.0)
        self.assertGreaterEqual(market.calc_market_score([poi("supermarket", -1)]), 0.0)

    def test_display_radius_does_not_change_score_but_can_change_results(self):
        rows = [
            poi("supermarket", 300, name="Near"),
            poi("convenience", 1800, name="Mid"),
            poi("supermarket", 4000, name="Far"),
        ]

        def candidates(_con, _nodes, _polys, _lat, _lon, radius):
            return [dict(row) for row in rows if row["d_lin"] <= radius]

        scores = []
        counts = []
        with mock.patch.object(app, "query_market_candidates", side_effect=candidates):
            for radius in (1000, 2500, 5000):
                _df, score, count, _nearest = app.analyze_market(
                    object(), "nodes", "polys", 50.0, 4.0, radius, 5)
                scores.append(score)
                counts.append(count)
        self.assertEqual(scores, [scores[0]] * 3)
        self.assertEqual(counts, [1, 2, 3])

    def test_topn_zero_negative_and_positive(self):
        rows = [poi("supermarket", d, name=f"S{d}") for d in (100, 200, 300)]

        with mock.patch.object(app, "query_market_candidates", return_value=rows):
            zero, _score, _count, _nearest = app.analyze_market(
                object(), "nodes", "polys", 50.0, 4.0, 2500, 0)
            negative, _score, _count, _nearest = app.analyze_market(
                object(), "nodes", "polys", 50.0, 4.0, 2500, -1)
            positive, _score, _count, _nearest = app.analyze_market(
                object(), "nodes", "polys", 50.0, 4.0, 2500, 2)
        self.assertTrue(zero.empty)
        self.assertTrue(negative.empty)
        self.assertEqual(len(positive), 2)


class MarketDeduplicationTests(unittest.TestCase):
    def test_matching_maximizes_cardinality_in_greedy_failure_graph(self):
        rows = [
            poi("supermarket", 1, name="Alpha", source="node", lat=50.0),
            poi("supermarket", 2, name="Alpha", source="node", lat=50.1),
            poi("supermarket", 3, name="Alpha", source="polygon", lat=50.2),
            poi("supermarket", 4, name="Alpha", source="polygon", lat=50.3),
        ]
        # N1-P1=10, N1-P2=20, N2-P1=20; N2-P2 has no valid edge.
        pairs = market._maximum_cardinality_min_distance_pairs(
            rows, [(0, 2, 10), (0, 3, 20), (1, 2, 20)])
        self.assertEqual(set(pairs), {(0, 3), (1, 2)})

    def test_matching_ties_are_deterministic(self):
        rows = [
            poi("supermarket", 1, name="Alpha", source="node", lat=50.0),
            poi("supermarket", 2, name="Alpha", source="node", lat=50.1),
            poi("supermarket", 3, name="Alpha", source="polygon", lat=50.2),
            poi("supermarket", 4, name="Alpha", source="polygon", lat=50.3),
        ]
        edges = [(0, 2, 10), (0, 3, 10), (1, 2, 10), (1, 3, 10)]
        first = market._maximum_cardinality_min_distance_pairs(rows, edges)
        second = market._maximum_cardinality_min_distance_pairs(rows, list(reversed(edges)))
        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)

    def test_matching_named_node_polygon_counts_once_and_keeps_closest(self):
        node = poi("supermarket", 50, name="  My   Market ", source="node")
        polygon = poi("supermarket", 80, name="my market", source="polygon",
                      lat=50.0003)
        result = market.deduplicate_market_pois([node, polygon])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source"], "node")
        self.assertEqual(result[0]["d_lin"], 50)

    def test_different_names_remain_separate(self):
        rows = [poi("supermarket", 50, name="Alpha", source="node"),
                poi("supermarket", 55, name="Beta", source="polygon", lat=50.0001)]
        self.assertEqual(len(market.deduplicate_market_pois(rows)), 2)

    def test_same_name_beyond_threshold_remains_separate(self):
        rows = [poi("supermarket", 50, name="Alpha", source="node"),
                poi("supermarket", 55, name="alpha", source="polygon", lat=50.001)]
        self.assertGreater(market.haversine_m(50.0, 4.0, 50.001, 4.0),
                           market.MARKET_NAMED_DEDUP_DISTANCE_M)
        self.assertEqual(len(market.deduplicate_market_pois(rows)), 2)

    def test_same_name_but_different_type_remains_separate(self):
        rows = [poi("supermarket", 50, name="Alpha", source="node"),
                poi("convenience", 55, name="alpha", source="polygon", lat=50.0001)]
        self.assertEqual(len(market.deduplicate_market_pois(rows)), 2)

    def test_unnamed_almost_colocated_cross_source_rows_count_once(self):
        rows = [poi("supermarket", 50, name=None, source="node"),
                poi("supermarket", 55, name=None, source="polygon", lat=50.00005)]
        self.assertEqual(len(market.deduplicate_market_pois(rows)), 1)

    def test_same_source_rows_are_never_merged(self):
        rows = [poi("supermarket", 50, name="Alpha", source="node"),
                poi("supermarket", 55, name="alpha", source="node", lat=50.0001)]
        self.assertEqual(len(market.deduplicate_market_pois(rows)), 2)

    def test_polygon_brand_fallback_is_not_assumed_equal_to_node_name(self):
        node = poi("supermarket", 50, name="Alpha", source="node")
        polygon = poi("supermarket", 55, name="Alpha", source="polygon", lat=50.0001)
        polygon["brand"] = "Alpha"
        self.assertEqual(len(market.deduplicate_market_pois([node, polygon])), 2)


class NonMarketRegressionTests(unittest.TestCase):
    def test_non_market_configuration_and_formula_are_unchanged(self):
        expected = {
            "health": {"D0": 4000, "w_prox": 7.0, "w_count": 3.0, "Nsat": 3,
                       "bonus_if_hospital": 1.0},
            "transit": {"D0": 800, "w_prox": 7.0, "w_count": 3.0, "Nsat": 5},
            "park": {"D0": 1200, "w_prox": 6.0, "w_count": 4.0, "Nsat": 3},
            "sport": {"D0": 1500, "w_prox": 5.0, "w_count": 5.0, "Nsat": 3},
        }
        self.assertNotIn("market", app.SCORING)
        self.assertNotIn("market", app.SCORES)
        self.assertNotIn("school", app.SCORING)
        self.assertNotIn("school", app.SCORES)
        for category, config in expected.items():
            self.assertEqual(app.SCORING[category], config)
            raw = (1 - min(300, config["D0"]) / config["D0"]) * config["w_prox"]
            raw += min(2, config["Nsat"]) / config["Nsat"] * config["w_count"]
            if category == "health":
                raw += 1.0
            self.assertAlmostEqual(
                app.calc_category_score(category, 2, 300, category == "health"),
                min(10.0, raw),
            )

    def test_generic_market_calls_fail_explicitly(self):
        message = "dedicated"
        with self.assertRaisesRegex(ValueError, message):
            app.calc_category_score("market", 1, 100)
        with self.assertRaisesRegex(ValueError, message):
            app.query_category(object(), None, None, "market", 50.0, 4.0, 2500, 5)

    def test_optional_dependency_stubs_do_not_leak(self):
        for module_name in STUBBED_MODULE_NAMES:
            self.assertNotIn(module_name, sys.modules)


if __name__ == "__main__":
    unittest.main()

from pathlib import Path
import json
import math
import sys
import unittest
from unittest.mock import patch

import pandas as pd


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import app_duckdb as app
    import server
finally:
    sys.path.pop(0)


class FakeProvider(server.AddressSuggestionProvider):
    def __init__(self, suggestions=None):
        self.suggestions = suggestions or []
        self.calls = []

    def suggest(self, query, lang, limit=6):
        self.calls.append((query, lang, limit))
        return self.suggestions[:limit]


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class AddressSuggestionsApiTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def test_short_query_returns_no_suggestions_without_calling_provider(self):
        provider = FakeProvider([{"label": "Unused", "lat": 1, "lon": 2,
                                  "result_type": "street"}])
        with patch.object(server, "ADDRESS_PROVIDER", provider):
            response = self.client.get("/api/address-suggestions?q=ab&lang=en")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["suggestions"], [])
        self.assertEqual(provider.calls, [])

    def test_unsupported_language_is_rejected(self):
        response = self.client.get("/api/address-suggestions?q=Leuven&lang=fr")
        self.assertEqual(response.status_code, 400)

    def test_missing_api_key_is_reported_without_breaking_preview(self):
        with (patch.object(server, "ADDRESS_PROVIDER", None),
              patch.dict(server.os.environ, {}, clear=True)):
            response = self.client.get("/api/address-suggestions?q=Leuven&lang=nl")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["available"])
        self.assertEqual(payload["suggestions"], [])
        self.assertIn("niet beschikbaar", payload["message"])

    def test_geoapify_response_is_normalized_and_belgium_filtered(self):
        calls = []

        def fake_get(url, params, timeout):
            calls.append((url, params, timeout))
            return FakeResponse({"results": [
                {"formatted": "Bondgenotenlaan 1, Leuven, België", "lat": 50.88,
                 "lon": 4.70, "result_type": "amenity"},
                {"formatted": "", "lat": 50.0, "lon": 4.0},
                {"formatted": "Invalid coordinates", "lat": "bad", "lon": 4.0},
            ]})

        provider = server.GeoapifyAddressSuggestionProvider("server-secret", http_get=fake_get)
        suggestions = provider.suggest("Bondgenotenlaan", "en", limit=6)

        self.assertEqual(suggestions, [{
            "label": "Bondgenotenlaan 1, Leuven, België",
            "lat": 50.88,
            "lon": 4.70,
            "result_type": "amenity",
        }])
        self.assertEqual(calls[0][1]["filter"], "countrycode:be")
        self.assertEqual(calls[0][1]["lang"], "en")
        self.assertEqual(calls[0][1]["limit"], 6)
        self.assertEqual(calls[0][1]["format"], "json")
        self.assertEqual(calls[0][1]["apiKey"], "server-secret")

    def test_json_results_preserve_multiple_frontend_contract_entries(self):
        provider = server.GeoapifyAddressSuggestionProvider(
            "test-key", http_get=lambda *_args, **_kwargs: FakeResponse({"results": [
                {"formatted": "Gijmelstraat 56, 3200 Aarschot, Belgium",
                 "lat": 51.0034977, "lon": 4.8405107, "result_type": "building"},
                {"formatted": "Gijmelstraat, 3200 Aarschot, Belgium",
                 "lat": "51.003", "lon": "4.841", "result_type": "street"},
            ]}))
        with patch.object(server, "ADDRESS_PROVIDER", provider):
            response = self.client.get(
                "/api/address-suggestions?q=Gijmelstraat%2056&lang=tr")
        payload = response.get_json()
        self.assertTrue(payload["available"])
        self.assertEqual(len(payload["suggestions"]), 2)
        self.assertEqual(payload["suggestions"][0], {
            "label": "Gijmelstraat 56, 3200 Aarschot, Belgium",
            "lat": 51.0034977, "lon": 4.8405107, "result_type": "building",
        })

    def test_empty_results_are_available_but_have_no_suggestions(self):
        provider = server.GeoapifyAddressSuggestionProvider(
            "test-key", http_get=lambda *_args, **_kwargs: FakeResponse({"results": []}))
        with patch.object(server, "ADDRESS_PROVIDER", provider):
            response = self.client.get("/api/address-suggestions?q=NoMatch&lang=en")
        self.assertEqual(response.get_json(), {"available": True, "suggestions": []})

    def test_malformed_result_entries_are_skipped_without_crashing(self):
        provider = server.GeoapifyAddressSuggestionProvider(
            "test-key", http_get=lambda *_args, **_kwargs: FakeResponse({"results": [
                None, "wrong", {"formatted": "Missing coordinates"},
                {"formatted": "Valid", "lat": 50.1, "lon": 4.1},
            ]}))
        with patch.object(server, "ADDRESS_PROVIDER", provider):
            response = self.client.get("/api/address-suggestions?q=Valid&lang=nl")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["suggestions"], [{
            "label": "Valid", "lat": 50.1, "lon": 4.1,
            "result_type": "unknown",
        }])

    def test_geojson_properties_are_supported_safely(self):
        provider = server.GeoapifyAddressSuggestionProvider(
            "test-key", http_get=lambda *_args, **_kwargs: FakeResponse({"features": [
                {"type": "Feature", "properties": {
                    "formatted": "Leuven, Belgium", "lat": 50.88,
                    "lon": 4.70, "result_type": "city"}},
            ]}))
        self.assertEqual(provider.suggest("Leuven", "en"), [{
            "label": "Leuven, Belgium", "lat": 50.88, "lon": 4.70,
            "result_type": "city",
        }])

    def test_key_configured_after_import_is_resolved_at_request_time(self):
        provider = FakeProvider([{"label": "Late key result", "lat": 51.0,
                                  "lon": 4.8, "result_type": "building"}])
        with (patch.object(server, "ADDRESS_PROVIDER", None),
              patch.dict(server.os.environ, {"GEOAPIFY_API_KEY": "late-key"},
                         clear=True),
              patch.object(server, "GeoapifyAddressSuggestionProvider",
                           return_value=provider) as provider_class):
            response = self.client.get("/api/address-suggestions?q=Gijmelstraat&lang=tr")
        self.assertTrue(response.get_json()["available"])
        provider_class.assert_called_once_with("late-key")

    def test_upstream_failure_is_safe_and_never_exposes_api_key(self):
        secret = "secret-must-not-leak"

        def failing_get(*_args, **_kwargs):
            raise server.requests.ConnectionError("request failed with " + secret)

        provider = server.GeoapifyAddressSuggestionProvider(secret, http_get=failing_get)
        with (patch.object(server, "ADDRESS_PROVIDER", provider),
              self.assertLogs(server.LOGGER, level="WARNING") as logs):
            response = self.client.get("/api/address-suggestions?q=Gijmelstraat&lang=en")
        payload = response.get_json()
        self.assertFalse(payload["available"])
        self.assertEqual(payload["suggestions"], [])
        self.assertNotIn(secret, response.get_data(as_text=True))
        self.assertNotIn(secret, "\n".join(logs.output))

    def test_unexpected_provider_failure_returns_safe_json(self):
        provider = FakeProvider()
        provider.suggest = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("private upstream detail"))
        with (patch.object(server, "ADDRESS_PROVIDER", provider),
              self.assertLogs(server.LOGGER, level="ERROR")):
            response = self.client.get(
                "/api/address-suggestions?q=Gijmelstraat&lang=en")

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(response.get_json(), {
            "error": {
                "code": "internal_error",
                "message": "Address suggestions are temporarily unavailable.",
            }
        })
        self.assertNotIn("private upstream detail", response.get_data(as_text=True))


class AnalyzeValidationApiTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def assert_json_error(self, response, status=400, code="invalid_request"):
        self.assertEqual(response.status_code, status)
        self.assertEqual(response.content_type, "application/json")
        payload = response.get_json()
        self.assertEqual(payload["error"]["code"], code)
        self.assertIsInstance(payload["error"]["message"], str)
        self.assertNotIn("<html", response.get_data(as_text=True).lower())
        return payload

    def post_raw(self, value):
        return self.client.post(
            "/api/analyze", data=value, content_type="application/json")

    def valid_payload(self, **updates):
        payload = {
            "address": "Selected Belgian address",
            "lat": 51.0034977,
            "lon": 4.8405107,
            "radius": 2500,
            "topn": 20,
            "lang": "en",
        }
        payload.update(updates)
        return payload

    def test_json_root_must_be_an_object(self):
        for value in ([], [1], "text", 123, True, False, None):
            with self.subTest(value=value):
                response = self.post_raw(json.dumps(value))
                self.assert_json_error(response)

    def test_object_without_address_or_coordinates_is_rejected(self):
        self.assert_json_error(self.client.post("/api/analyze", json={}))

    def test_malformed_json_returns_controlled_error(self):
        self.assert_json_error(self.post_raw('{"lat":'))

    def test_coordinate_booleans_and_non_numeric_values_are_rejected(self):
        for field in ("lat", "lon"):
            for value in (True, False, "not-a-number"):
                with self.subTest(field=field, value=value):
                    response = self.client.post(
                        "/api/analyze", json=self.valid_payload(**{field: value}))
                    self.assert_json_error(response)

    def test_non_finite_coordinates_are_rejected(self):
        for field in ("lat", "lon"):
            for value in (math.nan, math.inf, -math.inf):
                with self.subTest(field=field, value=value):
                    response = self.client.post(
                        "/api/analyze", json=self.valid_payload(**{field: value}))
                    self.assert_json_error(response)

    def test_coordinate_boundaries_and_numeric_strings_are_accepted(self):
        cases = ((90, 180), (-90, -180), ("51.0034977", "4.8405107"))
        result = {"categories": [], "overall": 0.0}
        with patch.object(server, "_run_preview_analysis", return_value=result):
            for lat, lon in cases:
                with self.subTest(lat=lat, lon=lon):
                    response = self.client.post(
                        "/api/analyze", json=self.valid_payload(lat=lat, lon=lon))
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.content_type, "application/json")

    def test_out_of_range_coordinates_are_rejected(self):
        cases = (
            {"lat": 90.0001}, {"lat": -90.0001}, {"lat": 999},
            {"lon": 180.0001}, {"lon": -180.0001}, {"lon": 999},
        )
        for update in cases:
            with self.subTest(update=update):
                response = self.client.post(
                    "/api/analyze", json=self.valid_payload(**update))
                self.assert_json_error(response)

    def test_partial_coordinate_pair_is_rejected_even_with_address(self):
        for payload in (
            {"address": "Leuven", "lat": 50.88},
            {"address": "Leuven", "lon": 4.70},
            {"lat": 50.88},
            {"lon": 4.70},
        ):
            with self.subTest(payload=payload):
                self.assert_json_error(
                    self.client.post("/api/analyze", json=payload))

    def test_address_must_be_a_non_blank_bounded_string(self):
        for address in ({"street": "Gijmelstraat"}, ["Gijmelstraat"], "", "   "):
            with self.subTest(address=address):
                response = self.client.post(
                    "/api/analyze", json={"address": address})
                self.assert_json_error(response)
        response = self.client.post(
            "/api/analyze", json={"address": "x" * (server.MAX_ADDRESS_LENGTH + 1)})
        self.assert_json_error(response)

    def test_address_at_max_length_and_coordinates_is_accepted(self):
        result = {"categories": [], "overall": 0.0}
        with patch.object(server, "_run_preview_analysis", return_value=result):
            response = self.client.post(
                "/api/analyze",
                json=self.valid_payload(address="x" * server.MAX_ADDRESS_LENGTH))
        self.assertEqual(response.status_code, 200)

    def test_valid_address_string_is_geocoded_and_accepted(self):
        result = {"categories": [], "overall": 0.0}
        with (patch.object(server, "DATA_MODE", "real"),
              patch.object(server, "geocode",
                           return_value=(50.8795, 4.7023, "Leuven, Belgium")),
              patch.object(server, "_run_preview_analysis",
                           return_value=result) as analysis):
            response = self.client.post(
                "/api/analyze", json={"address": "  Leuven  "})
        self.assertEqual(response.status_code, 200)
        analysis.assert_called_once_with(
            50.8795, 4.7023, "Leuven, Belgium",
            server.DEFAULT_RADIUS_M, server.TOP_N, "tr")

    def test_address_not_found_has_a_distinct_json_error(self):
        with (patch.object(server, "DATA_MODE", "real"),
              patch.object(server, "geocode",
                           side_effect=RuntimeError("not found"))):
            response = self.client.post(
                "/api/analyze", json={"address": "Missing Belgian address"})
        payload = self.assert_json_error(
            response, status=400, code="address_not_found")
        self.assertEqual(payload["error"]["message"],
                         "Address could not be geocoded.")

    def test_unexpected_geocoder_failure_uses_generic_json_500(self):
        private_detail = "private geocoder implementation detail"
        with (patch.object(server, "DATA_MODE", "real"),
              patch.object(server, "geocode",
                           side_effect=ValueError(private_detail)),
              self.assertLogs(server.LOGGER, level="ERROR")):
            response = self.client.post(
                "/api/analyze", json={"address": "Leuven"})
        self.assert_json_error(response, status=500, code="internal_error")
        self.assertNotIn(private_detail, response.get_data(as_text=True))

    def test_invalid_radius_language_and_topn_are_json_errors(self):
        for update in (
            {"radius": 1200}, {"radius": True},
            {"lang": "fr"}, {"lang": True},
            {"topn": 0}, {"topn": 21}, {"topn": True}, {"topn": 1.5},
        ):
            with self.subTest(update=update):
                response = self.client.post(
                    "/api/analyze", json=self.valid_payload(**update))
                self.assert_json_error(response)

    def test_unexpected_analysis_failure_returns_generic_json(self):
        private_detail = "C:\\private\\cache\\secret.parquet"
        with (patch.object(server, "_run_preview_analysis",
                           side_effect=RuntimeError(private_detail)),
              self.assertLogs(server.LOGGER, level="ERROR")):
            response = self.client.post(
                "/api/analyze", json=self.valid_payload())
        payload = self.assert_json_error(response, status=500, code="internal_error")
        body = response.get_data(as_text=True)
        self.assertEqual(payload["error"]["message"],
                         "The analysis could not be completed.")
        self.assertNotIn(private_detail, body)
        self.assertNotIn("Traceback", body)

    def test_missing_analysis_data_returns_json_503(self):
        with (patch.object(server, "_run_preview_analysis",
                           side_effect=FileNotFoundError("private cache path")),
              self.assertLogs(server.LOGGER, level="ERROR")):
            response = self.client.post(
                "/api/analyze", json=self.valid_payload())
        self.assert_json_error(
            response, status=503, code="service_unavailable")


class PopupSecurityContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_home_address_popup_uses_text_content(self):
        self.assertIn("bindPopup(this.homePopupContent(d.display_address, t))",
                      self.frontend)
        self.assertIn("title.textContent = String(address == null ? '' : address)",
                      self.frontend)
        self.assertNotIn("'<b>' + d.display_address", self.frontend)

    def test_poi_popup_uses_text_content_for_names_and_types(self):
        self.assertIn(
            "bindPopup(this.poiPopupContent(markerName, markerType, straight, it, t))",
            self.frontend,
        )
        self.assertIn("title.textContent = String(name == null ? '' : name)",
                      self.frontend)
        self.assertIn("if (type) this.appendPopupLine(wrapper, type)",
                      self.frontend)
        self.assertIn("line.textContent = String(value == null ? '' : value)",
                      self.frontend)
        self.assertNotIn("'<b>' + markerName", self.frontend)

    def test_hostile_markup_has_no_raw_popup_interpolation_path(self):
        hostile_values = ("<b>Injected</b>", "<img src=x onerror=alert(1)>")
        for hostile in hostile_values:
            with self.subTest(hostile=hostile):
                self.assertNotIn("bindPopup('" + hostile, self.frontend)
        popup_start = self.frontend.index("appendPopupLine(wrapper, value)")
        popup_end = self.frontend.index("addTiles()", popup_start)
        popup_helpers = self.frontend[popup_start:popup_end]
        self.assertNotIn("innerHTML", popup_helpers)
        self.assertNotIn("insertAdjacentHTML", popup_helpers)
        self.assertNotIn("outerHTML", popup_helpers)


class RadiusReanalysisApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.demo_dir, cls.demo_nodes, cls.demo_polys = server._create_demo_caches()
        cls.patches = [
            patch.object(server, "NODES_PATH", cls.demo_nodes),
            patch.object(server, "POLYS_PATH", cls.demo_polys),
            patch.object(
                server, "PARK_PATH",
                str(Path(cls.demo_nodes).with_name(
                    "be_park_destinations.parquet"))),
            patch.object(
                server, "SPORT_PATH",
                str(Path(cls.demo_nodes).with_name(
                    "be_sport_destinations.parquet"))),
            patch.object(server, "DATA_MODE", "demo"),
        ]
        for active_patch in cls.patches:
            active_patch.start()

    @classmethod
    def tearDownClass(cls):
        for active_patch in reversed(cls.patches):
            active_patch.stop()
        cls.demo_dir.cleanup()

    def setUp(self):
        server._result_cache.clear()
        self.client = server.app.test_client()

    def analyze(self, radius):
        response = self.client.post("/api/analyze", json={
            "address": "Selected Belgian address",
            "lat": server.DEMO_LAT,
            "lon": server.DEMO_LON,
            "radius": radius,
            "topn": 20,
            "lang": "en",
        })
        self.assertEqual(response.status_code, 200)
        return response.get_json()

    def test_radius_reanalysis_keeps_market_score_and_coordinates(self):
        payloads = [self.analyze(radius) for radius in (1000, 2500, 5000)]
        markets = [next(cat for cat in payload["categories"] if cat["key"] == "market")
                   for payload in payloads]

        self.assertEqual([market["score"] for market in markets], [9.2, 9.2, 9.2])
        self.assertEqual([market["count"] for market in markets], [3, 4, 4])
        self.assertEqual([len(market["items"]) for market in markets], [3, 4, 4])
        self.assertTrue(all(payload["display_address"] == "Selected Belgian address"
                            for payload in payloads))
        self.assertTrue(all(payload["lat"] == server.DEMO_LAT and
                            payload["lon"] == server.DEMO_LON for payload in payloads))
        self.assertTrue(all("Demo Marketplace" not in
                            [item["name"] for item in market["items"]]
                            for market in markets))
        self.assertTrue(all(
            {"straight_m", "walk_m", "walk_min", "drive_m", "drive_min"}
            <= set(item)
            for market in markets for item in market["items"]
        ))

    def test_market_breakdown_describes_existing_score(self):
        market = next(cat for cat in self.analyze(2500)["categories"]
                      if cat["key"] == "market")
        breakdown = market["score_breakdown"]

        self.assertEqual(breakdown["proximity_max"], 7.0)
        self.assertEqual(breakdown["choice_max"], 3.0)
        self.assertEqual(breakdown["effective_count_saturation"], 3.0)
        self.assertAlmostEqual(
            breakdown["proximity_points"] + breakdown["choice_points"],
            market["score"],
        )
        self.assertEqual(breakdown["final_score"], market["score"])

    def test_school_uses_dedicated_path_and_keeps_api_shape(self):
        schools = [next(cat for cat in self.analyze(radius)["categories"]
                        if cat["key"] == "school")
                   for radius in (1000, 2500, 5000)]

        self.assertEqual([category["score"] for category in schools],
                         [schools[0]["score"]] * 3)
        breakdowns = [category["score_breakdown"] for category in schools]
        self.assertEqual(breakdowns, [breakdowns[0]] * 3)
        self.assertEqual(set(breakdowns[0]), {
            "proximity_points", "choice_points", "nearest_core_school_m",
        })
        self.assertAlmostEqual(
            breakdowns[0]["proximity_points"] + breakdowns[0]["choice_points"],
            schools[0]["score"], places=1)
        self.assertEqual(
            [[item["type"] for item in category["items"]] for category in schools],
            [["school", "kindergarten"]] * 3,
        )

    def test_school_nearest_core_distance_ignores_nearer_kindergarten(self):
        candidates = [
            {"name": "Near Kindergarten", "amenity": "kindergarten", "lat": 50.0,
             "lon": 4.0, "source": "node", "d_lin": 100.0},
            {"name": "Core School", "amenity": "school", "lat": 50.001,
             "lon": 4.0, "source": "node", "d_lin": 500.0},
        ]
        with patch.object(server, "query_school_candidates", return_value=candidates):
            breakdown = server._school_score_breakdown(object(), 50.0, 4.0)
        self.assertEqual(breakdown["nearest_core_school_m"], 500.0)

    def test_health_uses_dedicated_path_and_exposes_fixed_breakdown(self):
        health_categories = [next(
            category for category in self.analyze(radius)["categories"]
            if category["key"] == "health") for radius in (1000, 2500, 5000)]
        self.assertEqual([category["score"] for category in health_categories],
                         [health_categories[0]["score"]] * 3)
        breakdowns = [category["score_breakdown"] for category in health_categories]
        self.assertEqual(breakdowns, [breakdowns[0]] * 3)
        self.assertEqual(set(breakdowns[0]), {
            "clinical_proximity_points", "choice_points", "hospital_points",
            "nearest_clinical_m", "nearest_hospital_m",
        })
        self.assertAlmostEqual(
            breakdowns[0]["clinical_proximity_points"]
            + breakdowns[0]["choice_points"] + breakdowns[0]["hospital_points"],
            health_categories[0]["score"], places=1)

    def test_park_uses_dedicated_cache_and_exposes_authoritative_breakdown(self):
        parks = [next(category for category in self.analyze(radius)["categories"]
                      if category["key"] == "park")
                 for radius in (1000, 2500, 5000)]
        self.assertEqual([category["score"] for category in parks],
                         [parks[0]["score"]] * 3)
        breakdowns = [category["score_breakdown"] for category in parks]
        for field in ("score_precise", "score_public", "primary_points",
                      "choice_points", "secondary_points", "winner"):
            self.assertEqual([breakdown[field] for breakdown in breakdowns],
                             [breakdowns[0][field]] * 3)
        self.assertEqual(breakdowns[0]["scoring_radius_m"], 2500)
        self.assertEqual(parks[0]["score"], breakdowns[0]["score_public"])
        self.assertEqual(parks[0]["items"][0]["park_class"], "park")
        self.assertNotIn("geometry_wkb", parks[0]["items"][0])

    def test_sport_uses_canonical_cache_and_fixed_radius_breakdown(self):
        sports = [next(category for category in self.analyze(radius)["categories"]
                       if category["key"] == "sport")
                  for radius in (1000, 2500, 5000)]
        self.assertEqual([category["score"] for category in sports],
                         [sports[0]["score"]] * 3)
        self.assertEqual([category["count"] for category in sports], [1, 2, 2])
        breakdowns = [category["score_breakdown"] for category in sports]
        for field in ("score_precise", "score", "best", "choice"):
            self.assertEqual([breakdown[field] for breakdown in breakdowns],
                             [breakdowns[0][field]] * 3)
        self.assertEqual(breakdowns[0]["scoring_radius_m"], 3000)
        self.assertEqual(sports[0]["score"], breakdowns[0]["score"])
        self.assertEqual(sports[0]["items"][0]["facility_class"],
                         "fitness_gym")
        self.assertEqual(sports[0]["items"][0]["sport_id"], "sport:demo:1")
        self.assertTrue(sports[0]["items"][0]["score_eligible"])
        self.assertNotIn("geometry_wkb", sports[0]["items"][0])

    def test_overall_uses_public_park_score_with_existing_weights(self):
        payload = self.analyze(2500)
        category_scores = {category["key"]: category["score"]
                           for category in payload["categories"]}
        expected = sum(category_scores[key] * app.OVERALL_WEIGHTS[key]
                       for key in server.CATS)
        self.assertEqual(payload["overall"], round(expected, 1))


class RealBelgiumParkPreviewApiTests(unittest.TestCase):
    cache_dir = SOURCE_DIR / "cache"
    park_path = cache_dir / "be_park_destinations.parquet"
    locations = {
        "Gijmelstraat": (51.0034977, 4.8405107, 5.697331540),
        "Grote Markt Aarschot": (50.9843, 4.8367, 8.533889681),
        "Leuven center": (50.8795, 4.7023, 8.648155664),
        "Scherpenheuvel": (50.9949, 4.9778, 4.209516994),
        "Diepenstraat Langdorp": (51.0129, 4.8930, 1.033141391),
    }

    @unittest.skipUnless(park_path.is_file(), "real Park cache unavailable")
    def test_five_locations_use_dedicated_scorer_through_preview_api(self):
        original_payload = server._category_payload

        def analysis(*, lat, lon, radius, topn, **_kwargs):
            with server.duckdb.connect() as con:
                _frame, _score, _count, _nearest, breakdown = server.analyze_park(
                    con, str(self.park_path), lat, lon, radius, topn)
            scores = {config["label"]: 0.0 for config in server.CATS.values()}
            scores[server.CATS["park"]["label"]] = breakdown["score_public"]
            return {"overall": 0.0, "scores": scores,
                    "breakdowns": {"park": breakdown}, "map_html": ""}

        def category_payload(con, category, lat, lon, radius, topn, score,
                             breakdown=None):
            if category == "park":
                return original_payload(
                    con, category, lat, lon, radius, topn, score, breakdown)
            return {"key": category, "score": score, "count": 0,
                    "nearest_m": None, "items": [], "score_breakdown": None}

        client = server.app.test_client()
        with (patch.object(server, "PARK_PATH", str(self.park_path)),
              patch.object(server, "analyze_location", side_effect=analysis),
              patch.object(server, "_category_payload",
                           side_effect=category_payload)):
            for name, (lat, lon, expected) in self.locations.items():
                scores = []
                breakdowns = []
                for radius in (1000, 2500, 5000):
                    server._result_cache.clear()
                    response = client.post("/api/analyze", json={
                        "address": name, "lat": lat, "lon": lon,
                        "radius": radius, "topn": 20, "lang": "en",
                    })
                    self.assertEqual(response.status_code, 200)
                    park = next(category for category in response.get_json()["categories"]
                                if category["key"] == "park")
                    scores.append(park["score"])
                    breakdown = park["score_breakdown"]
                    breakdowns.append(breakdown)
                    self.assertAlmostEqual(
                        breakdown["primary_points"] + breakdown["choice_points"]
                        + breakdown["secondary_points"],
                        breakdown["score_precise"])
                    legacy = (6.0 * max(0.0, 1.0 -
                                        (park["nearest_m"] or 1200) / 1200.0)
                              + 4.0 * min(park["count"], 3) / 3.0)
                    self.assertNotEqual(park["score"], round(min(10.0, legacy), 1))
                    self.assertTrue(all("geometry_wkb" not in item
                                        for item in park["items"]))
                with self.subTest(location=name):
                    self.assertEqual(scores, [round(expected, 1)] * 3)
                    self.assertEqual(breakdowns, [breakdowns[0]] * 3)
                    self.assertAlmostEqual(
                        breakdowns[0]["score_precise"], expected, places=8)


class RealBelgiumSportPreviewApiTests(unittest.TestCase):
    sport_path = SOURCE_DIR / "cache" / "be_sport_destinations.parquet"

    @unittest.skipUnless(sport_path.is_file(), "real Sport cache unavailable")
    def test_gijmel_winner_and_display_filter_contract(self):
        payloads = []
        with patch.object(server, "SPORT_PATH", str(self.sport_path)):
            for radius in (1000, 2500, 5000):
                with server.duckdb.connect() as con:
                    _frame, score, _count, _nearest, breakdown = server.analyze_sport(
                        con, str(self.sport_path), 51.0034977, 4.8405107,
                        radius, 20)
                    payloads.append(server._category_payload(
                        con, "sport", 51.0034977, 4.8405107, radius, 20,
                        score, breakdown))

        self.assertEqual([payload["score"] for payload in payloads],
                         [7.6, 7.6, 7.6])
        self.assertEqual([payload["score_breakdown"] for payload in payloads],
                         [payloads[0]["score_breakdown"]] * 3)
        winner = payloads[0]["score_breakdown"]["best"]["winner"]
        self.assertEqual(winner["display_name"], "Basic-Fit")
        self.assertEqual(winner["facility_class"], "fitness_gym")
        self.assertAlmostEqual(winner["distance_m"], 645.082966300914)

        self.assertEqual([payload["count"] for payload in payloads],
                         [1, 14, 31])
        self.assertEqual([len(payload["items"]) for payload in payloads],
                         [1, 14, 20])
        self.assertTrue(all(
            item["score_eligible"]
            for payload in payloads for item in payload["items"]
        ))
        self.assertNotIn(
            "sport:osm:way:189639931",
            [item["sport_id"] for payload in payloads
             for item in payload["items"]],
        )


class ParkFrontendContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_collapsed_summary_uses_backend_winner_not_generic_nearest(self):
        start = self.frontend.index("parkSummary(breakdown, t)")
        end = self.frontend.index("scoreColor(s)", start)
        summary = self.frontend[start:end]
        self.assertIn("breakdown.winner", summary)
        self.assertIn("winner.distance_m", summary)
        self.assertNotIn("nearest_m", summary)
        self.assertNotIn("count", summary)
        self.assertIn("parkSummaryStr", self.frontend)

    def test_park_components_and_total_use_backend_fields(self):
        for field in ("parkBreakdown.primary_points",
                      "parkBreakdown.choice_points",
                      "parkBreakdown.secondary_points"):
            self.assertIn(field, self.frontend)
        self.assertIn(
            "parkTotalStr: cat.key === 'park' ? this.fmtParkValue(cat.score)",
            self.frontend)
        self.assertNotIn(
            "parkBreakdown.primary_points + parkBreakdown.choice_points",
            self.frontend)

    def test_all_park_classes_are_localized_in_tr_en_nl(self):
        for key in ("park", "garden", "leisure_recreation",
                    "landuse_recreation", "village_green", "nature_reserve",
                    "national_park", "protected_area", "playground", "dog_park"):
            self.assertGreaterEqual(self.frontend.count(key + ":"), 6)
        for text in ("Açık hava dinlenme alanı", "Protected natural area",
                     "Beschermd natuurgebied", "Oyun alanı", "Playground",
                     "Speeltuin"):
            self.assertIn(text, self.frontend)
        self.assertIn("t.parkTypes[value] || t.parkGenericClass", self.frontend)

    def test_park_wording_is_plain_and_concise_in_all_languages(self):
        for text in (
            "Park skoru nasıl oluşuyor?",
            "En iyi park seçeneği",
            "Yakındaki diğer parklar",
            "Oyun alanları ve köpek parkları",
            "How is the Park score built?",
            "Best park option",
            "Other nearby parks",
            "Playgrounds and dog parks",
            "Hoe is de Parkscore opgebouwd?",
            "Beste parkoptie",
            "Andere parken in de buurt",
            "Speeltuinen en hondenweides",
        ):
            self.assertIn(text, self.frontend)
        for old_text in (
            "Ana park erişimi",
            "İkincil rekreasyon",
            "Main park access",
            "Secondary recreation",
            "Toegang tot een hoofdpark",
            "Secundaire recreatie",
        ):
            self.assertNotIn(old_text, self.frontend)

    def test_component_copy_uses_backend_counts_without_repeating_overview(self):
        self.assertIn(
            "parkBreakdown.choice.distinct_alternative_identity_count",
            self.frontend,
        )
        self.assertIn(
            "parkBreakdown.secondary.contributing_candidate_count",
            self.frontend,
        )
        for text in (
            "Ek puan sağlayan başka park seçeneği bulunamadı.",
            "1 other park option contributes extra points.",
            "No nearby playground or dog park contributes.",
            "1 andere parkoptie draagt bij aan de extra punten.",
            "Geen speeltuin of hondenweide in de buurt draagt bij.",
        ):
            self.assertIn(text, self.frontend)
        self.assertIn("{{ cat.parkChoiceCountStr }}", self.frontend)
        self.assertIn("{{ cat.parkSecondaryCountStr }}", self.frontend)
        self.assertNotIn("{{ t.parkChoiceShort }}", self.frontend)
        self.assertNotIn("{{ t.parkSecondaryShort }}", self.frontend)

    def test_unnamed_park_fallbacks_do_not_render_backend_placeholder(self):
        for text in ("İsimsiz park", "Naamloos park", "Unnamed park",
                     "İsimsiz oyun alanı", "Naamloze speeltuin",
                     "Unnamed playground"):
            self.assertIn(text, self.frontend)
        self.assertIn("String(value).trim().toLowerCase() !== 'unnamed'",
                      self.frontend)
        self.assertIn("t.parkUnnamedTypes[item.park_class]", self.frontend)

    def test_area_and_missing_area_are_handled_without_raw_values(self):
        self.assertIn("if (!Number.isFinite(value) || value <= 0) return ''",
                      self.frontend)
        self.assertIn("value < 10000", self.frontend)
        self.assertIn("value / 10000", self.frontend)
        self.assertIn("Intl.NumberFormat", self.frontend)

    def test_park_explanation_and_nearby_title_exist_in_all_languages(self):
        for text in (
            "Haritadaki 1 / 2,5 / 5 km seçimi Park skorunu değiştirmez.",
            "Changing the 1 / 2.5 / 5 km display radius does not change the Park score.",
            "Het wijzigen van de weergavestraal van 1 / 2,5 / 5 km verandert de Parkscore niet.",
            "Yakındaki park ve rekreasyon alanları",
            "Nearby parks and recreation",
            "Parken en recreatie in de buurt",
        ):
            self.assertIn(text, self.frontend)

    def test_park_list_and_map_share_localized_name_and_class_helpers(self):
        self.assertIn("name: cat.key === 'park' ? this.parkName(it, t)",
                      self.frontend)
        self.assertIn("this.parkClassLabel(it.park_class, t)", self.frontend)
        self.assertIn("const markerName = cat.key === 'park' ? this.parkName(it, t)",
                      self.frontend)
        self.assertIn("this.parkClassLabel(it.park_class, t)", self.frontend)

    def test_no_winner_and_secondary_only_states_use_safe_summary(self):
        self.assertIn("if (!winner) return t.parkNoWinner", self.frontend)
        for text in ("Yakında güçlü bir park seçeneği bulunamadı",
                     "No strong park option nearby",
                     "Geen sterke parkoptie in de buurt"):
            self.assertIn(text, self.frontend)


class TransitPreviewContractTests(unittest.TestCase):
    def setUp(self):
        server._result_cache.clear()

    @staticmethod
    def breakdown():
        return {
            "local_access_points": 4.770420569307268,
            "rail_access_points": 2.253276758092536,
            "local_scoring_radius_m": 1000,
            "rail_scoring_radius_m": 7500,
            "best_local_name": "Unique Local Winner",
            "best_local_distance_m": 295.1770734841513,
            "best_local_weekday_departures": 84.0,
            "best_local_saturday_departures": 57.0,
            "best_local_sunday_departures": 56.0,
            "best_rail_name": "Unique Rail Winner",
            "best_rail_distance_m": 2415.282592945211,
            "best_rail_weekday_departures": 169.0,
            "best_rail_saturday_departures": 30.0,
            "best_rail_sunday_departures": 30.0,
        }

    @staticmethod
    def legacy_frame():
        return pd.DataFrame([{
            "n_total": 140, "d_min": 149.0,
            "name": "Legacy Gijmelberglaan", "brand": None,
            "amenity": None, "railway": None, "highway": "bus_stop",
            "public_transport": "platform",
            "lat": 51.0042, "lon": 4.8423, "d_lin": 149.0,
            "walk_m": 186.0, "walk_s": 120.0,
            "drive_m": 209.0, "drive_s": 30.0,
        }])

    def test_run_preview_analysis_forwards_synthetic_transit_breakdown_unchanged(self):
        breakdown = self.breakdown()
        scores = {config["label"]: 5.0 for config in server.CATS.values()}
        scores[server.CATS["transit"]["label"]] = 7.0
        analysis = {"overall": 6.0, "scores": scores,
                    "breakdowns": {"transit": breakdown}}

        def category_payload(_con, category, _lat, _lon, _radius, _topn,
                             score, transit_breakdown=None):
            return {
                "key": category, "score": score, "count": 140,
                "nearest_m": 149, "items": [{"name": "Legacy item"}],
                "score_breakdown": transit_breakdown,
            }

        with (patch.object(server, "analyze_location", return_value=analysis),
              patch.object(server, "_category_payload",
                           side_effect=category_payload)):
            payload = server._run_preview_analysis(
                51.0034977, 4.8405107, "Gijmelstraat", 2500, 20, "en")

        transit = next(category for category in payload["categories"]
                       if category["key"] == "transit")
        self.assertIs(transit["score_breakdown"], breakdown)
        self.assertEqual(transit["score_breakdown"], self.breakdown())

    def test_transit_legacy_fields_and_authoritative_breakdown_remain_independent(self):
        breakdown = self.breakdown()
        with patch.object(server, "query_category",
                          return_value=self.legacy_frame()):
            payload = server._category_payload(
                object(), "transit", 51.0034977, 4.8405107,
                2500, 20, 7.0, breakdown)

        self.assertIs(payload["score_breakdown"], breakdown)
        self.assertEqual(payload["nearest_m"], 149)
        self.assertEqual(payload["score_breakdown"]["best_local_distance_m"],
                         295.1770734841513)
        self.assertEqual(payload["count"], 140)
        self.assertEqual(payload["items"][0]["name"],
                         "Legacy Gijmelberglaan")
        self.assertEqual(payload["items"][0]["display_type"], "bus_stop")

    def test_missing_transit_breakdown_is_safe_and_not_fabricated(self):
        with patch.object(server, "query_category",
                          return_value=self.legacy_frame()):
            payload = server._category_payload(
                object(), "transit", 51.0034977, 4.8405107,
                2500, 20, 7.0)
        self.assertIsNone(payload["score_breakdown"])
        self.assertEqual(payload["nearest_m"], 149)
        self.assertEqual(payload["count"], 140)

    def test_transit_display_classification_uses_only_preserved_source_tags(self):
        cases = (
            ({"amenity": "bus_station"}, "bus_station"),
            ({"railway": "station"}, "rail_station"),
            ({"railway": "halt"}, "rail_halt"),
            ({"railway": "tram_stop"}, "tram_stop"),
            ({"highway": "bus_stop"}, "bus_stop"),
            ({"public_transport": "platform"}, "transit_platform"),
            ({"public_transport": "stop_position"}, "transit_stop"),
            ({"railway": "subway_entrance"}, "transit_point"),
            ({}, "transit_point"),
            ({"public_transport": "unexpected"}, "transit_point"),
        )
        for tags, expected in cases:
            with self.subTest(tags=tags):
                self.assertEqual(server._transit_display_type(tags), expected)


class SchoolExplanationContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(encoding="utf-8")

    def test_school_explanation_exists_in_all_languages(self):
        for text in (
            "Okul skoru her zaman 2,5 km yarıçapındaki eğitim noktalarına göre hesaplanır.",
            "The School score is always calculated using education options within a 2.5 km radius.",
            "De schoolscore wordt altijd berekend op basis van onderwijsvoorzieningen binnen een straal van 2,5 km.",
            "Anaokulları yakınlık puanını belirlemez",
            "Kindergartens do not determine proximity points",
            "Kleuterscholen bepalen de nabijheidsscore niet",
        ):
            self.assertIn(text, self.frontend)

    def test_market_explanation_text_remains_unchanged(self):
        for text in (
            "Market skoru her zaman adresin 2,5 km çevresindeki market seçenekleri kullanılarak hesaplanır.",
            "The Market score is always calculated using grocery options within 2.5 km of the address.",
            "De marktscore wordt altijd berekend op basis van de winkelmogelijkheden binnen 2,5 km van het adres.",
        ):
            self.assertIn(text, self.frontend)

    def test_school_poi_types_are_translated_in_all_languages(self):
        for mapping in (
            "schoolTypes: { school: 'Okul', kindergarten: 'Anaokulu' }",
            "schoolTypes: { school: 'School', kindergarten: 'Kindergarten' }",
            "schoolTypes: { school: 'School', kindergarten: 'Kleuterschool' }",
        ):
            self.assertIn(mapping, self.frontend)

    def test_school_type_translation_is_reactive_and_has_raw_fallback(self):
        self.assertIn(
            "? (t.schoolTypes[it.type] || it.type || '')",
            self.frontend,
        )

    def test_non_school_non_health_poi_types_keep_existing_raw_rendering(self):
        self.assertIn(
            ": (cat.key === 'transit'",
            self.frontend,
        )
        self.assertIn(": (it.type || '')))", self.frontend)


class HealthExplanationContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(encoding="utf-8")

    def test_health_explanation_exists_in_all_languages(self):
        for text in (
            "Sağlık skoru, yerel klinik sağlık hizmetlerine yakınlık, yakındaki sağlık seçenekleri ve hastane erişimi olmak üzere üç bölümden oluşur.",
            "De gezondheidsscore bestaat uit drie onderdelen: nabijheid van lokale klinische zorg, lokale zorgkeuze en toegang tot een ziekenhuis.",
            "The Health score has three components: local clinical care proximity, local healthcare choice, and hospital access.",
            "Diş hekimi ve diğer uzman sağlık hizmetleri bu puanı etkilemez.",
            "Tandartsen en andere gespecialiseerde zorgdiensten beïnvloeden deze score niet.",
            "Dentists and other specialist healthcare services do not affect this score.",
        ):
            self.assertIn(text, self.frontend)

    def test_health_panel_renders_all_existing_breakdown_components(self):
        for field in (
            "cat.score_breakdown.clinical_proximity_points",
            "cat.score_breakdown.choice_points",
            "cat.score_breakdown.hospital_points",
        ):
            self.assertIn(field, self.frontend)
        for rendered_value in (
            "cat.clinicalProximityStr }} / 5",
            "cat.healthChoiceStr }} / 3",
            "cat.hospitalPointsStr }} / 2",
            "cat.healthScoreStr }} / 10",
        ):
            self.assertIn(rendered_value, self.frontend)

    def test_health_breakdown_does_not_enter_market_school_formatters(self):
        self.assertIn(
            "proximityStr: (cat.key === 'market' || cat.key === 'school') && cat.score_breakdown",
            self.frontend,
        )
        self.assertIn(
            "choiceStr: (cat.key === 'market' || cat.key === 'school') && cat.score_breakdown",
            self.frontend,
        )

    def test_health_scoring_distances_use_breakdown_fields_and_safe_null_fallbacks(self):
        self.assertIn("cat.score_breakdown.nearest_clinical_m", self.frontend)
        self.assertIn("cat.score_breakdown.nearest_hospital_m", self.frontend)
        self.assertIn("t.healthNearestClinicalNone", self.frontend)
        self.assertIn("t.healthNearestHospitalNone", self.frontend)
        self.assertIn("2,5 km içinde bulunamadı", self.frontend)
        self.assertIn("20 km içinde bulunamadı", self.frontend)

    def test_health_explanation_does_not_use_legacy_summary_fields(self):
        start = self.frontend.index("clinicalProximityStr:")
        end = self.frontend.index("limitNote:", start)
        health_view_model = self.frontend[start:end]
        self.assertNotIn("nearest_m", health_view_model)
        self.assertNotIn("n_total", health_view_model)
        self.assertNotIn("has_hospital", health_view_model)

    def test_health_summary_uses_scoring_distances_not_legacy_fields(self):
        start = self.frontend.index("healthCountStr:")
        end = self.frontend.index("toggleInfo:", start)
        summary_view_model = self.frontend[start:end]
        self.assertIn("cat.score_breakdown.nearest_clinical_m", summary_view_model)
        self.assertIn("cat.score_breakdown.nearest_hospital_m", summary_view_model)
        self.assertNotIn("cat.nearest_m", summary_view_model)
        self.assertNotIn("cat.has_hospital", summary_view_model)
        self.assertNotIn("hospY", summary_view_model)
        self.assertNotIn("hospN", summary_view_model)

    def test_health_summary_has_localized_null_fallbacks(self):
        for text in (
            "2,5 km içinde klinik sağlık noktası yok",
            "20 km içinde hastane tesisi yok",
            "geen klinische zorglocatie binnen 2,5 km",
            "geen ziekenhuisvoorziening binnen 20 km",
            "no clinical care location within 2.5 km",
            "no hospital facility within 20 km",
        ):
            self.assertIn(text, self.frontend)

    def test_health_poi_types_are_translated_in_all_languages(self):
        for mapping in (
            "healthTypes: { clinic: 'Klinik', doctor: 'Doktor', doctors: 'Doktor', pharmacy: 'Eczane', dentist: 'Diş hekimi', hospital: 'Hastane', physiotherapist: 'Fizyoterapist', psychotherapist: 'Psikoterapist', laboratory: 'Laboratuvar', blood_donation: 'Kan bağışı' }",
            "healthTypes: { clinic: 'Kliniek', doctor: 'Arts', doctors: 'Arts', pharmacy: 'Apotheek', dentist: 'Tandarts', hospital: 'Ziekenhuis', physiotherapist: 'Kinesitherapeut', psychotherapist: 'Psychotherapeut', laboratory: 'Laboratorium', blood_donation: 'Bloeddonatie' }",
            "healthTypes: { clinic: 'Clinic', doctor: 'Doctor', doctors: 'Doctor', pharmacy: 'Pharmacy', dentist: 'Dentist', hospital: 'Hospital', physiotherapist: 'Physiotherapist', psychotherapist: 'Psychotherapist', laboratory: 'Laboratory', blood_donation: 'Blood donation' }",
        ):
            self.assertIn(mapping, self.frontend)

    def test_health_type_translation_is_reactive_and_has_raw_fallback(self):
        self.assertIn(
            "? (t.healthTypes[it.type] || it.type || '')",
            self.frontend,
        )
        self.assertIn(
            "textTransform: (cat.key === 'school' || cat.key === 'health' || cat.key === 'transit' || cat.key === 'park' || cat.key === 'sport') ? 'none' : 'capitalize'",
            self.frontend,
        )

    def test_existing_market_and_school_explanations_remain_present(self):
        self.assertIn(
            "The Market score is always calculated using grocery options within 2.5 km of the address.",
            self.frontend,
        )
        self.assertIn(
            "The School score is always calculated using education options within a 2.5 km radius.",
            self.frontend,
        )


class TransitExplanationContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_frontend_references_authoritative_transit_breakdown_fields(self):
        for field in (
            "local_access_points", "rail_access_points",
            "best_local_logical_stop_id", "best_local_name",
            "best_local_distance_m", "best_local_weekday_departures",
            "best_local_saturday_departures", "best_local_sunday_departures",
            "best_local_operators", "best_local_modes",
            "best_rail_logical_station_id", "best_rail_name",
            "best_rail_distance_m", "best_rail_weekday_departures",
            "best_rail_saturday_departures", "best_rail_sunday_departures",
        ):
            self.assertIn("transitBreakdown." + field, self.frontend)

    def test_collapsed_summary_uses_scoring_distances_not_legacy_fields(self):
        start = self.frontend.index("transitSummary(breakdown, t)")
        end = self.frontend.index("scoreColor(s)", start)
        summary = self.frontend[start:end]
        self.assertIn("breakdown.best_local_distance_m", summary)
        self.assertIn("breakdown.best_rail_distance_m", summary)
        self.assertNotIn("nearest_m", summary)
        self.assertNotIn("count", summary)

    def test_transit_component_and_departure_labels_exist_in_all_languages(self):
        for text in (
            "Düzenli toplu taşıma", "Tren erişimi",
            "Regulier openbaar vervoer", "Treinbereikbaarheid",
            "Scheduled local transit", "Rail access",
            "Tipik seferler (06:00–22:00)",
            "Typische vertrekken (06:00–22:00)",
            "Typical departures (06:00–22:00)",
            "Hafta içi", "Weekdag", "Weekday",
            "Cumartesi", "Zaterdag", "Saturday",
            "Pazar", "Zondag", "Sunday",
        ):
            self.assertIn(text, self.frontend)

    def test_best_usable_rail_wording_exists_in_all_languages(self):
        for text in (
            "En kullanışlı tren istasyonu",
            "Meest bruikbare treinstation",
            "Best usable rail station",
        ):
            self.assertIn(text, self.frontend)

    def test_simplified_explanation_exists_in_all_languages(self):
        for text in (
            "1 km içindeki en kullanışlı düzenli durak değerlendirilir.",
            "250 metreye kadar mesafe açısından tam değer alınır",
            "7,5 km içindeki tren istasyonları mesafe ve tarifeli tren sıklığı",
            "The most useful regular scheduled stop within 1 km is evaluated.",
            "Distance receives full credit through 250 m",
            "Rail stations within 7.5 km are compared",
            "De meest bruikbare reguliere halte binnen 1 km wordt beoordeeld.",
            "Afstand krijgt volledige waarde tot 250 m",
            "Treinstations binnen 7,5 km worden vergeleken",
        ):
            self.assertIn(text, self.frontend)

    def test_engineering_details_are_removed_from_visible_explanation(self):
        for text in (
            "beş kat ağırlık", "yaklaşık 90 sefer",
            "azalan getiriye", "five-day weight",
            "weighted 90 departures", "diminishing returns",
            "vijf keer mee", "gewogen 90 vertrekken",
            "neemt geleidelijk af",
        ):
            self.assertNotIn(text, self.frontend)

    def test_flex_and_display_radius_explanations_exist_in_all_languages(self):
        for text in (
            "Flex / talebe bağlı ulaşım sayısal puana dahil değildir.",
            "Flex / vervoer op aanvraag telt niet mee in de numerieke score.",
            "Flex / on-demand transport is not included in the numeric score.",
            "gösterim yarıçapını değiştirmek Ulaşım skorunu değiştirmez",
            "weergavestraal van 1 / 2,5 / 5 km verandert de vervoersscore niet",
            "display radius does not change the Transit score",
        ):
            self.assertIn(text, self.frontend)

    def test_null_candidate_states_are_localized(self):
        for text in (
            "1 km içinde puanlamaya uygun düzenli toplu taşıma bulunamadı.",
            "7,5 km içinde puanlamaya uygun tren istasyonu bulunamadı.",
            "Geen regulier openbaar vervoer voor de score gevonden binnen 1 km.",
            "Geen geschikt treinstation voor de score gevonden binnen 7,5 km.",
            "No qualifying scheduled local transit found within 1 km.",
            "No qualifying rail station found within 7.5 km.",
        ):
            self.assertIn(text, self.frontend)

    def test_authoritative_total_is_not_recomputed_from_components(self):
        self.assertIn(
            "transitTotalStr: cat.key === 'transit' ? this.fmtTransitValue(cat.score) : ''",
            self.frontend,
        )
        self.assertIn(
            "scoreStr: cat.key === 'transit' ? this.fmtTransitValue(cat.score) : (cat.key === 'park' ? this.fmtParkValue(cat.score) : (cat.key === 'sport' ? this.fmtSportValue(cat.score) : this.fmtScore(cat.score)))",
            self.frontend,
        )
        self.assertNotIn(
            "transitBreakdown.local_access_points + transitBreakdown.rail_access_points",
            self.frontend,
        )

    def test_legacy_list_is_explicitly_display_only_in_all_languages(self):
        for text in (
            "Haritada gösterilen ulaşım noktaları",
            "Vervoerspunten op de kaart",
            "Transit points shown on map",
            "sayısal skorun hesaplama dökümü değildir",
            "niet de berekening van de numerieke score",
            "not the numeric score breakdown",
        ):
            self.assertIn(text, self.frontend)

    def test_mode_labels_use_existing_reactive_translation_object(self):
        for mapping in (
            "transitModes: { BUS: 'Otobüs', TRAM: 'Tramvay', METRO: 'Metro', LIGHT_RAIL: 'Hafif raylı sistem', RAIL: 'Tren' }",
            "transitModes: { BUS: 'Bus', TRAM: 'Tram', METRO: 'Metro', LIGHT_RAIL: 'Light rail', RAIL: 'Rail' }",
            "transitModes: { BUS: 'Bus', TRAM: 'Tram', METRO: 'Metro', LIGHT_RAIL: 'Lightrail', RAIL: 'Trein' }",
        ):
            self.assertIn(mapping, self.frontend)
        self.assertIn("t.transitModes[key] || mode", self.frontend)

    def test_transit_display_types_are_localized_with_generic_fallback(self):
        for mapping in (
            "bus_stop: 'Otobüs durağı', bus_station: 'Otobüs istasyonu', rail_station: 'Raylı sistem istasyonu', rail_halt: 'Tren durağı', tram_stop: 'Tramvay durağı', transit_platform: 'Toplu taşıma peronu', transit_stop: 'Ulaşım durağı', transit_point: 'Ulaşım noktası'",
            "bus_stop: 'Bushalte', bus_station: 'Busstation', rail_station: 'Railstation', rail_halt: 'Treinhalte', tram_stop: 'Tramhalte', transit_platform: 'OV-perron', transit_stop: 'Vervoershalte', transit_point: 'Vervoerspunt'",
            "bus_stop: 'Bus stop', bus_station: 'Bus station', rail_station: 'Rail station', rail_halt: 'Railway halt', tram_stop: 'Tram stop', transit_platform: 'Transit platform', transit_stop: 'Transit stop', transit_point: 'Transit point'",
        ):
            self.assertIn(mapping, self.frontend)
        self.assertNotIn("metro_station:", self.frontend)
        self.assertIn(
            "t.transitDisplayTypes[it.display_type] || t.transitDisplayTypes.transit_point",
            self.frontend,
        )

    def test_transit_rows_never_render_raw_backend_type(self):
        self.assertIn(
            "? (t.transitDisplayTypes[it.display_type] || t.transitDisplayTypes.transit_point)",
            self.frontend,
        )
        self.assertNotIn(
            "? (t.transitDisplayTypes[it.display_type] || it.type",
            self.frontend,
        )


class SportFrontendContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_collapsed_summary_uses_authoritative_winner(self):
        start = self.frontend.index("sportSummary(breakdown, t)")
        end = self.frontend.index("scoreColor(s)", start)
        summary = self.frontend[start:end]
        self.assertIn("this.sportWinner(breakdown)", summary)
        self.assertIn("winner.distance_m", summary)
        self.assertNotIn("nearest_m", summary)
        self.assertNotIn("category.count", summary)

    def test_two_component_breakdown_uses_backend_points_and_total(self):
        for source in (
            "sportBreakdown.best.points",
            "sportBreakdown.choice.points",
            "sportTotalStr: cat.key === 'sport' ? this.fmtSportValue(cat.score) : ''",
            "sportBestMaxStr: this.fmtSportValue(7.5)",
            "sportChoiceMaxStr: this.fmtSportValue(2.5)",
        ):
            self.assertIn(source, self.frontend)
        self.assertNotIn(
            "sportBreakdown.best.points + sportBreakdown.choice.points",
            self.frontend,
        )
        self.assertNotIn("sportDiversity", self.frontend)

    def test_compact_explanation_exists_in_all_languages(self):
        for text in (
            "Spor skoru, 3 km içindeki en kullanışlı spor seçeneğinin uzaklığına ve yakındaki diğer bağımsız spor seçeneklerine göre hesaplanır.",
            "Haritadaki 1 / 2,5 / 5 km seçimi Spor skorunu değiştirmez.",
            "The Sport score is based on the distance to the most useful sports option within 3 km and other independent sports options nearby.",
            "Changing the 1 / 2.5 / 5 km display radius does not change the Sport score.",
            "De Sportscore is gebaseerd op de afstand tot de meest bruikbare sportoptie binnen 3 km en andere onafhankelijke sportopties in de buurt.",
            "Het wijzigen van de weergavestraal van 1 / 2,5 / 5 km verandert de Sportscore niet.",
        ):
            self.assertIn(text, self.frontend)

    def test_all_facility_classes_are_localized_in_all_languages(self):
        classes = (
            "multi_sport_centre", "general_sports_centre", "sports_hall",
            "fitness_gym", "swimming", "stadium", "standalone_local",
            "specialized", "standalone_specialized",
        )
        for facility_class in classes:
            self.assertEqual(
                self.frontend.count(" " + facility_class + ":"),
                6,
                msg=facility_class,
            )
        for label in (
            "Çok amaçlı spor merkezi", "Multi-purpose sports centre",
            "Multifunctioneel sportcentrum", "İsimsiz yüzme tesisi",
            "Unnamed swimming facility", "Naamloze zwemvoorziening",
        ):
            self.assertIn(label, self.frontend)

    def test_rows_and_map_use_localized_name_and_class_helpers(self):
        for source in (
            "this.sportName(it, t)",
            "this.sportClassLabel(it.facility_class, t)",
            "this.sportName(sportWinner, t)",
            "this.sportClassLabel(sportWinner.facility_class, t)",
        ):
            self.assertIn(source, self.frontend)
        self.assertIn(
            "sportClassLabel(value, t) { return t.sportTypes[value] || t.sportGenericClass; }",
            self.frontend,
        )

    def test_technical_names_use_localized_class_fallback(self):
        helper_start = self.frontend.index("isTechnicalSportName(value)")
        helper_end = self.frontend.index("sportWinner(breakdown)", helper_start)
        helper = self.frontend[helper_start:helper_end]
        self.assertIn("sport(?::osm)?", helper)
        self.assertIn("node|way|relation", helper)
        self.assertIn("t.sportUnnamedTypes[item.facility_class]", helper)
        self.assertIn("t.sportUnnamedGeneric", helper)

    def test_ineligible_items_are_filtered_from_map_and_list(self):
        self.assertIn(
            "category.items.filter(item => item && item.score_eligible === true)",
            self.frontend,
        )
        self.assertGreaterEqual(
            self.frontend.count(
                "cat.key === 'sport' ? this.visibleSportItems(cat) : cat.items"),
            2,
        )
        self.assertIn(
            "const visibleCount = cat.count;",
            self.frontend,
        )
        self.assertIn(
            "d.categories.reduce((s, c) => s + (c.count || 0), 0)",
            self.frontend,
        )

    def test_zero_winner_and_zero_alternative_states_are_localized(self):
        for text in (
            "3 km içinde uygun spor seçeneği bulunamadı",
            "No suitable sports option found within 3 km",
            "Geen geschikte sportoptie gevonden binnen 3 km",
            "Ek puana katkı sağlayan başka spor seçeneği bulunamadı.",
            "No other sports option contributes extra points.",
            "Geen andere sportoptie draagt bij aan extra punten.",
        ):
            self.assertIn(text, self.frontend)

    def test_existing_category_explanations_remain_present(self):
        for text in (
            "The Market score is always calculated using grocery options within 2.5 km of the address.",
            "The School score is always calculated using education options within a 2.5 km radius.",
            "The Health score has three components",
            "The most useful regular scheduled stop within 1 km is evaluated.",
            "The Park score combines the best park option",
        ):
            self.assertIn(text, self.frontend)


class BasemapAttributionContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_active_frontend_uses_exact_stadia_eu_layers(self):
        self.assertIn(
            "https://tiles-eu.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",
            self.frontend,
        )
        self.assertIn(
            "https://tiles-eu.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png",
            self.frontend,
        )
        self.assertIn("maxZoom: 20", self.frontend)
        for stale_value in ("cartocdn.com", "light_all", "dark_all", "CARTO"):
            self.assertNotIn(stale_value, self.frontend)

    def test_stadia_layers_do_not_embed_a_client_api_key(self):
        tile_code = self.frontend[
            self.frontend.index("addTiles() {"):
            self.frontend.index("renderVals() {")
        ]
        for unsafe_value in ("?key=", "api_key", "apiKey", "STADIA_API_KEY"):
            self.assertNotIn(unsafe_value, tile_code)
        self.assertIn("domain authentication in production", tile_code)

    def test_leaflet_attribution_links_all_required_providers(self):
        attribution = (
            '&copy; <a href="https://stadiamaps.com/attribution/" '
            'target="_blank" rel="noopener noreferrer">Stadia Maps</a> &copy; <a '
            'href="https://openmaptiles.org/" target="_blank" rel="noopener noreferrer">OpenMapTiles</a> '
            '&copy; <a href="https://www.openstreetmap.org/copyright" '
            'target="_blank" rel="noopener noreferrer">OpenStreetMap contributors</a>'
        )
        self.assertIn(attribution, self.frontend)
        self.assertIn("L.map(this.mapEl, { zoomControl: true })", self.frontend)

    def test_theme_switch_replaces_only_the_tile_layer(self):
        self.assertIn("if (this.tiles) this.map.removeLayer(this.tiles)",
                      self.frontend)
        self.assertIn("const dark = this.state.theme === 'dark'", self.frontend)
        self.assertIn("this.tiles.addTo(this.map)", self.frontend)
        self.assertIn("this.tiles.bringToBack()", self.frontend)
        self.assertIn("if (this.map && this.tileTheme !== this.state.theme) this.addTiles()",
                      self.frontend)

    def test_tile_failure_is_localized_and_non_blocking(self):
        for message in (
            "Harita arka planı şu anda kullanılamıyor. Skorlar ve sonuç listeleri kullanılmaya devam edebilir.",
            "The map background is currently unavailable. Scores and result lists remain available.",
            "De kaartachtergrond is momenteel niet beschikbaar. Scores en resultaten blijven bruikbaar.",
        ):
            self.assertIn(message, self.frontend)
        self.assertIn("this.tiles.on('tileerror'", self.frontend)
        self.assertIn("if (generation !== this.tileGeneration || this.tileErrorShown) return",
                      self.frontend)
        handler = self.frontend[
            self.frontend.index("this.tiles.on('tileerror'"):
            self.frontend.index("this.tiles.addTo(this.map)")
        ]
        self.assertIn("this.setState({ tileWarning: true })", handler)
        for scoring_state in ("data:", "overall:", "categories:", "status:"):
            self.assertNotIn(scoring_state, handler)

    def test_geoapify_credit_is_linked_near_the_address_input(self):
        address_input = self.frontend.index('aria-autocomplete="list"')
        credit = self.frontend.index("Powered by Geoapify")
        radius_controls = self.frontend.index('value="{{ formError }}"', address_input)
        self.assertLess(address_input, credit)
        self.assertLess(credit, radius_controls)
        self.assertIn(
            '<a href="https://www.geoapify.com/" target="_blank" '
            'rel="noopener noreferrer">Powered by Geoapify</a>',
            self.frontend,
        )

    def test_data_sources_are_localized_and_credit_transit_operators(self):
        for label in ("Veri kaynakları", "Data sources", "Gegevensbronnen"):
            self.assertIn(label, self.frontend)
        for provider in (
            "OpenStreetMap contributors", "Stadia Maps", "OpenMapTiles",
            "Geoapify", "Belgian Mobility", "De Lijn", "STIB/MIVB", "TEC",
            "SNCB/NMBS",
        ):
            self.assertIn(provider, self.frontend)
        for feed_wording in (
            "GTFS ve açık veri akışlarına",
            "GTFS and open-data feeds",
            "GTFS- en opendatafeeds",
        ):
            self.assertIn(feed_wording, self.frontend)
        self.assertNotRegex(self.frontend, r"GTFS.{0,80}20\d{2}")

    def test_sources_modal_and_warning_have_responsive_overflow_guards(self):
        self.assertIn("width: min(520px, calc(100vw - 24px))", self.frontend)
        self.assertIn("overflow-x: hidden", self.frontend)
        self.assertIn(".leaflet-control-attribution { max-width: calc(100vw - 24px)",
                      self.frontend)
        self.assertIn("The map background is loaded from the third-party Stadia Maps service.",
                      self.frontend)


class NearbyAccessSemanticsContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_nearby_access_names_and_subtitles_exist_in_all_languages(self):
        for text in (
            "Yakın Çevre Erişim Skoru",
            "Yakındaki günlük ihtiyaçlara ve hizmetlere erişim",
            "Nearby Access Score",
            "Access to nearby everyday amenities and services",
            "Nabijheids- en voorzieningenscore",
            "Toegang tot dagelijkse voorzieningen en diensten in de buurt",
        ):
            self.assertIn(text, self.frontend)

    def test_liveability_claims_and_qualitative_bands_are_removed(self):
        for text in (
            "Bu adres, yaşamak için ne kadar uygun?",
            "Genel yaşanabilirlik",
            "How liveable is this address?",
            "Overall liveability",
            "Hoe leefbaar is dit adres?",
            "Algemene leefbaarheid",
            "An excellent location for daily life.",
            "Günlük yaşam için mükemmel bir konum.",
            "Een uitstekende locatie voor het dagelijks leven.",
            "overallComment",
            "const ci = overall",
        ):
            self.assertNotIn(text, self.frontend)

    def test_scope_limitations_and_rural_clarity_exist_in_all_languages(self):
        for text in (
            "Düşük bir skor, konumun yaşamak için kötü olduğu anlamına gelmez.",
            "Bu skor güvenliği, konut maliyetini, gürültü veya kirliliği, hizmet kalitesini ya da gerçek araç yolculuk süresini ölçmez.",
            "araçla erişilebilen daha uzaktaki hizmetler skora sınırlı veya hiç katkı sağlamayabilir.",
            "A low score does not mean the location is a bad place to live.",
            "This score does not measure safety, housing cost, noise or pollution, service quality, or actual driving time.",
            "services that are practical to reach by car but farther away may contribute little or nothing.",
            "Een lage score betekent niet dat de locatie een slechte plek is om te wonen.",
            "Deze score meet geen veiligheid, woonkosten, geluid of vervuiling, kwaliteit van dienstverlening of werkelijke reistijd met de auto.",
            "diensten die met de auto praktisch bereikbaar zijn maar verder weg liggen, tellen mogelijk weinig of niet mee.",
        ):
            self.assertIn(text, self.frontend)

    def test_distance_copy_is_honest_in_all_languages(self):
        for text in (
            "Tahmini mesafe ve süre",
            "Tahminler kuş uçuşu mesafeden türetilir; gerçek bir rota planlaması değildir.",
            "Estimated distance and time",
            "Estimates are derived from straight-line distance and are not route planning.",
            "Geschatte afstand en reistijd",
            "De schattingen zijn afgeleid van de hemelsbrede afstand en zijn geen routeplanning.",
        ):
            self.assertIn(text, self.frontend)
        for text in ("Gerçek mesafeler", "Real distances", "Echte afstanden"):
            self.assertNotIn(text, self.frontend)

    def test_zero_score_wording_is_scoped_and_safe(self):
        for text in (
            "Yakın değerlendirme yarıçapında skora katkı sağlayan uygun seçenek bulunamadı.",
            "No qualifying option that contributes to the score was found within the scoring radius.",
            "Geen geschikte optie die aan de score bijdraagt gevonden binnen de beoordelingsradius.",
        ):
            self.assertIn(text, self.frontend)
        self.assertIn("isZeroScore: Number(cat.score) === 0", self.frontend)
        self.assertIn("hasPositiveScore: Number(cat.score) !== 0", self.frontend)

    def test_general_radius_note_preserves_fixed_category_scoring(self):
        for text in (
            "Kategori skorları kendi sabit değerlendirme mesafelerini kullanır ve değişmez.",
            "Category scores use their own fixed evaluation distances and do not change.",
            "Categoriescores gebruiken hun eigen vaste beoordelingsafstanden en veranderen niet.",
        ):
            self.assertIn(text, self.frontend)
        self.assertIn("resultRadiusOpts", self.frontend)

    def test_existing_overall_weights_and_numeric_bindings_are_unchanged(self):
        self.assertEqual(app.OVERALL_WEIGHTS, {
            "market": 0.25,
            "school": 0.25,
            "health": 0.20,
            "transit": 0.15,
            "park": 0.10,
            "sport": 0.05,
        })
        self.assertIn(
            "overallStr: d ? this.fmtScore(overall) : ''",
            self.frontend,
        )
        self.assertIn("scoreStr: cat.key === 'transit'", self.frontend)

    def test_leuven_city_hall_example_and_coordinates_match(self):
        self.assertNotIn("Tervuursesteenweg", self.frontend)
        for address in (
            "Grote Markt 9, 3000 Leuven, Belçika",
            "Grote Markt 9, 3000 Leuven, Belgium",
            "Grote Markt 9, 3000 Leuven, België",
        ):
            self.assertIn(address, self.frontend)
        self.assertEqual(self.frontend.count("50.87871"), 4)
        self.assertEqual(self.frontend.count("4.70143"), 4)
        self.assertIn("DEF_ADDR() { return 'Grote Markt 9, 3000 Leuven, België'; }",
                      self.frontend)

    def test_autocomplete_result_types_are_reactively_localized(self):
        for mapping in (
            "building: 'Bina'", "building: 'Building'", "building: 'Gebouw'",
            "street: 'Sokak'", "street: 'Street'", "street: 'Straat'",
            "city: 'Şehir'", "city: 'City'", "city: 'Stad'",
            "amenity: 'Hizmet noktası'", "amenity: 'Amenity'",
            "amenity: 'Voorziening'",
        ):
            self.assertIn(mapping, self.frontend)
        self.assertIn(
            "resultType: this.addressResultType(suggestion.result_type, t)",
            self.frontend,
        )
        self.assertIn(
            "t.addressTypes[raw] || raw.replace(/_/g, ' ')",
            self.frontend,
        )

    def test_all_six_calculation_explanations_use_one_popover_pattern(self):
        for category in ("market", "school", "health", "transit", "park", "sport"):
            self.assertIn(
                'data-calculation-popover="' + category + '"',
                self.frontend,
            )
        self.assertEqual(
            self.frontend.count('class="calculation-popover" role="dialog"'),
            6,
        )
        self.assertEqual(
            self.frontend.count('aria-expanded="{{ cat.infoOpen }}"'),
            6,
        )
        self.assertEqual(
            self.frontend.count('aria-controls="{{ cat.calculationPopoverId }}"'),
            6,
        )
        self.assertIn("position: absolute", self.frontend)
        self.assertIn("position: fixed", self.frontend)

    def test_only_one_calculation_popover_can_be_open(self):
        self.assertIn("calculationPopover: null", self.frontend)
        self.assertIn("infoOpen: S.calculationPopover === cat.key", self.frontend)
        self.assertIn(
            "calculationPopover: S.calculationPopover === cat.key ? null : cat.key",
            self.frontend,
        )
        for legacy_state in (
            "marketInfoOpen", "schoolInfoOpen", "healthInfoOpen",
            "transitInfoOpen", "parkInfoOpen", "sportInfoOpen",
        ):
            self.assertNotIn(legacy_state, self.frontend)

    def test_outside_click_and_escape_close_calculation_popover(self):
        self.assertIn(
            "document.addEventListener('pointerdown', this.onDocumentPointerDown)",
            self.frontend,
        )
        self.assertIn(
            "target.closest('[data-calculation-popover]')",
            self.frontend,
        )
        self.assertIn("event.key === 'Escape'", self.frontend)
        self.assertIn(
            "document.addEventListener('keydown', this.onDocumentKeyDown)",
            self.frontend,
        )
        self.assertIn(
            "document.removeEventListener('pointerdown', this.onDocumentPointerDown)",
            self.frontend,
        )


if __name__ == "__main__":
    unittest.main()

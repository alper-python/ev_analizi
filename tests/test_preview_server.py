from pathlib import Path
import sys
import unittest
from unittest.mock import patch


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
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


class RadiusReanalysisApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.demo_dir, cls.demo_nodes, cls.demo_polys = server._create_demo_caches()
        cls.patches = [
            patch.object(server, "NODES_PATH", cls.demo_nodes),
            patch.object(server, "POLYS_PATH", cls.demo_polys),
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
            ": (cat.key === 'health' ? (t.healthTypes[it.type] || it.type || '') : (it.type || ''))",
            self.frontend,
        )


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
            "cat.key === 'health' ? (t.healthTypes[it.type] || it.type || '')",
            self.frontend,
        )
        self.assertIn(
            "textTransform: (cat.key === 'school' || cat.key === 'health') ? 'none' : 'capitalize'",
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


if __name__ == "__main__":
    unittest.main()

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


READY_RUNTIME = {
    "ready": True,
    "components": {
        name: {"ready": True, "status": "ok", "assets": {}}
        for name in ("poi", "transit", "park", "sport")
    },
}


class SecurityHeaderTests(unittest.TestCase):
    def setUp(self):
        server.limiter.reset()
        self.client = server.app.test_client()

    def assert_basic_headers(self, response):
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(
            response.headers["Referrer-Policy"],
            "strict-origin-when-cross-origin",
        )
        self.assertEqual(
            response.headers["Permissions-Policy"],
            "geolocation=(), camera=(), microphone=()",
        )

    def test_root_response_has_basic_headers(self):
        response = self.client.get("/")
        self.addCleanup(response.close)
        self.assertEqual(response.status_code, 200)
        self.assert_basic_headers(response)

    def test_render_forwarded_https_adds_hsts(self):
        response = self.client.get(
            "/", headers={"X-Forwarded-Proto": "https"})
        self.addCleanup(response.close)
        self.assert_basic_headers(response)
        self.assertEqual(
            response.headers["Strict-Transport-Security"],
            "max-age=15552000",
        )

    def test_local_http_does_not_add_hsts(self):
        response = self.client.get("/", base_url="http://localhost")
        self.addCleanup(response.close)
        self.assert_basic_headers(response)
        self.assertNotIn("Strict-Transport-Security", response.headers)


class ClientRateLimitKeyTests(unittest.TestCase):
    def test_valid_cf_connecting_ip_is_selected_and_normalized(self):
        with server.app.test_request_context(
                "/", headers={"CF-Connecting-IP": "2001:0db8::1"},
                environ_base={"REMOTE_ADDR": "127.0.0.1"}):
            self.assertEqual(server._client_rate_limit_key(), "2001:db8::1")

    def test_malformed_cf_connecting_ip_falls_back_to_remote_address(self):
        with server.app.test_request_context(
                "/", headers={"CF-Connecting-IP": "not-an-ip"},
                environ_base={"REMOTE_ADDR": "198.51.100.20"}):
            self.assertEqual(
                server._client_rate_limit_key(), "198.51.100.20")

    def test_missing_header_uses_remote_address(self):
        with server.app.test_request_context(
                "/", environ_base={"REMOTE_ADDR": "203.0.113.8"}):
            self.assertEqual(server._client_rate_limit_key(), "203.0.113.8")

    def test_missing_or_invalid_candidates_use_stable_fallback(self):
        with server.app.test_request_context(
                "/", environ_base={"REMOTE_ADDR": "invalid"}):
            self.assertEqual(server._client_rate_limit_key(), "unknown-client")


class EndpointRateLimitTests(unittest.TestCase):
    def setUp(self):
        server.limiter.reset()
        self.client = server.app.test_client()

    def tearDown(self):
        server.limiter.reset()

    def assert_rate_limited(self, response):
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(response.get_json(), {
            "error": {
                "code": "rate_limited",
                "message": "Too many requests. Please try again shortly.",
            }
        })
        self.assertNotIn("<html", response.get_data(as_text=True).lower())
        self.assertIn("Retry-After", response.headers)

    def test_health_is_not_rate_limited(self):
        with patch.object(
                server, "_runtime_readiness", return_value=READY_RUNTIME):
            responses = [self.client.get("/api/health") for _ in range(70)]
        self.assertTrue(all(response.status_code == 200 for response in responses))

    def test_analyze_allows_thirty_then_returns_json_429(self):
        with patch.object(
                server, "_analyze_api_response",
                side_effect=lambda: server.jsonify({"ok": True})):
            responses = [self.client.post("/api/analyze") for _ in range(30)]
            limited = self.client.post("/api/analyze")
        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assert_rate_limited(limited)

    def test_autocomplete_allows_sixty_then_returns_json_429(self):
        responses = [
            self.client.get("/api/address-suggestions?q=ab&lang=en")
            for _ in range(60)
        ]
        limited = self.client.get(
            "/api/address-suggestions?q=ab&lang=en")
        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assert_rate_limited(limited)


class SecurityFrontendContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frontend = (SOURCE_DIR / "static" / "index.html").read_text(
            encoding="utf-8")

    def test_rate_limit_messages_exist_in_all_languages(self):
        for message in (
            "Çok fazla istek gönderildi. Lütfen kısa bir süre sonra tekrar deneyin.",
            "Too many requests. Please try again shortly.",
            "Er zijn te veel verzoeken verzonden. Probeer het over een korte tijd opnieuw.",
        ):
            self.assertIn(message, self.frontend)

    def test_analyze_and_autocomplete_handle_rate_limits_separately(self):
        self.assertGreaterEqual(self.frontend.count("status === 429"), 2)
        self.assertGreaterEqual(
            self.frontend.count("j.error.code === 'rate_limited'"), 2)
        self.assertIn("rateLimited: [t.errRateLimitedT, t.errRateLimitedD]",
                      self.frontend)
        self.assertIn("rateLimited ? t.suggestRateLimited", self.frontend)


if __name__ == "__main__":
    unittest.main()

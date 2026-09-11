import math
from pathlib import Path
import sys
import unittest


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import transit_scoring as transit
finally:
    sys.path.pop(0)


def local(identifier="local", distance=0.0, service=90.0, **values):
    row = {
        "logical_stop_id": identifier,
        "name": identifier,
        "distance_m": distance,
        "seven_day_average": service,
        "weekday_departures": service,
        "saturday_departures": service,
        "sunday_departures": service,
        "operators": ["Operator"],
        "modes": ["BUS"],
    }
    row.update(values)
    return row


def rail(identifier="rail", distance=0.0, service=350.0, **values):
    row = {
        "logical_station_id": identifier,
        "name": identifier,
        "uic_code": identifier,
        "distance_m": distance,
        "seven_day_average": service,
        "weekday_departures": service,
        "saturday_departures": service,
        "sunday_departures": service,
    }
    row.update(values)
    return row


class LocalMathTests(unittest.TestCase):
    def test_service_factor_boundaries(self):
        self.assertEqual(transit.local_service_factor(0), 0.0)
        self.assertEqual(transit.local_service_factor(45), 0.5)
        self.assertEqual(transit.local_service_factor(90), 1.0)
        self.assertEqual(transit.local_service_factor(900), 1.0)

    def test_local_distance_factor_calibration_values(self):
        expected = {
            0: 1.0,
            100: 1.0,
            200: 1.0,
            250: 1.0,
            300: 14 / 15,
            400: 0.8,
            500: 2 / 3,
            600: 8 / 15,
            750: 1 / 3,
            900: 2 / 15,
            1000: 0.0,
            1001: 0.0,
        }
        for distance, factor in expected.items():
            with self.subTest(distance=distance):
                self.assertAlmostEqual(
                    transit.local_distance_factor(distance), factor)

    def test_local_distance_factor_exact_boundaries(self):
        self.assertEqual(transit.local_distance_factor(249.999), 1.0)
        self.assertEqual(transit.local_distance_factor(250), 1.0)
        self.assertLess(transit.local_distance_factor(250.001), 1.0)
        self.assertGreater(transit.local_distance_factor(999.999), 0.0)
        self.assertEqual(transit.local_distance_factor(1000), 0.0)
        self.assertEqual(transit.local_distance_factor(1000.001), 0.0)

    def test_utility_multiplies_service_and_distance(self):
        self.assertAlmostEqual(transit.local_utility(45, 500), 1 / 3)

    def test_best_candidate_uses_utility_not_distance(self):
        winner = transit.select_best_local([
            local("near-weak", 100, 9), local("far-useful", 400, 90)])
        self.assertEqual(winner["logical_stop_id"], "far-useful")

    def test_tie_breaks_by_distance_then_service_then_id(self):
        winner = transit.select_best_local([
            local("far", 625, 90), local("near", 0, 45)])
        self.assertEqual(winner["logical_stop_id"], "near")
        stable = transit.select_best_local([
            local("z", 100, 50), local("a", 100, 50)])
        self.assertEqual(stable["logical_stop_id"], "a")

    def test_no_candidate_and_local_max(self):
        self.assertIsNone(transit.select_best_local([]))
        result = transit.transit_score_components([local()], [])
        self.assertEqual(result["local_access_points"], 6.0)

    def test_boundary_tolerance_does_not_admit_a_meaningfully_outside_stop(self):
        boundary = transit.select_best_local([
            local("boundary", 1000.0 + 0.5e-6, 90)])
        self.assertEqual(boundary["logical_stop_id"], "boundary")
        self.assertEqual(boundary["utility"], 0.0)
        self.assertIsNone(transit.select_best_local([
            local("outside", 1000.001, 90)]))


class RailMathTests(unittest.TestCase):
    def test_service_factor_formula_and_cap(self):
        self.assertEqual(transit.rail_service_factor(0), 0.0)
        self.assertAlmostEqual(
            transit.rail_service_factor(129.29),
            math.log1p(129.29) / math.log(351))
        self.assertEqual(transit.rail_service_factor(350), 1.0)
        self.assertEqual(transit.rail_service_factor(1000), 1.0)

    def test_distance_factor_boundaries(self):
        self.assertEqual(transit.distance_factor(0, 7500), 1.0)
        self.assertEqual(transit.distance_factor(3750, 7500), 0.5)
        self.assertEqual(transit.distance_factor(7500, 7500), 0.0)
        self.assertEqual(transit.distance_factor(7501, 7500), 0.0)

    def test_utility_multiplies_service_and_distance(self):
        expected = transit.rail_service_factor(100) * 0.5
        self.assertAlmostEqual(transit.rail_utility(100, 3750), expected)

    def test_best_station_uses_utility_not_distance(self):
        winner = transit.select_best_rail([
            rail("near-weak", 500, 5), rail("far-strong", 2500, 350)])
        self.assertEqual(winner["logical_station_id"], "far-strong")

    def test_very_close_weaker_station_can_win(self):
        winner = transit.select_best_rail([
            rail("near", 0, 100), rail("far", 6500, 350)])
        self.assertEqual(winner["logical_station_id"], "near")

    def test_tie_break_and_rail_max(self):
        stable = transit.select_best_rail([
            rail("z", 100, 50), rail("a", 100, 50)])
        self.assertEqual(stable["logical_station_id"], "a")
        result = transit.transit_score_components([], [rail()])
        self.assertEqual(result["rail_access_points"], 4.0)

    def test_no_station(self):
        self.assertIsNone(transit.select_best_rail([]))


class TotalMathTests(unittest.TestCase):
    def test_components_add_and_clamp(self):
        result = transit.transit_score_components([local()], [rail()])
        self.assertEqual(result["local_access_points"], 6.0)
        self.assertEqual(result["rail_access_points"], 4.0)
        self.assertEqual(result["score"], 10.0)

    def test_no_candidates_return_zero_and_null_metadata(self):
        result = transit.transit_score_components([], [])
        self.assertEqual(result["score"], 0.0)
        self.assertEqual(result["public_score"], 0.0)
        self.assertIsNone(result["best_local_logical_stop_id"])
        self.assertIsNone(result["best_rail_logical_station_id"])

    def test_final_score_rounds_once_and_retains_full_precision(self):
        result = transit.transit_score_components(
            [local(distance=333.333, service=57.777)],
            [rail(distance=2345.678, service=123.456)])
        expected = (6 * transit.local_utility(57.777, 333.333)
                    + 4 * transit.rail_utility(123.456, 2345.678))
        self.assertEqual(result["score"], expected)
        self.assertEqual(result["public_score"], round(expected, 1))
        self.assertNotEqual(result["score"], result["public_score"])

    def test_explicit_scoring_radii_are_reported(self):
        result = transit.transit_score_components([], [])
        self.assertEqual(result["local_scoring_radius_m"], 1000)
        self.assertEqual(result["rail_scoring_radius_m"], 7500)


if __name__ == "__main__":
    unittest.main()

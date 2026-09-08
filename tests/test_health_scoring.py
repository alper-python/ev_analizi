import itertools
import math
from pathlib import Path
import sys
import unittest


SOURCE_DIR = Path(__file__).resolve().parents[1] / "belgium-location"
sys.path.insert(0, str(SOURCE_DIR))
try:
    import health_scoring as health
finally:
    sys.path.pop(0)


def poi(distance=0, name="Health POI", amenity=None, healthcare=None,
        source="node", lat=50.0, lon=4.0):
    return {
        "name": name,
        "brand": None,
        "lat": lat,
        "lon": lon,
        "amenity": amenity,
        "healthcare": healthcare,
        "source": source,
        "d_lin": float(distance),
    }


def moved(row, metres):
    result = dict(row)
    result["lat"] = float(row["lat"]) + math.degrees(metres / 6371000.0)
    return result


class HealthClassificationAndScoreTests(unittest.TestCase):
    def components(self, local=(), hospitals=()):
        return health.health_score_components(local, hospitals)

    def test_empty_population_scores_zero(self):
        self.assertEqual(self.components()["score"], 0.0)

    def test_one_clinical_poi_at_zero(self):
        result = self.components([poi(0, amenity="doctors")])
        self.assertEqual(result["clinical_proximity_points"], 5.0)
        self.assertEqual(result["choice_points"], 1.0)
        self.assertEqual(result["score"], 6.0)

    def test_one_clinical_poi_at_300m(self):
        factor = 1 - 300 / 2500
        result = self.components([poi(300, healthcare="doctor")])
        self.assertAlmostEqual(result["clinical_proximity_points"], 5 * factor)
        self.assertAlmostEqual(result["choice_points"], factor)

    def test_one_pharmacy_only_contributes_weighted_choice(self):
        result = self.components([poi(0, amenity="pharmacy")])
        self.assertEqual(result["clinical_proximity_points"], 0.0)
        self.assertEqual(result["choice_points"], 0.4)
        self.assertIsNone(result["nearest_clinical_m"])

    def test_clinical_and_pharmacy_choice_is_distance_weighted(self):
        result = self.components([
            poi(500, name="Doctor", amenity="doctors"),
            poi(1000, name="Pharmacy", healthcare="pharmacy"),
        ])
        self.assertAlmostEqual(result["effective_choice"], 0.8 + 0.4 * 0.6)

    def test_three_pharmacies_do_not_equal_three_clinical_facilities(self):
        pharmacies = [poi(0, name=str(i), amenity="pharmacy") for i in range(3)]
        clinical = [poi(0, name=str(i), amenity="doctors") for i in range(3)]
        self.assertAlmostEqual(self.components(pharmacies)["choice_points"], 1.2)
        self.assertEqual(self.components(clinical)["choice_points"], 3.0)

    def test_non_scoring_healthcare_types_contribute_zero(self):
        for amenity, healthcare in (
                ("dentist", "dentist"), (None, "physiotherapist"),
                (None, "psychotherapist"), (None, "laboratory"),
                (None, "blood_donation"), (None, "unknown")):
            with self.subTest(amenity=amenity, healthcare=healthcare):
                self.assertEqual(
                    self.components([poi(0, amenity=amenity,
                                         healthcare=healthcare)])["score"], 0.0)

    def test_doctors_and_counselling_is_clinical(self):
        row = poi(0, amenity="doctors", healthcare="counselling")
        self.assertEqual(health.health_local_class(row), "clinical_care")

    def test_doctors_and_pharmacy_is_pharmacy(self):
        row = poi(0, amenity="doctors", healthcare="pharmacy")
        self.assertEqual(health.health_local_class(row), "pharmacy")
        self.assertEqual(self.components([row])["clinical_proximity_points"], 0.0)

    def test_pharmacy_and_doctor_is_pharmacy(self):
        row = poi(0, amenity="pharmacy", healthcare="doctor")
        self.assertEqual(health.health_local_class(row), "pharmacy")

    def test_clinic_and_hospital_is_not_local(self):
        row = poi(0, amenity="clinic", healthcare="hospital")
        self.assertIsNone(health.health_local_class(row))
        self.assertTrue(health.qualifies_hospital(row))

    def test_hospital_and_rehabilitation_is_neither_local_nor_qualifying(self):
        row = poi(0, amenity="hospital", healthcare="rehabilitation")
        self.assertIsNone(health.health_local_class(row))
        self.assertFalse(health.qualifies_hospital(row))

    def test_clinic_and_non_core_healthcare_is_clinical(self):
        row = poi(0, amenity="clinic", healthcare="physiotherapist")
        self.assertEqual(health.health_local_class(row), "clinical_care")

    def test_healthcare_doctor_without_amenity_is_clinical(self):
        self.assertEqual(health.health_local_class(
            poi(0, healthcare="doctor")), "clinical_care")

    def test_healthcare_clinic_without_amenity_is_clinical(self):
        self.assertEqual(health.health_local_class(
            poi(0, healthcare="clinic")), "clinical_care")

    def test_explicit_rehabilitation_hospital_does_not_qualify(self):
        hospital = poi(1000, amenity="hospital", healthcare="rehabilitation")
        result = self.components([], [hospital])
        self.assertFalse(health.qualifies_hospital(hospital))
        self.assertEqual(result["hospital_points"], 0.0)

    def test_healthcare_hospital_qualifies(self):
        hospital = poi(1000, amenity="clinic", healthcare="hospital")
        self.assertTrue(health.qualifies_hospital(hospital))
        self.assertAlmostEqual(
            self.components([], [hospital])["hospital_points"], 2 * 0.95)

    def test_amenity_hospital_with_missing_healthcare_qualifies(self):
        for value in (None, "", "   "):
            with self.subTest(value=value):
                hospital = poi(1000, amenity="hospital", healthcare=value)
                self.assertTrue(health.qualifies_hospital(hospital))

    def test_local_radius_boundaries(self):
        cases = ((2499, 1, 2499.0), (2500, 1, 2500.0), (2501, 0, None))
        for distance, count, nearest in cases:
            with self.subTest(distance=distance):
                result = self.components([poi(distance, amenity="doctors")])
                self.assertEqual(result["clinical_count"], count)
                self.assertEqual(result["nearest_clinical_m"], nearest)
        self.assertGreater(self.components([poi(2499, amenity="doctors")])["score"], 0)
        self.assertEqual(self.components([poi(2500, amenity="doctors")])["score"], 0)
        self.assertEqual(self.components([poi(2501, amenity="doctors")])["score"], 0)

    def test_hospital_radius_boundaries(self):
        expected = ((19999, 19999.0, 0.0001), (20000, 20000.0, 0.0),
                    (20001, None, 0.0))
        for distance, nearest, points in expected:
            with self.subTest(distance=distance):
                result = self.components([], [poi(
                    distance, amenity="hospital", healthcare="hospital")])
                self.assertEqual(result["nearest_hospital_m"], nearest)
                self.assertAlmostEqual(result["hospital_points"], points)

    def test_nearest_pharmacy_cannot_replace_nearest_clinical(self):
        result = self.components([
            poi(10, name="Pharmacy", amenity="pharmacy"),
            poi(900, name="Doctor", amenity="doctors"),
        ])
        self.assertEqual(result["nearest_clinical_m"], 900)
        self.assertAlmostEqual(result["clinical_proximity_points"], 5 * (1 - 900 / 2500))

    def test_hospital_count_does_not_increase_hospital_points(self):
        nearest = poi(5000, name="Nearest", healthcare="hospital")
        farther = poi(10000, name="Farther", healthcare="hospital")
        one = self.components([], [nearest])["hospital_points"]
        many = self.components([], [nearest, farther, dict(farther)])["hospital_points"]
        self.assertEqual(one, many)

    def test_score_is_clamped_to_ten(self):
        local = [poi(0, name=str(i), amenity="doctors") for i in range(20)]
        hospital = [poi(0, amenity="hospital", healthcare="hospital")]
        self.assertEqual(health.calc_health_score(local, hospital), 10.0)


class HealthDeduplicationTests(unittest.TestCase):
    def test_named_clinical_node_polygon_duplicate_keeps_closest(self):
        node = poi(100, name="Medical Centre", amenity="doctors", source="node")
        polygon = moved(poi(80, name="medical centre", healthcare="doctor",
                            source="polygon"), 30)
        result = health.deduplicate_health_local_pois([node, polygon])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source"], "polygon")

    def test_doctor_and_clinic_same_name_are_compatible_named_duplicates(self):
        doctor = poi(100, name="Central Care", amenity="doctors")
        clinic = moved(poi(90, name="central care", amenity="clinic",
                           source="polygon"), 20)
        self.assertEqual(len(health.deduplicate_health_local_pois([doctor, clinic])), 1)

    def test_pharmacy_duplicate_merges(self):
        first = poi(100, name=" Élan   Apotheek ", amenity="pharmacy")
        second = moved(poi(90, name="élan apotheek", healthcare="pharmacy",
                           source="polygon"), 30)
        self.assertEqual(len(health.deduplicate_health_local_pois([first, second])), 1)

    def test_different_names_remain_separate(self):
        rows = [poi(100, name="Alpha", amenity="doctors"),
                moved(poi(90, name="Beta", amenity="clinic"), 5)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_named_same_name_beyond_75m_remains_separate(self):
        rows = [poi(100, name="Alpha", amenity="doctors"),
                moved(poi(90, name="alpha", amenity="clinic"), 76)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_named_exactly_75m_merges(self):
        rows = [poi(100, name="Alpha", amenity="doctors"),
                moved(poi(90, name="alpha", amenity="clinic"), 75)]
        self.assertAlmostEqual(health.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 75)
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 1)

    def test_named_just_over_75m_remains_separate(self):
        rows = [poi(100, name="Alpha", amenity="doctors"),
                moved(poi(90, name="alpha", amenity="clinic"), 75.001)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_named_node_node_duplicate_merges(self):
        rows = [poi(100, name="Alpha", amenity="doctors", source="node"),
                moved(poi(90, name="alpha", amenity="clinic", source="node"), 5)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 1)

    def test_named_polygon_polygon_duplicate_merges(self):
        rows = [poi(100, name="Alpha", amenity="doctors", source="polygon"),
                moved(poi(90, name="alpha", amenity="clinic",
                          source="polygon"), 5)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 1)

    def test_unnamed_same_subtype_within_10m_merges(self):
        rows = [poi(100, name=None, amenity="doctors"),
                moved(poi(90, name="", healthcare="doctor"), 9)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 1)

    def test_unnamed_over_10m_remains_separate(self):
        rows = [poi(100, name=None, amenity="pharmacy"),
                moved(poi(90, name="", healthcare="pharmacy"), 11)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_unnamed_exactly_10m_merges(self):
        rows = [poi(100, name=None, amenity="pharmacy"),
                moved(poi(90, name="", healthcare="pharmacy"), 10)]
        self.assertAlmostEqual(health.haversine_m(
            rows[0]["lat"], rows[0]["lon"], rows[1]["lat"], rows[1]["lon"]), 10)
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 1)

    def test_unnamed_just_over_10m_remains_separate(self):
        rows = [poi(100, name=None, amenity="pharmacy"),
                moved(poi(90, name="", healthcare="pharmacy"), 10.001)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_unnamed_neighboring_bucket_boundary_merges(self):
        base = poi(100, name=None, amenity="pharmacy")
        cell = health._spatial_cell(base, health.HEALTH_UNNAMED_DEDUP_DISTANCE_M)
        step = 0.01
        offset = 0.0
        first = base
        while offset < 30:
            candidate = moved(base, offset)
            if health._spatial_cell(candidate, health.HEALTH_UNNAMED_DEDUP_DISTANCE_M) != cell:
                first = moved(base, max(0.0, offset - 0.1))
                break
            offset += step
        second = moved(first, 9.0)
        self.assertNotEqual(
            health._spatial_cell(first, health.HEALTH_UNNAMED_DEDUP_DISTANCE_M),
            health._spatial_cell(second, health.HEALTH_UNNAMED_DEDUP_DISTANCE_M))
        second["d_lin"] = 90
        self.assertEqual(len(health.deduplicate_health_local_pois([first, second])), 1)

    def test_unnamed_different_subtypes_remain_separate(self):
        rows = [poi(100, name=None, amenity="doctors"),
                moved(poi(90, name="", amenity="clinic"), 5)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_complete_link_avoids_transitive_chain_merge(self):
        rows = [poi(30, name="Chain", amenity="doctors"),
                moved(poi(20, name="chain", amenity="clinic"), 60),
                moved(poi(10, name="CHAIN", amenity="doctors"), 120)]
        self.assertEqual(len(health.deduplicate_health_local_pois(rows)), 2)

    def test_deduplication_is_deterministic(self):
        rows = [poi(30, name="Alpha", amenity="doctors", source="node"),
                moved(poi(20, name="alpha", amenity="clinic",
                          source="polygon"), 20),
                moved(poi(10, name="ALPHA", healthcare="doctor"), 100)]
        expected = health.deduplicate_health_local_pois(rows)
        for ordering in itertools.permutations(rows):
            self.assertEqual(health.deduplicate_health_local_pois(ordering), expected)

    def test_non_scoring_types_are_removed_from_local_population(self):
        rows = [poi(0, amenity="dentist"), poi(0, healthcare="physiotherapist")]
        self.assertEqual(health.deduplicate_health_local_pois(rows), [])

    def test_duplicate_affects_proximity_and_choice_once_using_closest_row(self):
        farther = poi(500, name="Central Care", amenity="doctors", source="node")
        closer = moved(poi(300, name="central care", amenity="clinic",
                           source="polygon"), 30)
        rows = health.deduplicate_health_local_pois([farther, closer])
        components = health.health_score_components(rows, [])
        factor = 1 - 300 / 2500
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["d_lin"], 300)
        self.assertAlmostEqual(components["clinical_proximity_points"], 5 * factor)
        self.assertAlmostEqual(components["choice_points"], factor)


if __name__ == "__main__":
    unittest.main()

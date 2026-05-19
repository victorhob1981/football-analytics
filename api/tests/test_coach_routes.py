from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class CoachRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.coaches.db_client.fetch_all")
    def test_coaches_list_returns_rows_pagination_and_context(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "coach_id": 37634192,
                "coach_name": "Filipe Luís",
                "photo_url": "https://cdn.sportmonks.com/images/soccer/coaches/10/37634192.png",
                "has_real_photo": True,
                "tenure_count": 1,
                "active_tenures": 1,
                "matches": 18,
                "wins": 12,
                "draws": 4,
                "losses": 2,
                "points": 40,
                "adjusted_ppm": 2.1543,
                "points_per_match": 2.2222,
                "last_match_date": "2026-03-29",
                "team_id": 1024,
                "team_name": "Flamengo",
                "active": True,
                "temporary": False,
                "start_date": "2026-01-10",
                "end_date": None,
                "league_id": 648,
                "league_name": "Serie A",
                "season": 2025,
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/coaches?competitionId=71&seasonId=2025")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        item = payload["data"]["items"][0]

        self.assertEqual(item["coachId"], "37634192")
        self.assertEqual(item["coachName"], "Filipe Luís")
        self.assertEqual(item["photoUrl"], "https://cdn.sportmonks.com/images/soccer/coaches/10/37634192.png")
        self.assertTrue(item["hasRealPhoto"])
        self.assertEqual(item["teamId"], "1024")
        self.assertEqual(item["matches"], 18)
        self.assertEqual(item["adjustedPpm"], 2.1543)
        self.assertEqual(item["pointsPerMatch"], 2.2222)
        self.assertEqual(item["context"]["competitionId"], "71")
        self.assertEqual(item["context"]["competitionKey"], "brasileirao_a")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")
        self.assertEqual(payload["meta"]["pagination"]["totalCount"], 1)

    @patch("api.src.routers.coaches.db_client.fetch_all")
    def test_coach_profile_returns_summary_tenures_and_section_coverage(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "coach_id": 37634192,
                "coach_name": "Filipe Luís",
                "photo_url": "https://cdn.sportmonks.com/images/soccer/coaches/10/37634192.png",
                "has_real_photo": True,
                "tenure_count": 2,
                "active_tenures": 1,
                "teams_count": 2,
                "total_matches": 25,
                "total_wins": 16,
                "total_draws": 5,
                "total_losses": 4,
                "total_points": 53,
                "total_adjusted_ppm": 2.0445,
                "total_points_per_match": 2.12,
                "total_last_match_date": "2026-03-29",
                "current_team_id": 1024,
                "current_team_name": "Flamengo",
                "current_active": True,
                "current_temporary": False,
                "current_start_date": "2026-01-10",
                "current_end_date": None,
                "coach_tenure_id": 104902,
                "team_id": 1024,
                "team_name": "Flamengo",
                "active": True,
                "temporary": False,
                "start_date": "2026-01-10",
                "end_date": None,
                "matches": 18,
                "wins": 12,
                "draws": 4,
                "losses": 2,
                "points": 40,
                "points_per_match": 2.2222,
                "last_match_date": "2026-03-29",
                "league_id": 648,
                "league_name": "Serie A",
                "season": 2025,
            },
            {
                "coach_id": 37634192,
                "coach_name": "Filipe Luís",
                "photo_url": "https://cdn.sportmonks.com/images/soccer/coaches/10/37634192.png",
                "has_real_photo": True,
                "tenure_count": 2,
                "active_tenures": 1,
                "teams_count": 2,
                "total_matches": 25,
                "total_wins": 16,
                "total_draws": 5,
                "total_losses": 4,
                "total_points": 53,
                "total_adjusted_ppm": 2.0445,
                "total_points_per_match": 2.12,
                "total_last_match_date": "2026-03-29",
                "current_team_id": 1024,
                "current_team_name": "Flamengo",
                "current_active": True,
                "current_temporary": False,
                "current_start_date": "2026-01-10",
                "current_end_date": None,
                "coach_tenure_id": 56128,
                "team_id": 40,
                "team_name": "Liverpool",
                "active": False,
                "temporary": False,
                "start_date": "2024-07-01",
                "end_date": "2025-05-20",
                "matches": 7,
                "wins": 4,
                "draws": 1,
                "losses": 2,
                "points": 13,
                "points_per_match": 1.8571,
                "last_match_date": "2025-05-20",
                "league_id": 8,
                "league_name": "Premier League",
                "season": 2024,
            },
        ]

        response = self.client.get("/api/v1/coaches/37634192?competitionId=71&seasonId=2025")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["coach"]["coachId"], "37634192")
        self.assertEqual(data["coach"]["photoUrl"], "https://cdn.sportmonks.com/images/soccer/coaches/10/37634192.png")
        self.assertTrue(data["coach"]["hasRealPhoto"])
        self.assertEqual(data["coach"]["teamId"], "1024")
        self.assertEqual(data["summary"]["tenureCount"], 2)
        self.assertEqual(data["summary"]["matches"], 25)
        self.assertEqual(data["summary"]["adjustedPpm"], 2.0445)
        self.assertEqual(data["summary"]["pointsPerMatch"], 2.12)
        self.assertEqual(data["tenures"][0]["coachTenureId"], "104902")
        self.assertEqual(data["tenures"][0]["context"]["competitionKey"], "brasileirao_a")
        self.assertEqual(data["tenures"][1]["context"]["competitionKey"], "premier_league")
        self.assertEqual(data["sectionCoverage"]["overview"]["status"], "complete")
        self.assertEqual(data["sectionCoverage"]["tenures"]["status"], "complete")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

    @patch("api.src.routers.coaches.db_client.fetch_all")
    def test_coach_profile_returns_not_found_when_coach_does_not_exist(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = []

        response = self.client.get("/api/v1/coaches/999999")

        self.assertEqual(response.status_code, 404)
        payload = response.json()
        self.assertEqual(payload["code"], "NOT_FOUND")
        self.assertEqual(payload["details"]["coachId"], "999999")

    @patch("api.src.routers.coaches.db_client.fetch_all")
    def test_coach_profile_binds_last_n_and_coach_id_in_correct_order(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "coach_id": 37634192,
                "coach_name": "Filipe Luís",
                "photo_url": None,
                "has_real_photo": False,
                "tenure_count": 1,
                "active_tenures": 1,
                "teams_count": 1,
                "total_matches": 5,
                "total_wins": 3,
                "total_draws": 1,
                "total_losses": 1,
                "total_points": 10,
                "total_adjusted_ppm": 1.5,
                "total_points_per_match": 2.0,
                "total_last_match_date": "2026-03-29",
                "current_team_id": 1024,
                "current_team_name": "Flamengo",
                "current_active": True,
                "current_temporary": False,
                "current_start_date": "2026-01-10",
                "current_end_date": None,
                "coach_tenure_id": 104902,
                "team_id": 1024,
                "team_name": "Flamengo",
                "active": True,
                "temporary": False,
                "start_date": "2026-01-10",
                "end_date": None,
                "matches": 5,
                "wins": 3,
                "draws": 1,
                "losses": 1,
                "points": 10,
                "points_per_match": 2.0,
                "last_match_date": "2026-03-29",
                "league_id": 648,
                "league_name": "Serie A",
                "season": 2025,
            }
        ]

        response = self.client.get("/api/v1/coaches/37634192")

        self.assertEqual(response.status_code, 200)
        params = fetch_all_mock.call_args[0][1]
        self.assertEqual(params[0], 37634192)
        self.assertIsNone(params[1])
        self.assertIsNone(params[2])
        self.assertEqual(params[3], 37634192)
        self.assertEqual(params[4], 10)
        self.assertEqual(params[5], 10)


if __name__ == "__main__":
    unittest.main()

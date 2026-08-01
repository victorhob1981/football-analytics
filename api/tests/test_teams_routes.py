from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class TeamsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.teams.db_client.fetch_all")
    def test_default_team_list_uses_serving_summary(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = []

        response = self.client.get("/api/v1/teams")

        self.assertEqual(response.status_code, 200)
        query = fetch_all_mock.call_args.args[0]
        self.assertIn("mart.team_serving_summary", query)
        self.assertNotIn("mart.fact_matches", query)

    @patch("api.src.routers.teams.db_client.fetch_all")
    def test_team_search_filters_both_match_branches_before_aggregation(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = []

        response = self.client.get("/api/v1/teams?search=Flamengo")

        self.assertEqual(response.status_code, 200)
        query = fetch_all_mock.call_args.args[0]
        self.assertIn("home_team.team_name ilike", query)
        self.assertIn("away_team.team_name ilike", query)
        self.assertNotIn("a.team_name ilike", query)

    @patch("api.src.routers.teams.db_client.fetch_all")
    def test_club_catalog_uses_relevance_and_serializes_type_and_documentation(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "team_id": 3000000000311,
                "team_name": "Barcelona",
                "team_type": "club",
                "country_or_territory": "Espanha",
                "competition_count": 6,
                "season_count": 24,
                "first_match_at": "1995-08-01",
                "last_match_at": "2025-05-25",
                "stadium_name": "Camp Nou",
                "matches_played": 1125,
                "wins": 748,
                "draws": 190,
                "losses": 187,
                "goals_for": 2440,
                "goals_against": 890,
                "goal_diff": 1550,
                "points": 2434,
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/teams?entityType=club&sortBy=relevance&pageSize=20")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["scope"]["kind"], "archive")
        self.assertFalse(payload["data"]["scope"]["isExhaustive"])
        item = payload["data"]["items"][0]
        self.assertEqual(item["teamType"], "club")
        self.assertEqual(item["competitionCount"], 6)
        self.assertEqual(item["seasonCount"], 24)
        self.assertEqual(item["countryOrTerritory"], "Espanha")
        self.assertEqual(item["stadiumName"], "Camp Nou")
        query = fetch_all_mock.call_args.args[0].lower()
        self.assertIn("team_type", query)
        self.assertIn("season_count", query)

    @patch("api.src.routers.teams.db_client.fetch_all")
    def test_filtered_club_catalog_recalculates_relevance(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = []

        response = self.client.get(
            "/api/v1/teams?entityType=club&competitionId=71&seasonId=23628&sortBy=relevance&pageSize=20"
        )

        self.assertEqual(response.status_code, 200)
        query = fetch_all_mock.call_args.args[0].lower()
        self.assertIn("season_count", query)
        self.assertIn("competition_count", query)
        self.assertIn("matches_played", query)


if __name__ == "__main__":
    unittest.main()

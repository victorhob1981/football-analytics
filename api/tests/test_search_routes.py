from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class SearchRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_search_requires_minimum_meaningful_query_length(self) -> None:
        response = self.client.get("/api/v1/search?q=a")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertEqual(payload["details"]["q"], "a")

    def test_search_rejects_unknown_types(self) -> None:
        response = self.client.get("/api/v1/search?q=liv&types=coach")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertIn("coach", payload["details"]["types"][0])

    @patch("api.src.routers.search.db_client.fetch_all")
    def test_search_returns_grouped_results_with_canonical_contexts(
        self,
        fetch_all_mock,
    ) -> None:
        fetch_all_mock.side_effect = [
            [
                {
                    "competition_id": 8,
                    "search_rank": 0,
                }
            ],
            [
                {
                    "team_id": 40,
                    "team_name": "Liverpool",
                    "league_id": 8,
                    "league_name": "Premier League",
                    "season": 2024,
                    "matches_played": 38,
                    "search_rank": 0,
                }
            ],
            [
                {
                    "player_id": 306,
                    "player_name": "Mohamed Salah",
                    "team_id": 40,
                    "team_name": "Liverpool",
                    "position_name": "RW",
                    "league_id": 8,
                    "league_name": "Premier League",
                    "season": 2024,
                    "matches_played": 38,
                    "search_rank": 0,
                    "context_priority": 0,
                    "last_match_date": "2025-05-25",
                }
            ],
            [
                {
                    "match_id": 19135048,
                    "league_id": 8,
                    "league_name": "Premier League",
                    "season": 2024,
                    "round_number": 38,
                    "kickoff_at": "2025-05-25T15:00:00Z",
                    "status": "FT",
                    "home_team_id": 40,
                    "home_team_name": "Liverpool",
                    "away_team_id": 50,
                    "away_team_name": "Crystal Palace",
                    "home_score": 2,
                    "away_score": 1,
                }
            ],
        ]

        response = self.client.get("/api/v1/search?q=liv&competitionId=8&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["query"], "liv")
        self.assertEqual(data["totalResults"], 4)
        self.assertEqual([group["type"] for group in data["groups"]], ["competition", "team", "player", "match"])
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

        competition_item = data["groups"][0]["items"][0]
        self.assertEqual(competition_item["competitionId"], "8")
        self.assertEqual(competition_item["competitionKey"], "premier_league")

        team_item = data["groups"][1]["items"][0]
        self.assertEqual(team_item["teamId"], "40")
        self.assertEqual(team_item["defaultContext"]["competitionId"], "8")
        self.assertEqual(team_item["defaultContext"]["seasonId"], "2024")

        player_item = data["groups"][2]["items"][0]
        self.assertEqual(player_item["playerId"], "306")
        self.assertEqual(player_item["teamName"], "Liverpool")
        self.assertEqual(player_item["defaultContext"]["competitionId"], "8")
        self.assertEqual(player_item["defaultContext"]["seasonId"], "2024")

        match_item = data["groups"][3]["items"][0]
        self.assertEqual(match_item["matchId"], "19135048")
        self.assertEqual(match_item["homeTeamName"], "Liverpool")
        self.assertEqual(match_item["defaultContext"]["competitionId"], "8")
        self.assertEqual(match_item["defaultContext"]["seasonId"], "2024")

    @patch("api.src.routers.search.db_client.fetch_all")
    def test_search_reports_partial_coverage_when_nonnavigable_results_are_skipped(
        self,
        fetch_all_mock,
    ) -> None:
        fetch_all_mock.side_effect = [
            [],
            [],
            [],
            [
                {
                    "match_id": 19135048,
                    "league_id": 8,
                    "league_name": "Premier League",
                    "season": None,
                    "round_number": 38,
                    "kickoff_at": "2025-05-25T15:00:00Z",
                    "status": "FT",
                    "home_team_id": 40,
                    "home_team_name": "Liverpool",
                    "away_team_id": 50,
                    "away_team_name": "Crystal Palace",
                    "home_score": 2,
                    "away_score": 1,
                }
            ],
        ]

        response = self.client.get("/api/v1/search?q=liver")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["totalResults"], 0)
        self.assertEqual(payload["meta"]["coverage"]["status"], "empty")
        self.assertEqual(payload["meta"]["coverage"]["percentage"], 0)

    @patch("api.src.routers.search.db_client.fetch_all")
    def test_search_returns_brazilian_team_with_canonical_context_when_available(
        self,
        fetch_all_mock,
    ) -> None:
        fetch_all_mock.side_effect = [
            [],
            [
                {
                    "team_id": 20,
                    "team_name": "Flamengo",
                    "league_id": 648,
                    "league_name": "Serie A",
                    "season": 2024,
                    "last_match_date": "2026-03-20",
                    "matches_played": 20,
                    "search_rank": 0,
                    "context_priority": 0,
                }
            ],
            [],
            [],
        ]

        response = self.client.get("/api/v1/search?q=Flamengo&competitionId=71&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["query"], "Flamengo")
        self.assertEqual(data["totalResults"], 1)
        team_group = data["groups"][1]
        self.assertEqual(team_group["type"], "team")
        self.assertEqual(team_group["total"], 1)
        team_item = team_group["items"][0]
        self.assertEqual(team_item["teamName"], "Flamengo")
        self.assertEqual(team_item["defaultContext"]["competitionId"], "71")
        self.assertEqual(team_item["defaultContext"]["competitionKey"], "brasileirao_a")
        self.assertEqual(
            team_item["defaultContext"]["competitionName"],
            "Campeonato Brasileiro Série A",
        )
        self.assertEqual(team_item["defaultContext"]["seasonId"], "2024")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

        team_query_params = fetch_all_mock.call_args_list[1].args[1]
        self.assertEqual(
            team_query_params[7],
            [71, 648, 651, 390, 1122, 732, 654, 8, 2, 564, 384, 82, 301],
        )
        self.assertTrue(team_query_params[8])
        self.assertEqual(team_query_params[10], [71, 648])


if __name__ == "__main__":
    unittest.main()

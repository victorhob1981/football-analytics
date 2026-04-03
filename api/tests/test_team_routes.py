from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class TeamRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.teams.db_client.fetch_all")
    @patch("api.src.routers.teams.db_client.fetch_one")
    def test_team_contexts_returns_canonical_contexts(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {"team_id": 20, "team_name": "Clube Teste"}
        fetch_all_mock.return_value = [
            {
                "league_id": 71,
                "league_name": "Campeonato Brasileiro Série A",
                "season": 2024,
                "last_match_date": "2026-03-20",
                "matches_played": 20,
            }
        ]

        response = self.client.get("/api/v1/teams/20/contexts")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["defaultContext"]["competitionId"], "71")
        self.assertEqual(data["defaultContext"]["competitionKey"], "brasileirao_a")
        self.assertEqual(data["availableContexts"][0]["seasonLabel"], "2024")

    @patch("api.src.routers.teams.db_client.fetch_all")
    @patch("api.src.routers.teams.db_client.fetch_one")
    def test_team_contexts_maps_provider_competition_ids_to_canonical_contexts(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {"team_id": 1024, "team_name": "Flamengo"}
        fetch_all_mock.return_value = [
            {
                "league_id": 648,
                "league_name": "Serie A",
                "season": 2025,
                "last_match_date": "2026-03-20",
                "matches_played": 20,
            }
        ]

        response = self.client.get("/api/v1/teams/1024/contexts?competitionId=71&seasonId=2025")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["defaultContext"]["competitionId"], "71")
        self.assertEqual(data["defaultContext"]["competitionKey"], "brasileirao_a")
        self.assertEqual(
            data["defaultContext"]["competitionName"],
            "Campeonato Brasileiro Série A",
        )
        self.assertEqual(data["defaultContext"]["seasonId"], "2025")
        self.assertEqual(data["availableContexts"][0]["competitionId"], "71")

    def test_team_profile_requires_explicit_competition_and_season_context(self) -> None:
        response = self.client.get("/api/v1/teams/20")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertIn("competitionId", payload["details"]["missing"])
        self.assertIn("seasonId", payload["details"]["missing"])

    @patch("api.src.routers.teams.db_client.fetch_all")
    def test_teams_list_returns_rows_and_pagination(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "team_id": 20,
                "team_name": "Clube Teste",
                "position": 2,
                "total_teams": 20,
                "matches_played": 10,
                "wins": 6,
                "draws": 2,
                "losses": 2,
                "goals_for": 18,
                "goals_against": 9,
                "goal_diff": 9,
                "points": 20,
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/teams?competitionId=71&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["items"][0]["teamId"], "20")
        self.assertEqual(data["items"][0]["points"], 20)
        self.assertEqual(data["items"][0]["position"], 2)
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")
        self.assertEqual(payload["meta"]["pagination"]["totalCount"], 1)

    @patch("api.src.routers.teams.db_client.fetch_all")
    @patch("api.src.routers.teams.db_client.fetch_one")
    def test_team_profile_returns_overview_squad_stats_and_section_coverage(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.side_effect = [
            {"team_id": 20, "team_name": "Clube Teste"},
            {"league_id": 71, "league_name": "Campeonato Brasileiro Série A"},
            {
                "matches_played": 5,
                "wins": 3,
                "draws": 1,
                "losses": 1,
                "goals_for": 9,
                "goals_against": 4,
                "goal_diff": 5,
                "clean_sheets": 2,
                "failed_to_score": 1,
                "points": 10,
            },
            {"position": 2, "total_teams": 20},
            {"available_count": 4, "total_count": 5},
        ]
        fetch_all_mock.side_effect = [
            [
                {
                    "match_id": "1001",
                    "played_at": "2026-03-20",
                    "opponent_team_id": "30",
                    "opponent_team_name": "Adversario Teste",
                    "venue_role": "home",
                    "goals_for": 2,
                    "goals_against": 1,
                }
            ],
            [
                {
                    "player_id": "300",
                    "player_name": "Atleta Teste",
                    "position_name": "FW",
                    "jersey_number": 9,
                    "appearances": 4,
                    "starts": 3,
                    "minutes_played": 320,
                    "average_minutes": 80.0,
                    "last_appearance_at": "2026-03-20",
                }
            ],
            [
                {
                    "period_key": "2025-03",
                    "period_year": 2025,
                    "period_month": 3,
                    "matches": 3,
                    "wins": 2,
                    "draws": 1,
                    "losses": 0,
                    "goals_for": 6,
                    "goals_against": 2,
                    "goal_diff": 4,
                    "points": 7,
                }
            ],
        ]

        response = self.client.get("/api/v1/teams/20?competitionId=71&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["team"]["teamId"], "20")
        self.assertEqual(data["team"]["competitionId"], "71")
        self.assertEqual(data["summary"]["points"], 10)
        self.assertEqual(data["standing"]["position"], 2)
        self.assertEqual(data["form"], ["win"])
        self.assertEqual(data["recentMatches"][0]["result"], "win")
        self.assertEqual(data["squad"][0]["playerId"], "300")
        self.assertEqual(data["squad"][0]["minutesPlayed"], 320)
        self.assertEqual(data["stats"]["cleanSheets"], 2)
        self.assertEqual(data["stats"]["trend"][0]["label"], "03/2025")
        self.assertEqual(data["sectionCoverage"]["overview"]["status"], "complete")
        self.assertEqual(data["sectionCoverage"]["squad"]["status"], "partial")
        self.assertEqual(data["sectionCoverage"]["stats"]["status"], "complete")
        self.assertEqual(payload["meta"]["coverage"]["status"], "partial")


if __name__ == "__main__":
    unittest.main()

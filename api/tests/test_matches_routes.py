from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class MatchesRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.matches.db_client.fetch_all")
    def test_matches_list_supports_team_id_filter(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "match_id": "1001",
                "fixture_id": "1001",
                "competition_id": "71",
                "competition_name": "Campeonato Brasileiro Série A",
                "season_id": "2024",
                "round_id": "5",
                "kickoff_at": "2026-03-20T19:00:00Z",
                "status": "FT",
                "venue_name": "Arena Teste",
                "home_team_id": "20",
                "home_team_name": "Clube Teste",
                "away_team_id": "30",
                "away_team_name": "Adversario Teste",
                "home_score": 2,
                "away_score": 1,
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/matches?teamId=20")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["items"][0]["matchId"], "1001")

        query_params = fetch_all_mock.call_args.args[1]
        self.assertEqual(query_params[0], 20)
        self.assertEqual(query_params[1], 20)

    @patch("api.src.routers.matches.db_client.fetch_all")
    @patch("api.src.routers.matches.db_client.fetch_one")
    def test_match_center_returns_rich_sections_and_section_coverage(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {
            "match_id": "1001",
            "fixture_id": "1001",
            "competition_id": "8",
            "competition_name": "Premier League",
            "season_id": "2024",
            "round_id": "29",
            "kickoff_at": "2026-03-20T19:00:00Z",
            "status": "FT",
            "venue_name": "Arena Teste",
            "home_team_id": "10",
            "home_team_name": "Mandante",
            "away_team_id": "20",
            "away_team_name": "Visitante",
            "home_score": 2,
            "away_score": 1,
        }
        fetch_all_mock.side_effect = [
            [
                {
                    "event_id": "goal-1",
                    "minute": 12,
                    "second": None,
                    "period": None,
                    "type": "goal",
                    "detail": "Finalizacao cruzada",
                    "team_id": "10",
                    "team_name": "Mandante",
                    "player_id": "501",
                    "player_name": "Atacante Teste",
                }
            ],
            [
                {
                    "player_id": "501",
                    "player_name": "Atacante Teste",
                    "team_id": "10",
                    "team_name": "Mandante",
                    "position": "FW",
                    "formation_field": "attack",
                    "formation_position": 1,
                    "shirt_number": 9,
                    "is_starter": True,
                    "minutes_played": 90,
                },
                {
                    "player_id": "601",
                    "player_name": "Meia Teste",
                    "team_id": "20",
                    "team_name": "Visitante",
                    "position": "MF",
                    "formation_field": "midfield",
                    "formation_position": 2,
                    "shirt_number": 8,
                    "is_starter": True,
                    "minutes_played": 84,
                },
            ],
            [
                {
                    "team_id": "10",
                    "team_name": "Mandante",
                    "total_shots": 15,
                    "shots_on_goal": 6,
                    "ball_possession": 58,
                    "total_passes": 510,
                    "passes_accurate": 452,
                    "passes_pct": 88.6,
                    "corner_kicks": 7,
                    "fouls": 12,
                    "yellow_cards": 2,
                    "red_cards": 0,
                    "goalkeeper_saves": 3,
                },
                {
                    "team_id": "20",
                    "team_name": "Visitante",
                    "total_shots": 9,
                    "shots_on_goal": 3,
                    "ball_possession": 42,
                    "total_passes": None,
                    "passes_accurate": None,
                    "passes_pct": None,
                    "corner_kicks": 4,
                    "fouls": 16,
                    "yellow_cards": 4,
                    "red_cards": 1,
                    "goalkeeper_saves": 4,
                },
            ],
            [
                {
                    "player_id": "501",
                    "player_name": "Atacante Teste",
                    "team_id": "10",
                    "team_name": "Mandante",
                    "position_name": "FW",
                    "is_starter": True,
                    "minutes_played": 90,
                    "goals": 1,
                    "assists": 0,
                    "shots_total": 4,
                    "shots_on_goal": 2,
                    "passes_total": 19,
                    "key_passes": 3,
                    "tackles": 1,
                    "interceptions": 0,
                    "duels": 7,
                    "fouls_committed": 2,
                    "yellow_cards": 1,
                    "red_cards": 0,
                    "goalkeeper_saves": 0,
                    "clean_sheets": 0,
                    "xg": 0.82,
                    "rating": 8.4,
                }
            ],
        ]

        response = self.client.get("/api/v1/matches/1001?competitionId=8&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]
        self.assertEqual(data["timeline"][0]["eventId"], "goal-1")
        self.assertEqual(data["lineups"][0]["formationField"], "attack")
        self.assertEqual(data["lineups"][0]["formationPosition"], 1)
        self.assertEqual(data["teamStats"][0]["possessionPct"], 58.0)
        self.assertEqual(data["teamStats"][0]["passAccuracyPct"], 88.6)
        self.assertEqual(data["teamStats"][1]["totalPasses"], None)
        self.assertEqual(data["playerStats"][0]["shotsOnGoal"], 2.0)
        self.assertEqual(data["playerStats"][0]["passesTotal"], 19.0)
        self.assertEqual(data["playerStats"][0]["keyPasses"], 3.0)
        self.assertEqual(data["playerStats"][0]["positionName"], "FW")
        self.assertEqual(data["sectionCoverage"]["timeline"]["status"], "complete")
        self.assertEqual(data["sectionCoverage"]["lineups"]["status"], "partial")
        self.assertEqual(data["sectionCoverage"]["teamStats"]["status"], "partial")
        self.assertEqual(data["sectionCoverage"]["playerStats"]["status"], "partial")
        self.assertEqual(payload["meta"]["coverage"]["status"], "partial")

    @patch("api.src.routers.matches.db_client.fetch_all")
    @patch("api.src.routers.matches.db_client.fetch_one")
    def test_match_center_marks_empty_sections_when_requested_blocks_have_no_rows(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {
            "match_id": "1002",
            "fixture_id": "1002",
            "competition_id": "71",
            "competition_name": "Campeonato Brasileiro Série A",
            "season_id": "2024",
            "round_id": "10",
            "kickoff_at": "2026-03-20T19:00:00Z",
            "status": "FT",
            "venue_name": "Arena Teste",
            "home_team_id": "110",
            "home_team_name": "Mandante",
            "away_team_id": "120",
            "away_team_name": "Visitante",
            "home_score": 0,
            "away_score": 0,
        }
        fetch_all_mock.side_effect = [[], [], [], []]

        response = self.client.get("/api/v1/matches/1002?competitionId=71&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["sectionCoverage"]["timeline"]["status"], "empty")
        self.assertEqual(payload["data"]["sectionCoverage"]["lineups"]["status"], "empty")
        self.assertEqual(payload["data"]["sectionCoverage"]["teamStats"]["status"], "empty")
        self.assertEqual(payload["data"]["sectionCoverage"]["playerStats"]["status"], "empty")
        self.assertEqual(payload["meta"]["coverage"]["status"], "empty")


if __name__ == "__main__":
    unittest.main()

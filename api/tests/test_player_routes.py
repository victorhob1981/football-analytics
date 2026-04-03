from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class PlayerRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.players.db_client.fetch_all")
    @patch("api.src.routers.players.db_client.fetch_one")
    def test_players_list_returns_rows_and_pagination(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {"available_count": 1, "total_count": 1}
        fetch_all_mock.return_value = [
            {
                "player_id": 306,
                "player_name": "Mohamed Salah",
                "team_id": 40,
                "team_name": "Liverpool",
                "position_name": "FW",
                "nationality": "Egypt",
                "matches_played": 10,
                "minutes_played": 840,
                "goals": 8,
                "assists": 4,
                "shots_total": 31,
                "yellow_cards": 1,
                "red_cards": 0,
                "rating": 8.3,
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/players?competitionId=8&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        item = payload["data"]["items"][0]
        self.assertEqual(item["playerId"], "306")
        self.assertEqual(item["playerName"], "Mohamed Salah")
        self.assertEqual(item["teamId"], "40")
        self.assertEqual(item["nationality"], "Egypt")
        self.assertEqual(item["goals"], 8.0)
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")
        self.assertEqual(payload["meta"]["pagination"]["totalCount"], 1)

    @patch("api.src.routers.players.db_client.fetch_all")
    @patch("api.src.routers.players.db_client.fetch_one")
    def test_player_profile_returns_overview_history_matches_stats_and_section_coverage(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.side_effect = [
            {"player_id": 306, "player_name": "Mohamed Salah", "nationality": "Egypt"},
            {
                "team_id": 40,
                "team_name": "Liverpool",
                "position_name": "FW",
                "matches_played": 8,
                "last_match_date": "2026-03-20",
                "minutes_played": 720,
                "goals": 6,
                "assists": 4,
                "shots_total": 24,
                "shots_on_target": 12,
                "passes_attempted": 180,
                "yellow_cards": 1,
                "red_cards": 0,
                "rating": 8.4,
            },
            {"count_matches": 6},
            {"count_matches": 8},
        ]
        fetch_all_mock.side_effect = [
            [
                {
                    "fixture_id": "19135048",
                    "match_id": "19135048",
                    "played_at": "2026-03-20T19:00:00Z",
                    "competition_id": "8",
                    "competition_name": "Premier League",
                    "season_id": "2024",
                    "round_id": "29",
                    "team_id": "40",
                    "team_name": "Liverpool",
                    "opponent_team_id": "33",
                    "opponent_name": "Crystal Palace",
                    "venue_role": "home",
                    "goals_for": 2,
                    "goals_against": 1,
                    "minutes_played": 90,
                    "goals": 1,
                    "assists": 1,
                    "shots_total": 5,
                    "shots_on_goal": 3,
                    "passes_total": 28,
                    "rating": 8.9,
                }
            ],
            [
                {
                    "league_id": 8,
                    "league_name": "Premier League",
                    "season": 2024,
                    "team_id": 40,
                    "team_name": "Liverpool",
                    "matches_played": 34,
                    "minutes_played": 2980,
                    "goals": 26,
                    "assists": 14,
                    "rating": 8.2,
                    "last_match_date": "2026-03-20",
                },
                {
                    "league_id": 2,
                    "league_name": "UEFA Champions League",
                    "season": 2024,
                    "team_id": 40,
                    "team_name": "Liverpool",
                    "matches_played": 9,
                    "minutes_played": 780,
                    "goals": 5,
                    "assists": 3,
                    "rating": 8.0,
                    "last_match_date": "2026-02-14",
                },
            ],
            [
                {
                    "period_key": "2026-03",
                    "period_year": 2026,
                    "period_month": 3,
                    "matches_played": 4,
                    "minutes_played": 360,
                    "goals": 3,
                    "assists": 2,
                    "shots_total": 11,
                    "shots_on_target": 6,
                    "passes_attempted": 92,
                    "rating": 8.5,
                }
            ],
        ]

        response = self.client.get("/api/v1/players/306?competitionId=8&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["player"]["playerId"], "306")
        self.assertEqual(data["player"]["teamId"], "40")
        self.assertEqual(data["player"]["nationality"], "Egypt")
        self.assertEqual(data["summary"]["goals"], 6.0)
        self.assertEqual(data["recentMatches"][0]["matchId"], "19135048")
        self.assertEqual(data["recentMatches"][0]["result"], "win")
        self.assertEqual(data["recentMatches"][0]["competitionId"], "8")
        self.assertEqual(data["history"][0]["competitionKey"], "premier_league")
        self.assertEqual(data["history"][1]["competitionKey"], "champions_league")
        self.assertEqual(data["stats"]["goalsPer90"], 0.75)
        self.assertEqual(data["stats"]["shotsOnTargetPct"], 50.0)
        self.assertEqual(data["stats"]["trend"][0]["label"], "03/2026")
        self.assertEqual(data["sectionCoverage"]["overview"]["status"], "partial")
        self.assertEqual(data["sectionCoverage"]["matches"]["status"], "partial")
        self.assertEqual(data["sectionCoverage"]["history"]["status"], "complete")
        self.assertEqual(data["sectionCoverage"]["stats"]["status"], "partial")
        self.assertEqual(payload["meta"]["coverage"]["status"], "partial")


if __name__ == "__main__":
    unittest.main()

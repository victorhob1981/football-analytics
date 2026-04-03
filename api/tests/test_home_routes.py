from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class HomeRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.home.db_client.fetch_all")
    @patch("api.src.routers.home.db_client.fetch_one")
    def test_home_returns_archive_competitions_and_editorial_with_canonical_contexts(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {
            "competitions": 10,
            "seasons": 50,
            "matches": 15265,
            "players": 21654,
        }
        fetch_all_mock.side_effect = [
            [
                {
                    "league_id": 648,
                    "league_name": "Serie A",
                    "matches_count": 1900,
                    "seasons_count": 5,
                    "min_season": 2021,
                    "max_season": 2025,
                    "match_statistics_count": 1898,
                    "lineups_count": 1897,
                    "events_count": 1900,
                    "player_statistics_count": 1896,
                },
                {
                    "league_id": 651,
                    "league_name": "Serie B",
                    "matches_count": 1894,
                    "seasons_count": 5,
                    "min_season": 2021,
                    "max_season": 2025,
                    "match_statistics_count": 1890,
                    "lineups_count": 1889,
                    "events_count": 1894,
                    "player_statistics_count": 1888,
                },
                {
                    "league_id": 8,
                    "league_name": "Premier League",
                    "matches_count": 1900,
                    "seasons_count": 5,
                    "min_season": 2020,
                    "max_season": 2024,
                    "match_statistics_count": 1900,
                    "lineups_count": 1899,
                    "events_count": 1900,
                    "player_statistics_count": 1898,
                },
            ],
            [
                {
                    "slot": 1,
                    "league_id": 2,
                    "league_name": "Champions League",
                    "season": 2024,
                    "player_id": 160258,
                    "player_name": "Raphinha",
                    "team_id": 83,
                    "team_name": "FC Barcelona",
                    "matches_played": 14,
                    "goals": 13,
                    "assists": 11,
                    "rating": 8.34,
                    "last_match_date": "2025-05-06",
                },
                {
                    "slot": 2,
                    "league_id": 1122,
                    "league_name": "Copa Libertadores",
                    "season": 2024,
                    "player_id": 524329,
                    "player_name": "Pedrinho",
                    "team_id": 3427,
                    "team_name": "Atlético Mineiro",
                    "matches_played": 6,
                    "goals": 2,
                    "assists": 1,
                    "rating": 8.52,
                    "last_match_date": "2024-05-28",
                },
            ],
        ]

        response = self.client.get("/api/v1/home")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["archiveSummary"]["competitions"], 10)
        self.assertEqual(data["archiveSummary"]["players"], 21654)
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

        competitions = data["competitions"]
        brasileirao_b = next(
            competition for competition in competitions if competition["competitionId"] == "651"
        )
        self.assertEqual(brasileirao_b["competitionKey"], "brasileirao_b")
        self.assertEqual(brasileirao_b["latestContext"]["seasonLabel"], "2025")

        premier_league = next(
            competition for competition in competitions if competition["competitionId"] == "8"
        )
        self.assertEqual(premier_league["latestContext"]["seasonLabel"], "2024/2025")

        highlights = data["editorialHighlights"]
        self.assertEqual(len(highlights), 2)
        self.assertEqual(highlights[0]["playerName"], "Raphinha")
        self.assertEqual(highlights[0]["context"]["seasonLabel"], "2024/2025")
        self.assertEqual(highlights[1]["context"]["competitionId"], "390")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app
from api.src.routers.standings import (
    StandingsGroup,
    StandingsRound,
    StandingsScope,
    StandingsStage,
)


class StandingsRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_standings_requires_explicit_competition_and_season_context(self) -> None:
        response = self.client.get("/api/v1/standings")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertIn("competitionId", payload["details"]["missing"])
        self.assertIn("seasonId", payload["details"]["missing"])

    @patch("api.src.routers.standings._fetch_standings_rows")
    @patch("api.src.routers.standings._fetch_stage_rounds")
    @patch("api.src.routers.standings._resolve_standings_stage")
    @patch("api.src.routers.standings._resolve_standings_scope")
    def test_standings_returns_round_metadata_and_rows(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
        fetch_rounds_mock,
        fetch_rows_mock,
    ) -> None:
        resolve_scope_mock.return_value = StandingsScope(
            competition_id=8,
            season_id=2024,
            provider_season_id=23614,
            competition_name="Premier League",
            competition_key="premier_league",
            season_label="2024_25",
        )
        resolve_stage_mock.return_value = StandingsStage(
            stage_id=77471288,
            stage_name="Regular Season",
            stage_format="league_table",
            expected_teams=20,
        )
        fetch_rounds_mock.return_value = [
            StandingsRound(
                round_id=37,
                provider_round_id=339272,
                round_name="37",
                label="Rodada 37",
                starting_at="2025-05-16",
                ending_at="2025-05-20",
                is_current=False,
            ),
            StandingsRound(
                round_id=38,
                provider_round_id=339273,
                round_name="38",
                label="Rodada 38",
                starting_at="2025-05-25",
                ending_at="2025-05-25",
                is_current=True,
            ),
        ]
        fetch_rows_mock.return_value = [
            {
                "position": 1,
                "team_id": "40",
                "team_name": "Liverpool",
                "matches_played": 38,
                "wins": 25,
                "draws": 9,
                "losses": 4,
                "goals_for": 86,
                "goals_against": 41,
                "goal_diff": 45,
                "points": 84,
            }
        ]

        response = self.client.get("/api/v1/standings?competitionId=8&seasonId=2024&roundId=38")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["competition"]["competitionId"], "8")
        self.assertEqual(data["competition"]["seasonId"], "2024")
        self.assertEqual(data["selectedRound"]["roundId"], "38")
        self.assertEqual(data["currentRound"]["roundId"], "38")
        self.assertEqual(data["stage"]["expectedTeams"], 20)
        self.assertEqual(data["rows"][0]["teamName"], "Liverpool")
        self.assertEqual(payload["meta"]["coverage"]["status"], "partial")

    @patch("api.src.routers.standings._resolve_standings_stage")
    @patch("api.src.routers.standings._resolve_standings_scope")
    def test_standings_returns_empty_coverage_when_stage_is_unavailable(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
    ) -> None:
        resolve_scope_mock.return_value = StandingsScope(
            competition_id=390,
            season_id=2024,
            provider_season_id=99999,
            competition_name="Copa Libertadores da América",
            competition_key="libertadores",
            season_label="2024",
        )
        resolve_stage_mock.return_value = None

        response = self.client.get("/api/v1/standings?competitionId=390&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["data"]["rows"], [])
        self.assertEqual(payload["data"]["rounds"], [])
        self.assertEqual(payload["meta"]["coverage"]["status"], "empty")

    @patch("api.src.routers.standings._resolve_standings_stage")
    @patch("api.src.routers.standings._resolve_standings_scope")
    def test_standings_requires_group_id_for_grouped_stage(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
    ) -> None:
        resolve_scope_mock.return_value = StandingsScope(
            competition_id=390,
            season_id=2024,
            provider_season_id=23614,
            competition_name="Copa Libertadores da América",
            competition_key="libertadores",
            season_label="2024",
        )
        resolve_stage_mock.return_value = StandingsStage(
            stage_id=77468966,
            stage_name="Group Stage",
            stage_format="group_table",
            expected_teams=32,
        )

        response = self.client.get("/api/v1/standings?competitionId=390&seasonId=2024&stageId=77468966")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertEqual(payload["details"]["required"], ["groupId"])

    @patch("api.src.routers.standings._fetch_group_standings_rows")
    @patch("api.src.routers.standings._fetch_group_rounds")
    @patch("api.src.routers.standings._resolve_group")
    @patch("api.src.routers.standings._resolve_standings_stage")
    @patch("api.src.routers.standings._resolve_standings_scope")
    def test_standings_returns_group_scoped_rows_when_group_id_is_informed(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
        resolve_group_mock,
        fetch_group_rounds_mock,
        fetch_group_rows_mock,
    ) -> None:
        resolve_scope_mock.return_value = StandingsScope(
            competition_id=390,
            season_id=2024,
            provider_season_id=23614,
            competition_name="Copa Libertadores da América",
            competition_key="libertadores",
            season_label="2024",
        )
        resolve_stage_mock.return_value = StandingsStage(
            stage_id=77468966,
            stage_name="Group Stage",
            stage_format="group_table",
            expected_teams=32,
        )
        resolve_group_mock.return_value = StandingsGroup(
            group_id="group-a",
            group_name="Group A",
            group_order=1,
            expected_teams=4,
        )
        fetch_group_rounds_mock.return_value = [
            StandingsRound(
                round_id=6,
                provider_round_id=555,
                round_name="6",
                label="Rodada 6",
                starting_at="2024-05-28",
                ending_at="2024-05-30",
                is_current=True,
            )
        ]
        fetch_group_rows_mock.return_value = [
            {
                "position": 1,
                "team_id": "1095",
                "team_name": "Fluminense",
                "games_played": 6,
                "won": 4,
                "draw": 2,
                "lost": 0,
                "goals_for": 10,
                "goals_against": 3,
                "goal_diff": 7,
                "points": 14,
            }
        ]

        response = self.client.get(
            "/api/v1/standings?competitionId=390&seasonId=2024&stageId=77468966&groupId=group-a"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["group"]["groupId"], "group-a")
        self.assertEqual(payload["data"]["stage"]["stageFormat"], "group_table")
        self.assertEqual(payload["data"]["rows"][0]["teamName"], "Fluminense")
        self.assertEqual(payload["meta"]["coverage"]["status"], "partial")


if __name__ == "__main__":
    unittest.main()

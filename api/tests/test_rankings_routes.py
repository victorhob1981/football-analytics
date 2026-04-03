from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app
from api.src.routers.rankings import RankingStageScope


class RankingsRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.rankings.db_client.fetch_one")
    def test_ranking_rejects_incompatible_stage_scope(self, fetch_one_mock) -> None:
        fetch_one_mock.return_value = {
            "stage_id": 77468966,
            "stage_name": "Group Stage",
            "stage_format": "group_table",
        }

        response = self.client.get(
            "/api/v1/rankings/player-goals?competitionId=390&seasonId=2024&stageId=77468966&stageFormat=knockout"
        )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertEqual(payload["details"]["resolvedStageFormat"], "group_table")

    @patch("api.src.routers.rankings._player_ranking_coverage")
    @patch("api.src.routers.rankings._fetch_player_ranking_rows")
    @patch("api.src.routers.rankings._validate_ranking_stage_scope")
    def test_ranking_returns_resolved_stage_metadata_when_stage_filter_is_valid(
        self,
        validate_stage_scope_mock,
        fetch_rows_mock,
        coverage_mock,
    ) -> None:
        validate_stage_scope_mock.return_value = RankingStageScope(
            stage_id=77468966,
            stage_name="Group Stage",
            stage_format="group_table",
        )
        fetch_rows_mock.return_value = (
            [
                {
                    "player_id": 10,
                    "player_name": "Player Test",
                    "team_id": 20,
                    "team_name": "Team Test",
                    "rank": 1,
                    "metric_value": 4,
                    "matches_played": 3,
                    "minutes_played": 270,
                }
            ],
            1,
        )
        coverage_mock.return_value = {
            "status": "complete",
            "percentage": 100,
            "label": "Player ranking coverage",
        }

        response = self.client.get(
            "/api/v1/rankings/player-goals?competitionId=390&seasonId=2024&stageId=77468966&stageFormat=group_table"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["stage"]["stageName"], "Group Stage")
        self.assertEqual(payload["data"]["stage"]["stageFormat"], "group_table")
        self.assertEqual(payload["data"]["rows"][0]["entityName"], "Player Test")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.core.config import get_settings
from api.src.main import create_app
from api.src.routers.v2 import _world_cup_team_result


def _match(stage_name: str, home_goals: int, away_goals: int) -> dict[str, object]:
    return {
        "match_id": 1,
        "competition_key": "fifa_world_cup_mens",
        "competition_name": "Copa do Mundo FIFA",
        "edition_key": "wc_2022",
        "season_label": "2022",
        "stage_key": stage_name.lower().replace(" ", "_"),
        "round_key": stage_name.lower().replace(" ", "_"),
        "stage_name": stage_name,
        "round_name": stage_name,
        "group_key": None,
        "match_date": None,
        "venue_name": None,
        "home_team_id": 10,
        "home_team_name": "Argentina",
        "away_team_id": 20,
        "away_team_name": "France",
        "home_goals": home_goals,
        "away_goals": away_goals,
        "status": "finished",
    }


class V2WorldCupContractTests(unittest.TestCase):
    def test_team_detail_exposes_the_frontend_contract_from_mart_v2(self) -> None:
        settings = replace(get_settings(), data_layer="serving_v2")
        rows = [_match("Final", 3, 2)]

        with (
            patch("api.src.routers.v2._world_cup_match_rows", return_value=rows),
            patch("api.src.routers.v2._world_cup_team_scorer_rows", return_value=[]),
            patch("api.src.routers.v2._world_cup_team_assets", return_value={}),
            patch("api.src.routers.v2._world_cup_team_historical_scorers", return_value=[]),
        ):
            response = TestClient(create_app(settings)).get("/api/v1/world-cup/teams/10")

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["team"]["teamName"], "Argentina")
        self.assertEqual(payload["team"]["bestResultLabel"], "Campeão")
        self.assertEqual(payload["team"]["titlesCount"], 1)
        self.assertEqual(payload["participations"][0]["resultLabel"], "Campeão")
        self.assertEqual(payload["historicalScorers"], [])

    def test_tied_final_does_not_invent_a_champion(self) -> None:
        result = _world_cup_team_result(10, [_match("Final", 3, 3)])

        self.assertEqual(result, ("Final (desempate não publicado)", 2))


if __name__ == "__main__":
    unittest.main()

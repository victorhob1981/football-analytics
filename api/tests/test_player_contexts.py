from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class PlayerContextsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.players.db_client.fetch_all")
    @patch("api.src.routers.players.db_client.fetch_one")
    def test_player_contexts_returns_canonical_contexts_and_honors_preference(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {"player_id": 10, "player_name": "Atacante Teste"}
        fetch_all_mock.return_value = [
            {
                "league_id": 390,
                "league_name": "Copa Libertadores da América",
                "season": 2024,
                "last_match_date": "2026-03-18",
                "matches_played": 8,
            },
            {
                "league_id": 71,
                "league_name": "Campeonato Brasileiro Série A",
                "season": 2024,
                "last_match_date": "2026-03-10",
                "matches_played": 12,
            },
        ]

        response = self.client.get("/api/v1/players/10/contexts?competitionId=71&seasonId=2024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["defaultContext"]["competitionId"], "71")
        self.assertEqual(data["defaultContext"]["competitionKey"], "brasileirao_a")
        self.assertEqual(data["defaultContext"]["seasonId"], "2024")
        self.assertEqual(len(data["availableContexts"]), 2)
        self.assertEqual(data["availableContexts"][0]["competitionId"], "390")
        self.assertIsNotNone(payload["meta"]["requestId"])

    @patch("api.src.routers.players.db_client.fetch_all")
    @patch("api.src.routers.players.db_client.fetch_one")
    def test_player_contexts_maps_provider_competition_ids_to_canonical_contexts(
        self,
        fetch_one_mock,
        fetch_all_mock,
    ) -> None:
        fetch_one_mock.return_value = {"player_id": 219418, "player_name": "Giorgian de Arrascaeta"}
        fetch_all_mock.return_value = [
            {
                "league_id": 648,
                "league_name": "Serie A",
                "season": 2025,
                "last_match_date": "2026-03-18",
                "matches_played": 10,
            },
            {
                "league_id": 1122,
                "league_name": "Copa Libertadores",
                "season": 2025,
                "last_match_date": "2026-03-10",
                "matches_played": 6,
            },
        ]

        response = self.client.get("/api/v1/players/219418/contexts?competitionId=648&seasonId=2025")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        data = payload["data"]

        self.assertEqual(data["defaultContext"]["competitionId"], "71")
        self.assertEqual(data["defaultContext"]["competitionKey"], "brasileirao_a")
        self.assertEqual(
            data["defaultContext"]["competitionName"],
            "Campeonato Brasileiro Série A",
        )
        self.assertEqual(data["availableContexts"][0]["competitionId"], "71")
        self.assertEqual(data["availableContexts"][1]["competitionId"], "390")


if __name__ == "__main__":
    unittest.main()

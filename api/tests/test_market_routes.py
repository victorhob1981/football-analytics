from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class MarketRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.market.db_client.fetch_all")
    def test_market_transfers_returns_rows_and_pagination(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "transfer_id": 484589,
                "player_id": 37656652,
                "player_name": "Geovany Quenda",
                "from_team_id": 58,
                "from_team_name": "Sporting CP",
                "to_team_id": 18,
                "to_team_name": "Chelsea",
                "transfer_date": "2026-07-01",
                "completed": False,
                "career_ended": False,
                "type_id": 219,
                "amount": "52140000.0",
                "_total_count": 1,
            }
        ]

        response = self.client.get("/api/v1/market/transfers?competitionId=71&seasonId=2025")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        item = payload["data"]["items"][0]

        self.assertEqual(item["transferId"], "484589")
        self.assertEqual(item["playerName"], "Geovany Quenda")
        self.assertEqual(item["fromTeamName"], "Sporting CP")
        self.assertEqual(item["toTeamName"], "Chelsea")
        self.assertEqual(item["amount"], "52140000.0")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")
        self.assertEqual(payload["meta"]["pagination"]["totalCount"], 1)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app


class SearchApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.search.db_client.fetch_all")
    def test_match_search_uses_indexable_candidates(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = []

        response = self.client.get("/api/v1/search?q=flamengo&types=match")

        self.assertEqual(response.status_code, 200)
        query = fetch_all_mock.call_args.args[0]
        self.assertIn("candidate_matches as", query)
        self.assertIn("fm.home_team_id = mt.team_id", query)
        self.assertIn("fm.away_team_id = mt.team_id", query)
        self.assertIn("cross join lateral", query)
        self.assertNotIn("fm.match_id::text", query)

    @patch("api.src.routers.search.db_client.fetch_all")
    def test_team_search_serializes_normalized_team_type_without_dropping_unknown(
        self, fetch_all_mock
    ) -> None:
        fetch_all_mock.return_value = [
            {
                "team_id": 42,
                "team_name": "Equipe desconhecida",
                "team_type": "unsupported_type",
                "competition_id": 71,
                "competition_name": "Premier League",
                "season": 23628,
            }
        ]

        response = self.client.get("/api/v1/search?q=equipe&types=team")

        self.assertEqual(response.status_code, 200)
        item = response.json()["data"]["groups"][0]["items"][0]
        self.assertEqual(item["teamType"], "unknown")
        query = fetch_all_mock.call_args.args[0].lower()
        self.assertIn("mart.team_serving_summary", query)
        self.assertIn("team_type", query)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.core.config import get_settings as load_settings
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

    @patch("api.src.routers.search.get_settings")
    @patch("api.src.routers.search.db_client.fetch_all")
    def test_serving_v2_search_uses_one_catalog_query(self, fetch_all_mock, get_settings_mock) -> None:
        get_settings_mock.return_value = replace(load_settings(), data_layer="serving_v2")
        fetch_all_mock.return_value = [
            {
                "entity_type": "team",
                "entity_id": "3000000000284",
                "label": "Clube de Regatas do Flamengo",
                "subtitle": "Brasil",
                "search_text": "clube de regatas do flamengo brasil",
                "href": "/clubs/3000000000284",
                "competition_key": None,
                "edition_key": None,
                "publication_state": "published",
                "metadata": {"team_type": "club"},
                "result_rank": 1,
                "context_competition_key": "brasileirao_a",
                "context_competition_name": "Campeonato Brasileiro Série A",
                "context_season_label": "2005",
            }
        ]

        response = self.client.get("/api/v1/search?q=flamengo&types=team")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["groups"][0]["items"][0]["teamType"], "club")
        self.assertEqual(fetch_all_mock.call_count, 1)
        self.assertIn("serving_v2.search_document", fetch_all_mock.call_args.args[0])


if __name__ == "__main__":
    unittest.main()

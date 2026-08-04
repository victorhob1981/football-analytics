from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.core.config import get_settings
from api.src.main import create_app
from api.src.routers.v2 import _competition_key


class V2SurfaceTests(unittest.TestCase):
    def test_serving_v2_app_registers_the_public_read_surface(self) -> None:
        settings = replace(get_settings(), data_layer="serving_v2")
        app = create_app(settings)
        paths = {route.path for route in app.routes}

        for path in (
            "/api/v1/home",
            "/api/v1/competition-editions",
            "/api/v1/competition-structure",
            "/api/v1/teams",
            "/api/v1/players",
            "/api/v1/coaches",
            "/api/v1/matches",
            "/api/v1/standings",
            "/api/v1/rankings/{rankingType}",
            "/api/v1/analytics/overview",
            "/api/v1/market/transfers",
            "/api/v1/search",
            "/api/v1/world-cup/hub",
            "/api/v1/insights",
        ):
            self.assertIn(path, paths)

    def test_serving_v2_route_module_does_not_reference_legacy_source_schemas(self) -> None:
        from pathlib import Path

        source = Path("api/src/routers/v2.py").read_text(encoding="utf-8").lower()
        self.assertNotIn(" from raw.", source)
        self.assertNotIn(" join raw.", source)
        self.assertNotIn(" from mart.", source)
        self.assertNotIn(" join mart.", source)
        self.assertNotIn("_analytics_empty", source)

    def test_v2_accepts_catalog_keys_in_the_legacy_competition_id_slot(self) -> None:
        from starlette.requests import Request

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/matches",
            "headers": [],
            "query_string": b"competitionId=brasileirao_a",
            "server": ("testserver", 80),
            "client": ("testclient", 1),
            "scheme": "http",
        }

        self.assertEqual(_competition_key(Request(scope)), "brasileirao_a")

    def test_serving_v2_insights_returns_a_curated_payload(self) -> None:
        settings = replace(get_settings(), data_layer="serving_v2")
        with patch(
            "api.src.routers.v2.db_client.fetch_one",
            return_value={
                "match_count": 2,
                "goal_count": 3,
                "first_match_date": "2024-01-01",
                "last_match_date": "2024-01-02",
            },
        ):
            response = TestClient(create_app(settings)).get("/api/v1/insights?entityType=global")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"][0]["insight_id"], "v2-global-match-volume")

    def test_serving_v2_trends_are_backed_by_the_curated_layer(self) -> None:
        settings = replace(get_settings(), data_layer="serving_v2")
        with patch(
            "api.src.routers.v2.db_client.fetch_all",
            return_value=[{"period": "2025-01", "period_label": "2025-01", "value": 8, "sample_size": 4}],
        ):
            response = TestClient(create_app(settings)).get(
                "/api/v1/analytics/trends?metric=goals&periodType=month"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["series"][0]["value"], 8)

    def test_serving_v2_superlatives_are_backed_by_the_curated_layer(self) -> None:
        settings = replace(get_settings(), data_layer="serving_v2")
        with patch(
            "api.src.routers.v2.db_client.fetch_all",
            return_value=[
                {
                    "entity_id": "1",
                    "entity_label": "A 4x3 B",
                    "value": 7,
                    "scope": "competition/2025",
                    "sample_size": 10,
                }
            ],
        ):
            response = TestClient(create_app(settings)).get(
                "/api/v1/analytics/superlatives?category=most_goals_match"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["records"][0]["value"], 7)


if __name__ == "__main__":
    unittest.main()

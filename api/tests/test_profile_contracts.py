from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app
from api.src.routers import players, teams


class PlayerProfileContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch.object(players.db_client, "fetch_all")
    def test_player_career_groups_passages_and_preserves_team_type(self, fetch_all_mock) -> None:
        fetch_all_mock.return_value = [
            {
                "team_id": 100,
                "team_name": "Clube A",
                "team_type": "club",
                "competition_count": 2,
                "season_count": 3,
                "matches_played": 12,
                "minutes_played": 900,
                "goals": 8,
                "assists": 3,
                "first_match_at": "2019-01-01",
                "last_match_at": "2022-12-01",
                "career_competition_count": 3,
                "career_season_count": 4,
                "career_first_match_at": "2019-01-01",
                "career_last_match_at": "2024-12-01",
            },
            {
                "team_id": 200,
                "team_name": "Brasil",
                "team_type": "national_team",
                "competition_count": 1,
                "season_count": 1,
                "matches_played": 4,
                "minutes_played": 300,
                "goals": 2,
                "assists": 1,
                "first_match_at": "2020-01-01",
                "last_match_at": "2024-01-01",
                "career_competition_count": 3,
                "career_season_count": 4,
                "career_first_match_at": "2019-01-01",
                "career_last_match_at": "2024-12-01",
            },
        ]

        career = players._fetch_player_career(10)

        self.assertEqual(career["teamCount"], 2)
        self.assertEqual(career["clubCount"], 1)
        self.assertEqual(career["nationalTeamCount"], 1)
        self.assertEqual(career["competitionCount"], 3)
        self.assertEqual(career["seasonCount"], 4)
        self.assertEqual(career["teams"][0]["teamType"], "club")
        self.assertEqual(career["teams"][1]["teamType"], "national_team")
        query = fetch_all_mock.call_args.args[0].lower()
        self.assertIn("group by pms.team_id", query)
        self.assertIn("team_type", query)

    @patch.object(players, "_fetch_player_career", return_value={
        "teamCount": 2,
        "clubCount": 1,
        "nationalTeamCount": 1,
        "competitionCount": 3,
        "seasonCount": 4,
        "firstMatchAt": "2019-01-01",
        "lastMatchAt": "2024-12-01",
        "teams": [
            {"teamId": "100", "teamName": "Clube A", "teamType": "club"},
            {"teamId": "200", "teamName": "Brasil", "teamType": "national_team"},
        ],
    })
    @patch.object(players, "_profile_coverage", return_value={"status": "complete", "label": "Player profile coverage"})
    @patch.object(players, "_fetch_player_profile_meta", return_value={"hasHistoricalStats": True})
    @patch.object(players.db_client, "fetch_one")
    def test_player_profile_serializes_career_contract(
        self,
        fetch_one_mock,
        _profile_meta_mock,
        _coverage_mock,
        _career_mock,
    ) -> None:
        fetch_one_mock.side_effect = [
            {"player_id": 10, "player_name": "Jogador A", "nationality": "BR"},
            {
                "team_id": 100,
                "team_name": "Clube A",
                "position_name": "Forward",
                "matches_played": 12,
                "last_match_date": "2024-12-01",
                "minutes_played": 900,
                "goals": 8,
                "assists": 3,
            },
        ]

        response = self.client.get(
            "/api/v1/players/10?includeRecentMatches=false&includeHistory=false&includeStats=false"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["career"]["clubCount"], 1)
        self.assertEqual(payload["career"]["teams"][1]["teamType"], "national_team")
        self.assertEqual(payload["summary"]["goals"], 8)


class TeamProfileContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def _foundation(self) -> dict[str, object]:
        return {
            "identity": {
                "teamType": "club",
                "officialName": "Flamengo",
                "countryOrTerritory": "Brasil",
                "city": None,
                "foundedYear": None,
                "stadiumName": "Maracanã",
                "stadiumCapacity": None,
            },
            "archive": {
                "competitionCount": 4,
                "seasonCount": 18,
                "matchesPlayed": 400,
                "firstMatchAt": "1980-01-01",
                "lastMatchAt": "2025-12-01",
            },
            "identityCoverage": {"status": "complete", "percentage": 100, "label": "Team identity coverage"},
            "archiveCoverage": {"status": "complete", "percentage": 100, "label": "Team archive coverage"},
        }

    @patch.object(teams.db_client, "fetch_one")
    def test_team_profile_foundation_serializes_live_summary_counts(self, fetch_one_mock) -> None:
        fetch_one_mock.return_value = {
            "team_id": 1024,
            "team_name": "Flamengo",
            "team_type": "club",
            "competition_count": 4,
            "season_count": 18,
            "matches_played": 400,
        }

        foundation = teams._fetch_team_profile_foundation(1024, "Flamengo")

        self.assertEqual(foundation["archive"]["competitionCount"], 4)
        self.assertEqual(foundation["archive"]["seasonCount"], 18)
        self.assertEqual(foundation["archive"]["matchesPlayed"], 400)

    def _request_profile(self, *, honors: object) -> dict[str, object]:
        with (
            patch.object(teams, "_fetch_team_profile_foundation", return_value=self._foundation()),
            patch.object(teams, "_load_team_honors", return_value=honors),
            patch.object(teams.db_client, "fetch_one") as fetch_one_mock,
        ):
            fetch_one_mock.side_effect = [
                {"team_id": 100, "team_name": "Flamengo"},
                {"league_id": 71, "league_name": "Liga"},
                {
                    "matches_played": 10,
                    "wins": 6,
                    "draws": 2,
                    "losses": 2,
                    "goals_for": 20,
                    "goals_against": 8,
                    "goal_diff": 12,
                    "clean_sheets": 4,
                    "failed_to_score": 1,
                    "points": 20,
                },
                {"position": 2, "total_teams": 20},
            ]
            response = self.client.get(
                "/api/v1/teams/100?competitionId=71&seasonId=23628"
                "&includeRecentMatches=false&includeSquad=false&includeStats=false"
            )

        self.assertEqual(response.status_code, 200)
        return response.json()["data"]

    def test_team_profile_serializes_identity_archive_honors_and_current_fields(self) -> None:
        honors = {
            "criterionLabel": "Títulos oficiais selecionados para o acervo histórico.",
            "total": 1,
            "items": [{"competitionName": "Mundial", "scope": "mundial", "sourceName": "official", "confidence": "high"}],
            "coverage": {"status": "complete", "percentage": 100, "label": "Honors coverage"},
        }

        data = self._request_profile(honors=honors)

        self.assertEqual(data["identity"]["teamType"], "club")
        self.assertEqual(data["archive"]["seasonCount"], 18)
        self.assertEqual(data["honors"]["total"], 1)
        self.assertEqual(data["summary"]["wins"], 6)
        self.assertEqual(data["standing"]["position"], 2)
        self.assertEqual(data["sectionCoverage"]["honors"]["status"], "complete")

    def test_team_profile_allows_nullable_honors_with_coverage(self) -> None:
        data = self._request_profile(honors=None)

        self.assertIsNone(data["honors"])
        self.assertEqual(data["sectionCoverage"]["honors"]["status"], "empty")

    def test_honors_loader_reads_canonical_seed(self) -> None:
        honors = teams._load_team_honors(1024, "Flamengo")

        self.assertIsNotNone(honors)
        self.assertGreater(honors["total"], 0)
        self.assertEqual(honors["items"][0]["sourceName"], "flamengo_official")

    def test_honors_loader_falls_back_to_name_when_id_is_unmapped(self) -> None:
        honors = teams._load_team_honors(9999, "Flamengo")

        self.assertIsNotNone(honors)
        self.assertGreater(honors["total"], 0)


if __name__ == "__main__":
    unittest.main()

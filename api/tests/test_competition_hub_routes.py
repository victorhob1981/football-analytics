from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.src.main import app
from api.src.routers.competition_hub import (
    CompetitionGroup,
    CompetitionRound,
    CompetitionSeasonScope,
    CompetitionStage,
)


class CompetitionHubRoutesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch("api.src.routers.competition_hub._fetch_structure_transitions")
    @patch("api.src.routers.competition_hub._fetch_stage_groups")
    @patch("api.src.routers.competition_hub._fetch_competition_stages")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_competition_structure_returns_ordered_stages_groups_and_transitions(
        self,
        resolve_scope_mock,
        fetch_stages_mock,
        fetch_groups_mock,
        fetch_transitions_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="champions_league",
            competition_name="UEFA Champions League",
            competition_id=2,
            season_label="2024_25",
            season_id=2024,
            provider_season_id=28135,
            format_family="hybrid",
            season_format_code="ucl_league_table_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="uefa_league_phase_standard_v1",
            tie_rule_code="uefa_stage_governed_tie_rules_v1",
        )
        fetch_stages_mock.return_value = [
            CompetitionStage(
                stage_id=1,
                stage_name="League Stage",
                stage_code="league_stage",
                stage_format="league_table",
                stage_order=1,
                standings_context_mode="single_table",
                bracket_context_mode="not_applicable",
                group_mode="not_applicable",
                elimination_mode="not_applicable",
                is_current=False,
            ),
            CompetitionStage(
                stage_id=2,
                stage_name="Round of 16",
                stage_code="round_of_16",
                stage_format="knockout",
                stage_order=2,
                standings_context_mode="not_applicable",
                bracket_context_mode="knockout",
                group_mode="not_applicable",
                elimination_mode="standard",
                is_current=True,
            ),
        ]
        fetch_groups_mock.return_value = {}
        fetch_transitions_mock.return_value = {
            1: [
                {
                    "progressionScope": "table_position",
                    "progressionType": "qualified",
                    "positionFrom": 1,
                    "positionTo": 8,
                    "tieOutcome": None,
                    "toStageId": "2",
                    "toStageName": "Round of 16",
                    "toStageFormat": "knockout",
                    "toStageOrder": 2,
                }
            ]
        }

        response = self.client.get(
            "/api/v1/competition-structure?competitionKey=champions_league&seasonLabel=2024/2025"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["competition"]["seasonFormatCode"], "ucl_league_table_knockout_v1")
        self.assertEqual(payload["data"]["stages"][0]["stageFormat"], "league_table")
        self.assertEqual(payload["data"]["stages"][0]["transitions"][0]["progressionScope"], "table_position")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

    @patch("api.src.routers.competition_hub._resolve_group")
    @patch("api.src.routers.competition_hub._resolve_stage")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_group_standings_returns_clear_error_for_unknown_group(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
        resolve_group_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="libertadores",
            competition_name="Copa Libertadores da América",
            competition_id=390,
            season_label="2024",
            season_id=2024,
            provider_season_id=23614,
            format_family="hybrid",
            season_format_code="lib_qualification_group_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="conmebol_group_standard_v1",
            tie_rule_code="conmebol_stage_governed_tie_rules_v1",
        )
        resolve_stage_mock.return_value = CompetitionStage(
            stage_id=77468966,
            stage_name="Group Stage",
            stage_code="group_stage",
            stage_format="group_table",
            stage_order=4,
            standings_context_mode="grouped_table",
            bracket_context_mode="not_applicable",
            group_mode="multiple_groups",
            elimination_mode="not_applicable",
            is_current=False,
        )
        resolve_group_mock.return_value = None

        response = self.client.get(
            "/api/v1/group-standings?competitionKey=libertadores&seasonLabel=2024&stageId=77468966&groupId=missing"
        )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["code"], "INVALID_QUERY_PARAM")
        self.assertEqual(payload["details"]["groupId"], "missing")

    @patch("api.src.routers.competition_hub._resolve_stage")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_ties_reject_non_knockout_stage(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="champions_league",
            competition_name="UEFA Champions League",
            competition_id=2,
            season_label="2024_25",
            season_id=2024,
            provider_season_id=28135,
            format_family="hybrid",
            season_format_code="ucl_league_table_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="uefa_league_phase_standard_v1",
            tie_rule_code="uefa_stage_governed_tie_rules_v1",
        )
        resolve_stage_mock.return_value = CompetitionStage(
            stage_id=1,
            stage_name="League Stage",
            stage_code="league_stage",
            stage_format="league_table",
            stage_order=1,
            standings_context_mode="single_table",
            bracket_context_mode="not_applicable",
            group_mode="not_applicable",
            elimination_mode="not_applicable",
            is_current=False,
        )

        response = self.client.get(
            "/api/v1/ties?competitionKey=champions_league&seasonLabel=2024/2025&stageId=1"
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["details"]["stageFormat"], "league_table")

    @patch("api.src.routers.competition_hub._fetch_stage_ties")
    @patch("api.src.routers.competition_hub._resolve_stage")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_ties_accept_placement_match_stage(
        self,
        resolve_scope_mock,
        resolve_stage_mock,
        fetch_stage_ties_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="fifa_world_cup",
            competition_name="FIFA World Cup",
            competition_id=2000,
            season_label="2022",
            season_id=2022,
            provider_season_id=2022,
            format_family="hybrid",
            season_format_code="fwc_32_group_knockout_v1",
            participant_scope="national_team",
            group_ranking_rule_code="fifa_group_standard_32_v1",
            tie_rule_code="single_leg_extra_time_penalties_v1",
        )
        resolve_stage_mock.return_value = CompetitionStage(
            stage_id=99,
            stage_name="Third place play-off",
            stage_code="third_place_playoff",
            stage_format="placement_match",
            stage_order=8,
            standings_context_mode="not_applicable",
            bracket_context_mode="knockout",
            group_mode="not_applicable",
            elimination_mode="standard",
            is_current=False,
        )
        fetch_stage_ties_mock.return_value = [
            {
                "tie_id": "placement-1",
                "tie_order": 1,
                "home_team_id": "11",
                "home_side_team_name": "Croatia",
                "away_team_id": "22",
                "away_side_team_name": "Morocco",
                "match_count": 1,
                "first_leg_at": "2022-12-17T15:00:00Z",
                "last_leg_at": "2022-12-17T15:00:00Z",
                "home_side_goals": 2,
                "away_side_goals": 1,
                "winner_team_id": "11",
                "winner_team_name": "Croatia",
                "resolution_type": "single_match",
                "has_extra_time_match": False,
                "has_penalties_match": False,
                "next_stage_id": None,
                "next_stage_name": None,
            }
        ]

        response = self.client.get(
            "/api/v1/ties?competitionKey=fifa_world_cup&seasonLabel=2022&stageId=99"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["stage"]["stageFormat"], "placement_match")
        self.assertEqual(payload["data"]["ties"][0]["winnerTeamName"], "Croatia")

    @patch("api.src.routers.competition_hub._fetch_team_progression")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_team_progression_returns_structural_path(
        self,
        resolve_scope_mock,
        fetch_progression_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="libertadores",
            competition_name="Copa Libertadores da América",
            competition_id=390,
            season_label="2024",
            season_id=2024,
            provider_season_id=23614,
            format_family="hybrid",
            season_format_code="lib_qualification_group_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="conmebol_group_standard_v1",
            tie_rule_code="conmebol_stage_governed_tie_rules_v1",
        )
        fetch_progression_mock.return_value = [
            {
                "stage_progression_id": "p1",
                "team_id": "1095",
                "team_name": "Fluminense",
                "from_stage_id": "77468966",
                "from_stage_name": "Group Stage",
                "from_stage_format": "group_table",
                "from_stage_order": 4,
                "to_stage_id": "77468965",
                "to_stage_name": "Round of 16",
                "to_stage_format": "knockout",
                "to_stage_order": 5,
                "progression_scope": "group_position",
                "progression_type": "qualified",
                "source_position": 1,
                "tie_outcome": None,
                "group_id": "group-a",
                "group_name": "Group A",
            }
        ]

        response = self.client.get(
            "/api/v1/team-progression?competitionKey=libertadores&seasonLabel=2024&teamId=1095"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["team"]["teamName"], "Fluminense")
        self.assertEqual(payload["data"]["progression"][0]["fromStageFormat"], "group_table")
        self.assertEqual(payload["data"]["progression"][0]["progressionType"], "qualified")

    @patch("api.src.routers.competition_hub._fetch_competition_season_comparisons")
    @patch("api.src.routers.competition_hub._fetch_competition_stage_analytics")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_competition_analytics_returns_stage_metrics_and_season_comparison(
        self,
        resolve_scope_mock,
        fetch_stage_analytics_mock,
        fetch_comparison_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="champions_league",
            competition_name="UEFA Champions League",
            competition_id=2,
            season_label="2024_25",
            season_id=2024,
            provider_season_id=28135,
            format_family="hybrid",
            season_format_code="ucl_league_table_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="uefa_league_phase_standard_v1",
            tie_rule_code="uefa_stage_governed_tie_rules_v1",
        )
        fetch_stage_analytics_mock.return_value = [
            {
                "stage_id": "1",
                "stage_name": "League Stage",
                "stage_code": "league_stage",
                "stage_format": "league_table",
                "sort_order": 1,
                "is_current": True,
                "match_count": 144,
                "team_count": 36,
                "group_count": 0,
                "average_goals": 3.04,
                "home_wins": 61,
                "draws": 32,
                "away_wins": 51,
                "tie_count": 0,
                "resolved_ties": 0,
                "inferred_ties": 0,
            },
            {
                "stage_id": "2",
                "stage_name": "Round of 16",
                "stage_code": "round_of_16",
                "stage_format": "knockout",
                "sort_order": 2,
                "is_current": False,
                "match_count": 16,
                "team_count": 16,
                "group_count": 0,
                "average_goals": 2.56,
                "home_wins": 8,
                "draws": 3,
                "away_wins": 5,
                "tie_count": 8,
                "resolved_ties": 8,
                "inferred_ties": 0,
            },
        ]
        fetch_comparison_mock.return_value = [
            {
                "season_label": "2024_25",
                "format_family": "hybrid",
                "season_format_code": "ucl_league_table_knockout_v1",
                "participant_scope": "club",
                "match_count": 189,
                "stage_count": 10,
                "table_stage_count": 1,
                "knockout_stage_count": 6,
                "group_count": 0,
                "tie_count": 68,
                "average_goals": 2.94,
            },
            {
                "season_label": "2023_24",
                "format_family": "hybrid",
                "season_format_code": "ucl_group_knockout_v1",
                "participant_scope": "club",
                "match_count": 125,
                "stage_count": 11,
                "table_stage_count": 1,
                "knockout_stage_count": 5,
                "group_count": 8,
                "tie_count": 61,
                "average_goals": 2.99,
            },
        ]

        response = self.client.get(
            "/api/v1/competition-analytics?competitionKey=champions_league&seasonLabel=2024/2025"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["competition"]["seasonFormatCode"], "ucl_league_table_knockout_v1")
        self.assertEqual(payload["data"]["seasonSummary"]["knockoutStages"], 1)
        self.assertEqual(payload["data"]["stageAnalytics"][1]["tieCount"], 8)
        self.assertEqual(payload["data"]["seasonComparisons"][1]["seasonFormatCode"], "ucl_group_knockout_v1")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

    @patch("api.src.routers.competition_hub._fetch_competition_season_comparisons")
    @patch("api.src.routers.competition_hub._fetch_competition_stage_analytics")
    @patch("api.src.routers.competition_hub._resolve_competition_scope")
    def test_competition_analytics_marks_partial_coverage_when_a_stage_has_no_materialized_signal(
        self,
        resolve_scope_mock,
        fetch_stage_analytics_mock,
        fetch_comparison_mock,
    ) -> None:
        resolve_scope_mock.return_value = CompetitionSeasonScope(
            competition_key="champions_league",
            competition_name="UEFA Champions League",
            competition_id=2,
            season_label="2024_25",
            season_id=2024,
            provider_season_id=28135,
            format_family="hybrid",
            season_format_code="ucl_league_table_knockout_v1",
            participant_scope="club",
            group_ranking_rule_code="uefa_league_phase_standard_v1",
            tie_rule_code="uefa_stage_governed_tie_rules_v1",
        )
        fetch_stage_analytics_mock.return_value = [
            {
                "stage_id": "1",
                "stage_name": "League Stage",
                "stage_code": "league_stage",
                "stage_format": "league_table",
                "sort_order": 1,
                "is_current": True,
                "match_count": 144,
                "team_count": 36,
                "group_count": 0,
                "average_goals": 3.04,
                "home_wins": 61,
                "draws": 32,
                "away_wins": 51,
                "tie_count": 0,
                "resolved_ties": 0,
                "inferred_ties": 0,
            },
            {
                "stage_id": "2",
                "stage_name": "Round of 16",
                "stage_code": "round_of_16",
                "stage_format": "knockout",
                "sort_order": 2,
                "is_current": False,
                "match_count": 0,
                "team_count": 0,
                "group_count": 0,
                "average_goals": None,
                "home_wins": 0,
                "draws": 0,
                "away_wins": 0,
                "tie_count": 0,
                "resolved_ties": 0,
                "inferred_ties": 0,
            },
        ]
        fetch_comparison_mock.return_value = []

        response = self.client.get(
            "/api/v1/competition-analytics?competitionKey=champions_league&seasonLabel=2024/2025"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["meta"]["coverage"]["status"], "partial")

    @patch("api.src.routers.competition_hub.db_client.fetch_one")
    @patch("api.src.routers.competition_hub._fetch_team_journey_history_rows")
    def test_team_journey_history_returns_multiple_seasons_with_stage_results(
        self,
        fetch_journey_rows_mock,
        fetch_one_mock,
    ) -> None:
        fetch_journey_rows_mock.return_value = [
            {
                "season_label": "2024",
                "format_family": "knockout",
                "season_format_code": "cdb_knockout_v1",
                "stage_id": "100",
                "stage_name": "Round of 16",
                "stage_format": "knockout",
                "stage_order": 4,
                "matches_played": 2,
                "wins": 1,
                "draws": 0,
                "losses": 1,
                "goals_for": 3,
                "goals_against": 2,
                "progression_type": "qualified",
                "tie_outcome": "winner",
                "source_position": None,
                "group_id": None,
                "group_name": None,
                "tie_count": 1,
                "ties_won": 1,
                "ties_lost": 0,
                "is_champion": False,
                "is_runner_up": False,
            },
            {
                "season_label": "2024",
                "format_family": "knockout",
                "season_format_code": "cdb_knockout_v1",
                "stage_id": "101",
                "stage_name": "Quarter-finals",
                "stage_format": "knockout",
                "stage_order": 5,
                "matches_played": 2,
                "wins": 0,
                "draws": 1,
                "losses": 1,
                "goals_for": 1,
                "goals_against": 2,
                "progression_type": "eliminated",
                "tie_outcome": "loser",
                "source_position": None,
                "group_id": None,
                "group_name": None,
                "tie_count": 1,
                "ties_won": 0,
                "ties_lost": 1,
                "is_champion": False,
                "is_runner_up": False,
            },
            {
                "season_label": "2023",
                "format_family": "knockout",
                "season_format_code": "cdb_knockout_v1",
                "stage_id": "200",
                "stage_name": "Final",
                "stage_format": "knockout",
                "stage_order": 7,
                "matches_played": 2,
                "wins": 2,
                "draws": 0,
                "losses": 0,
                "goals_for": 4,
                "goals_against": 1,
                "progression_type": "qualified",
                "tie_outcome": "winner",
                "source_position": None,
                "group_id": None,
                "group_name": None,
                "tie_count": 1,
                "ties_won": 1,
                "ties_lost": 0,
                "is_champion": True,
                "is_runner_up": False,
            },
        ]
        fetch_one_mock.return_value = {"team_name": "Flamengo"}

        response = self.client.get("/api/v1/team-journey-history?competitionKey=copa_do_brasil&teamId=1024")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["team"]["teamName"], "Flamengo")
        self.assertEqual(payload["data"]["seasons"][0]["summary"]["finalOutcome"], "eliminated")
        self.assertEqual(payload["data"]["seasons"][1]["summary"]["finalOutcome"], "champion")
        self.assertEqual(payload["data"]["seasons"][0]["stages"][0]["stageResult"], "qualified")
        self.assertEqual(payload["data"]["seasons"][0]["stages"][1]["stageResult"], "eliminated")
        self.assertEqual(payload["meta"]["coverage"]["status"], "complete")

    @patch("api.src.routers.competition_hub.db_client.fetch_one")
    @patch("api.src.routers.competition_hub._fetch_team_journey_history_rows")
    def test_team_journey_history_marks_unknown_for_terminal_stage_without_structural_champion_proof(
        self,
        fetch_journey_rows_mock,
        fetch_one_mock,
    ) -> None:
        fetch_journey_rows_mock.return_value = [
            {
                "season_label": "2022",
                "format_family": "hybrid",
                "season_format_code": "fwc_32_group_knockout_v1",
                "stage_id": "300",
                "stage_name": "Third place play-off",
                "stage_format": "placement_match",
                "stage_order": 8,
                "matches_played": 1,
                "wins": 1,
                "draws": 0,
                "losses": 0,
                "goals_for": 2,
                "goals_against": 1,
                "progression_type": None,
                "tie_outcome": "winner",
                "source_position": None,
                "group_id": None,
                "group_name": None,
                "tie_count": 1,
                "ties_won": 1,
                "ties_lost": 0,
                "is_structurally_terminal_stage": True,
                "is_championship_stage": False,
                "is_champion": False,
                "is_runner_up": False,
            }
        ]
        fetch_one_mock.return_value = {"team_name": "Croatia"}

        response = self.client.get("/api/v1/team-journey-history?competitionKey=fifa_world_cup&teamId=11")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["seasons"][0]["stages"][0]["stageResult"], "unknown")
        self.assertEqual(payload["data"]["seasons"][0]["summary"]["finalOutcome"], "unknown")
        self.assertEqual(payload["meta"]["coverage"]["status"], "unknown")


if __name__ == "__main__":
    unittest.main()

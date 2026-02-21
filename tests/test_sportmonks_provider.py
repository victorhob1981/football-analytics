from __future__ import annotations

from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.providers.sportmonks import SportMonksProvider


def test_sportmonks_provider_maps_fixtures_payload():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    provider._paginate_fixtures_between = lambda date_from, date_to: (
        [
            {
                "id": 1001,
                "league_id": 71,
                "season_id": 2024,
                "starting_at": "2024-05-01 20:00:00",
                "starting_at_timestamp": 1714593600,
                "participants": [
                    {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                    {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                ],
                "scores": [
                    {"participant_id": 10, "description": "FT", "score": {"goals": 2}},
                    {"participant_id": 11, "description": "FT", "score": {"goals": 1}},
                ],
                "state": {"short_name": "FT", "name": "Finished"},
                "venue": {"id": 7, "name": "Arena", "city_name": "Sao Paulo"},
                "league": {"name": "Serie A"},
                "round": {"name": "Round 5"},
                "stage": {"name": "Regular Season"},
            },
            {
                "id": 1002,
                "league_id": 39,
                "season_id": 2024,
            },
        ],
        {"x-ratelimit-remaining": "99"},
        {"pagination": {"total": 2}},
    )

    payload, headers = provider.get_fixtures(
        league_id=71,
        season=2024,
        date_from="2024-05-01",
        date_to="2024-05-31",
    )

    assert payload["provider"] == "sportmonks"
    assert payload["entity_type"] == "fixtures"
    assert len(payload["response"]) == 1
    assert payload["response"][0]["fixture"]["id"] == 1001
    assert payload["response"][0]["teams"]["home"]["id"] == 10
    assert payload["response"][0]["goals"]["away"] == 1
    assert headers["x-ratelimit-remaining"] == "99"


def test_sportmonks_provider_maps_statistics_and_events_payload():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        if "statistics" in str(params.get("include", "")):
            return (
                {
                    "data": {
                        "statistics": [
                            {
                                "participant_id": 10,
                                "participant": {"id": 10, "name": "Home FC"},
                                "type": {"developer_name": "SHOTS_ON_TARGET"},
                                "data": {"value": 5},
                            },
                            {
                                "participant_id": 11,
                                "participant": {"id": 11, "name": "Away FC"},
                                "type": {"developer_name": "FOULS"},
                                "data": {"value": 12},
                            },
                        ]
                    }
                },
                {"x-ratelimit-remaining": "98"},
            )
        return (
            {
                "data": {
                    "events": [
                        {
                            "minute": 8,
                            "extra_minute": None,
                            "participant_id": 10,
                            "participant": {"id": 10, "name": "Home FC"},
                            "player_id": 100,
                            "player": {"id": 100, "name": "Player A"},
                            "related_player_id": 101,
                            "relatedplayer": {"id": 101, "name": "Player B"},
                            "type": {"name": "Goal"},
                            "info": "Open Play",
                            "result": "1-0",
                        }
                    ]
                }
            },
            {"x-ratelimit-remaining": "97"},
        )

    provider._request = _fake_request

    stats_payload, stats_headers = provider.get_fixture_statistics(fixture_id=1001)
    events_payload, events_headers = provider.get_fixture_events(fixture_id=1001)

    assert stats_payload["entity_type"] == "statistics"
    assert len(stats_payload["response"]) == 2
    assert stats_payload["response"][0]["team"]["id"] == 10
    assert stats_payload["response"][0]["statistics"][0]["type"] == "shots_on_goal"
    assert events_payload["entity_type"] == "match_events"
    assert events_payload["response"][0]["team"]["id"] == 10
    assert events_payload["response"][0]["type"] == "Goal"
    assert stats_headers["x-ratelimit-remaining"] == "98"
    assert events_headers["x-ratelimit-remaining"] == "97"


def test_sportmonks_provider_maps_extended_entities_payload():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        if endpoint == "/leagues/648":
            return (
                {
                    "data": {
                        "id": 648,
                        "name": "Serie A",
                        "seasons": [
                            {"id": 23265, "name": "2024", "starting_at": "2024-04-01", "ending_at": "2024-12-15"}
                        ],
                    },
                    "rate_limit": {"remaining": 1000},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "1000"},
            )
        if endpoint == "/stages/seasons/23265":
            return (
                {
                    "data": [{"id": 9001, "league_id": 648, "season_id": 23265, "name": "Regular Season"}],
                    "rate_limit": {"remaining": 999},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "999"},
            )
        if endpoint == "/rounds/seasons/23265":
            return (
                {
                    "data": [{"id": 8001, "league_id": 648, "season_id": 23265, "stage_id": 9001, "name": "1"}],
                    "rate_limit": {"remaining": 998},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "998"},
            )
        if endpoint == "/fixtures/1001":
            return (
                {
                    "data": {
                        "participants": [
                            {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                            {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                        ],
                        "lineups": [
                            {
                                "id": 1,
                                "fixture_id": 1001,
                                "team_id": 10,
                                "player_id": 100,
                                "player_name": "Player A",
                                "position_id": 99,
                                "formation_field": "4-3-3",
                                "formation_position": 8,
                                "type_id": 11,
                                "jersey_number": 8,
                                "player": {"id": 100, "name": "Player A"},
                                "position": {"id": 99, "name": "Midfielder", "developer_name": "MIDFIELDER"},
                                "details": [
                                    {
                                        "id": 5001,
                                        "value": 2,
                                        "type": {"name": "Shots Total", "developer_name": "SHOTS_TOTAL"},
                                    }
                                ],
                            }
                        ],
                    },
                    "rate_limit": {"remaining": 997},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "997"},
            )
        if endpoint == "/players/100":
            if params.get("include") == "transfers":
                return (
                    {
                        "data": {
                            "id": 100,
                            "name": "Player A",
                            "transfers": [
                                {
                                    "id": 7001,
                                    "from_team_id": 77,
                                    "to_team_id": 10,
                                    "type_id": 5,
                                    "position_id": 99,
                                    "date": "2024-01-10",
                                    "completed": True,
                                    "career_ended": False,
                                    "amount": "1000000",
                                }
                            ],
                        },
                        "rate_limit": {"remaining": 996},
                        "subscription": {},
                        "timezone": "UTC",
                    },
                    {"x-ratelimit-remaining": "996"},
                )
            return (
                {
                    "data": {
                        "id": 100,
                        "name": "Player A",
                        "statistics": [
                            {
                                "id": 3001,
                                "team": {"id": 10, "name": "Home FC"},
                                "season": {"id": 23265, "name": "2024", "league_id": 648},
                                "position": {"id": 99, "name": "Midfielder"},
                                "details": [
                                    {
                                        "id": 90001,
                                        "value": 12,
                                        "type": {"name": "Goals", "developer_name": "GOALS"},
                                    }
                                ],
                            }
                        ],
                    },
                    "rate_limit": {"remaining": 995},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "995"},
            )
        if endpoint == "/teams/10":
            if params.get("include") == "coaches":
                return (
                    {
                        "data": {
                            "id": 10,
                            "name": "Home FC",
                            "coaches": [
                                {
                                    "id": 501,
                                    "coach_id": 200,
                                    "position_id": 1,
                                    "active": True,
                                    "temporary": False,
                                    "start": "2024-01-01",
                                    "end": None,
                                }
                            ],
                        },
                        "rate_limit": {"remaining": 994},
                        "subscription": {},
                        "timezone": "UTC",
                    },
                    {"x-ratelimit-remaining": "994"},
                )
            return (
                {
                    "data": {
                        "id": 10,
                        "name": "Home FC",
                        "sidelined": [
                            {
                                "id": 601,
                                "player_id": 101,
                                "type_id": 7,
                                "category": "injury",
                                "season_id": 2024,
                                "start_date": "2024-04-01",
                                "end_date": None,
                                "games_missed": 3,
                                "completed": False,
                            }
                        ],
                    },
                    "rate_limit": {"remaining": 993},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "993"},
            )
        if endpoint == "/fixtures/head-to-head/10/11":
            return (
                {
                    "data": [
                        {
                            "id": 4001,
                            "league_id": 648,
                            "season_id": 23265,
                            "starting_at": "2024-07-01 20:00:00",
                            "starting_at_timestamp": 1720000000,
                            "participants": [
                                {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                                {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                            ],
                            "scores": [
                                {"participant_id": 10, "description": "FT", "score": {"goals": 1}},
                                {"participant_id": 11, "description": "FT", "score": {"goals": 0}},
                            ],
                            "league": {"id": 648, "name": "Serie A"},
                            "season": {"id": 23265, "name": "2024"},
                            "round": {"name": "Round 10"},
                            "stage": {"name": "Regular Season"},
                            "state": {"short_name": "FT", "name": "Finished"},
                            "venue": {"id": 100, "name": "Arena", "city_name": "Sao Paulo"},
                        }
                    ],
                    "rate_limit": {"remaining": 992},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "992"},
            )
        raise AssertionError(f"endpoint inesperado: {endpoint} params={params}")

    provider._request = _fake_request

    competition_payload, _ = provider.get_competition_structure(league_id=648, season=2024)
    lineups_payload, _ = provider.get_fixture_lineups(fixture_id=1001)
    player_match_payload, _ = provider.get_fixture_player_statistics(fixture_id=1001)
    player_season_payload, _ = provider.get_player_season_statistics(player_id=100, season=2024, league_id=648)
    transfers_payload, _ = provider.get_player_transfers(player_id=100)
    sidelined_payload, _ = provider.get_team_sidelined(team_id=10, season=2024)
    coaches_payload, _ = provider.get_team_coaches(team_id=10)
    h2h_payload, _ = provider.get_head_to_head(team_id=10, opponent_id=11)

    assert competition_payload["entity_type"] == "competition_structure"
    assert competition_payload["response"][0]["season_id"] == 23265

    assert lineups_payload["entity_type"] == "fixture_lineups"
    assert lineups_payload["response"][0]["team"]["id"] == 10
    assert lineups_payload["response"][0]["details"][0]["type"] == "total_shots"

    assert player_match_payload["entity_type"] == "fixture_player_statistics"
    assert player_match_payload["response"][0]["player"]["id"] == 100
    assert player_match_payload["response"][0]["statistics"][0]["type"] == "total_shots"

    assert player_season_payload["entity_type"] == "player_season_statistics"
    assert player_season_payload["response"][0]["season"]["league_id"] == 648

    assert transfers_payload["entity_type"] == "player_transfers"
    assert transfers_payload["response"][0]["transfer_id"] == 7001

    assert sidelined_payload["entity_type"] == "team_sidelined"
    assert sidelined_payload["response"][0]["player"]["id"] == 101

    assert coaches_payload["entity_type"] == "team_coaches"
    assert coaches_payload["response"][0]["coach_id"] == 200

    assert h2h_payload["entity_type"] == "head_to_head"
    assert h2h_payload["response"][0]["fixture"]["id"] == 4001

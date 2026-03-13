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
                "season_id": 23265,
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
                "season": {"id": 23265, "year": 2024, "name": "2024"},
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


def test_sportmonks_provider_filters_head_to_head_by_local_scope():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        assert endpoint == "/fixtures/head-to-head/10/11"
        assert params == {"include": "participants;scores;scores.type;league;season;state;round;stage;venue"}
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
                        "season": {"id": 23265, "name": "2024", "year": 2024},
                        "round": {"name": "Round 10"},
                        "stage": {"name": "Regular Season"},
                        "state": {"short_name": "FT", "name": "Finished"},
                        "venue": {"id": 100, "name": "Arena", "city_name": "Sao Paulo"},
                    },
                    {
                        "id": 4002,
                        "league_id": 8,
                        "season_id": 23265,
                        "starting_at": "2024-07-08 20:00:00",
                        "starting_at_timestamp": 1720600000,
                        "participants": [
                            {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                            {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                        ],
                        "scores": [],
                        "league": {"id": 8, "name": "Premier League"},
                        "season": {"id": 23265, "name": "2024", "year": 2024},
                        "round": {"name": "Round 11"},
                        "stage": {"name": "Regular Season"},
                        "state": {"short_name": "FT", "name": "Finished"},
                        "venue": {"id": 101, "name": "Arena 2", "city_name": "London"},
                    },
                    {
                        "id": 4003,
                        "league_id": 648,
                        "season_id": 22000,
                        "starting_at": "2023-07-01 20:00:00",
                        "starting_at_timestamp": 1688241600,
                        "participants": [
                            {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                            {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                        ],
                        "scores": [],
                        "league": {"id": 648, "name": "Serie A"},
                        "season": {"id": 22000, "name": "2023", "year": 2023},
                        "round": {"name": "Round 9"},
                        "stage": {"name": "Regular Season"},
                        "state": {"short_name": "FT", "name": "Finished"},
                        "venue": {"id": 102, "name": "Arena 3", "city_name": "Sao Paulo"},
                    },
                ],
                "rate_limit": {"remaining": 992},
                "subscription": {},
                "timezone": "UTC",
            },
            {"x-ratelimit-remaining": "992"},
        )

    provider._request = _fake_request

    payload, _ = provider.get_head_to_head(
        team_id=10,
        opponent_id=11,
        league_id=648,
        season=2024,
        season_label="2024",
        provider_season_id=23265,
    )

    assert [row["fixture"]["id"] for row in payload["response"]] == [4001]
    assert payload["provider_meta"]["rows_received"] == 3
    assert payload["provider_meta"]["rows_kept"] == 1
    assert payload["provider_meta"]["rows_rejected_by_scope"] == 2
    assert payload["provider_meta"]["rows_rejected_by_league"] == 1
    assert payload["provider_meta"]["rows_rejected_by_season"] == 1
    assert payload["provider_meta"]["scope_filter"] == {
        "league_id": 648,
        "season": 2024,
        "season_label": "2024",
        "provider_season_id": 23265,
    }
    assert payload["source_params"]["league_id"] == 648
    assert payload["source_params"]["season"] == 2024
    assert payload["source_params"]["season_label"] == "2024"
    assert payload["source_params"]["provider_season_id"] == 23265


def test_sportmonks_provider_resolves_cross_year_season_by_start_year():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    called_endpoints: list[str] = []

    def _fake_request(*, endpoint, params):
        called_endpoints.append(endpoint)
        if endpoint == "/leagues/8":
            return (
                {
                    "data": {
                        "id": 8,
                        "name": "Premier League",
                        "seasons": [
                            {"id": 16036, "year": 2019, "name": "2019/2020", "starting_at": "2019-08-09", "ending_at": "2020-07-26"},
                            {"id": 17420, "year": 2020, "name": "2020/2021", "starting_at": "2020-09-12", "ending_at": "2021-05-23"},
                        ],
                    }
                },
                {},
            )
        if endpoint == "/stages/seasons/17420":
            return ({"data": []}, {})
        if endpoint == "/rounds/seasons/17420":
            return ({"data": []}, {})
        if endpoint == "/standings/seasons/17420":
            return ({"data": [{"league_id": 8, "season_id": 17420, "participant_id": 10}]}, {})
        raise AssertionError(f"endpoint inesperado: {endpoint} params={params}")

    provider._request = _fake_request

    competition_payload, _ = provider.get_competition_structure(league_id=8, season=2020)
    standings_payload, _ = provider.get_standings(league_id=8, season=2020)

    assert competition_payload["response"][0]["season_id"] == 17420
    assert standings_payload["response"][0]["season_id"] == 17420
    assert "/stages/seasons/17420" in called_endpoints
    assert "/standings/seasons/17420" in called_endpoints


def test_sportmonks_provider_does_not_shift_calendar_season_by_end_date():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        if endpoint == "/leagues/648":
            return (
                {
                    "data": {
                        "id": 648,
                        "name": "Serie A",
                        "seasons": [
                            {"id": 15001, "year": 2020, "name": "2020", "starting_at": "2020-08-09", "ending_at": "2021-02-26"},
                            {"id": 15002, "year": 2021, "name": "2021", "starting_at": "2021-05-29", "ending_at": "2021-12-10"},
                        ],
                    }
                },
                {},
            )
        if endpoint == "/stages/seasons/15002":
            return ({"data": []}, {})
        if endpoint == "/rounds/seasons/15002":
            return ({"data": []}, {})
        raise AssertionError(f"endpoint inesperado: {endpoint} params={params}")

    provider._request = _fake_request

    competition_payload, _ = provider.get_competition_structure(league_id=648, season=2021)

    assert competition_payload["response"][0]["season_id"] == 15002


def test_sportmonks_provider_filters_fixtures_by_start_year_and_strong_identity():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    provider._paginate_fixtures_between = lambda date_from, date_to: (
        [
            {
                "id": 1001,
                "league_id": 8,
                "season_id": 16036,
                "starting_at": "2020-07-20 20:00:00",
                "starting_at_timestamp": 1595275200,
                "participants": [
                    {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                    {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                ],
                "scores": [],
                "state": {"short_name": "FT", "name": "Finished"},
                "venue": {"id": 7, "name": "Arena", "city_name": "London"},
                "league": {"name": "Premier League"},
                "season": {"id": 16036, "name": "2019/2020", "starting_at": "2019-08-09", "ending_at": "2020-07-26"},
            },
            {
                "id": 1002,
                "league_id": 8,
                "season_id": 17420,
                "starting_at": "2020-09-15 20:00:00",
                "starting_at_timestamp": 1600200000,
                "participants": [
                    {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                    {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                ],
                "scores": [],
                "state": {"short_name": "FT", "name": "Finished"},
                "venue": {"id": 7, "name": "Arena", "city_name": "London"},
                "league": {"name": "Premier League"},
                "season": {"id": 17420, "name": "2020/2021", "starting_at": "2020-09-12", "ending_at": "2021-05-23"},
            },
        ],
        {},
        {"pagination": {"total": 2}},
    )

    payload, _ = provider.get_fixtures(
        league_id=8,
        season=2020,
        season_label="2020_21",
        provider_season_id=17420,
        date_from="2020-07-01",
        date_to="2021-05-31",
    )

    assert [row["fixture"]["id"] for row in payload["response"]] == [1002]
    assert payload["response"][0]["league"]["season"] == 2020


def test_sportmonks_provider_tracks_paginated_fixture_requests_in_provider_meta():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        assert endpoint == "/fixtures/between/2024-05-01/2024-05-31"
        if params["page"] == 1:
            return (
                {
                    "data": [
                        {
                            "id": 1001,
                            "league_id": 71,
                            "season_id": 23265,
                            "starting_at": "2024-05-01 20:00:00",
                            "starting_at_timestamp": 1714593600,
                            "participants": [
                                {"id": 10, "name": "Home FC", "meta": {"location": "home"}},
                                {"id": 11, "name": "Away FC", "meta": {"location": "away"}},
                            ],
                            "scores": [],
                            "state": {"short_name": "FT", "name": "Finished"},
                            "venue": {"id": 7, "name": "Arena", "city_name": "Sao Paulo"},
                            "league": {"name": "Serie A"},
                            "season": {"id": 23265, "year": 2024, "name": "2024"},
                        }
                    ],
                    "pagination": {"has_more": True},
                    "rate_limit": {"remaining": 99},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "99"},
            )
        if params["page"] == 2:
            return (
                {
                    "data": [
                        {
                            "id": 1002,
                            "league_id": 71,
                            "season_id": 23265,
                            "starting_at": "2024-05-10 20:00:00",
                            "starting_at_timestamp": 1715367600,
                            "participants": [
                                {"id": 12, "name": "Home B", "meta": {"location": "home"}},
                                {"id": 13, "name": "Away B", "meta": {"location": "away"}},
                            ],
                            "scores": [],
                            "state": {"short_name": "FT", "name": "Finished"},
                            "venue": {"id": 8, "name": "Arena 2", "city_name": "Rio"},
                            "league": {"name": "Serie A"},
                            "season": {"id": 23265, "year": 2024, "name": "2024"},
                        }
                    ],
                    "pagination": {"has_more": False},
                    "rate_limit": {"remaining": 98},
                    "subscription": {},
                    "timezone": "UTC",
                },
                {"x-ratelimit-remaining": "98"},
            )
        raise AssertionError(f"pagina inesperada: {params['page']}")

    provider._request = _fake_request

    payload, headers = provider.get_fixtures(
        league_id=71,
        season=2024,
        date_from="2024-05-01",
        date_to="2024-05-31",
    )

    assert len(payload["response"]) == 2
    assert payload["provider_meta"]["pages_used"] == 2
    assert headers["x-ratelimit-remaining"] == "98"


def test_sportmonks_provider_team_sidelined_keeps_cross_year_second_half_entries():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        assert endpoint == "/teams/10"
        assert params == {"include": "sidelined"}
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
                            "season_id": 17420,
                            "start_date": "2020-09-15",
                            "end_date": None,
                            "games_missed": 2,
                            "completed": False,
                        },
                        {
                            "id": 602,
                            "player_id": 102,
                            "type_id": 7,
                            "category": "injury",
                            "season_id": 17420,
                            "start_date": "2021-01-10",
                            "end_date": None,
                            "games_missed": 4,
                            "completed": False,
                        },
                        {
                            "id": 603,
                            "player_id": 103,
                            "type_id": 7,
                            "category": "injury",
                            "season_id": 20000,
                            "start_date": "2022-01-10",
                            "end_date": None,
                            "games_missed": 1,
                            "completed": False,
                        },
                    ],
                }
            },
            {},
        )

    provider._request = _fake_request

    payload, _ = provider.get_team_sidelined(team_id=10, season=2020)

    assert [row["player"]["id"] for row in payload["response"]] == [101, 102]


def test_sportmonks_provider_team_sidelined_prefers_provider_season_id_over_date_fallback():
    provider = SportMonksProvider(api_key="test-key", base_url="https://api.test")

    def _fake_request(*, endpoint, params):
        assert endpoint == "/teams/10"
        assert params == {"include": "sidelined"}
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
                            "season_id": 17420,
                            "start_date": "2020-09-15",
                            "end_date": None,
                            "games_missed": 2,
                            "completed": False,
                        },
                        {
                            "id": 602,
                            "player_id": 102,
                            "type_id": 7,
                            "category": "injury",
                            "season_id": 17420,
                            "start_date": "2021-01-10",
                            "end_date": None,
                            "games_missed": 4,
                            "completed": False,
                        },
                        {
                            "id": 603,
                            "player_id": 103,
                            "type_id": 7,
                            "category": "injury",
                            "season_id": 16036,
                            "start_date": "2020-03-10",
                            "end_date": None,
                            "games_missed": 3,
                            "completed": False,
                        },
                    ],
                }
            },
            {},
        )

    provider._request = _fake_request

    payload, _ = provider.get_team_sidelined(team_id=10, season=2020, provider_season_id=17420)

    assert [row["player"]["id"] for row in payload["response"]] == [101, 102]

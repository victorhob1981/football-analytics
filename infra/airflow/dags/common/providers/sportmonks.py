from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import re
from typing import Any

from common.http_client import ProviderHttpClient

from .base import ProviderAdapter
from .envelope import build_envelope


SPORTMONKS_TO_CANONICAL_STAT_NAME = {
    "SHOTS_ON_TARGET": "shots_on_goal",
    "SHOTS_OFF_TARGET": "shots_off_goal",
    "SHOTS_TOTAL": "total_shots",
    "SHOTS_BLOCKED": "blocked_shots",
    "SHOTS_INSIDEBOX": "shots_inside_box",
    "SHOTS_OUTSIDEBOX": "shots_outside_box",
    "FOULS": "fouls",
    "CORNERS": "corner_kicks",
    "OFFSIDES": "offsides",
    "BALL_POSSESSION": "ball_possession",
    "YELLOWCARDS": "yellow_cards",
    "REDCARDS": "red_cards",
    "SAVES": "goalkeeper_saves",
    "PASSES": "total_passes",
    "SUCCESSFUL_PASSES": "passes_accurate",
    "SUCCESSFUL_PASSES_PERCENTAGE": "passes_pct",
}


class SportMonksProvider(ProviderAdapter):
    name = "sportmonks"

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        requests_per_minute: int | None = None,
    ):
        self._api_key = api_key
        self._client = ProviderHttpClient.from_env(
            provider=self.name,
            base_url=base_url,
            requests_per_minute=requests_per_minute,
        )

    def _request(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        payload, headers = self._client.request_json(
            endpoint=endpoint,
            params={"api_token": self._api_key, **params},
        )
        return payload, headers

    @staticmethod
    def _resolve_home_away(participants: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
        home = next((p for p in participants if (p.get("meta") or {}).get("location") == "home"), None)
        away = next((p for p in participants if (p.get("meta") or {}).get("location") == "away"), None)
        if home and away:
            return home, away

        fallback = list(participants[:2]) + [{}, {}]
        return fallback[0], fallback[1]

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _paginate_fixtures_between(
        self,
        *,
        date_from: str,
        date_to: str,
    ) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, Any]]:
        page = 1
        rows: list[dict[str, Any]] = []
        last_headers: dict[str, str] = {}
        last_meta: dict[str, Any] = {}

        while True:
            payload, headers = self._request(
                endpoint=f"/fixtures/between/{date_from}/{date_to}",
                params={
                    "include": "participants;scores;scores.type;league;season;venue;state;round;stage",
                    "per_page": 100,
                    "page": page,
                },
            )
            data = payload.get("data") or []
            if isinstance(data, dict):
                rows.append(data)
            elif isinstance(data, list):
                rows.extend(data)

            pagination = payload.get("pagination") or {}
            last_headers = headers
            last_meta = {
                "pagination": pagination,
                "subscription": payload.get("subscription", {}),
                "rate_limit": payload.get("rate_limit", {}),
                "timezone": payload.get("timezone"),
            }
            if not pagination.get("has_more"):
                break
            page += 1

        return rows, last_headers, last_meta

    @staticmethod
    def _extract_goals(
        scores: list[dict[str, Any]],
        *,
        home_team_id: int | None,
        away_team_id: int | None,
        preferred_descriptions: set[str] | None = None,
    ) -> tuple[int | None, int | None]:
        preferred = preferred_descriptions or {"CURRENT", "FT", "FULLTIME"}
        score_rows = [s for s in scores if str(s.get("description", "")).upper() in preferred]
        if not score_rows:
            score_rows = scores

        home_goals = None
        away_goals = None
        for row in score_rows:
            participant_id = row.get("participant_id")
            goals = (row.get("score") or {}).get("goals")
            if participant_id == home_team_id:
                home_goals = goals
            elif participant_id == away_team_id:
                away_goals = goals
            elif participant_id is None:
                participant_side = ((row.get("score") or {}).get("participant") or "").lower()
                if participant_side == "home":
                    home_goals = goals
                elif participant_side == "away":
                    away_goals = goals
        return home_goals, away_goals

    def _map_fixture_row(self, row: dict[str, Any], season: int) -> dict[str, Any]:
        participants = row.get("participants") or []
        home_team, away_team = self._resolve_home_away(participants)
        home_team_id = self._as_int(home_team.get("id"))
        away_team_id = self._as_int(away_team.get("id"))

        scores = row.get("scores") or []
        home_goals, away_goals = self._extract_goals(
            scores,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
        )
        home_goals_ht, away_goals_ht = self._extract_goals(
            scores,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            preferred_descriptions={"1ST_HALF", "1STHALF", "HALF_TIME", "HALFTIME", "HT"},
        )
        home_goals_ft, away_goals_ft = self._extract_goals(
            scores,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            preferred_descriptions={"2ND_HALF", "2NDHALF", "FT", "FULLTIME", "CURRENT"},
        )

        state = row.get("state") or {}
        venue = row.get("venue") or {}
        league = row.get("league") or {}
        round_info = row.get("round") or {}
        stage_info = row.get("stage") or {}
        weather = row.get("weather_report") or row.get("weatherreport") or {}

        round_name = round_info.get("name")
        stage_name = stage_info.get("name")
        if round_name and stage_name:
            league_round = f"{stage_name} - {round_name}"
        else:
            league_round = round_name or stage_name

        kickoff_raw = row.get("starting_at")
        kickoff_utc = None
        if isinstance(kickoff_raw, str):
            try:
                kickoff_utc = (
                    datetime.strptime(kickoff_raw, "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=timezone.utc)
                    .isoformat()
                )
            except ValueError:
                kickoff_utc = kickoff_raw

        return {
            "fixture": {
                "id": self._as_int(row.get("id")),
                "date": kickoff_utc,
                "timestamp": self._as_int(row.get("starting_at_timestamp")),
                "timezone": "UTC",
                "referee": row.get("referee_name"),
                "referee_id": self._as_int(row.get("referee_id")),
                "attendance": self._as_int(row.get("attendance")),
                "stage_id": self._as_int(row.get("stage_id")) or self._as_int(stage_info.get("id")),
                "round_id": self._as_int(row.get("round_id")) or self._as_int(round_info.get("id")),
                "weather": {
                    "description": weather.get("description"),
                    "temperature_c": self._as_float(weather.get("temperature")),
                    "wind_kph": self._as_float(weather.get("wind")),
                },
                "venue": {
                    "id": self._as_int(venue.get("id")),
                    "name": venue.get("name"),
                    "city": venue.get("city_name"),
                },
                "status": {
                    "short": state.get("short_name") or state.get("developer_name"),
                    "long": state.get("name"),
                },
            },
            "league": {
                "id": self._as_int(row.get("league_id")),
                "name": league.get("name"),
                "season": season,
                "round": league_round,
            },
            "teams": {
                "home": {"id": home_team_id, "name": home_team.get("name")},
                "away": {"id": away_team_id, "name": away_team.get("name")},
            },
            "goals": {
                "home": self._as_int(home_goals),
                "away": self._as_int(away_goals),
                "home_ht": self._as_int(home_goals_ht),
                "away_ht": self._as_int(away_goals_ht),
                "home_ft": self._as_int(home_goals_ft) if home_goals_ft is not None else self._as_int(home_goals),
                "away_ft": self._as_int(away_goals_ft) if away_goals_ft is not None else self._as_int(away_goals),
            },
        }

    @staticmethod
    def _season_matches(row: dict[str, Any], season: int) -> bool:
        if str(row.get("season_id")) == str(season):
            return True
        season_obj = row.get("season") or {}
        season_name = str(season_obj.get("name") or "")
        start_at = str(season_obj.get("starting_at") or "")
        end_at = str(season_obj.get("ending_at") or "")
        season_text = str(season)
        return season_text in season_name or start_at.startswith(season_text) or end_at.startswith(season_text)

    @staticmethod
    def _season_row_matches(season_row: dict[str, Any], season: int) -> bool:
        season_text = str(season)
        if str(season_row.get("id")) == season_text:
            return True
        if str(season_row.get("year")) == season_text:
            return True
        name = str(season_row.get("name") or "")
        start_at = str(season_row.get("starting_at") or "")
        end_at = str(season_row.get("ending_at") or "")
        return season_text in name or start_at.startswith(season_text) or end_at.startswith(season_text)

    def _resolve_season_id(self, *, league_id: int, season: int) -> int:
        payload, _headers = self._request(
            endpoint=f"/leagues/{league_id}",
            params={"include": "seasons"},
        )
        league_data = payload.get("data") or {}
        seasons = league_data.get("seasons") or []
        if isinstance(seasons, dict):
            seasons = [seasons]
        for season_row in seasons:
            if self._season_row_matches(season_row, season):
                season_id = self._as_int(season_row.get("id"))
                if season_id is not None:
                    return season_id
        raise RuntimeError(
            "Nao foi possivel resolver season_id no SportMonks "
            f"para league_id={league_id} season={season}."
        )

    def get_fixtures(
        self,
        *,
        league_id: int,
        season: int,
        date_from: str,
        date_to: str,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        rows, headers, provider_meta = self._paginate_fixtures_between(
            date_from=date_from,
            date_to=date_to,
        )
        filtered = [
            row
            for row in rows
            if str(row.get("league_id")) == str(league_id) and self._season_matches(row, season)
        ]
        mapped_rows = [self._map_fixture_row(row, season) for row in filtered]
        payload = build_envelope(
            provider=self.name,
            entity_type="fixtures",
            response=mapped_rows,
            source_params={
                "league_id": league_id,
                "season": season,
                "date_from": date_from,
                "date_to": date_to,
            },
            provider_meta={**provider_meta, "endpoint": "/fixtures/between/{from}/{to}"},
        )
        return payload, headers

    @staticmethod
    def _metric_name(stat_type: dict[str, Any]) -> str:
        developer_name = stat_type.get("developer_name")
        if developer_name in SPORTMONKS_TO_CANONICAL_STAT_NAME:
            return SPORTMONKS_TO_CANONICAL_STAT_NAME[developer_name]
        return stat_type.get("name") or str(stat_type.get("id") or "unknown")

    @staticmethod
    def _canonical_metric_name(raw_name: Any) -> str:
        if raw_name is None:
            return "unknown"
        normalized = re.sub(r"[^a-z0-9]+", "_", str(raw_name).strip().lower())
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized or "unknown"

    @classmethod
    def _lineup_detail_metric_name(cls, detail_type: dict[str, Any]) -> str:
        developer_name = detail_type.get("developer_name")
        if developer_name in SPORTMONKS_TO_CANONICAL_STAT_NAME:
            return SPORTMONKS_TO_CANONICAL_STAT_NAME[developer_name]
        if developer_name:
            return cls._canonical_metric_name(developer_name)
        return cls._canonical_metric_name(detail_type.get("name") or detail_type.get("code") or detail_type.get("id"))

    @staticmethod
    def _extract_stat_value(raw: Any) -> Any:
        if isinstance(raw, dict):
            if "value" in raw:
                return raw.get("value")
            if len(raw) == 1:
                return next(iter(raw.values()))
            return raw
        return raw

    def get_fixture_statistics(
        self,
        *,
        fixture_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/fixtures/{fixture_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={"include": "statistics;statistics.type;statistics.participant"},
        )
        fixture_data = payload.get("data") or {}
        stats_rows = fixture_data.get("statistics") or []
        grouped: dict[int, dict[str, Any]] = defaultdict(
            lambda: {"team": {"id": None, "name": None}, "statistics": []}
        )

        for stat in stats_rows:
            participant = stat.get("participant") or {}
            participant_id = self._as_int(stat.get("participant_id")) or self._as_int(participant.get("id"))
            if participant_id is None:
                continue
            group = grouped[participant_id]
            group["team"]["id"] = participant_id
            group["team"]["name"] = participant.get("name")

            stat_type = stat.get("type") or {}
            group["statistics"].append(
                {
                    "type": self._metric_name(stat_type),
                    "value": self._extract_stat_value(stat.get("data")),
                }
            )

        response_rows = list(grouped.values())
        canonical = build_envelope(
            provider=self.name,
            entity_type="statistics",
            response=response_rows,
            source_params={"fixture": fixture_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_fixture_events(
        self,
        *,
        fixture_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/fixtures/{fixture_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={
                "include": "events;events.type;events.participant;events.player;events.relatedplayer"
            },
        )
        fixture_data = payload.get("data") or {}
        events = fixture_data.get("events") or []
        response_rows = []
        for event in events:
            participant = event.get("participant") or {}
            event_type = event.get("type") or {}
            player = event.get("player") or {}
            related_player = event.get("relatedplayer") or {}
            detail = event.get("info") or event.get("addition") or event_type.get("name")
            response_rows.append(
                {
                    "time": {
                        "elapsed": self._as_int(event.get("minute")),
                        "extra": self._as_int(event.get("extra_minute")),
                    },
                    "team": {
                        "id": self._as_int(event.get("participant_id")) or self._as_int(participant.get("id")),
                        "name": participant.get("name"),
                    },
                    "player": {
                        "id": self._as_int(event.get("player_id")) or self._as_int(player.get("id")),
                        "name": event.get("player_name") or player.get("name"),
                    },
                    "assist": {
                        "id": self._as_int(event.get("related_player_id")) or self._as_int(related_player.get("id")),
                        "name": event.get("related_player_name") or related_player.get("name"),
                    },
                    "type": event_type.get("name") or event_type.get("developer_name"),
                    "detail": detail,
                    "comments": event.get("result"),
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="match_events",
            response=response_rows,
            source_params={"fixture": fixture_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_standings(
        self,
        *,
        league_id: int,
        season: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        season_id = self._resolve_season_id(league_id=league_id, season=season)
        endpoint = f"/standings/seasons/{season_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={"include": "participant;details.type"},
        )
        response_rows = payload.get("data") or []
        if isinstance(response_rows, dict):
            response_rows = [response_rows]
        canonical = build_envelope(
            provider=self.name,
            entity_type="standings",
            response=response_rows,
            source_params={
                "league_id": league_id,
                "season": season,
                "season_id": season_id,
            },
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def _resolve_season_context(
        self,
        *,
        league_id: int,
        season: int,
    ) -> tuple[int, dict[str, Any], dict[str, Any], dict[str, str]]:
        payload, headers = self._request(
            endpoint=f"/leagues/{league_id}",
            params={"include": "seasons"},
        )
        league_data = payload.get("data") or {}
        seasons = league_data.get("seasons") or []
        if isinstance(seasons, dict):
            seasons = [seasons]

        for season_row in seasons:
            if self._season_row_matches(season_row, season):
                season_id = self._as_int(season_row.get("id"))
                if season_id is not None:
                    return season_id, league_data, season_row, headers

        raise RuntimeError(
            "Nao foi possivel resolver season_id no SportMonks "
            f"para league_id={league_id} season={season}."
        )

    def get_competition_structure(
        self,
        *,
        league_id: int,
        season: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        season_id, league_data, season_row, league_headers = self._resolve_season_context(
            league_id=league_id,
            season=season,
        )
        stages_payload, stages_headers = self._request(
            endpoint=f"/stages/seasons/{season_id}",
            params={},
        )
        rounds_payload, rounds_headers = self._request(
            endpoint=f"/rounds/seasons/{season_id}",
            params={},
        )

        stages = stages_payload.get("data") or []
        rounds = rounds_payload.get("data") or []
        if isinstance(stages, dict):
            stages = [stages]
        if isinstance(rounds, dict):
            rounds = [rounds]

        response_rows = [
            {
                "league": league_data,
                "season": season_row,
                "season_id": season_id,
                "stages": stages,
                "rounds": rounds,
            }
        ]
        canonical = build_envelope(
            provider=self.name,
            entity_type="competition_structure",
            response=response_rows,
            source_params={
                "league_id": league_id,
                "season": season,
                "season_id": season_id,
            },
            provider_meta={
                "endpoint": "/leagues/{id} + /stages/seasons/{season_id} + /rounds/seasons/{season_id}",
                "rate_limit": {
                    "league": league_headers,
                    "stages": stages_payload.get("rate_limit", {}),
                    "rounds": rounds_payload.get("rate_limit", {}),
                },
                "subscription": {
                    "league": (league_data.get("subscription") if isinstance(league_data, dict) else None),
                    "stages": stages_payload.get("subscription", {}),
                    "rounds": rounds_payload.get("subscription", {}),
                },
                "headers": {
                    "league": league_headers,
                    "stages": stages_headers,
                    "rounds": rounds_headers,
                },
            },
        )
        return canonical, rounds_headers or stages_headers or league_headers

    def _fixture_lineups_payload(
        self,
        *,
        fixture_id: int,
    ) -> tuple[dict[str, Any], dict[str, str], str]:
        endpoint = f"/fixtures/{fixture_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={
                "include": (
                    "participants;"
                    "lineups;"
                    "lineups.player;"
                    "lineups.position;"
                    "lineups.details;"
                    "lineups.details.type"
                )
            },
        )
        return payload, headers, endpoint

    def get_fixture_lineups(
        self,
        *,
        fixture_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        payload, headers, endpoint = self._fixture_lineups_payload(fixture_id=fixture_id)
        fixture_data = payload.get("data") or {}
        participants = fixture_data.get("participants") or []
        team_names = {self._as_int(participant.get("id")): participant.get("name") for participant in participants}

        lineups = fixture_data.get("lineups") or []
        response_rows = []
        for lineup in lineups:
            team_id = self._as_int(lineup.get("team_id"))
            player = lineup.get("player") or {}
            position = lineup.get("position") or {}
            player_id = self._as_int(lineup.get("player_id")) or self._as_int(player.get("id"))
            details = []
            for detail in lineup.get("details") or []:
                detail_type = detail.get("type") or {}
                details.append(
                    {
                        "type": self._lineup_detail_metric_name(detail_type),
                        "raw_type_name": detail_type.get("name"),
                        "developer_name": detail_type.get("developer_name"),
                        "value": detail.get("value"),
                        "raw_value": detail.get("data"),
                    }
                )

            response_rows.append(
                {
                    "fixture_id": fixture_id,
                    "lineup_id": self._as_int(lineup.get("id")),
                    "team": {"id": team_id, "name": team_names.get(team_id)},
                    "player": {
                        "id": player_id,
                        "name": lineup.get("player_name") or player.get("display_name") or player.get("name"),
                    },
                    "position": {
                        "id": self._as_int(lineup.get("position_id")) or self._as_int(position.get("id")),
                        "name": position.get("name"),
                        "developer_name": position.get("developer_name"),
                    },
                    "formation_field": lineup.get("formation_field"),
                    "formation_position": lineup.get("formation_position"),
                    "lineup_type_id": self._as_int(lineup.get("type_id")),
                    "jersey_number": self._as_int(lineup.get("jersey_number")),
                    "details": details,
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="fixture_lineups",
            response=response_rows,
            source_params={"fixture": fixture_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_fixture_player_statistics(
        self,
        *,
        fixture_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        payload, headers, endpoint = self._fixture_lineups_payload(fixture_id=fixture_id)
        fixture_data = payload.get("data") or {}
        participants = fixture_data.get("participants") or []
        team_names = {self._as_int(participant.get("id")): participant.get("name") for participant in participants}
        lineups = fixture_data.get("lineups") or []

        grouped: dict[tuple[int | None, int | None], dict[str, Any]] = {}
        for lineup in lineups:
            team_id = self._as_int(lineup.get("team_id"))
            player = lineup.get("player") or {}
            player_id = self._as_int(lineup.get("player_id")) or self._as_int(player.get("id"))
            key = (team_id, player_id)
            if key not in grouped:
                grouped[key] = {
                    "fixture_id": fixture_id,
                    "team": {"id": team_id, "name": team_names.get(team_id)},
                    "player": {
                        "id": player_id,
                        "name": lineup.get("player_name") or player.get("display_name") or player.get("name"),
                    },
                    "statistics": [],
                }

            for detail in lineup.get("details") or []:
                detail_type = detail.get("type") or {}
                grouped[key]["statistics"].append(
                    {
                        "type": self._lineup_detail_metric_name(detail_type),
                        "raw_type_name": detail_type.get("name"),
                        "developer_name": detail_type.get("developer_name"),
                        "value": detail.get("value"),
                        "raw_value": detail.get("data"),
                    }
                )

        response_rows = list(grouped.values())
        canonical = build_envelope(
            provider=self.name,
            entity_type="fixture_player_statistics",
            response=response_rows,
            source_params={"fixture": fixture_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_player_season_statistics(
        self,
        *,
        player_id: int,
        season: int | None = None,
        league_id: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/players/{player_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={"include": "statistics.details.type;statistics.season;statistics.team;statistics.position"},
        )
        player_data = payload.get("data") or {}
        stats_rows = player_data.get("statistics") or []
        response_rows = []
        for stat in stats_rows:
            season_info = stat.get("season") or {}
            if season is not None and not self._season_row_matches(season_info, season):
                continue
            if league_id is not None and str(season_info.get("league_id")) != str(league_id):
                continue

            details = []
            for detail in stat.get("details") or []:
                detail_type = detail.get("type") or {}
                details.append(
                    {
                        "type": self._lineup_detail_metric_name(detail_type),
                        "raw_type_name": detail_type.get("name"),
                        "developer_name": detail_type.get("developer_name"),
                        "value": detail.get("value"),
                    }
                )

            response_rows.append(
                {
                    "player": {
                        "id": self._as_int(player_data.get("id")),
                        "name": player_data.get("display_name") or player_data.get("name"),
                    },
                    "team": {
                        "id": self._as_int((stat.get("team") or {}).get("id")),
                        "name": (stat.get("team") or {}).get("name"),
                    },
                    "season": {
                        "id": self._as_int(season_info.get("id")),
                        "name": season_info.get("name"),
                        "league_id": self._as_int(season_info.get("league_id")),
                        "starting_at": season_info.get("starting_at"),
                        "ending_at": season_info.get("ending_at"),
                    },
                    "position": stat.get("position") or {},
                    "statistics": details,
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="player_season_statistics",
            response=response_rows,
            source_params={"player_id": player_id, "season": season, "league_id": league_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_player_transfers(
        self,
        *,
        player_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/players/{player_id}"
        payload, headers = self._request(endpoint=endpoint, params={"include": "transfers"})
        player_data = payload.get("data") or {}
        transfers = player_data.get("transfers") or []
        response_rows = []
        for transfer in transfers:
            response_rows.append(
                {
                    "transfer_id": self._as_int(transfer.get("id")),
                    "player": {
                        "id": self._as_int(player_data.get("id")),
                        "name": player_data.get("display_name") or player_data.get("name"),
                    },
                    "from_team_id": self._as_int(transfer.get("from_team_id")),
                    "to_team_id": self._as_int(transfer.get("to_team_id")),
                    "type_id": self._as_int(transfer.get("type_id")),
                    "position_id": self._as_int(transfer.get("position_id")),
                    "date": transfer.get("date"),
                    "completed": transfer.get("completed"),
                    "career_ended": transfer.get("career_ended"),
                    "amount": transfer.get("amount"),
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="player_transfers",
            response=response_rows,
            source_params={"player_id": player_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_team_sidelined(
        self,
        *,
        team_id: int,
        season: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/teams/{team_id}"
        payload, headers = self._request(endpoint=endpoint, params={"include": "sidelined"})
        team_data = payload.get("data") or {}
        sidelined_rows = team_data.get("sidelined") or []
        response_rows = []
        for sidelined in sidelined_rows:
            start_date = str(sidelined.get("start_date") or "")
            season_id_raw = sidelined.get("season_id")
            if season is not None:
                if str(season_id_raw) != str(season) and not start_date.startswith(str(season)):
                    continue
            response_rows.append(
                {
                    "sidelined_id": self._as_int(sidelined.get("id")),
                    "team": {"id": self._as_int(team_data.get("id")), "name": team_data.get("name")},
                    "player": {"id": self._as_int(sidelined.get("player_id")), "name": None},
                    "type_id": self._as_int(sidelined.get("type_id")),
                    "category": sidelined.get("category"),
                    "season_id": self._as_int(season_id_raw),
                    "start_date": sidelined.get("start_date"),
                    "end_date": sidelined.get("end_date"),
                    "games_missed": self._as_int(sidelined.get("games_missed")),
                    "completed": sidelined.get("completed"),
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="team_sidelined",
            response=response_rows,
            source_params={"team_id": team_id, "season": season},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_team_coaches(
        self,
        *,
        team_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/teams/{team_id}"
        payload, headers = self._request(endpoint=endpoint, params={"include": "coaches"})
        team_data = payload.get("data") or {}
        coaches_rows = team_data.get("coaches") or []
        response_rows = []
        for coach in coaches_rows:
            response_rows.append(
                {
                    "team": {"id": self._as_int(team_data.get("id")), "name": team_data.get("name")},
                    "coach_id": self._as_int(coach.get("coach_id")),
                    "coach_tenure_id": self._as_int(coach.get("id")),
                    "position_id": self._as_int(coach.get("position_id")),
                    "active": bool(coach.get("active")) if coach.get("active") is not None else None,
                    "temporary": bool(coach.get("temporary")) if coach.get("temporary") is not None else None,
                    "start": coach.get("start"),
                    "end": coach.get("end"),
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="team_coaches",
            response=response_rows,
            source_params={"team_id": team_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

    def get_head_to_head(
        self,
        *,
        team_id: int,
        opponent_id: int,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/fixtures/head-to-head/{team_id}/{opponent_id}"
        payload, headers = self._request(
            endpoint=endpoint,
            params={"include": "participants;scores;scores.type;league;season;state;round;stage;venue"},
        )
        fixtures = payload.get("data") or []
        if isinstance(fixtures, dict):
            fixtures = [fixtures]

        mapped_rows = []
        for row in fixtures:
            participants = row.get("participants") or []
            home_team, away_team = self._resolve_home_away(participants)
            home_team_id = self._as_int(home_team.get("id"))
            away_team_id = self._as_int(away_team.get("id"))
            scores = row.get("scores") or []
            home_goals, away_goals = self._extract_goals(
                scores,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
            )
            state = row.get("state") or {}
            venue = row.get("venue") or {}
            league = row.get("league") or {}
            season_info = row.get("season") or {}
            round_info = row.get("round") or {}
            stage_info = row.get("stage") or {}
            round_name = round_info.get("name")
            stage_name = stage_info.get("name")
            if round_name and stage_name:
                league_round = f"{stage_name} - {round_name}"
            else:
                league_round = round_name or stage_name

            mapped_rows.append(
                {
                    "fixture": {
                        "id": self._as_int(row.get("id")),
                        "date": row.get("starting_at"),
                        "timestamp": self._as_int(row.get("starting_at_timestamp")),
                        "timezone": payload.get("timezone", "UTC"),
                        "referee": None,
                        "venue": {
                            "id": self._as_int(venue.get("id")),
                            "name": venue.get("name"),
                            "city": venue.get("city_name") or venue.get("city"),
                        },
                        "status": {
                            "short": state.get("short_name") or state.get("developer_name"),
                            "long": state.get("name"),
                        },
                    },
                    "league": {
                        "id": self._as_int(row.get("league_id")) or self._as_int(league.get("id")),
                        "name": league.get("name"),
                        "season": season_info.get("name"),
                        "season_id": self._as_int(row.get("season_id")) or self._as_int(season_info.get("id")),
                        "round": league_round,
                    },
                    "teams": {
                        "home": {"id": home_team_id, "name": home_team.get("name")},
                        "away": {"id": away_team_id, "name": away_team.get("name")},
                    },
                    "goals": {"home": self._as_int(home_goals), "away": self._as_int(away_goals)},
                }
            )

        canonical = build_envelope(
            provider=self.name,
            entity_type="head_to_head",
            response=mapped_rows,
            source_params={"team_id": team_id, "opponent_id": opponent_id},
            provider_meta={
                "endpoint": endpoint,
                "rate_limit": payload.get("rate_limit", {}),
                "subscription": payload.get("subscription", {}),
                "timezone": payload.get("timezone"),
            },
        )
        return canonical, headers

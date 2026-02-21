from __future__ import annotations

from typing import Any

import pandas as pd


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def build_competition_structure_dataframes(payloads: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    league_rows: list[dict[str, Any]] = []
    season_rows: list[dict[str, Any]] = []
    stage_rows: list[dict[str, Any]] = []
    round_rows: list[dict[str, Any]] = []

    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            league = row.get("league") or {}
            season = row.get("season") or {}
            season_id = _as_int(row.get("season_id")) or _as_int(season.get("id"))
            league_id = _as_int(league.get("id"))

            league_rows.append(
                {
                    "provider": provider,
                    "league_id": league_id,
                    "league_name": league.get("name"),
                    "country_id": _as_int(league.get("country_id")),
                    "payload": league,
                }
            )
            season_rows.append(
                {
                    "provider": provider,
                    "season_id": season_id,
                    "league_id": league_id,
                    "season_year": _as_int(str(season.get("name", ""))[:4]) if season.get("name") else None,
                    "season_name": season.get("name"),
                    "starting_at": season.get("starting_at"),
                    "ending_at": season.get("ending_at"),
                    "payload": season,
                }
            )

            for stage in row.get("stages") or []:
                stage_rows.append(
                    {
                        "provider": provider,
                        "stage_id": _as_int(stage.get("id")),
                        "season_id": _as_int(stage.get("season_id")) or season_id,
                        "league_id": _as_int(stage.get("league_id")) or league_id,
                        "stage_name": stage.get("name"),
                        "sort_order": _as_int(stage.get("sort_order")),
                        "finished": stage.get("finished"),
                        "is_current": stage.get("is_current"),
                        "starting_at": stage.get("starting_at"),
                        "ending_at": stage.get("ending_at"),
                        "payload": stage,
                    }
                )
            for round_info in row.get("rounds") or []:
                round_rows.append(
                    {
                        "provider": provider,
                        "round_id": _as_int(round_info.get("id")),
                        "stage_id": _as_int(round_info.get("stage_id")),
                        "season_id": _as_int(round_info.get("season_id")) or season_id,
                        "league_id": _as_int(round_info.get("league_id")) or league_id,
                        "round_name": round_info.get("name"),
                        "finished": round_info.get("finished"),
                        "is_current": round_info.get("is_current"),
                        "starting_at": round_info.get("starting_at"),
                        "ending_at": round_info.get("ending_at"),
                        "games_in_week": _as_int(round_info.get("games_in_current_week")),
                        "payload": round_info,
                    }
                )

    leagues = pd.DataFrame(league_rows)
    seasons = pd.DataFrame(season_rows)
    stages = pd.DataFrame(stage_rows)
    rounds = pd.DataFrame(round_rows)

    if leagues.empty:
        leagues = _empty_df(["provider", "league_id", "league_name", "country_id", "payload"])
    if seasons.empty:
        seasons = _empty_df(["provider", "season_id", "league_id", "season_year", "season_name", "starting_at", "ending_at", "payload"])
    if stages.empty:
        stages = _empty_df(
            ["provider", "stage_id", "season_id", "league_id", "stage_name", "sort_order", "finished", "is_current", "starting_at", "ending_at", "payload"]
        )
    if rounds.empty:
        rounds = _empty_df(
            [
                "provider",
                "round_id",
                "stage_id",
                "season_id",
                "league_id",
                "round_name",
                "finished",
                "is_current",
                "starting_at",
                "ending_at",
                "games_in_week",
                "payload",
            ]
        )

    leagues = leagues.dropna(subset=["league_id"]).drop_duplicates(subset=["provider", "league_id"], keep="last").copy()
    seasons = seasons.dropna(subset=["season_id"]).drop_duplicates(subset=["provider", "season_id"], keep="last").copy()
    stages = stages.dropna(subset=["stage_id"]).drop_duplicates(subset=["provider", "stage_id"], keep="last").copy()
    rounds = rounds.dropna(subset=["round_id"]).drop_duplicates(subset=["provider", "round_id"], keep="last").copy()

    return {
        "competition_leagues": leagues,
        "competition_seasons": seasons,
        "competition_stages": stages,
        "competition_rounds": rounds,
    }


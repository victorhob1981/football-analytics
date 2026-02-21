from __future__ import annotations

from typing import Any

import pandas as pd


def _flatten_fixtures_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    errors = payload.get("errors")
    if errors:
        raise RuntimeError(f"Erros no payload de fixtures: {errors}")

    rows: list[dict[str, Any]] = []
    fixtures = payload.get("response", []) or []
    source_provider = payload.get("provider")
    for item in fixtures:
        fixture = item.get("fixture", {}) or {}
        league = item.get("league", {}) or {}
        teams = item.get("teams", {}) or {}
        goals = item.get("goals", {}) or {}

        venue = fixture.get("venue") or {}
        status = fixture.get("status") or {}

        rows.append(
            {
                "fixture_id": fixture.get("id"),
                "source_provider": source_provider,
                "date_utc": fixture.get("date"),
                "timestamp": fixture.get("timestamp"),
                "timezone": fixture.get("timezone"),
                "referee": fixture.get("referee"),
                "referee_id": fixture.get("referee_id"),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("name"),
                "venue_city": venue.get("city"),
                "status_short": status.get("short"),
                "status_long": status.get("long"),
                "league_id": league.get("id"),
                "league_name": league.get("name"),
                "season": league.get("season"),
                "round": league.get("round"),
                "stage_id": fixture.get("stage_id"),
                "round_id": fixture.get("round_id"),
                "attendance": fixture.get("attendance"),
                "weather_description": (fixture.get("weather") or {}).get("description"),
                "weather_temperature_c": (fixture.get("weather") or {}).get("temperature_c"),
                "weather_wind_kph": (fixture.get("weather") or {}).get("wind_kph"),
                "home_team_id": (teams.get("home") or {}).get("id"),
                "home_team_name": (teams.get("home") or {}).get("name"),
                "away_team_id": (teams.get("away") or {}).get("id"),
                "away_team_name": (teams.get("away") or {}).get("name"),
                "home_goals": goals.get("home"),
                "away_goals": goals.get("away"),
                "home_goals_ht": goals.get("home_ht"),
                "away_goals_ht": goals.get("away_ht"),
                "home_goals_ft": goals.get("home_ft"),
                "away_goals_ft": goals.get("away_ft"),
            }
        )
    return rows


def build_fixtures_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    all_rows: list[dict[str, Any]] = []
    for payload in payloads:
        all_rows.extend(_flatten_fixtures_payload(payload))

    if not all_rows:
        raise RuntimeError("Nenhuma linha de fixtures gerada a partir dos payloads raw.")

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["fixture_id"]).drop_duplicates(subset=["fixture_id"]).copy()
    df["date"] = df["date_utc"].astype(str).str[:10]
    df["year"] = df["date"].str[:4]
    df["month"] = df["date"].str[5:7]
    return df

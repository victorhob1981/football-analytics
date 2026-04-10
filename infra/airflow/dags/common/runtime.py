from __future__ import annotations

from datetime import date, timedelta
from typing import Any
import os

from common.providers import get_default_league_id, get_default_provider, normalize_provider_name, provider_env_prefix


DEFAULT_LEAGUE_ID = get_default_league_id()
DEFAULT_SEASON = 2024
DEFAULT_PROVIDER = get_default_provider()

DEFAULT_FIXTURE_WINDOWS_BY_SEASON: dict[int, list[tuple[str, str]]] = {
    2024: [
        ("2024-04-13", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
        ("2024-10-01", "2024-12-08"),
    ]
}
CATALOG_SPLIT_YEAR_CHUNK_DAYS = 90


def _safe_int(value: Any, default_value: int, field_name: str) -> int:
    if value is None:
        return default_value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def _raw_runtime_inputs(context: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    return params, conf


def resolve_runtime_params(context: dict[str, Any]) -> dict[str, Any]:
    params, conf = _raw_runtime_inputs(context)
    raw_provider = conf.get("provider", params.get("provider", DEFAULT_PROVIDER))
    provider_value = str(raw_provider or "").strip()
    provider = normalize_provider_name(provider_value) if provider_value else DEFAULT_PROVIDER
    env_prefix = provider_env_prefix(provider)
    provider_default_league = get_default_league_id(provider)
    default_league = _safe_int(
        os.getenv(f"{env_prefix}_DEFAULT_LEAGUE_ID", str(provider_default_league)),
        provider_default_league,
        "default_league_id",
    )
    default_season = _safe_int(
        os.getenv(f"{env_prefix}_DEFAULT_SEASON", str(DEFAULT_SEASON)),
        DEFAULT_SEASON,
        "default_season",
    )
    league_id = _safe_int(
        conf.get("league_id", params.get("league_id", default_league)),
        default_league,
        "league_id",
    )
    season = _safe_int(
        conf.get("season", conf.get("season_id", params.get("season", params.get("season_id", default_season)))),
        default_season,
        "season",
    )
    return {
        "league_id": league_id,
        "season": season,
        "provider": provider,
    }


def _chunk_fixture_windows(
    season_start_date: str,
    season_end_date: str,
    *,
    chunk_days: int = CATALOG_SPLIT_YEAR_CHUNK_DAYS,
) -> list[tuple[str, str]]:
    start = date.fromisoformat(season_start_date)
    end = date.fromisoformat(season_end_date)
    if end < start:
        raise ValueError(
            f"Intervalo de season invalido no catalogo: start={season_start_date} end={season_end_date}"
        )

    windows: list[tuple[str, str]] = []
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end)
        windows.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(days=1)
    return windows


def _resolve_catalog_fixture_scope(
    *,
    provider_name: str,
    league_id: int,
    season: int,
) -> dict[str, str] | None:
    dsn = os.getenv("FOOTBALL_PG_DSN")
    if not dsn:
        return None

    from sqlalchemy import create_engine, text

    engine = create_engine(dsn)
    sql = text(
        """
        SELECT
          sc.season_label,
          sc.season_start_date,
          sc.season_end_date
        FROM control.competition_provider_map cpm
        JOIN control.season_catalog sc
          ON sc.provider = cpm.provider
         AND sc.competition_key = cpm.competition_key
        WHERE cpm.provider = :provider
          AND cpm.provider_league_id = :league_id
          AND LEFT(sc.season_label, 4) = :season_prefix
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(
            sql,
            {
                "provider": provider_name,
                "league_id": league_id,
                "season_prefix": str(season),
            },
        ).mappings().all()

    if not rows:
        return None
    if len(rows) > 1:
        sample = [dict(row) for row in rows[:10]]
        raise RuntimeError(
            "Catalogo de seasons ambiguo para fixture windows "
            f"provider={provider_name} league_id={league_id} season={season}. Escopos: {sample}"
        )

    scope = dict(rows[0])
    for field_name in ("season_start_date", "season_end_date"):
        raw_value = scope.get(field_name)
        if raw_value is not None:
            scope[field_name] = raw_value.isoformat() if hasattr(raw_value, "isoformat") else str(raw_value)
    return scope


def resolve_fixture_windows(
    context: dict[str, Any],
    season: int,
    *,
    provider_name: str | None = None,
    league_id: int | None = None,
) -> list[tuple[str, str]]:
    params, conf = _raw_runtime_inputs(context)
    if season < 1900 or season > 2100:
        raise ValueError(
            f"Parametro season invalido para janelas de fixtures: {season}. "
            "Use o ano da temporada (ex.: 2024), nao o season_id numerico do provider."
        )
    configured = conf.get("fixture_windows", params.get("fixture_windows"))
    if configured:
        windows: list[tuple[str, str]] = []
        for item in configured:
            if isinstance(item, dict):
                date_from = str(item.get("from", "")).strip()
                date_to = str(item.get("to", "")).strip()
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                date_from = str(item[0]).strip()
                date_to = str(item[1]).strip()
            else:
                raise ValueError(f"Formato invalido em fixture_windows: {item}")
            if not date_from or not date_to:
                raise ValueError(f"Janela invalida em fixture_windows: {item}")
            windows.append((date_from, date_to))
        if windows:
            return windows

    if provider_name is not None and league_id is not None:
        catalog_scope = _resolve_catalog_fixture_scope(
            provider_name=provider_name,
            league_id=league_id,
            season=season,
        )
        if catalog_scope:
            season_label = str(catalog_scope.get("season_label") or "").strip()
            season_start_date = str(catalog_scope.get("season_start_date") or "").strip()
            season_end_date = str(catalog_scope.get("season_end_date") or "").strip()
            if season_start_date and season_end_date:
                is_split_year_scope = "_" in season_label or season_start_date[:4] != season_end_date[:4]
                if is_split_year_scope:
                    return _chunk_fixture_windows(season_start_date, season_end_date)

    if season in DEFAULT_FIXTURE_WINDOWS_BY_SEASON:
        return DEFAULT_FIXTURE_WINDOWS_BY_SEASON[season]
    return [(f"{season}-01-01", f"{season}-12-31")]

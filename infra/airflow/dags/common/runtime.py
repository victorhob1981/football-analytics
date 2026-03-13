from __future__ import annotations

from typing import Any
import os

from common.competition_identity import derive_season_label
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

MIN_OPERATIONAL_SEASON_YEAR = 1900
MAX_OPERATIONAL_SEASON_YEAR = 2100


def _generic_fixture_windows(season: int) -> list[tuple[str, str]]:
    # Default generic window covers both year-calendar and cross-year competitions.
    return [
        (f"{season}-01-01", f"{season}-06-30"),
        (f"{season}-07-01", f"{season}-12-31"),
        (f"{season + 1}-01-01", f"{season + 1}-06-30"),
    ]


def _safe_int(value: Any, default_value: int, field_name: str) -> int:
    if value is None:
        return default_value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def _validate_operational_season_year(season: int, field_name: str) -> int:
    if season < MIN_OPERATIONAL_SEASON_YEAR or season > MAX_OPERATIONAL_SEASON_YEAR:
        raise ValueError(
            f"Parametro invalido para {field_name}: {season}. "
            "Use o ano da temporada (ex.: 2024), nao o season_id numerico do provider."
        )
    return season


def _resolve_runtime_season(raw_value: Any, default_value: int, field_name: str) -> tuple[int, str | None]:
    if raw_value is None:
        return default_value, None
    if isinstance(raw_value, str):
        candidate = raw_value.strip()
        if not candidate:
            return default_value, None
        if candidate.isdigit():
            return _validate_operational_season_year(int(candidate), field_name), None
        season_label = derive_season_label(season_name=candidate)
        if season_label is None:
            raise ValueError(f"Parametro invalido para {field_name}: {raw_value}")
        return _validate_operational_season_year(int(season_label.split("_", 1)[0]), field_name), season_label
    return _validate_operational_season_year(_safe_int(raw_value, default_value, field_name), field_name), None


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
    raw_season = conf.get("season", conf.get("season_id", params.get("season", params.get("season_id", default_season))))
    season, derived_season_label = _resolve_runtime_season(raw_season, default_season, "season")
    season = _validate_operational_season_year(season, "season")
    raw_season_label = conf.get("season_label", params.get("season_label"))
    season_label = None
    if raw_season_label is not None:
        season_label = derive_season_label(season_name=str(raw_season_label))
        if season_label is None:
            raise ValueError(f"Parametro invalido para season_label: {raw_season_label}")
    if season_label is None:
        season_label = derived_season_label
    if season_label is not None:
        season_from_label = int(season_label.split("_", 1)[0])
        if season != season_from_label:
            raise ValueError(
                "Parametros inconsistentes para temporada: "
                f"season={season} e season_label={season_label}."
            )
    provider_season_id = _optional_int(
        conf.get("provider_season_id", params.get("provider_season_id")),
        "provider_season_id",
    )
    return {
        "league_id": league_id,
        "season": season,
        "season_label": season_label,
        "provider_season_id": provider_season_id,
        "provider": provider,
    }


def resolve_fixture_windows(context: dict[str, Any], season: int) -> list[tuple[str, str]]:
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

    if season in DEFAULT_FIXTURE_WINDOWS_BY_SEASON:
        return DEFAULT_FIXTURE_WINDOWS_BY_SEASON[season]
    return _generic_fixture_windows(season)

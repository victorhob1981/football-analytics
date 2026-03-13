from __future__ import annotations

from datetime import date, datetime
from numbers import Integral
import re
from typing import Any


_CROSS_YEAR_PATTERN = re.compile(r"(?P<start>\d{4})\D+(?P<end>\d{2,4})")
_YEAR_PATTERN = re.compile(r"(?P<year>\d{4})")


def _is_missing(raw_value: Any) -> bool:
    if raw_value is None:
        return True
    if isinstance(raw_value, str):
        return not raw_value.strip()
    try:
        return raw_value != raw_value
    except Exception:
        return False


def _parse_date(raw_value: Any) -> date | None:
    if _is_missing(raw_value):
        return None
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
        candidate = raw_value.strip()
        if not candidate:
            return None
        candidate = candidate[:10]
        try:
            return datetime.strptime(candidate, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _normalize_end_year(raw_value: str, start_year: int) -> int | None:
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    if parsed < 100:
        century = (start_year // 100) * 100
        return century + parsed
    return parsed


def looks_like_provider_identifier(raw_value: Any) -> bool:
    if _is_missing(raw_value):
        return False
    candidate = str(raw_value).strip()
    return candidate.isdigit() and len(candidate) > 4


def derive_season_label(
    *,
    season: Any = None,
    season_name: Any = None,
    start_date: Any = None,
    end_date: Any = None,
) -> str | None:
    if isinstance(season_name, str):
        normalized_name = season_name.strip()
        if normalized_name:
            cross_year_match = _CROSS_YEAR_PATTERN.search(normalized_name)
            if cross_year_match:
                start_year = int(cross_year_match.group("start"))
                end_year = _normalize_end_year(cross_year_match.group("end"), start_year)
                if end_year is not None and end_year != start_year:
                    return f"{start_year}_{end_year % 100:02d}"
            year_match = _YEAR_PATTERN.search(normalized_name)
            if year_match:
                return year_match.group("year")

    parsed_start = _parse_date(start_date)
    parsed_end = _parse_date(end_date)
    if parsed_start and parsed_end:
        start_year = getattr(parsed_start, "year", None)
        end_year = getattr(parsed_end, "year", None)
        if isinstance(start_year, Integral) and isinstance(end_year, Integral):
            if start_year != end_year:
                return f"{start_year}_{end_year % 100:02d}"
            return str(start_year)

    if not _is_missing(season):
        if looks_like_provider_identifier(season):
            return None
        try:
            return str(int(season))
        except (TypeError, ValueError):
            pass

    return None

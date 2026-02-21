from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..core.filters import GlobalFilters, VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/players", tags=["players"])

PlayersSortBy = Literal["playerName", "minutesPlayed", "goals", "assists", "rating"]
SortDirection = Literal["asc", "desc"]


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _to_player_id(player_id: str) -> int:
    try:
        return int(player_id)
    except ValueError as exc:
        raise AppError(
            message="Invalid player id. Expected integer-compatible identifier.",
            code="INVALID_QUERY_PARAM",
            status=400,
            details={"playerId": player_id},
        ) from exc


def _player_scope_filters_sql(filters: GlobalFilters) -> tuple[str, list[Any]]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)

    if filters.venue == VenueFilter.home:
        where_clauses.append("pms.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        where_clauses.append("pms.team_id = fm.away_team_id")

    return " and ".join(where_clauses), params


def _player_coverage(filters: GlobalFilters) -> dict[str, Any]:
    match_where: list[str] = ["1=1"]
    match_params: list[Any] = []
    append_fact_match_filters(match_where, match_params, alias="fm", filters=filters)

    player_where, player_params = _player_scope_filters_sql(filters)
    query = f"""
        with scoped_matches as (
            select distinct fm.match_id
            from mart.fact_matches fm
            where {" and ".join(match_where)}
        ),
        scoped_stats as (
            select distinct pms.match_id
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where {player_where}
        )
        select
            (select count(*) from scoped_stats) as available_count,
            (select count(*) from scoped_matches) as total_count;
    """
    row = db_client.fetch_one(query, [*match_params, *player_params]) or {}
    available = int(row.get("available_count") or 0)
    total = int(row.get("total_count") or 0)
    return build_coverage_from_counts(available, total, "Player stats coverage")


@router.get("")
def get_players(
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    search: str | None = None,
    teamId: str | None = None,
    position: str | None = None,
    minMinutes: int | None = Query(default=None, ge=0),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
    sortBy: PlayersSortBy = "goals",
    sortDirection: SortDirection = "desc",
) -> dict[str, Any]:
    global_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=roundId,
        venue=venue,
        last_n=lastN,
        date_start=dateStart,
        date_end=dateEnd,
        date_range_start=dateRangeStart,
        date_range_end=dateRangeEnd,
    )

    where_sql, where_params = _player_scope_filters_sql(global_filters)
    sort_column = {
        "playerName": "f.player_name",
        "minutesPlayed": "f.minutes_played",
        "goals": "f.goals",
        "assists": "f.assists",
        "rating": "f.rating",
    }[sortBy]
    sort_dir = "asc" if sortDirection == "asc" else "desc"
    offset = (page - 1) * pageSize

    team_id_int: int | None = None
    if teamId is not None and teamId.strip() != "":
        if not teamId.strip().isdigit():
            raise AppError(
                message="Invalid value for 'teamId'. Expected integer.",
                code="INVALID_QUERY_PARAM",
                status=400,
                details={"teamId": teamId},
            )
        team_id_int = int(teamId)
    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    position_pattern = f"%{position.strip()}%" if position and position.strip() else None

    query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_id,
                pms.team_name,
                pms.position_name,
                pms.match_id,
                pms.match_date,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                coalesce(pms.shots_total, 0) as shots_total,
                coalesce(pms.yellow_cards, 0) as yellow_cards,
                coalesce(pms.red_cards, 0) as red_cards,
                pms.rating,
                row_number() over (
                    partition by pms.player_id
                    order by pms.match_date desc, pms.match_id desc
                ) as rn_recent
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where {where_sql}
        ),
        filtered_scoped as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        aggregated as (
            select
                fs.player_id,
                max(fs.player_name) as player_name,
                count(distinct fs.match_id) as matches_played,
                sum(fs.minutes_played)::numeric as minutes_played,
                sum(fs.goals)::numeric as goals,
                sum(fs.assists)::numeric as assists,
                sum(fs.shots_total)::numeric as shots_total,
                sum(fs.yellow_cards)::numeric as yellow_cards,
                sum(fs.red_cards)::numeric as red_cards,
                avg(fs.rating)::numeric as rating
            from filtered_scoped fs
            group by fs.player_id
        ),
        latest_context as (
            select distinct on (fs.player_id)
                fs.player_id,
                fs.team_id,
                fs.team_name,
                fs.position_name
            from filtered_scoped fs
            order by fs.player_id, fs.match_date desc, fs.match_id desc
        ),
        final_rows as (
            select
                a.player_id,
                a.player_name,
                lc.team_id,
                lc.team_name,
                lc.position_name,
                a.matches_played,
                a.minutes_played,
                a.goals,
                a.assists,
                a.shots_total,
                a.yellow_cards,
                a.red_cards,
                a.rating
            from aggregated a
            left join latest_context lc
              on lc.player_id = a.player_id
            where (%s::text is null or a.player_name ilike %s)
              and (%s::bigint is null or lc.team_id = %s)
              and (%s::text is null or coalesce(lc.position_name, '') ilike %s)
              and (%s::numeric is null or a.minutes_played >= %s)
        )
        select
            f.*,
            count(*) over() as _total_count
        from final_rows f
        order by {sort_column} {sort_dir}, f.player_id asc
        limit %s offset %s;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            search_pattern,
            search_pattern,
            team_id_int,
            team_id_int,
            position_pattern,
            position_pattern,
            minMinutes,
            minMinutes,
            pageSize,
            offset,
        ],
    )

    total_count = int(rows[0]["_total_count"]) if rows else 0
    pagination = build_pagination(page, pageSize, total_count)
    coverage = _player_coverage(global_filters)

    items = [
        {
            "playerId": str(row["player_id"]),
            "playerName": row["player_name"],
            "teamId": str(row["team_id"]) if row.get("team_id") is not None else None,
            "teamName": row.get("team_name"),
            "position": row.get("position_name"),
            "nationality": None,
            "matchesPlayed": int(row.get("matches_played") or 0),
            "minutesPlayed": float(row.get("minutes_played") or 0),
            "goals": float(row.get("goals") or 0),
            "assists": float(row.get("assists") or 0),
            "shotsTotal": float(row.get("shots_total") or 0),
            "passAccuracyPct": None,
            "yellowCards": float(row.get("yellow_cards") or 0),
            "redCards": float(row.get("red_cards") or 0),
            "rating": float(row["rating"]) if row.get("rating") is not None else None,
        }
        for row in rows
    ]

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        pagination=pagination,
        coverage=coverage,
    )


def _profile_coverage(player_id: int, filters: GlobalFilters) -> dict[str, Any]:
    where_mart, params_mart = _player_scope_filters_sql(filters)
    mart_query = f"""
        select count(distinct pms.match_id) as count_matches
        from mart.player_match_summary pms
        inner join mart.fact_matches fm
          on fm.match_id = pms.match_id
        where pms.player_id = %s
          and {where_mart};
    """
    mart_row = db_client.fetch_one(mart_query, [player_id, *params_mart]) or {}
    available = int(mart_row.get("count_matches") or 0)

    raw_clauses: list[str] = ["1=1", "fps.player_id = %s"]
    raw_params: list[Any] = [player_id]
    append_fact_match_filters(raw_clauses, raw_params, alias="fm", filters=filters)
    if filters.venue == VenueFilter.home:
        raw_clauses.append("fps.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        raw_clauses.append("fps.team_id = fm.away_team_id")

    raw_query = f"""
        select count(distinct fps.fixture_id) as count_matches
        from raw.fixture_player_statistics fps
        inner join mart.fact_matches fm
          on fm.match_id = fps.fixture_id
        where {" and ".join(raw_clauses)};
    """
    raw_row = db_client.fetch_one(raw_query, raw_params) or {}
    total = int(raw_row.get("count_matches") or 0)

    return build_coverage_from_counts(available, total, "Player profile coverage")


@router.get("/{playerId}")
def get_player_profile(
    playerId: str,
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    includeRecentMatches: bool = True,
    recentMatchesLimit: int = Query(default=10, ge=1, le=50),
) -> dict[str, Any]:
    player_id = _to_player_id(playerId)
    global_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=roundId,
        venue=venue,
        last_n=lastN,
        date_start=dateStart,
        date_end=dateEnd,
        date_range_start=dateRangeStart,
        date_range_end=dateRangeEnd,
    )

    player_ref = db_client.fetch_one(
        "select player_id, player_name from mart.dim_player where player_id = %s;",
        [player_id],
    )
    if player_ref is None:
        raise AppError(
            message="Player not found.",
            code="PLAYER_NOT_FOUND",
            status=404,
            details={"playerId": playerId},
        )

    where_sql, where_params = _player_scope_filters_sql(global_filters)
    summary_query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_id,
                pms.team_name,
                pms.position_name,
                pms.match_id,
                pms.match_date,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                coalesce(pms.shots_total, 0) as shots_total,
                coalesce(pms.shots_on_goal, 0) as shots_on_goal,
                coalesce(pms.passes_total, 0) as passes_attempted,
                coalesce(pms.yellow_cards, 0) as yellow_cards,
                coalesce(pms.red_cards, 0) as red_cards,
                pms.rating,
                row_number() over (
                    partition by pms.player_id
                    order by pms.match_date desc, pms.match_id desc
                ) as rn_recent
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where pms.player_id = %s
              and {where_sql}
        ),
        filtered_scoped as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        latest_context as (
            select
                fs.team_id,
                fs.team_name,
                fs.position_name
            from filtered_scoped fs
            order by fs.match_date desc, fs.match_id desc
            limit 1
        )
        select
            (select team_id from latest_context) as team_id,
            (select team_name from latest_context) as team_name,
            (select position_name from latest_context) as position_name,
            count(distinct fs.match_id) as matches_played,
            sum(fs.minutes_played)::numeric as minutes_played,
            sum(fs.goals)::numeric as goals,
            sum(fs.assists)::numeric as assists,
            sum(fs.shots_total)::numeric as shots_total,
            sum(fs.shots_on_goal)::numeric as shots_on_target,
            sum(fs.passes_attempted)::numeric as passes_attempted,
            sum(fs.yellow_cards)::numeric as yellow_cards,
            sum(fs.red_cards)::numeric as red_cards,
            avg(fs.rating)::numeric as rating
        from filtered_scoped fs;
    """

    summary_row = db_client.fetch_one(
        summary_query,
        [
            player_id,
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
        ],
    ) or {}

    recent_matches: list[dict[str, Any]] | None = None
    if includeRecentMatches:
        recent_query = f"""
            with scoped as (
                select
                    pms.player_id,
                    pms.team_id,
                    pms.match_id,
                    pms.match_date,
                    coalesce(pms.minutes_played, 0) as minutes_played,
                    coalesce(pms.goals, 0) as goals,
                    coalesce(pms.assists, 0) as assists,
                    pms.rating,
                    row_number() over (
                        partition by pms.player_id
                        order by pms.match_date desc, pms.match_id desc
                    ) as rn_recent
                from mart.player_match_summary pms
                inner join mart.fact_matches fm
                  on fm.match_id = pms.match_id
                where pms.player_id = %s
                  and {where_sql}
            ),
            filtered_scoped as (
                select *
                from scoped
                where (%s::int is null or rn_recent <= %s)
            )
            select
                fs.match_id::text as fixture_id,
                fs.match_date as played_at,
                case
                    when fs.team_id = fm.home_team_id then away_team.team_name
                    else home_team.team_name
                end as opponent_name,
                fs.minutes_played,
                fs.goals,
                fs.assists,
                fs.rating
            from filtered_scoped fs
            inner join mart.fact_matches fm
              on fm.match_id = fs.match_id
            left join mart.dim_team home_team
              on home_team.team_id = fm.home_team_id
            left join mart.dim_team away_team
              on away_team.team_id = fm.away_team_id
            order by fs.match_date desc, fs.match_id desc
            limit %s;
        """
        recent_rows = db_client.fetch_all(
            recent_query,
            [
                player_id,
                *where_params,
                global_filters.last_n,
                global_filters.last_n,
                recentMatchesLimit,
            ],
        )
        recent_matches = [
            {
                "fixtureId": row["fixture_id"],
                "playedAt": row.get("played_at"),
                "opponentName": row.get("opponent_name"),
                "minutesPlayed": float(row.get("minutes_played") or 0),
                "goals": float(row.get("goals") or 0),
                "assists": float(row.get("assists") or 0),
                "rating": float(row["rating"]) if row.get("rating") is not None else None,
            }
            for row in recent_rows
        ]

    coverage = _profile_coverage(player_id, global_filters)

    player_payload = {
        "playerId": str(player_ref["player_id"]),
        "playerName": player_ref["player_name"],
        "teamId": str(summary_row["team_id"]) if summary_row.get("team_id") is not None else None,
        "teamName": summary_row.get("team_name"),
        "position": summary_row.get("position_name"),
        "nationality": None,
    }
    summary_payload = {
        "matchesPlayed": int(summary_row.get("matches_played") or 0),
        "minutesPlayed": float(summary_row.get("minutes_played") or 0),
        "goals": float(summary_row.get("goals") or 0),
        "assists": float(summary_row.get("assists") or 0),
        "shotsTotal": float(summary_row.get("shots_total") or 0),
        "shotsOnTarget": float(summary_row.get("shots_on_target") or 0),
        "passesCompleted": None,
        "passesAttempted": float(summary_row.get("passes_attempted") or 0),
        "passAccuracyPct": None,
        "yellowCards": float(summary_row.get("yellow_cards") or 0),
        "redCards": float(summary_row.get("red_cards") or 0),
        "rating": float(summary_row["rating"]) if summary_row.get("rating") is not None else None,
    }

    data: dict[str, Any] = {
        "player": player_payload,
        "summary": summary_payload,
    }
    if includeRecentMatches:
        data["recentMatches"] = recent_matches or []

    return build_api_response(
        data,
        request_id=_request_id(request),
        coverage=coverage,
    )

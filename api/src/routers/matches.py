from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..core.filters import GlobalFilters, VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/matches", tags=["matches"])

MatchesSortBy = Literal["kickoffAt", "status", "homeTeamName", "awayTeamName"]
SortDirection = Literal["asc", "desc"]


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _to_match_id(match_id: str) -> int:
    try:
        return int(match_id)
    except ValueError as exc:
        raise AppError(
            message="Invalid match id. Expected integer-compatible identifier.",
            code="INVALID_QUERY_PARAM",
            status=400,
            details={"matchId": match_id},
        ) from exc


def _match_filters_sql(filters: GlobalFilters) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(clauses, params, alias="fm", filters=filters)
    return " and ".join(clauses), params


@router.get("")
def get_matches(
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
    status: str | None = None,
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
    sortBy: MatchesSortBy = "kickoffAt",
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

    where_sql, where_params = _match_filters_sql(global_filters)
    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    status_pattern = f"%{status.strip()}%" if status and status.strip() else None
    sort_column = {
        "kickoffAt": "f.kickoff_at",
        "status": "f.status",
        "homeTeamName": "f.home_team_name",
        "awayTeamName": "f.away_team_name",
    }[sortBy]
    sort_dir = "asc" if sortDirection == "asc" else "desc"
    offset = (page - 1) * pageSize

    query = f"""
        with scoped_matches as (
            select
                fm.match_id,
                fm.league_id,
                fm.season,
                fm.round_number,
                fm.date_day,
                fm.home_team_id,
                fm.away_team_id,
                fm.home_goals,
                fm.away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered_matches as (
            select *
            from scoped_matches sm
            where (%s::int is null or sm.rn_recent <= %s)
        ),
        enriched as (
            select
                fm.match_id::text as match_id,
                fm.match_id::text as fixture_id,
                fm.league_id::text as competition_id,
                coalesce(dc.league_name, fm.league_id::text) as competition_name,
                fm.season::text as season_id,
                fm.round_number::text as round_id,
                rf.date_utc as kickoff_at,
                coalesce(rf.status_short, rf.status_long) as status,
                dv.venue_name as venue_name,
                fm.home_team_id::text as home_team_id,
                home_team.team_name as home_team_name,
                fm.away_team_id::text as away_team_id,
                away_team.team_name as away_team_name,
                fm.home_goals as home_score,
                fm.away_goals as away_score
            from filtered_matches fm
            left join raw.fixtures rf
              on rf.fixture_id = fm.match_id
            left join mart.dim_competition dc
              on dc.league_id = fm.league_id
            left join mart.dim_team home_team
              on home_team.team_id = fm.home_team_id
            left join mart.dim_team away_team
              on away_team.team_id = fm.away_team_id
            left join mart.dim_venue dv
              on dv.venue_id = rf.venue_id
        ),
        filtered_rows as (
            select *
            from enriched e
            where (%s::text is null or e.home_team_name ilike %s or e.away_team_name ilike %s)
              and (%s::text is null or coalesce(e.status, '') ilike %s)
        )
        select
            f.*,
            count(*) over() as _total_count
        from filtered_rows f
        order by {sort_column} {sort_dir} nulls last, f.match_id desc
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
            search_pattern,
            status_pattern,
            status_pattern,
            pageSize,
            offset,
        ],
    )
    total_count = int(rows[0]["_total_count"]) if rows else 0
    pagination = build_pagination(page, pageSize, total_count)

    items = [
        {
            "matchId": row["match_id"],
            "fixtureId": row["fixture_id"],
            "competitionId": row["competition_id"],
            "competitionName": row["competition_name"],
            "seasonId": row["season_id"],
            "roundId": row["round_id"],
            "kickoffAt": row.get("kickoff_at"),
            "status": row.get("status"),
            "venueName": row.get("venue_name"),
            "homeTeamId": row["home_team_id"],
            "homeTeamName": row.get("home_team_name"),
            "awayTeamId": row["away_team_id"],
            "awayTeamName": row.get("away_team_name"),
            "homeScore": row.get("home_score"),
            "awayScore": row.get("away_score"),
        }
        for row in rows
    ]

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        pagination=pagination,
        coverage=None,
    )


def _build_match_sections_coverage(
    *,
    include_timeline: bool,
    include_lineups: bool,
    include_player_stats: bool,
    timeline_count: int,
    lineups_count: int,
    player_stats_count: int,
) -> dict[str, Any]:
    requested = 0
    available = 0

    if include_timeline:
        requested += 1
        if timeline_count > 0:
            available += 1
    if include_lineups:
        requested += 1
        if lineups_count > 0:
            available += 1
    if include_player_stats:
        requested += 1
        if player_stats_count > 0:
            available += 1

    label = "Match center sections coverage"
    return build_coverage_from_counts(available, requested, label)


@router.get("/{matchId}")
def get_match_center(
    matchId: str,
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
    includeTimeline: bool = True,
    includeLineups: bool = True,
    includePlayerStats: bool = True,
) -> dict[str, Any]:
    match_id = _to_match_id(matchId)
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

    match_where, match_params = _match_filters_sql(global_filters)
    match_query = f"""
        select
            fm.match_id::text as match_id,
            fm.match_id::text as fixture_id,
            fm.league_id::text as competition_id,
            coalesce(dc.league_name, fm.league_id::text) as competition_name,
            fm.season::text as season_id,
            fm.round_number::text as round_id,
            rf.date_utc as kickoff_at,
            coalesce(rf.status_short, rf.status_long) as status,
            dv.venue_name as venue_name,
            fm.home_team_id::text as home_team_id,
            home_team.team_name as home_team_name,
            fm.away_team_id::text as away_team_id,
            away_team.team_name as away_team_name,
            fm.home_goals as home_score,
            fm.away_goals as away_score
        from mart.fact_matches fm
        left join raw.fixtures rf
          on rf.fixture_id = fm.match_id
        left join mart.dim_competition dc
          on dc.league_id = fm.league_id
        left join mart.dim_team home_team
          on home_team.team_id = fm.home_team_id
        left join mart.dim_team away_team
          on away_team.team_id = fm.away_team_id
        left join mart.dim_venue dv
          on dv.venue_id = rf.venue_id
        where fm.match_id = %s
          and {match_where}
        limit 1;
    """
    match_row = db_client.fetch_one(match_query, [match_id, *match_params])
    if match_row is None:
        raise AppError(
            message="Match not found.",
            code="MATCH_NOT_FOUND",
            status=404,
            details={"matchId": matchId},
        )

    data: dict[str, Any] = {
        "match": {
            "matchId": match_row["match_id"],
            "fixtureId": match_row["fixture_id"],
            "competitionId": match_row["competition_id"],
            "competitionName": match_row["competition_name"],
            "seasonId": match_row["season_id"],
            "roundId": match_row["round_id"],
            "kickoffAt": match_row.get("kickoff_at"),
            "status": match_row.get("status"),
            "venueName": match_row.get("venue_name"),
            "homeTeamId": match_row["home_team_id"],
            "homeTeamName": match_row.get("home_team_name"),
            "awayTeamId": match_row["away_team_id"],
            "awayTeamName": match_row.get("away_team_name"),
            "homeScore": match_row.get("home_score"),
            "awayScore": match_row.get("away_score"),
        }
    }

    timeline_rows: list[dict[str, Any]] = []
    lineup_rows: list[dict[str, Any]] = []
    player_stat_rows: list[dict[str, Any]] = []

    if includeTimeline:
        timeline_query = """
            select
                fme.event_id,
                fme.time_elapsed as minute,
                cast(null as integer) as second,
                cast(null as text) as period,
                fme.event_type as type,
                fme.event_detail as detail,
                fme.team_id::text as team_id,
                team.team_name,
                fme.player_id::text as player_id,
                player.player_name
            from mart.fact_match_events fme
            left join mart.dim_team team
              on team.team_id = fme.team_id
            left join mart.dim_player player
              on player.player_id = fme.player_id
            where fme.match_id = %s
            order by fme.time_elapsed asc nulls last, fme.event_id asc;
        """
        timeline_result = db_client.fetch_all(timeline_query, [match_id])
        timeline_rows = [
            {
                "eventId": row.get("event_id"),
                "minute": row.get("minute"),
                "second": row.get("second"),
                "period": row.get("period"),
                "type": row.get("type"),
                "detail": row.get("detail"),
                "teamId": row.get("team_id"),
                "teamName": row.get("team_name"),
                "playerId": row.get("player_id"),
                "playerName": row.get("player_name"),
            }
            for row in timeline_result
        ]
        data["timeline"] = timeline_rows

    if includeLineups:
        lineups_query = """
            select
                ffl.player_id::text as player_id,
                ffl.player_name,
                ffl.team_id::text as team_id,
                team.team_name as team_name,
                ffl.position_name as position,
                ffl.jersey_number as shirt_number,
                ffl.is_starter
            from mart.fact_fixture_lineups ffl
            left join mart.dim_team team
              on team.team_id = ffl.team_id
            where ffl.match_id = %s
            order by ffl.team_id asc, ffl.is_starter desc, ffl.jersey_number asc nulls last;
        """
        lineups_result = db_client.fetch_all(lineups_query, [match_id])
        lineup_rows = [
            {
                "playerId": row.get("player_id"),
                "playerName": row.get("player_name"),
                "teamId": row.get("team_id"),
                "teamName": row.get("team_name"),
                "position": row.get("position"),
                "shirtNumber": row.get("shirt_number"),
                "isStarter": row.get("is_starter"),
            }
            for row in lineups_result
        ]
        data["lineups"] = lineup_rows

    if includePlayerStats:
        player_stats_query = """
            select
                fps.player_id::text as player_id,
                fps.player_name,
                fps.team_id::text as team_id,
                coalesce(fps.team_name, team.team_name) as team_name,
                fps.minutes_played,
                fps.goals,
                fps.assists,
                fps.shots_total,
                cast(null as numeric) as passes_completed,
                fps.rating
            from mart.fact_fixture_player_stats fps
            left join mart.dim_team team
              on team.team_id = fps.team_id
            where fps.match_id = %s
            order by fps.team_id asc, fps.player_name asc;
        """
        stats_result = db_client.fetch_all(player_stats_query, [match_id])
        player_stat_rows = [
            {
                "playerId": row.get("player_id"),
                "playerName": row.get("player_name"),
                "teamId": row.get("team_id"),
                "teamName": row.get("team_name"),
                "minutesPlayed": float(row["minutes_played"]) if row.get("minutes_played") is not None else None,
                "goals": float(row["goals"]) if row.get("goals") is not None else None,
                "assists": float(row["assists"]) if row.get("assists") is not None else None,
                "shotsTotal": float(row["shots_total"]) if row.get("shots_total") is not None else None,
                "passesCompleted": row.get("passes_completed"),
                "rating": float(row["rating"]) if row.get("rating") is not None else None,
            }
            for row in stats_result
        ]
        data["playerStats"] = player_stat_rows

    coverage = _build_match_sections_coverage(
        include_timeline=includeTimeline,
        include_lineups=includeLineups,
        include_player_stats=includePlayerStats,
        timeline_count=len(timeline_rows),
        lineups_count=len(lineup_rows),
        player_stats_count=len(player_stat_rows),
    )

    return build_api_response(
        data,
        request_id=_request_id(request),
        coverage=coverage,
    )

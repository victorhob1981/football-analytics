from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_pagination
from ..core.errors import AppError
from ..core.filters import GlobalFilters, VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/clubs", tags=["clubs"])

ClubsOrderBy = Literal["points", "goalsFor", "goalsAgainst"]


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _to_team_id(team_id: str) -> int:
    try:
        return int(team_id)
    except ValueError as exc:
        raise AppError(
            message="Invalid club id. Expected integer-compatible identifier.",
            code="INVALID_QUERY_PARAM",
            status=400,
            details={"clubId": team_id},
        ) from exc


def _match_filters_sql(filters: GlobalFilters) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(clauses, params, alias="fm", filters=filters)
    return " and ".join(clauses), params


@router.get("")
def get_clubs(
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
    month: str | None = None,
    search: str | None = None,
    orderBy: ClubsOrderBy = "points",
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
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
        month=month,
    )

    where_sql, where_params = _match_filters_sql(global_filters)
    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    offset = (page - 1) * pageSize
    venue_scope = global_filters.venue.value

    sort_clause = (
        "j.goals_against asc, j.points desc, j.goal_difference desc, j.goals_for desc"
        if orderBy == "goalsAgainst"
        else "j.goals_for desc, j.points desc, j.goal_difference desc"
        if orderBy == "goalsFor"
        else "j.points desc, j.goal_difference desc, j.goals_for desc"
    )

    query = f"""
        with scoped_matches as (
            select
                fm.match_id,
                fm.season,
                fm.round_number,
                fm.date_day,
                fm.home_team_id,
                fm.away_team_id,
                coalesce(fm.home_goals, 0) as home_goals,
                coalesce(fm.away_goals, 0) as away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered_matches as (
            select *
            from scoped_matches sm
            where (%s::int is null or sm.rn_recent <= %s)
        ),
        team_matches as (
            select
                fm.match_id,
                fm.season,
                fm.round_number,
                fm.date_day,
                fm.home_team_id as team_id,
                'home'::text as venue_scope,
                fm.home_goals as goals_for,
                fm.away_goals as goals_against,
                case
                    when fm.home_goals > fm.away_goals then 'W'
                    when fm.home_goals = fm.away_goals then 'D'
                    else 'L'
                end as result
            from filtered_matches fm

            union all

            select
                fm.match_id,
                fm.season,
                fm.round_number,
                fm.date_day,
                fm.away_team_id as team_id,
                'away'::text as venue_scope,
                fm.away_goals as goals_for,
                fm.home_goals as goals_against,
                case
                    when fm.away_goals > fm.home_goals then 'W'
                    when fm.away_goals = fm.home_goals then 'D'
                    else 'L'
                end as result
            from filtered_matches fm
        ),
        venue_filtered as (
            select *
            from team_matches tm
            where (%s::text = 'all' or tm.venue_scope = %s)
        ),
        recent_form_rows as (
            select
                tm.*,
                row_number() over (
                    partition by tm.team_id
                    order by tm.date_day desc nulls last, tm.match_id desc
                ) as rn_form
            from venue_filtered tm
        ),
        recent_form as (
            select
                team_id,
                array_agg(result order by date_day desc nulls last, match_id desc) filter (where rn_form <= 5) as recent_form
            from recent_form_rows
            group by team_id
        ),
        aggregated as (
            select
                tm.team_id,
                count(*)::int as matches_played,
                sum(case when tm.result = 'W' then 1 else 0 end)::int as wins,
                sum(case when tm.result = 'D' then 1 else 0 end)::int as draws,
                sum(case when tm.result = 'L' then 1 else 0 end)::int as losses,
                sum(tm.goals_for)::int as goals_for,
                sum(tm.goals_against)::int as goals_against,
                sum(case when tm.result = 'W' then 3 when tm.result = 'D' then 1 else 0 end)::int as points,
                (sum(tm.goals_for) - sum(tm.goals_against))::int as goal_difference
            from venue_filtered tm
            group by tm.team_id
        ),
        season_context as (
            select coalesce(%s::int, max(fm.season)) as season_id
            from filtered_matches fm
        ),
        standings_scope as (
            select
                se.team_id,
                se.position,
                row_number() over (
                    partition by se.team_id
                    order by se.round desc
                ) as rn
            from mart.standings_evolution se
            cross join season_context sc
            where (sc.season_id is null or se.season = sc.season_id)
              and (%s::int is null or se.round = %s)
        ),
        joined as (
            select
                ag.team_id::text as team_id,
                coalesce(dt.team_name, ag.team_id::text) as team_name,
                dt.logo_url as logo_url,
                coalesce(
                    ss.position,
                    dense_rank() over (
                        order by ag.points desc, ag.goal_difference desc, ag.goals_for desc, ag.team_id asc
                    )
                )::int as position,
                ag.points,
                ag.matches_played,
                ag.wins,
                ag.draws,
                ag.losses,
                ag.goals_for,
                ag.goals_against,
                ag.goal_difference,
                rf.recent_form
            from aggregated ag
            left join mart.dim_team dt
              on dt.team_id = ag.team_id
            left join standings_scope ss
              on ss.team_id = ag.team_id
             and ss.rn = 1
            left join recent_form rf
              on rf.team_id = ag.team_id
        ),
        filtered_rows as (
            select *
            from joined j
            where (%s::text is null or j.team_name ilike %s)
        )
        select
            j.*,
            count(*) over() as _total_count
        from filtered_rows j
        order by {sort_clause}, j.team_id asc
        limit %s offset %s;
    """
    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            venue_scope,
            venue_scope,
            global_filters.season_id,
            global_filters.round_id,
            global_filters.round_id,
            search_pattern,
            search_pattern,
            pageSize,
            offset,
        ],
    )

    total_count = int(rows[0]["_total_count"]) if rows else 0
    pagination = build_pagination(page, pageSize, total_count)

    items = [
        {
            "teamId": row["team_id"],
            "teamName": row.get("team_name"),
            "logoUrl": row.get("logo_url"),
            "position": int(row["position"]) if row.get("position") is not None else None,
            "points": int(row["points"]) if row.get("points") is not None else None,
            "matchesPlayed": int(row["matches_played"]) if row.get("matches_played") is not None else None,
            "wins": int(row["wins"]) if row.get("wins") is not None else None,
            "draws": int(row["draws"]) if row.get("draws") is not None else None,
            "losses": int(row["losses"]) if row.get("losses") is not None else None,
            "goalsFor": int(row["goals_for"]) if row.get("goals_for") is not None else None,
            "goalsAgainst": int(row["goals_against"]) if row.get("goals_against") is not None else None,
            "goalDifference": int(row["goal_difference"]) if row.get("goal_difference") is not None else None,
            "recentForm": row.get("recent_form") or [],
        }
        for row in rows
    ]

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        pagination=pagination,
        coverage=None,
    )


@router.get("/{clubId}")
def get_club_profile(
    clubId: str,
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
    month: str | None = None,
    includeRecentMatches: bool = True,
) -> dict[str, Any]:
    del includeRecentMatches

    team_id = _to_team_id(clubId)
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
        month=month,
    )

    club_row = db_client.fetch_one(
        """
            select
                dt.team_id::text as team_id,
                dt.team_name,
                dt.logo_url
            from mart.dim_team dt
            where dt.team_id = %s
            limit 1;
        """,
        [team_id],
    )
    if club_row is None:
        raise AppError(
            message="Club not found.",
            code="CLUB_NOT_FOUND",
            status=404,
            details={"clubId": clubId},
        )

    where_sql, where_params = _match_filters_sql(global_filters)
    venue_scope = global_filters.venue.value

    summary_row = db_client.fetch_one(
        f"""
            with scoped_matches as (
                select
                    fm.match_id,
                    fm.date_day,
                    fm.home_team_id,
                    fm.away_team_id,
                    coalesce(fm.home_goals, 0) as home_goals,
                    coalesce(fm.away_goals, 0) as away_goals,
                    row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
                from mart.fact_matches fm
                where {where_sql}
            ),
            filtered_matches as (
                select *
                from scoped_matches sm
                where (%s::int is null or sm.rn_recent <= %s)
            ),
            team_matches as (
                select
                    fm.home_team_id as team_id,
                    'home'::text as venue_scope,
                    fm.home_goals as goals_for,
                    fm.away_goals as goals_against,
                    case
                        when fm.home_goals > fm.away_goals then 'W'
                        when fm.home_goals = fm.away_goals then 'D'
                        else 'L'
                    end as result
                from filtered_matches fm
                where fm.home_team_id = %s

                union all

                select
                    fm.away_team_id as team_id,
                    'away'::text as venue_scope,
                    fm.away_goals as goals_for,
                    fm.home_goals as goals_against,
                    case
                        when fm.away_goals > fm.home_goals then 'W'
                        when fm.away_goals = fm.home_goals then 'D'
                        else 'L'
                    end as result
                from filtered_matches fm
                where fm.away_team_id = %s
            ),
            venue_filtered as (
                select *
                from team_matches tm
                where (%s::text = 'all' or tm.venue_scope = %s)
            )
            select
                count(*)::int as matches_played,
                sum(case when tm.result = 'W' then 1 else 0 end)::int as wins,
                sum(case when tm.result = 'D' then 1 else 0 end)::int as draws,
                sum(case when tm.result = 'L' then 1 else 0 end)::int as losses,
                sum(tm.goals_for)::int as goals_for,
                sum(tm.goals_against)::int as goals_against,
                sum(case when tm.result = 'W' then 3 when tm.result = 'D' then 1 else 0 end)::int as points
            from venue_filtered tm;
        """,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            team_id,
            team_id,
            venue_scope,
            venue_scope,
        ],
    )

    if summary_row and summary_row.get("matches_played"):
        matches_played = int(summary_row.get("matches_played") or 0)
        wins = int(summary_row.get("wins") or 0)
        draws = int(summary_row.get("draws") or 0)
        losses = int(summary_row.get("losses") or 0)
        goals_for = int(summary_row.get("goals_for") or 0)
        goals_against = int(summary_row.get("goals_against") or 0)
        points = int(summary_row.get("points") or 0)
        goal_difference = goals_for - goals_against
        win_rate = (wins / matches_played) * 100 if matches_played > 0 else None
        season_payload: dict[str, Any] | None = {
            "points": points,
            "matchesPlayed": matches_played,
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "goalsFor": goals_for,
            "goalsAgainst": goals_against,
            "goalDifference": goal_difference,
            "winRate": round(win_rate, 2) if win_rate is not None else None,
        }
    else:
        season_payload = None

    payload = {
        "club": {
            "teamId": club_row["team_id"],
            "teamName": club_row.get("team_name"),
            "logoUrl": club_row.get("logo_url"),
        },
        "season": season_payload,
    }

    return build_api_response(
        payload,
        request_id=_request_id(request),
        coverage=None,
    )

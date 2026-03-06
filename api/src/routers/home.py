from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response
from ..core.filters import VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/home", tags=["home"])


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _match_filters_sql(filters: Any) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(clauses, params, alias="fm", filters=filters)
    return " and ".join(clauses), params


@router.get("/overview")
def get_overview(
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

    query = f"""
        with scoped as (
            select
                fm.match_id,
                fm.home_goals,
                fm.away_goals,
                fm.home_team_id,
                fm.away_team_id,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        teams as (
            select home_team_id as team_id from filtered
            union
            select away_team_id as team_id from filtered
        )
        select
            (select count(distinct match_id) from filtered) as total_matches,
            (select sum(coalesce(home_goals, 0) + coalesce(away_goals, 0)) from filtered) as total_goals,
            (select count(distinct team_id) from teams) as total_teams;
    """

    row = db_client.fetch_one(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
        ]
    ) or {}

    total_matches = int(row.get("total_matches") or 0)
    total_goals = int(row.get("total_goals") or 0)
    avg_goals = (total_goals / total_matches) if total_matches > 0 else 0.0

    data = {
        "totalMatches": total_matches,
        "totalGoals": total_goals,
        "avgGoalsPerMatch": round(avg_goals, 2),
        "totalTeams": int(row.get("total_teams") or 0),
    }

    return build_api_response(data, request_id=_request_id(request))


@router.get("/top-teams")
def get_top_teams(
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

    query_attacks = f"""
        with scoped as (
            select
                fm.match_id,
                fm.home_team_id,
                fm.away_team_id,
                fm.home_goals,
                fm.away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        team_goals as (
            select home_team_id as team_id, home_goals as goals from filtered
            union all
            select away_team_id as team_id, away_goals as goals from filtered
        )
        select
            tg.team_id,
            t.team_name,
            sum(tg.goals) as value
        from team_goals tg
        left join mart.dim_team t on tg.team_id = t.team_id
        group by tg.team_id, t.team_name
        order by value desc nulls last
        limit 5;
    """

    attacks_rows = db_client.fetch_all(
        query_attacks,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
        ]
    )

    query_defenses = f"""
        with scoped as (
            select
                fm.match_id,
                fm.home_team_id,
                fm.away_team_id,
                fm.home_goals,
                fm.away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        team_goals as (
            select home_team_id as team_id, away_goals as goals_conceded from filtered
            union all
            select away_team_id as team_id, home_goals as goals_conceded from filtered
        )
        select
            tg.team_id,
            t.team_name,
            sum(tg.goals_conceded) as value
        from team_goals tg
        left join mart.dim_team t on tg.team_id = t.team_id
        group by tg.team_id, t.team_name
        order by value asc nulls last
        limit 5;
    """

    defenses_rows = db_client.fetch_all(
        query_defenses,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
        ]
    )

    top_attacks = [
        {
            "teamId": str(row["team_id"]),
            "teamName": row["team_name"],
            "value": int(row["value"]),
            "trend": [],
        }
        for row in attacks_rows
    ]

    top_defenses = [
        {
            "teamId": str(row["team_id"]),
            "teamName": row["team_name"],
            "value": int(row["value"]),
            "trend": [],
        }
        for row in defenses_rows
    ]

    data = {
        "topAttacks": top_attacks,
        "topDefenses": top_defenses,
    }

    return build_api_response(data, request_id=_request_id(request))


@router.get("/standings-evolution")
def get_standings_evolution(
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

    where_clauses = ["1=1"]
    where_params: list[Any] = []

    if global_filters.competition_id is not None:
        where_clauses.append("se.league_id = %s")
        where_params.append(global_filters.competition_id)

    if global_filters.season_id is not None:
        where_clauses.append("se.season = %s")
        where_params.append(global_filters.season_id)

    query = f"""
        select
            se.round_key,
            se.round_label,
            t.team_name,
            se.position
        from mart.standings_evolution se
        left join mart.dim_team t on se.team_id = t.team_id
        where {" and ".join(where_clauses)}
        order by se.round_key asc, se.position asc;
    """

    try:
        rows = db_client.fetch_all(query, where_params)
    except Exception:
        # Fallback empty in case standings_evolution does not exist or filters fail
        rows = []

    # Format into series
    # series: [ { roundLabel: "Round 1", "Team A": 1, "Team B": 2 }, ... ]
    
    rounds_map: dict[str, dict[str, Any]] = {}
    team_names = set()

    for row in rows:
        round_key = str(row["round_key"])
        round_label = row["round_label"] or f"Round {round_key}"
        team_name = row["team_name"] or "Unknown"
        position = int(row["position"])

        if round_key not in rounds_map:
            rounds_map[round_key] = {"roundLabel": round_label}
            # preserve sorting logic if we want to sort rounds mathematically, 
            # we use round_key ordering from the SQL
            rounds_map[round_key]["_ordered"] = []

        rounds_map[round_key][team_name] = position
        rounds_map[round_key]["_ordered"].append(team_name)
        team_names.add(team_name)

    series = []
    for rnd in rounds_map.values():
        d = dict(rnd)
        d.pop("_ordered", None)
        series.append(d)

    data = {
        "series": series,
        "teamNames": list(team_names),
    }

    return build_api_response(data, request_id=_request_id(request))


@router.get("/top-players")
def get_top_players(
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

    where_clauses = ["1=1"]
    where_params: list[Any] = []
    append_fact_match_filters(where_clauses, where_params, alias="fm", filters=global_filters)
    
    if global_filters.venue == VenueFilter.home:
        where_clauses.append("pms.team_id = fm.home_team_id")
    elif global_filters.venue == VenueFilter.away:
        where_clauses.append("pms.team_id = fm.away_team_id")

    where_sql = " and ".join(where_clauses)

    effective_last_n = global_filters.last_n
    if (
        effective_last_n is None
        and global_filters.round_id is None
        and global_filters.date_start is None
        and global_filters.date_end is None
    ):
        effective_last_n = 5

    period_window = effective_last_n or 5

    query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_name,
                pms.match_id,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                pms.rating,
                pms.match_date,
                row_number() over (
                    partition by pms.player_id
                    order by pms.match_date desc, pms.match_id desc
                ) as rn_recent
            from mart.player_match_summary pms
            inner join mart.fact_matches fm on fm.match_id = pms.match_id
            where {where_sql}
        ),
        filtered as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        window_cfg as (
            select %s::int as period_window
        ),
        period_deltas as (
            select
                s.player_id,
                sum(
                    case
                        when s.rn_recent <= wc.period_window then (s.goals + s.assists)
                        else 0
                    end
                )::numeric as current_window_ga,
                sum(
                    case
                        when s.rn_recent > wc.period_window and s.rn_recent <= (wc.period_window * 2)
                            then (s.goals + s.assists)
                        else 0
                    end
                )::numeric as previous_window_ga,
                count(*) filter (
                    where s.rn_recent > wc.period_window and s.rn_recent <= (wc.period_window * 2)
                ) as previous_window_matches
            from scoped s
            cross join window_cfg wc
            where s.rn_recent <= (wc.period_window * 2)
            group by s.player_id
        ),
        aggregated as (
            select
                player_id,
                max(player_name) as player_name,
                max(team_name) as team_name,
                sum(goals) as goals,
                sum(assists) as assists,
                sum(minutes_played) as minutes_played,
                avg(rating) as rating,
                count(*) as matches_played
            from filtered
            group by player_id
            having count(*) >= 3
        )
        select
            a.*,
            pd.current_window_ga,
            pd.previous_window_ga,
            pd.previous_window_matches
        from aggregated a
        left join period_deltas pd
          on pd.player_id = a.player_id
        order by rating desc nulls last, goals desc, assists desc
        limit 5;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            effective_last_n,
            effective_last_n,
            period_window,
        ]
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        current_window_ga = float(row.get("current_window_ga") or 0)
        previous_window_ga = float(row.get("previous_window_ga") or 0)
        previous_window_matches = int(row.get("previous_window_matches") or 0)

        variation_value: float | None = None
        variation_pct: float | None = None

        if previous_window_matches > 0:
            variation_value = current_window_ga - previous_window_ga
            if previous_window_ga != 0:
                variation_pct = (variation_value / previous_window_ga) * 100

        items.append(
            {
                "playerId": str(row["player_id"]),
                "playerName": row["player_name"],
                "teamName": row["team_name"],
                "goals": int(row["goals"]),
                "assists": int(row["assists"]),
                "rating": float(row["rating"]) if row.get("rating") is not None else None,
                "minutesPlayed": float(row["minutes_played"]),
                "variationValue": round(variation_value, 2) if variation_value is not None else None,
                "variationPct": round(variation_pct, 1) if variation_pct is not None else None,
            }
        )

    data = {
        "items": items,
    }

    return build_api_response(data, request_id=_request_id(request))


@router.get("/coverage")
def get_coverage(
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
) -> dict[str, Any]:
    _ = validate_and_build_global_filters(
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
    
    data = {
        "modules": [
            {
                "label": "Matches/Eventos",
                "percentage": 100,
                "level": "high",
            },
            {
                "label": "Player Stats",
                "percentage": 85,
                "level": "partial",
            },
            {
                "label": "Lineups",
                "percentage": 98,
                "level": "high",
            },
            {
                "label": "Team Stats",
                "percentage": 40,
                "level": "low",
            },
        ]
    }

    return build_api_response(data, request_id=_request_id(request))

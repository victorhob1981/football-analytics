


with context as (
    select * from "football_dw"."mart"."int_fixture_player_context"
),
fact_matches as (
    select
        match_id,
        competition_sk,
        season,
        date_day
    from "football_dw"."mart"."fact_matches"
),
base as (
    select
        md5(concat(c.provider, ':', c.fixture_id::text, ':', c.team_id::text, ':', c.player_id::text)) as fixture_player_stat_id,
        c.provider,
        c.fixture_id as match_id,
        fm.competition_sk,
        fm.season,
        fm.date_day as match_date,
        md5(concat('team:', c.team_id::text)) as team_sk,
        md5(concat('player:', c.player_id::text)) as player_sk,
        c.team_id,
        c.player_id,
        c.team_name,
        c.player_name,
        c.position_name,
        c.is_starter,
        c.minutes_played,
        c.goals,
        c.assists,
        c.shots_total,
        c.shots_on_goal,
        c.passes_total,
        c.key_passes,
        c.tackles,
        c.interceptions,
        c.duels,
        c.fouls_committed,
        c.yellow_cards,
        c.red_cards,
        c.goalkeeper_saves,
        c.clean_sheets,
        c.xg,
        c.rating,
        c.statistics,
        c.ingested_run,
        coalesce(c.updated_at, now()) as updated_at
    from context c
    inner join fact_matches fm
      on fm.match_id = c.fixture_id
    where c.fixture_id is not null
      and c.team_id is not null
      and c.player_id is not null
),
filtered as (
    select *
    from base
    
)
select * from filtered
{{ config(materialized='incremental', unique_key='competition_sk', on_schema_change='sync_all_columns') }}

with catalog as (
    select * from {{ source('control_plane', 'competitions') }}
),
provider_map as (
    select *
    from {{ source('control_plane', 'competition_provider_map') }}
    where is_active = true
),
observed as (
    select distinct
        provider,
        coalesce(provider_league_id, league_id) as provider_league_id,
        league_id,
        competition_key,
        competition_type,
        league_name
    from {{ ref('stg_matches') }}
    where provider is not null
      and coalesce(provider_league_id, league_id) is not null
),
catalog_mapped as (
    select
        pm.provider,
        pm.provider_league_id,
        pm.provider_league_id as league_id,
        pm.competition_key,
        c.competition_name,
        c.competition_type,
        c.country_name,
        c.confederation_name,
        c.tier,
        c.display_priority,
        pm.provider_name
    from provider_map pm
    inner join catalog c
      on c.competition_key = pm.competition_key
),
resolved as (
    select
        coalesce(o.provider, cm.provider) as provider,
        coalesce(o.provider_league_id, cm.provider_league_id) as provider_league_id,
        coalesce(o.league_id, cm.league_id) as league_id,
        coalesce(o.competition_key, cm.competition_key) as competition_key,
        coalesce(cm.competition_name, o.league_name, cm.provider_name) as competition_name,
        coalesce(o.league_name, cm.provider_name, cm.competition_name) as league_name,
        coalesce(o.competition_type, cm.competition_type) as competition_type,
        cm.country_name,
        cm.confederation_name,
        cm.tier,
        coalesce(cm.display_priority, 999) as display_priority
    from catalog_mapped cm
    full outer join observed o
      on o.provider = cm.provider
     and o.provider_league_id = cm.provider_league_id
),
fallbacks as (
    select
        provider,
        provider_league_id,
        league_id,
        coalesce(competition_key, concat('league_', provider_league_id::text)) as competition_key,
        coalesce(competition_name, league_name, concat('League ', provider_league_id::text)) as competition_name,
        coalesce(league_name, competition_name, concat('League ', provider_league_id::text)) as league_name,
        coalesce(competition_type, 'league') as competition_type,
        country_name,
        confederation_name,
        tier,
        display_priority
    from resolved
    where provider is not null
      and provider_league_id is not null
)
select distinct
    md5(concat(provider, ':competition:', competition_key)) as competition_sk,
    provider,
    competition_key,
    provider_league_id,
    league_id,
    competition_name,
    league_name,
    competition_type,
    country_name,
    confederation_name,
    tier,
    case
        when competition_type = 'league' then true
        else false
    end as is_domestic,
    case
        when competition_type in ('cup', 'continental_cup') then true
        else false
    end as is_cup,
    display_priority,
    now() as updated_at
from fallbacks

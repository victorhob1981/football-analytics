{{ config(materialized='incremental', unique_key='match_id', on_schema_change='sync_all_columns') }}

with base as (
    select * from {{ ref('int_fact_matches_base') }}
),
filtered as (
    select
        base.*,
        now() as updated_at
    from base
    {% if is_incremental() %}
    where base.date_day >= (
        select coalesce(max(date_day), date '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)
select * from filtered

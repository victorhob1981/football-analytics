{{ config(materialized='incremental', unique_key='event_id', on_schema_change='sync_all_columns') }}

with base as (
    select * from {{ ref('int_fact_match_events_base') }}
),
filtered as (
    select *
    from base
    {% if is_incremental() %}
    where coalesce(updated_at, now()) > (
        select coalesce(max(updated_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)
select * from filtered

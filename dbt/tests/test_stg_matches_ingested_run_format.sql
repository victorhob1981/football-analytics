-- Regra: ingested_run em stg_matches deve ser parseavel para timestamptz.
-- Rationale: fact_matches incremental usa watermark de carga derivado de ingested_run.

select *
from {{ ref('stg_matches') }}
where ingested_run is null
   or ingested_run !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}Z$'


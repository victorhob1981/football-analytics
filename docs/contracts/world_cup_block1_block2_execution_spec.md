# World Cup Block 1/2 Execution Spec

## Scope
This document is the execution contract for the World Cup operational foundation only.

Included in this round:
- Block 1: snapshot gate foundation
- Block 2: minimum control and audit structures

Excluded from this round:
- bronze loaders
- ingestion DAGs/tasks
- snapshot population
- bronze to silver mapping
- raw publish beyond table foundation
- any change to `raw.provider_entity_map`
- any change to `raw.match_events`
- any operational scope beyond FIFA World Cup 2022

## Structures created in this round

### `control.wc_source_snapshots`
Purpose:
- operational manifest for local snapshots, versioning, licensing, and provenance

### `silver.wc_coverage_manifest`
Purpose:
- effective coverage registry by `edition + domain + source`

### `silver.wc_source_divergences`
Purpose:
- audit divergences between sources before canonical publication

### `control.wc_entity_match_review_queue`
Purpose:
- explicit manual review queue, initially focused on entity matching review

### `raw.wc_match_events`
Purpose:
- permanent World Cup raw table for rich match events, without premature convergence to `raw.match_events`

## Existing dependency not changed now

### `raw.provider_entity_map`
Status:
- existing table
- dependency for later blocks
- not altered in this round

Current foundation reference:
- `db/migrations/20260218190000_provider_foundation.sql`

## Frozen decisions implemented/documented in this round

### Allowed `source_name`
- `statsbomb_open_data`
- `fjelstul_worldcup`
- `openfootball_worldcup`
- `openfootball_worldcup_more`

These values are enforced by `CHECK` constraints in the new structures that store `source_name`.

### Snapshot checksum policy
- snapshot checksums are represented by a manifest file inside the snapshot directory
- `control.wc_source_snapshots.checksum_sha256` stores the SHA256 of that checksum manifest file
- this field does not store a directory hash directly

### Snapshot sources in operational scope now
- `statsbomb_open_data`
- `fjelstul_worldcup`

### Sources not in operational scope now
- `openfootball_worldcup`
- `openfootball_worldcup_more`

These remain allowed in source constraints because they are part of the accepted World Cup source contract, but they are not activated in Block 1.

### Future `internal_id` policy
Frozen for later implementation only:
- format: `prefix + UUIDv7`
- storage type: `text`

Examples:
- `match__<uuidv7>`
- `player__<uuidv7>`

Not implemented in this round:
- no ID emission
- no `provider_entity_map` extension
- no canonical identity issuance

### Review queue uniqueness
`control.wc_entity_match_review_queue` does not rely on nullable unique semantics for `edition_key`.

Implemented uniqueness rule:
- `entity_type + source_name + source_external_id + coalesce(edition_key, 'GLOBAL')`

### `raw.wc_match_events`
Frozen decisions implemented:
- permanent raw table
- not partitioned in this first migration
- no GIN index on `event_payload` now
- no FK to `raw.match_events`

## Expected snapshot paths

### StatsBomb
`data/snapshots/world-cup/statsbomb-open-data/<commit_sha>/`

### Fjelstul
`data/snapshots/world-cup/fjelstul-worldcup/<version_or_commit>/`

## Block status after this round

### Block 1
Foundation only:
- table created
- no snapshot rows inserted yet

### Block 2
Foundation only:
- schemas/tables/indexes/constraints created
- no ingestion or publication logic added

## Non-goals for this round
- no 2018 activation
- no historical scope
- no Kaggle operationalization
- no openfootball operationalization
- no bronze/silver/raw load execution
- no quality gate execution

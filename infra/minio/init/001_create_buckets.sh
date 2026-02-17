#!/bin/sh

set -eu

MINIO_ENDPOINT_URL="${MINIO_ENDPOINT_URL:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY nao definida}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:?MINIO_SECRET_KEY nao definida}"

mc alias set local "$MINIO_ENDPOINT_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"

mc mb local/football-bronze
mc mb local/football-silver
mc mb local/football-gold

mc policy set public local/football-bronze
mc policy set public local/football-silver
mc policy set public local/football-gold

echo "Buckets criados com sucesso."

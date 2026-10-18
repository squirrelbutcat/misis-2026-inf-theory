#!/usr/bin/env bash
# Выполнить SQL-файл в Trino: ./scripts/run_sql.sh sql/01_bronze.sql
# SET SESSION внутри файла действует на все следующие запросы этого файла.
set -euo pipefail
f="$1"; shift
docker cp "$f" trino:/tmp/run.sql
docker exec trino trino --output-format ALIGNED --file /tmp/run.sql "$@"

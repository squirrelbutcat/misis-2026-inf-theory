#!/usr/bin/env bash
# Остановить стек. С флагом --purge — удалить контейнеры и тома (все данные).
set -euo pipefail
C="trino lakekeeper lakehouse-pg lakehouse-minio"
if [[ "${1:-}" == "--purge" ]]; then
  docker rm -f $C; docker volume rm minio-data pg-data
else
  docker stop $C
fi

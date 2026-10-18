#!/usr/bin/env bash
# Поднимает стек MinIO(Silo) + PostgreSQL + Lakekeeper + Trino (как в гайде курса, macOS / Docker Desktop).
# Повторный запуск безопасен: существующие контейнеры просто стартуют, данные живут в томах.
set -euo pipefail
cd "$(dirname "$0")/.."
H="--add-host host.docker.internal:host-gateway"
PG_URL='postgresql://postgres:postgres@host.docker.internal:5432/postgres'
ENC='This-is-NOT-Secure!'

# run_once <name> <docker run args...> — создать контейнер, если его нет; иначе запустить
run_once() {
  local name="$1"; shift
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker start "$name" >/dev/null && echo "[=] $name уже есть — запущен"
  else
    docker run -d --name "$name" "$@" >/dev/null && echo "[+] $name создан"
  fi
}
wait_for() { local what="$1"; shift; printf '[..] жду %s' "$what"; until "$@" >/dev/null 2>&1; do printf '.'; sleep 2; done; echo ' ok'; }

# 1. MinIO (форк Silo: официальные образы MinIO больше не раздаются) — S3-хранилище, порты 9000 (API) и 9001 (консоль)
run_once lakehouse-minio $H -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data pgsty/silo:latest server /data --console-address ":9001"
wait_for "MinIO" curl -s -o /dev/null http://localhost:9000
# бакет warehouse — сюда Trino пишет файлы таблиц
docker run --rm $H --entrypoint /bin/sh pgsty/mc:latest -c \
  "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing m/warehouse"

# 2. PostgreSQL — хранит метаданные каталога Lakekeeper
run_once lakehouse-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
  -v pg-data:/var/lib/postgresql/data postgres:17
wait_for "PostgreSQL" docker exec lakehouse-pg pg_isready -U postgres

# 3. Lakekeeper — Iceberg REST Catalog: миграции схемы БД (идемпотентно), затем сервер на 8181
docker run --rm $H -e "LAKEKEEPER__PG_ENCRYPTION_KEY=$ENC" \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG_URL" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG_URL" \
  quay.io/lakekeeper/catalog:latest-main migrate
run_once lakekeeper $H -p 8181:8181 -e "LAKEKEEPER__PG_ENCRYPTION_KEY=$ENC" \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG_URL" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG_URL" \
  quay.io/lakekeeper/catalog:latest-main serve
wait_for "Lakekeeper" curl -sf http://localhost:8181/health
# принять Terms of Use и зарегистрировать warehouse demo (повторно — вернёт «уже есть», это норм)
curl -s -X POST http://localhost:8181/management/v1/bootstrap -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'; echo
curl -s -X POST http://localhost:8181/management/v1/warehouse -H "Content-Type: application/json" \
  -d @scripts/create-warehouse.json; echo

# 4. Trino — SQL-движок; каталоги lakekeeper (Iceberg) и tpch (генератор) из папки catalog/
run_once trino $H -p 8080:8080 -v "$(pwd)/catalog:/etc/trino/catalog" trinodb/trino:476
wait_for "Trino" sh -c 'curl -sf http://localhost:8080/v1/info | grep -q "\"starting\":false"'
docker exec trino trino --execute "SHOW CATALOGS"
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'

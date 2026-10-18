#!/usr/bin/env bash
# Раздел 5: Q16 на Bronze с разными кодеками. Перед каждым кодеком — рестарт Trino и MinIO
# (сброс кэша метаданных Trino и page cache), затем 1 холодный и 3 тёплых прогона.
# Предварительно: ./scripts/run_sql.sh sql/06_compression_bronze.sql
set -euo pipefail
cd "$(dirname "$0")/.."
CODECS="${CODECS:-gzip zstd}"
mkdir -p results
OUT=results/compression_runs.csv
echo "codec,run,wall_s,exec_ms" > "$OUT"
TIMEFORMAT=%R

wait_trino() { until curl -sf http://localhost:8080/v1/info | grep -q '"starting":false'; do sleep 2; done; sleep 5; }
q() { docker exec trino trino --output-format TSV --execute "$1"; }

for c in $CODECS; do
  sql=$(sed "s/__SCHEMA__/bronze_$c/g" sql/07_q16_on_bronze.sql)
  echo "== $c: рестарт trino + minio (холодный старт)"
  docker restart trino lakehouse-minio >/dev/null
  wait_trino
  for run in cold warm1 warm2 warm3; do
    wall=$( { time docker exec trino trino --execute "$sql" >/dev/null 2>&1; } 2>&1 )
    # чистое время выполнения последнего запроса по этой схеме из system.runtime.queries
    exec_ms=$(q "SELECT date_diff('millisecond', started, \"end\") FROM system.runtime.queries
                 WHERE query LIKE '%bronze_$c.part%' AND query NOT LIKE '%runtime.queries%' AND state = 'FINISHED'
                 ORDER BY created DESC LIMIT 1")
    echo "$c,$run,$wall,$exec_ms" | tee -a "$OUT"
  done
done

echo; echo "== Размер Bronze по кодекам (МБ = MiB)"
for c in $CODECS; do
  q "SELECT '$c' AS codec, round(sum(file_size_in_bytes) / 1048576.0, 2) AS mb FROM (
       SELECT file_size_in_bytes FROM lakekeeper.bronze_$c.\"part\$files\"
       UNION ALL SELECT file_size_in_bytes FROM lakekeeper.bronze_$c.\"partsupp\$files\"
       UNION ALL SELECT file_size_in_bytes FROM lakekeeper.bronze_$c.\"supplier\$files\")"
done | tee results/compression_sizes.tsv
echo; echo "Готово: $OUT, results/compression_sizes.tsv"

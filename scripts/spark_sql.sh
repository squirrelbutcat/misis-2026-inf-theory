#!/usr/bin/env bash
# Раздел 6.2: Spark читает те же Iceberg-таблицы через тот же Lakekeeper.
# Без аргументов — выполняет sql/spark_bronze.sql; с аргументом -i — интерактивный spark-sql.
set -euo pipefail
cd "$(dirname "$0")/.."
J=spark/jars
[[ -f $J/iceberg-spark-runtime-3.5_2.12-1.7.1.jar ]] || curl -fsSL -o $J/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
[[ -f $J/iceberg-aws-bundle-1.7.1.jar ]] || curl -fsSL -o $J/iceberg-aws-bundle-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
ARGS=(-f /opt/spark/work/spark_bronze.sql)
[[ "${1:-}" == "-i" ]] && ARGS=()
docker run -it --rm --add-host host.docker.internal:host-gateway \
  -v "$(pwd)/spark/conf:/opt/spark/conf" \
  -v "$(pwd)/$J:/opt/spark/iceberg-jars" \
  -v "$(pwd)/sql:/opt/spark/work" \
  apache/spark:3.5.3 /opt/spark/bin/spark-sql "${ARGS[@]}"

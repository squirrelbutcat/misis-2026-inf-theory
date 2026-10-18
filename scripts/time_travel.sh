#!/usr/bin/env bash
# Раздел 6.1: «порча» Silver и восстановление из снапшота (time travel). Скрипт печатает каждый шаг.
set -euo pipefail
T='lakekeeper.silver.part_supplier'
S='lakekeeper.silver."part_supplier$snapshots"'
q() { echo "trino> $1"; docker exec trino trino --output-format TSV --execute "$1"; echo; }

SNAP=$(docker exec trino trino --output-format TSV --execute "SELECT snapshot_id FROM $S ORDER BY committed_at DESC LIMIT 1")
echo "Снапшот до порчи: $SNAP"; echo
q "SELECT count(*) AS rows_size3 FROM $T WHERE size = 3"
q "DELETE FROM $T WHERE size = 3"                                   # «случайно» удалили партицию size=3
q "SELECT count(*) AS rows_size3_after_delete FROM $T WHERE size = 3"
q "SELECT count(*) AS rows_size3_in_old_snapshot FROM $T FOR VERSION AS OF $SNAP WHERE size = 3"
q "INSERT INTO $T SELECT * FROM $T FOR VERSION AS OF $SNAP WHERE size = 3"   # восстановление
q "SELECT count(*) AS rows_size3_restored FROM $T WHERE size = 3"
q "SELECT committed_at, snapshot_id, parent_id, operation FROM $S ORDER BY committed_at"

-- Раздел 6.2: Spark видит те же таблицы через тот же REST-каталог Lakekeeper (без копирования данных)
SHOW NAMESPACES IN lakekeeper;
SHOW TABLES IN lakekeeper.bronze;
SELECT count(*) AS part_rows     FROM lakekeeper.bronze.part;
SELECT count(*) AS partsupp_rows FROM lakekeeper.bronze.partsupp;
SELECT suppkey, name, comment FROM lakekeeper.bronze.supplier WHERE comment LIKE '%Customer%Complaints%' LIMIT 10;

-- Метаданные Iceberg из Spark: снапшоты и файлы Bronze
SELECT committed_at, snapshot_id, operation FROM lakekeeper.bronze.partsupp.snapshots;
SELECT count(*) AS files, round(sum(file_size_in_bytes) / 1048576, 2) AS mb FROM lakekeeper.bronze.partsupp.files;

-- Тот же Q16 по Bronze на Spark (для сравнения времени с Trino — смотри строку "Time taken")
SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
FROM lakekeeper.bronze.partsupp ps
JOIN lakekeeper.bronze.part p ON p.partkey = ps.partkey
WHERE p.brand <> 'Brand#45'
  AND p.type NOT LIKE 'MEDIUM POLISHED%'
  AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps.suppkey NOT IN (SELECT suppkey FROM lakekeeper.bronze.supplier WHERE comment LIKE '%Customer%Complaints%')
GROUP BY p.brand, p.type, p.size
ORDER BY supplier_cnt DESC, p.brand, p.type, p.size
LIMIT 20;

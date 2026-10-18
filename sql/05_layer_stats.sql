-- Цифры для таблиц раздела 4: строки, число файлов, вес (МБ = MiB).
SELECT 'bronze.part' AS tbl,
       (SELECT count(*) FROM lakekeeper.bronze.part) AS row_cnt,
       (SELECT count(*) FROM lakekeeper.bronze."part$files") AS files,
       (SELECT round(sum(file_size_in_bytes) / 1048576.0, 2) FROM lakekeeper.bronze."part$files") AS mb
UNION ALL
SELECT 'bronze.partsupp' AS tbl,
       (SELECT count(*) FROM lakekeeper.bronze.partsupp) AS row_cnt,
       (SELECT count(*) FROM lakekeeper.bronze."partsupp$files") AS files,
       (SELECT round(sum(file_size_in_bytes) / 1048576.0, 2) FROM lakekeeper.bronze."partsupp$files") AS mb
UNION ALL
SELECT 'bronze.supplier' AS tbl,
       (SELECT count(*) FROM lakekeeper.bronze.supplier) AS row_cnt,
       (SELECT count(*) FROM lakekeeper.bronze."supplier$files") AS files,
       (SELECT round(sum(file_size_in_bytes) / 1048576.0, 2) FROM lakekeeper.bronze."supplier$files") AS mb
UNION ALL
SELECT 'silver.part_supplier' AS tbl,
       (SELECT count(*) FROM lakekeeper.silver.part_supplier) AS row_cnt,
       (SELECT count(*) FROM lakekeeper.silver."part_supplier$files") AS files,
       (SELECT round(sum(file_size_in_bytes) / 1048576.0, 2) FROM lakekeeper.silver."part_supplier$files") AS mb
UNION ALL
SELECT 'gold.q16_supplier_cnt' AS tbl,
       (SELECT count(*) FROM lakekeeper.gold.q16_supplier_cnt) AS row_cnt,
       (SELECT count(*) FROM lakekeeper.gold."q16_supplier_cnt$files") AS files,
       (SELECT round(sum(file_size_in_bytes) / 1048576.0, 2) FROM lakekeeper.gold."q16_supplier_cnt$files") AS mb;

-- Формат и кодек, реально записанные в метаданные Iceberg (SHOW CREATE TABLE кодек не показывает)
SELECT 'bronze.part' AS tbl, key, value FROM lakekeeper.bronze."part$properties"
WHERE key IN ('write.format.default', 'write.parquet.compression-codec')
UNION ALL
SELECT 'bronze.partsupp' AS tbl, key, value FROM lakekeeper.bronze."partsupp$properties"
WHERE key IN ('write.format.default', 'write.parquet.compression-codec')
UNION ALL
SELECT 'bronze.supplier' AS tbl, key, value FROM lakekeeper.bronze."supplier$properties"
WHERE key IN ('write.format.default', 'write.parquet.compression-codec')
UNION ALL
SELECT 'silver.part_supplier' AS tbl, key, value FROM lakekeeper.silver."part_supplier$properties"
WHERE key IN ('write.format.default', 'write.parquet.compression-codec')
UNION ALL
SELECT 'gold.q16_supplier_cnt' AS tbl, key, value FROM lakekeeper.gold."q16_supplier_cnt$properties"
WHERE key IN ('write.format.default', 'write.parquet.compression-codec')
ORDER BY tbl, key;

-- Партиции Silver: должно быть 50 строк (size = 1..50)
SELECT count(*) AS partitions, sum(record_count) AS rows, sum(file_count) AS files
FROM lakekeeper.silver."part_supplier$partitions";

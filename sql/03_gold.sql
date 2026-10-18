-- GOLD: витрина — ответ на Q16 (число поставщиков на brand/type/size).
CREATE SCHEMA IF NOT EXISTS lakekeeper.gold;
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE OR REPLACE TABLE lakekeeper.gold.q16_supplier_cnt WITH (format = 'PARQUET') AS
SELECT brand, type, size, count(DISTINCT suppkey) AS supplier_cnt
FROM lakekeeper.silver.part_supplier
WHERE brand <> 'Brand#45'
  AND type NOT LIKE 'MEDIUM POLISHED%'
  AND size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY brand, type, size;

-- Результат в порядке исходного запроса
SELECT * FROM lakekeeper.gold.q16_supplier_cnt
ORDER BY supplier_cnt DESC, brand, type, size
LIMIT 20;

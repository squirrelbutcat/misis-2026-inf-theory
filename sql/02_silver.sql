-- SILVER: очищенный факт «деталь — поставщик».
--  * джойн partsupp ⋈ part сделан один раз;
--  * поставщики с жалобами (comment LIKE '%Customer%Complaints%') исключены;
--  * оставлены только колонки, нужные для анализа деталей и поставщиков.
-- Партиционирование по size: Q16 берёт 8 размеров из 50 → Trino читает ~16% файлов.
CREATE SCHEMA IF NOT EXISTS lakekeeper.silver;
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE OR REPLACE TABLE lakekeeper.silver.part_supplier
WITH (format = 'PARQUET', partitioning = ARRAY['size'])
AS
SELECT ps.partkey, ps.suppkey, p.brand, p.type, p.size
FROM lakekeeper.bronze.partsupp AS ps
JOIN lakekeeper.bronze.part     AS p ON p.partkey = ps.partkey
WHERE ps.suppkey NOT IN (
  SELECT s.suppkey
  FROM lakekeeper.bronze.supplier AS s
  WHERE s.comment LIKE '%Customer%Complaints%'
);

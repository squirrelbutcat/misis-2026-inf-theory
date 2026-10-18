-- Раздел 5: две копии Bronze с разными кодеками (тот же sf5, тот же формат Parquet).

CREATE SCHEMA IF NOT EXISTS lakekeeper.bronze_gzip;
SET SESSION lakekeeper.compression_codec = 'GZIP';
CREATE OR REPLACE TABLE lakekeeper.bronze_gzip.part WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.part;
CREATE OR REPLACE TABLE lakekeeper.bronze_gzip.partsupp WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.partsupp;
CREATE OR REPLACE TABLE lakekeeper.bronze_gzip.supplier WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.supplier;

CREATE SCHEMA IF NOT EXISTS lakekeeper.bronze_zstd;
SET SESSION lakekeeper.compression_codec = 'ZSTD';
CREATE OR REPLACE TABLE lakekeeper.bronze_zstd.part WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.part;
CREATE OR REPLACE TABLE lakekeeper.bronze_zstd.partsupp WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.partsupp;
CREATE OR REPLACE TABLE lakekeeper.bronze_zstd.supplier WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.supplier;

-- Проверка, что кодек реально применился (ожидается gzip / zstd)
SELECT 'bronze_gzip' AS schema_name, value AS codec FROM lakekeeper.bronze_gzip."partsupp$properties" WHERE key = 'write.parquet.compression-codec'
UNION ALL
SELECT 'bronze_zstd', value FROM lakekeeper.bronze_zstd."partsupp$properties" WHERE key = 'write.parquet.compression-codec';

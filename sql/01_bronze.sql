-- BRONZE: сырые таблицы TPC-H sf5 «как есть», все колонки, без фильтров.
-- Parquet + ZSTD, без партиционирования. Кодек — свойство СЕССИИ, задаётся до CREATE.
CREATE SCHEMA IF NOT EXISTS lakekeeper.bronze;
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE OR REPLACE TABLE lakekeeper.bronze.part     WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.part;
CREATE OR REPLACE TABLE lakekeeper.bronze.partsupp WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.partsupp;
CREATE OR REPLACE TABLE lakekeeper.bronze.supplier WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf5.supplier;

-- Раздел 6.1: schema evolution на Silver. В конце схема возвращается к исходной, чтобы Gold пересобирался.

-- 0. Исходное состояние: колонки, число файлов и снапшотов
DESCRIBE lakekeeper.silver.part_supplier;
SELECT (SELECT count(*) FROM lakekeeper.silver."part_supplier$files")     AS files,
       (SELECT count(*) FROM lakekeeper.silver."part_supplier$snapshots") AS snapshots;

-- 1. ADD COLUMN — меняются только метаданные (новый metadata.json), parquet-файлы не переписываются
ALTER TABLE lakekeeper.silver.part_supplier ADD COLUMN load_note varchar;
SELECT load_note, count(*) AS row_cnt FROM lakekeeper.silver.part_supplier GROUP BY load_note;  -- у старых строк NULL
SELECT (SELECT count(*) FROM lakekeeper.silver."part_supplier$files")     AS files,             -- столько же, сколько в шаге 0
       (SELECT count(*) FROM lakekeeper.silver."part_supplier$snapshots") AS snapshots;         -- новых снапшотов нет

-- 2. RENAME COLUMN — Iceberg хранит колонки по id, поэтому старые файлы читаются под новым именем
ALTER TABLE lakekeeper.silver.part_supplier RENAME COLUMN brand TO part_brand;
SELECT part_brand, count(*) AS row_cnt FROM lakekeeper.silver.part_supplier GROUP BY part_brand ORDER BY part_brand LIMIT 5;
DESCRIBE lakekeeper.silver.part_supplier;

-- 3. Возврат к исходной схеме
ALTER TABLE lakekeeper.silver.part_supplier RENAME COLUMN part_brand TO brand;
ALTER TABLE lakekeeper.silver.part_supplier DROP COLUMN load_note;
DESCRIBE lakekeeper.silver.part_supplier;

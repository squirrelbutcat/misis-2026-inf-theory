-- Проверка: каталоги на месте и масштаб sf5 генерируется
SHOW CATALOGS;
SELECT count(*) AS part_rows     FROM tpch.sf5.part;      -- ожидается 1 000 000 (200 000 × 5)
SELECT count(*) AS partsupp_rows FROM tpch.sf5.partsupp;  -- ожидается 4 000 000 (800 000 × 5)
SELECT count(*) AS supplier_rows FROM tpch.sf5.supplier;  -- ожидается 50 000 (10 000 × 5)

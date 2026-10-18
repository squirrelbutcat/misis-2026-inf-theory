-- Проверка: Gold == исходный Q16, посчитанный напрямую по tpch.sf5.
-- Ожидается: gold_minus_ref = 0, ref_minus_gold = 0, gold_rows = ref_rows.
WITH ref AS (
  SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
  FROM tpch.sf5.partsupp AS ps
  JOIN tpch.sf5.part AS p ON p.partkey = ps.partkey
  WHERE p.brand <> 'Brand#45'
    AND p.type NOT LIKE 'MEDIUM POLISHED%'
    AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.suppkey NOT IN (SELECT suppkey FROM tpch.sf5.supplier WHERE comment LIKE '%Customer%Complaints%')
  GROUP BY p.brand, p.type, p.size
),
gold AS (SELECT brand, type, size, supplier_cnt FROM lakekeeper.gold.q16_supplier_cnt)
SELECT 'gold_minus_ref' AS check_name, count(*) AS n FROM (SELECT * FROM gold EXCEPT SELECT * FROM ref)
UNION ALL SELECT 'ref_minus_gold', count(*) FROM (SELECT * FROM ref EXCEPT SELECT * FROM gold)
UNION ALL SELECT 'gold_rows', count(*) FROM gold
UNION ALL SELECT 'ref_rows',  count(*) FROM ref;

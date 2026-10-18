-- Q16 целиком по Bronze. __SCHEMA__ подставляет scripts/bench_compression.sh (bronze_gzip / bronze_zstd).
SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
FROM lakekeeper.__SCHEMA__.partsupp AS ps
JOIN lakekeeper.__SCHEMA__.part AS p ON p.partkey = ps.partkey
WHERE p.brand <> 'Brand#45'
  AND p.type NOT LIKE 'MEDIUM POLISHED%'
  AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps.suppkey NOT IN (
    SELECT suppkey FROM lakekeeper.__SCHEMA__.supplier WHERE comment LIKE '%Customer%Complaints%')
GROUP BY p.brand, p.type, p.size
ORDER BY supplier_cnt DESC, p.brand, p.type, p.size

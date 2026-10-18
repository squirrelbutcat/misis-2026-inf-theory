-- Время последних запросов из system.runtime.queries (живёт до рестарта Trino).
-- exec_ms — чистое выполнение, без очереди и без старта CLI.
SELECT query_id, state,
       queued_time_ms, planning_time_ms,
       date_diff('millisecond', started, "end") AS exec_ms,
       substr(query, 1, 80) AS query_head
FROM system.runtime.queries
WHERE query NOT LIKE '%runtime.queries%'
ORDER BY created DESC
LIMIT 20;

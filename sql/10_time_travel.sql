-- Раздел 6.1: восстановление данных из снапшота. Автоматическая версия — scripts/time_travel.sh.
-- <SNAPSHOT_ID> — id снапшота ДО порчи (шаг 1).

-- 1. История таблицы: берём последний snapshot_id
SELECT committed_at, snapshot_id, parent_id, operation
FROM lakekeeper.silver."part_supplier$snapshots" ORDER BY committed_at;

-- 2. «Порча»: удалили партицию size = 3
DELETE FROM lakekeeper.silver.part_supplier WHERE size = 3;
SELECT count(*) FROM lakekeeper.silver.part_supplier WHERE size = 3;                                 -- 0

-- 3. Time travel: в старом снапшоте строки на месте
SELECT count(*) FROM lakekeeper.silver.part_supplier FOR VERSION AS OF <SNAPSHOT_ID> WHERE size = 3;

-- 4a. Восстановление на месте: дописываем удалённые строки из старого снапшота
INSERT INTO lakekeeper.silver.part_supplier
SELECT * FROM lakekeeper.silver.part_supplier FOR VERSION AS OF <SNAPSHOT_ID> WHERE size = 3;

-- 4b. Альтернатива: восстановить всю таблицу-копию на момент снапшота
-- CREATE TABLE lakekeeper.silver.part_supplier_restored AS
-- SELECT * FROM lakekeeper.silver.part_supplier FOR VERSION AS OF <SNAPSHOT_ID>;

SELECT count(*) FROM lakekeeper.silver.part_supplier WHERE size = 3;                                 -- снова как до порчи

-- =============================================================================
-- Master data — รันหลัง 01_schema.sql
-- ไม่ idempotent (รันซ้ำจะชน PK)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1 line เดียวตาม POC scope
-- -----------------------------------------------------------------------------
INSERT INTO production_line (line_id, name, area) VALUES
    (1, 'Battery Assembly Line', 'MAIN');


-- -----------------------------------------------------------------------------
-- 3 machines — ชื่อ "M01/M02/M03" ต้องตรงกับ tag machine_id ใน InfluxDB เป๊ะ ๆ
--   M01 = Smelting Furnace
--   M02 = Plate Assembly Unit
--   M03 = Formation Charger
-- -----------------------------------------------------------------------------
INSERT INTO machine (machine_id, name, line_id) VALUES
    (1, 'M01', 1),
    (2, 'M02', 1),
    (3, 'M03', 1);


-- -----------------------------------------------------------------------------
-- 3 products (ตาม scope เดิม)
-- -----------------------------------------------------------------------------
INSERT INTO product (product_id, name) VALUES
    (1, 'Car Battery 12V 60Ah Standard'),
    (2, 'Car Battery 12V 75Ah Premium'),
    (3, 'Truck Battery 12V 100Ah Heavy');


-- -----------------------------------------------------------------------------
-- Sync SERIAL sequence หลัง hardcode explicit IDs เพื่อให้ INSERT ครั้งถัดไป
-- ไม่ชน PK
-- -----------------------------------------------------------------------------
SELECT setval('production_line_line_id_seq', (SELECT MAX(line_id)     FROM production_line));
SELECT setval('machine_machine_id_seq',      (SELECT MAX(machine_id)  FROM machine));
SELECT setval('product_product_id_seq',      (SELECT MAX(product_id)  FROM product));

-- =============================================================================
-- Master data seed — run AFTER 01_schema.sql.
-- Idempotent-unfriendly: re-running will raise unique-key violations.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Production line + machines + process stages
-- -----------------------------------------------------------------------------

INSERT INTO production_line (line_id, name, area, capacity_batches_hr) VALUES
    (1, 'Assembly Line 1', 'MAIN', 3);

-- Only the 3 instrumented machines per CLAUDE.md §2.
INSERT INTO machine (machine_id, name, type, line_id, ideal_cycle_sec, status) VALUES
    (1, 'Smelting Furnace #1',    'FURNACE',   1, 120, 'ACTIVE'),
    (2, 'Plate Assembly Unit #1', 'ASSEMBLER', 1,  45, 'ACTIVE'),
    (3, 'Formation Charger #1',   'CHARGER',   1, 300, 'ACTIVE');

-- 10 stages; only stages 1/5/8 tie to an instrumented machine.
INSERT INTO process_stage (stage_id, name, sequence, machine_id, ideal_cycle_sec) VALUES
    ( 1, 'Lead Smelting',       1,    1, 120),
    ( 2, 'Cutting',             2, NULL,  60),
    ( 3, 'Milling to Paste',    3, NULL,  90),
    ( 4, 'Grid Pressing',       4, NULL,  75),
    ( 5, 'Plate Assembly',      5,    2,  45),
    ( 6, 'Case Boxing',         6, NULL,  60),
    ( 7, 'Acid Filling',        7, NULL, 120),
    ( 8, 'Formation Charging',  8,    3, 300),
    ( 9, 'QC Final',            9, NULL, 180),
    (10, 'Finished Good',      10, NULL,  30);

SELECT setval('production_line_line_id_seq', (SELECT MAX(line_id)     FROM production_line));
SELECT setval('machine_machine_id_seq',      (SELECT MAX(machine_id)  FROM machine));
SELECT setval('process_stage_stage_id_seq',  (SELECT MAX(stage_id)    FROM process_stage));

-- -----------------------------------------------------------------------------
-- Products
-- -----------------------------------------------------------------------------

INSERT INTO product (product_id, sku, name, voltage_v, capacity_ah) VALUES
    (1, 'BAT-12V-60AH',  'Car Battery 12V 60Ah Standard',  12.60, 60.0),
    (2, 'BAT-12V-75AH',  'Car Battery 12V 75Ah Premium',   12.60, 75.0),
    (3, 'BAT-12V-100AH', 'Truck Battery 12V 100Ah Heavy',  12.60, 100.0);

SELECT setval('product_product_id_seq', (SELECT MAX(product_id) FROM product));

-- -----------------------------------------------------------------------------
-- Raw materials
-- -----------------------------------------------------------------------------

INSERT INTO raw_material (material_id, name, type, unit, hazard_class) VALUES
    (1, 'Lead',           'Pb',    'kg', 'Class 8'),
    (2, 'Sulfuric Acid',  'H2SO4', 'L',  'Class 8'),
    (3, 'Polypropylene',  'PP',    'kg', NULL),
    (4, 'Copper',         'Cu',    'kg', 'Class 9'),
    (5, 'Metal Grid',     NULL,    'pcs', NULL);

SELECT setval('raw_material_material_id_seq', (SELECT MAX(material_id) FROM raw_material));

-- -----------------------------------------------------------------------------
-- Bill of materials — per-unit consumption for each product
-- -----------------------------------------------------------------------------
-- Rough realistic quantities for a lead-acid car battery.

INSERT INTO bill_of_material (product_id, material_id, qty_per_unit, unit) VALUES
    -- BAT-12V-60AH
    (1, 1, 10.0000, 'kg'),   -- Lead
    (1, 2,  3.5000, 'L'),    -- H2SO4
    (1, 3,  0.8000, 'kg'),   -- Polypropylene
    (1, 4,  0.2000, 'kg'),   -- Copper
    (1, 5,  6.0000, 'pcs'),  -- Grid
    -- BAT-12V-75AH
    (2, 1, 12.5000, 'kg'),
    (2, 2,  4.2000, 'L'),
    (2, 3,  0.9000, 'kg'),
    (2, 4,  0.2500, 'kg'),
    (2, 5,  6.0000, 'pcs'),
    -- BAT-12V-100AH
    (3, 1, 17.0000, 'kg'),
    (3, 2,  5.5000, 'L'),
    (3, 3,  1.1000, 'kg'),
    (3, 4,  0.3500, 'kg'),
    (3, 5,  8.0000, 'pcs');

-- -----------------------------------------------------------------------------
-- Suppliers
-- -----------------------------------------------------------------------------

INSERT INTO supplier (supplier_id, name, contact, lead_time_days) VALUES
    (1, 'Thai Lead Industries',   'sales@thailead.co.th',         7),
    (2, 'Bangkok Chemical Corp',  'orders@bkkchem.co.th',         5),
    (3, 'Asia Polymer Ltd',       'contact@asiapolymer.com',     14),
    (4, 'Siam Metals',            'info@siammetals.co.th',       10);

SELECT setval('supplier_supplier_id_seq', (SELECT MAX(supplier_id) FROM supplier));

-- -----------------------------------------------------------------------------
-- Seed inventory (opening balances for the mock-data window)
-- -----------------------------------------------------------------------------

INSERT INTO inventory (material_id, qty_on_hand, qty_reserved, reorder_level, warehouse_loc) VALUES
    (1, 5000.000,  200.000, 1000.000, 'WH-A01'),
    (2, 2000.000,  100.000,  500.000, 'WH-B02'),
    (3,  800.000,   50.000,  200.000, 'WH-C01'),
    (4,  300.000,   20.000,   80.000, 'WH-C02'),
    (5, 4000.000,  100.000, 1000.000, 'WH-A02');

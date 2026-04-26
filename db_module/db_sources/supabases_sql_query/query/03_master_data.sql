-- =============================================================================
-- Master Data — Line COS Battery Assembly MES
-- Run after 01_schema.sql
-- ไม่ idempotent (รันซ้ำจะชน PK)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. production_line — 1 line ตาม POC scope
-- -----------------------------------------------------------------------------
INSERT INTO production_line (line_id, name, area) VALUES
    (1, 'Battery Assembly Line COS', 'MAIN');

-- -----------------------------------------------------------------------------
-- 2. machine — 3 stations sequential flow (M01 → M02 → M03)
--   ชื่อ "M01/M02/M03" ต้องตรงกับ tag machine_id ใน InfluxDB เป๊ะ ๆ
--   M01 = Plate Stacking Station
--   M02 = Cover Sealing Station
--   M03 = Terminal Welding Station
-- -----------------------------------------------------------------------------
INSERT INTO machine (machine_id, line_id, machine_code, machine_type, sequence_position, install_date) VALUES
    (1, 1, 'M01', 'STACKING', 1, '2024-01-15'),
    (2, 1, 'M02', 'SEALING',  2, '2024-01-15'),
    (3, 1, 'M03', 'WELDING',  3, '2024-01-15');

-- -----------------------------------------------------------------------------
-- 3. battery_model — 3 models ตาม scope เดิม
-- -----------------------------------------------------------------------------
INSERT INTO battery_model (
    model_id, model_code, name,
    spec_plate_count, spec_weight_kg, spec_terminal_type,
    casing_part_no, cover_part_no,
    dim_length_mm, dim_width_mm, dim_height_mm,
    is_active
) VALUES
    (1, 'BAT-12V-60AH',  'Car Battery 12V 60Ah Standard',
        9,  14.50, 'TYPE_A', 'CS-60AH', 'CV-60AH',
        242.0, 175.0, 190.0, 'Y'),
    (2, 'BAT-12V-75AH',  'Car Battery 12V 75Ah Premium',
        11, 17.80, 'TYPE_A', 'CS-75AH', 'CV-75AH',
        278.0, 175.0, 190.0, 'Y'),
    (3, 'BAT-12V-100AH', 'Truck Battery 12V 100Ah Heavy',
        13, 24.00, 'TYPE_B', 'CS-100AH', 'CV-100AH',
        330.0, 173.0, 240.0, 'Y');

-- -----------------------------------------------------------------------------
-- 4. batch_status — state machine vocabulary
-- -----------------------------------------------------------------------------
INSERT INTO batch_status (status_code, description, is_finished) VALUES
    ('CREATED',   'Batch created, not yet started',          'N'),
    ('STARTED',   'Batch in production',                     'N'),
    ('PAUSED',    'Batch paused (break, fault, etc.)',       'N'),
    ('RESUMED',   'Batch resumed after pause',               'N'),
    ('HELD',      'Batch held by QA for investigation',      'N'),
    ('COMPLETED', 'Batch completed successfully',            'Y'),
    ('ABORTED',   'Batch aborted (cannot resume)',           'Y');

-- -----------------------------------------------------------------------------
-- 5. event_reason — shared lookup (ใช้ทั้ง batch_status_event + downtime_event)
-- -----------------------------------------------------------------------------
INSERT INTO event_reason (reason_code, description, category, is_planned) VALUES
    -- Planned events
    ('LUNCH_BREAK',           'Lunch break',                   'BATCH_HOLD',       'Y'),
    ('SHIFT_CHANGE',          'Shift handover',                'BATCH_HOLD',       'Y'),
    ('PLANNED_PM',            'Preventive maintenance',        'MACHINE_DOWNTIME', 'Y'),
    ('CHANGEOVER',            'Product changeover',            'BATCH_HOLD',       'Y'),
    -- Unplanned events
    ('EQUIPMENT_FAULT',       'Machine equipment failure',     'MACHINE_DOWNTIME', 'N'),
    ('POWER_OUTAGE',          'Electrical power loss',         'MACHINE_DOWNTIME', 'N'),
    ('MATERIAL_SHORTAGE',     'Raw material out of stock',     'BATCH_HOLD',       'N'),
    ('QUALITY_INVESTIGATION', 'QA hold for defect analysis',   'BATCH_HOLD',       'N'),
    ('SENSOR_ALARM',          'Sensor threshold exceeded',     'MACHINE_DOWNTIME', 'N'),
    ('OPERATOR_ERROR',        'Manual intervention error',     'BATCH_ABORT',      'N');

-- -----------------------------------------------------------------------------
-- 6. defect_type — recursive hierarchy for QC inspection
-- Insert parents first → children second (FK self-reference order matters)
-- -----------------------------------------------------------------------------

-- Parents (root categories)
INSERT INTO defect_type (defect_code, parent_code, description, severity, category) VALUES
    ('TERMINAL', NULL, 'Terminal-related defects',           NULL, 'ROOT'),
    ('COVER',    NULL, 'Cover-related defects',              NULL, 'ROOT'),
    ('WELDING',  NULL, 'Welding joint defects',              NULL, 'ROOT'),
    ('PLATE',    NULL, 'Plate arrangement defects',          NULL, 'ROOT'),
    ('CASING',   NULL, 'Battery casing defects',             NULL, 'ROOT');

-- Children (specific defects)
INSERT INTO defect_type (defect_code, parent_code, description, severity, category) VALUES
    -- Terminal defects
    ('TERMINAL_LOOSE',      'TERMINAL', 'Terminal not tightened to spec',  4, 'LEAF'),
    ('TERMINAL_MISALIGNED', 'TERMINAL', 'Terminal position offset',        3, 'LEAF'),
    ('TERMINAL_BURN_MARK',  'TERMINAL', 'Burn mark from over-welding',     3, 'LEAF'),

    -- Cover defects
    ('COVER_GAP',           'COVER',    'Cover not sealed flush, gap visible', 5, 'LEAF'),
    ('COVER_CRACK',         'COVER',    'Crack on cover surface',          5, 'LEAF'),
    ('COVER_MISFIT',        'COVER',    'Cover does not fit casing',       4, 'LEAF'),

    -- Welding defects
    ('WELD_INCOMPLETE',     'WELDING',  'Weld not fully fused',            5, 'LEAF'),
    ('WELD_OVER',           'WELDING',  'Excessive welding causing burn',  3, 'LEAF'),
    ('WELD_POROSITY',       'WELDING',  'Porosity in weld joint',          4, 'LEAF'),

    -- Plate defects
    ('PLATE_REVERSED',      'PLATE',    'Plate inserted with reversed polarity', 5, 'LEAF'),
    ('PLATE_MISSING',       'PLATE',    'Plate count below spec',          5, 'LEAF'),
    ('PLATE_DOUBLE',        'PLATE',    'Two plates stacked together',     4, 'LEAF'),

    -- Casing defects
    ('CASING_SCRATCH',      'CASING',   'Surface scratch on casing',       2, 'LEAF'),
    ('CASING_CRACK',        'CASING',   'Crack on casing wall',            5, 'LEAF'),
    ('CASING_DEFORM',       'CASING',   'Deformation from heat or impact', 4, 'LEAF');

-- =============================================================================
-- Sync SERIAL sequences after explicit ID inserts
-- =============================================================================
SELECT setval('production_line_line_id_seq',  (SELECT MAX(line_id)     FROM production_line));
SELECT setval('machine_machine_id_seq',       (SELECT MAX(machine_id)  FROM machine));
SELECT setval('battery_model_model_id_seq',   (SELECT MAX(model_id)    FROM battery_model));
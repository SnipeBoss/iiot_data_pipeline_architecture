-- ============================================================
-- DIM Sync Procedures (sync from Supabase OLTP)
-- 
-- Pattern: MERGE BY src_id (NOT delete+insert)
-- Why: preserve surrogate key stability across syncs
--      → FACT FK ที่ชี้ surrogate ไม่หายแม้ source ขยับ
--
-- Note: Supabase data ลง Oracle ผ่าน Airflow DAG → STG_LINE,
--       STG_BATTERY_MODEL, STG_MACHINE (transient tables)
--       SP นี้อ่าน STG → MERGE เข้า DIM
-- ============================================================

-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_SYNC_DIM_LINE                                        │
-- │  Source: production_line table from Supabase             │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_SYNC_DIM_LINE AS
BEGIN
    -- MERGE pattern: insert new, update existing, never delete
    -- (preserve surrogate keys for FACT FK integrity)
    MERGE INTO DIM_LINE d
    USING (
        SELECT 
            line_id      AS src_id,
            'L' || LPAD(line_id, 2, '0') AS line_code,
            name         AS line_name,
            area         AS area
        FROM STG_LINE
    ) src
    ON (d.line_src_id = src.src_id)
    WHEN MATCHED THEN UPDATE SET
        d.line_code     = src.line_code,
        d.line_name     = src.line_name,
        d.area          = src.area,
        d.is_active     = 'Y'
    WHEN NOT MATCHED THEN INSERT (
        line_id, line_src_id, line_code, line_name, area, 
        process_type, is_active
    ) VALUES (
        SEQ_DIM_LINE.NEXTVAL,
        src.src_id,
        src.line_code,
        src.line_name,
        src.area,
        CASE 
            WHEN UPPER(src.line_name) LIKE '%COS%' THEN 'ASSEMBLY'
            WHEN UPPER(src.line_name) LIKE '%FORMATION%' THEN 'FORMATION'
            WHEN UPPER(src.line_name) LIKE '%PASTING%' THEN 'PASTING'
            ELSE 'UNKNOWN'
        END,
        'Y'
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SYNC_DIM_LINE;
/

-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_SYNC_DIM_BATTERY_MODEL                               │
-- │  Source: battery_model table from Supabase               │
-- │  Includes: derived capacity_class column                 │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_SYNC_DIM_BATTERY_MODEL AS
BEGIN
    MERGE INTO DIM_BATTERY_MODEL d
    USING (
        SELECT 
            model_id            AS src_id,
            model_code,
            name                AS model_name,
            spec_plate_count,
            spec_weight_kg,
            spec_terminal_type,
            dim_length_mm,
            dim_width_mm,
            dim_height_mm,
            -- Derived: classify by capacity (60AH/75AH/100AH)
            CASE 
                WHEN model_code LIKE '%60AH%'  THEN 'Standard'
                WHEN model_code LIKE '%75AH%'  THEN 'Premium'
                WHEN model_code LIKE '%100AH%' THEN 'Heavy'
                ELSE 'Unknown'
            END AS capacity_class,
            'Lead-Acid' AS chemistry,
            is_active
        FROM STG_BATTERY_MODEL
    ) src
    ON (d.model_src_id = src.src_id)
    WHEN MATCHED THEN UPDATE SET
        d.model_code         = src.model_code,
        d.model_name         = src.model_name,
        d.spec_plate_count   = src.spec_plate_count,
        d.spec_weight_kg     = src.spec_weight_kg,
        d.spec_terminal_type = src.spec_terminal_type,
        d.dim_length_mm      = src.dim_length_mm,
        d.dim_width_mm       = src.dim_width_mm,
        d.dim_height_mm      = src.dim_height_mm,
        d.capacity_class     = src.capacity_class,
        d.chemistry          = src.chemistry,
        d.is_active          = src.is_active
    WHEN NOT MATCHED THEN INSERT (
        model_id, model_src_id, model_code, model_name,
        spec_plate_count, spec_weight_kg, spec_terminal_type,
        dim_length_mm, dim_width_mm, dim_height_mm,
        capacity_class, chemistry, is_active
    ) VALUES (
        SEQ_DIM_BATTERY_MODEL.NEXTVAL,
        src.src_id,
        src.model_code,
        src.model_name,
        src.spec_plate_count,
        src.spec_weight_kg,
        src.spec_terminal_type,
        src.dim_length_mm,
        src.dim_width_mm,
        src.dim_height_mm,
        src.capacity_class,
        src.chemistry,
        src.is_active
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SYNC_DIM_BATTERY_MODEL;
/

-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_SYNC_DIM_MACHINE                                     │
-- │  Source: machine JOIN production_line from Supabase      │
-- │  Denormalize line_name into DIM_MACHINE                  │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_SYNC_DIM_MACHINE AS
BEGIN
    -- DIM_LINE must exist first (FK constraint)
    -- Run order: SP_SYNC_DIM_LINE → SP_SYNC_DIM_MACHINE
    MERGE INTO DIM_MACHINE d
    USING (
        SELECT 
            m.machine_id        AS src_id,
            m.machine_code,
            m.machine_type,
            m.sequence_position,
            l.line_id           AS surrogate_line_id,    -- DW key
            l.line_name,
            m.install_date,
            m.is_active
        FROM STG_MACHINE m
        JOIN DIM_LINE l ON l.line_src_id = m.line_id    -- lookup business→surrogate
    ) src
    ON (d.machine_src_id = src.src_id)
    WHEN MATCHED THEN UPDATE SET
        d.machine_code      = src.machine_code,
        d.machine_type      = src.machine_type,
        d.sequence_position = src.sequence_position,
        d.line_id           = src.surrogate_line_id,
        d.line_name         = src.line_name,
        d.install_date      = src.install_date,
        d.is_active         = src.is_active
    WHEN NOT MATCHED THEN INSERT (
        machine_id, machine_src_id, machine_code, machine_type,
        sequence_position, line_id, line_name, install_date, is_active
    ) VALUES (
        SEQ_DIM_MACHINE.NEXTVAL,
        src.src_id,
        src.machine_code,
        src.machine_type,
        src.sequence_position,
        src.surrogate_line_id,
        src.line_name,
        src.install_date,
        src.is_active
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SYNC_DIM_MACHINE;
/

-- ┌──────────────────────────────────────────────────────────┐
-- │  Master sync orchestrator                                │
-- │  Run all DIMs in dependency order                        │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_SYNC_ALL_DIMS AS
BEGIN
    SP_SYNC_DIM_LINE;                  -- Must run first (DIM_MACHINE depends on it)
    SP_SYNC_DIM_BATTERY_MODEL;
    SP_SYNC_DIM_MACHINE;
END SP_SYNC_ALL_DIMS;
/
-- =============================================================================
-- Inventory pipeline: 2 new STG tables + SP_LOAD_FACT_INVENTORY.
-- Run AFTER 01_dw_ddl.sql (facts/dims exist) and 03_sp_fact_loaders.sql.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Idempotent teardown for re-runs.
-- -----------------------------------------------------------------------------
BEGIN
    FOR r IN (
        SELECT 'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE' AS stmt
        FROM user_tables
        WHERE table_name IN ('STG_INVENTORY', 'STG_MATERIAL_CONSUMPTION')
    ) LOOP
        EXECUTE IMMEDIATE r.stmt;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- STG tables
-- -----------------------------------------------------------------------------

-- Current snapshot — 5 rows, one per material. Reloaded on each DAG run.
CREATE TABLE STG_INVENTORY (
    material_id      NUMBER,
    qty_on_hand      NUMBER,
    qty_reserved     NUMBER,
    reorder_level    NUMBER,
    warehouse_loc    VARCHAR2(50),
    updated_at       TIMESTAMP,
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

-- Transactional — scoped to the extract day.
CREATE TABLE STG_MATERIAL_CONSUMPTION (
    consumption_id   NUMBER,
    batch_id         NUMBER,
    material_id      NUMBER,
    qty_used         NUMBER,
    consumed_at      TIMESTAMP,
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_INVENTORY — one row per (material, date).
-- Simplified model (mock data doesn't track historical receipts per day):
--   qty_consumed = SUM(qty_used) from STG_MATERIAL_CONSUMPTION for the date
--   qty_closing  = current qty_on_hand from STG_INVENTORY
--   qty_received = 0 (no per-day receipts in extract)
--   qty_opening  = qty_closing + qty_consumed - qty_received
--   stock_value  = NULL (no price table)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_INVENTORY(p_date IN DATE) IS
    v_date_id NUMBER;
BEGIN
    SELECT date_id INTO v_date_id
    FROM DIM_DATE WHERE full_date = TRUNC(p_date);

    DELETE FROM FACT_INVENTORY WHERE date_id = v_date_id;

    FOR rec IN (
        SELECT dm.material_id,
               dm.material_src_id,
               NVL(inv.qty_on_hand, 0)                                  AS qty_closing,
               NVL((SELECT SUM(mc.qty_used)
                      FROM STG_MATERIAL_CONSUMPTION mc
                     WHERE mc.material_id = dm.material_src_id
                       AND TRUNC(mc.consumed_at) = TRUNC(p_date)), 0)   AS qty_consumed
          FROM DIM_MATERIAL dm
          LEFT JOIN STG_INVENTORY inv ON inv.material_id = dm.material_src_id
    ) LOOP
        INSERT INTO FACT_INVENTORY (
            inventory_id, date_id, material_id,
            qty_opening, qty_received, qty_consumed, qty_closing, stock_value
        ) VALUES (
            SEQ_FACT_INVENTORY.NEXTVAL,
            v_date_id,
            rec.material_id,
            rec.qty_closing + rec.qty_consumed,   -- opening (backward derivation)
            0,                                    -- received (not tracked per-day)
            rec.qty_consumed,
            rec.qty_closing,
            NULL
        );
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No DIM_DATE row for ' || TO_CHAR(p_date));
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

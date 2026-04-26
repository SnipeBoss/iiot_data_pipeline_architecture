-- ┌──────────────────────────────────────────────────────────┐
-- │  1. DIM_DATE — 5 years calendar (2024-2028)              │
-- │     Smart key YYYYMMDD, no sequence                      │
-- └──────────────────────────────────────────────────────────┘
DECLARE
    start_date    DATE := DATE '2024-01-01';
    end_date      DATE := DATE '2028-12-31';
    v_curr_date  DATE;
    current_id    NUMBER;
    iso_dow       NUMBER;

BEGIN

    -- Set Current Date
    v_curr_date := start_date;

    -- Loop ทุกวันใน range
    WHILE v_curr_date <= end_date LOOP

        -- Calculate Value
        current_id := TO_NUMBER(TO_CHAR(v_curr_date, 'YYYYMMDD'));
        iso_dow    := (TRUNC(v_curr_date) - TRUNC(v_curr_date, 'IW')) + 1;

        -- MERGE
        MERGE INTO DIM_DATE
        USING (
            SELECT current_id AS new_date_id 
            FROM DUAL
        ) source_row 
        ON (DIM_DATE.date_id = source_row.new_date_id)


        WHEN NOT MATCHED THEN INSERT (
            date_id, full_date, year, quarter, month_number, month_name,
            week_number, day_of_month, day_of_week, day_name,
            is_weekend, is_holiday
        ) VALUES (
            current_id,
            v_curr_date,
            EXTRACT(YEAR FROM v_curr_date),
            TO_NUMBER(TO_CHAR(v_curr_date, 'Q')),
            EXTRACT(MONTH FROM v_curr_date),
            TO_CHAR(v_curr_date, 'Month', 'NLS_DATE_LANGUAGE=ENGLISH'),
            TO_NUMBER(TO_CHAR(v_curr_date, 'IW')),
            EXTRACT(DAY FROM v_curr_date),

            -- ISO day_of_week: 1=Mon..7=Sun (locale-independent)
            iso_dow,
            TO_CHAR(v_curr_date, 'Day', 'NLS_DATE_LANGUAGE=ENGLISH'),

            -- Saturday=6, Sunday=7 → Y, weekday → N
            CASE WHEN iso_dow IN (6, 7) THEN 'Y' ELSE 'N' END,
            'N'
        );

        -- เลื่อนไปวันถัดไป
        v_curr_date := v_curr_date + 1;
    END LOOP;
    COMMIT;
END;
/


-- ┌──────────────────────────────────────────────────────────┐
-- │  2. DIM_SHIFT — junk dim (DAY/NIGHT)                     │
-- │     2 rows fixed, ไม่ใช้ sequence                        │
-- └──────────────────────────────────────────────────────────┘

-- Day shift (07:30-16:30)
MERGE INTO DIM_SHIFT
USING (SELECT 1 AS new_shift_id FROM DUAL) source_row
    ON (DIM_SHIFT.shift_id = source_row.new_shift_id)

WHEN NOT MATCHED THEN INSERT (
    shift_id, shift_code, shift_name,
    start_hour, start_minute, end_hour, end_minute, crosses_midnight
) VALUES (
    1, 'DAY', 'Day Shift', 7, 30, 16, 30, 'N'
);

-- Night shift (17:30-06:30 next day)
MERGE INTO DIM_SHIFT
USING (SELECT 2 AS new_shift_id FROM DUAL) source_row
    ON (DIM_SHIFT.shift_id = source_row.new_shift_id)

WHEN NOT MATCHED THEN INSERT (
    shift_id, shift_code, shift_name,
    start_hour, start_minute, end_hour, end_minute, crosses_midnight
) VALUES (
    2, 'NIGHT', 'Night Shift', 17, 30, 6, 30, 'Y'
);

COMMIT;








-- ┌──────────────────────────────────────────────────────────┐
-- │  3. DIM_METRIC — sensor metric definitions               │
-- │     metric_name MUST match Influx field name exactly     │
-- └──────────────────────────────────────────────────────────┘

-- M01: Furnace temperature
MERGE INTO DIM_METRIC
USING (SELECT 1 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    1, 'temperature_c', 'celsius', 'M01',
    25, 70, 80, 'Furnace temperature reading'
);

-- All machines: run/idle indicator
MERGE INTO DIM_METRIC
USING (SELECT 2 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    2, 'machine_state_num', 'binary', NULL,
    0, 1, NULL, 'Run/idle indicator (0=idle, 1=running)'
);

-- M02: Cycle count
MERGE INTO DIM_METRIC
USING (SELECT 3 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    3, 'cycle_count', 'count', 'M02',
    0, 900, NULL, 'Cycle count per 15-min window'
);

-- M02: Vibration
MERGE INTO DIM_METRIC
USING (SELECT 4 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    4, 'vibration_g', 'g-force', 'M02',
    0, 3, 5, 'Vibration RMS amplitude'
);

-- M03: Welding current
MERGE INTO DIM_METRIC
USING (SELECT 5 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    5, 'current_a', 'ampere', 'M03',
    0, 50, 60, 'Welding current'
);

-- M03: Welding voltage
MERGE INTO DIM_METRIC
USING (SELECT 6 AS new_metric_id FROM DUAL) source_row
    ON (DIM_METRIC.metric_id = source_row.new_metric_id)

WHEN NOT MATCHED THEN INSERT (
    metric_id, metric_name, unit, machine_code,
    normal_min, normal_max, critical_threshold, description
) VALUES (
    6, 'voltage_v', 'volt', 'M03',
    220, 240, 250, 'Welding voltage'
);

COMMIT;





-- ┌──────────────────────────────────────────────────────────┐
-- │  4. DIM_DEFECT_TYPE — recursive hierarchy (20 rows)      │
-- │     Order: parents (5) first, then leaves (15)           │
-- │     Reason: child rows reference parent_defect_id (FK)   │
-- └──────────────────────────────────────────────────────────┘

-- ── Parents (hierarchy_level=1, is_leaf=N) ────────────────

-- Root 1: TERMINAL
MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 1 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    1, 'TERMINAL', NULL, NULL,
    1, 'N', 'Terminal-related defects', NULL, 'ROOT'
);

-- Root 2: COVER
MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 2 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    2, 'COVER', NULL, NULL,
    1, 'N', 'Cover-related defects', NULL, 'ROOT'
);

-- Root 3: WELDING
MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 3 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    3, 'WELDING', NULL, NULL,
    1, 'N', 'Welding joint defects', NULL, 'ROOT'
);

-- Root 4: PLATE
MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 4 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    4, 'PLATE', NULL, NULL,
    1, 'N', 'Plate arrangement defects', NULL, 'ROOT'
);

-- Root 5: CASING
MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 5 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    5, 'CASING', NULL, NULL,
    1, 'N', 'Battery casing defects', NULL, 'ROOT'
);


-- ── Terminal leaves (parent_defect_id=1) ──────────────────

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 6 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    6, 'TERMINAL_LOOSE', 1, 'TERMINAL',
    2, 'Y', 'Terminal not tightened to spec', 4, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 7 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    7, 'TERMINAL_MISALIGNED', 1, 'TERMINAL',
    2, 'Y', 'Terminal position offset', 3, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 8 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    8, 'TERMINAL_BURN_MARK', 1, 'TERMINAL',
    2, 'Y', 'Burn mark from over-welding', 3, 'LEAF'
);


-- ── Cover leaves (parent_defect_id=2) ─────────────────────

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 9 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    9, 'COVER_GAP', 2, 'COVER',
    2, 'Y', 'Cover not sealed flush', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 10 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    10, 'COVER_CRACK', 2, 'COVER',
    2, 'Y', 'Crack on cover surface', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 11 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    11, 'COVER_MISFIT', 2, 'COVER',
    2, 'Y', 'Cover does not fit casing', 4, 'LEAF'
);


-- ── Welding leaves (parent_defect_id=3) ───────────────────

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 12 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    12, 'WELD_INCOMPLETE', 3, 'WELDING',
    2, 'Y', 'Weld not fully fused', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 13 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    13, 'WELD_OVER', 3, 'WELDING',
    2, 'Y', 'Excessive welding causing burn', 3, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 14 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    14, 'WELD_POROSITY', 3, 'WELDING',
    2, 'Y', 'Porosity in weld joint', 4, 'LEAF'
);


-- ── Plate leaves (parent_defect_id=4) ─────────────────────

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 15 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    15, 'PLATE_REVERSED', 4, 'PLATE',
    2, 'Y', 'Reversed polarity insertion', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 16 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    16, 'PLATE_MISSING', 4, 'PLATE',
    2, 'Y', 'Plate count below spec', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 17 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    17, 'PLATE_DOUBLE', 4, 'PLATE',
    2, 'Y', 'Two plates stacked together', 4, 'LEAF'
);


-- ── Casing leaves (parent_defect_id=5) ────────────────────

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 18 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    18, 'CASING_SCRATCH', 5, 'CASING',
    2, 'Y', 'Surface scratch on casing', 2, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 19 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    19, 'CASING_CRACK', 5, 'CASING',
    2, 'Y', 'Crack on casing wall', 5, 'LEAF'
);

MERGE INTO DIM_DEFECT_TYPE
USING (SELECT 20 AS new_defect_id FROM DUAL) source_row
    ON (DIM_DEFECT_TYPE.defect_id = source_row.new_defect_id)

WHEN NOT MATCHED THEN INSERT (
    defect_id, defect_code, parent_defect_id, parent_code,
    hierarchy_level, is_leaf, description, severity, category
) VALUES (
    20, 'CASING_DEFORM', 5, 'CASING',
    2, 'Y', 'Deformation from heat/impact', 4, 'LEAF'
);

COMMIT;




-- ┌──────────────────────────────────────────────────────────┐
-- │  Verification queries                                     │
-- │  Expected counts:                                         │
-- │    DIM_DATE         1827 (5 years)                        │
-- │    DIM_SHIFT           2                                  │
-- │    DIM_METRIC          6                                  │
-- │    DIM_DEFECT_TYPE    20                                  │
-- └──────────────────────────────────────────────────────────┘
SELECT 'DIM_DATE' AS dim_table, COUNT(*) AS row_count FROM DIM_DATE
UNION ALL SELECT 'DIM_SHIFT',         COUNT(*) FROM DIM_SHIFT
UNION ALL SELECT 'DIM_METRIC',        COUNT(*) FROM DIM_METRIC
UNION ALL SELECT 'DIM_DEFECT_TYPE',   COUNT(*) FROM DIM_DEFECT_TYPE;
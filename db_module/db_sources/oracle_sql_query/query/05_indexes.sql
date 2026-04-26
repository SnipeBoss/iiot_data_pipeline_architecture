-- ============================================================
-- DW Indexes — FK joins + time-range queries
-- Run after 02_schema_fact.sql (tables must exist)
-- ============================================================

-- ┌──────────────────────────────────────────────────────────┐
-- │  FACT_PRODUCTION indexes                                 │
-- └──────────────────────────────────────────────────────────┘
CREATE INDEX idx_fp_date     ON FACT_PRODUCTION(date_id);
CREATE INDEX idx_fp_line     ON FACT_PRODUCTION(line_id);
CREATE INDEX idx_fp_shift    ON FACT_PRODUCTION(shift_id);
CREATE INDEX idx_fp_model    ON FACT_PRODUCTION(model_id);
CREATE INDEX idx_fp_batch    ON FACT_PRODUCTION(batch_src_id);
CREATE INDEX idx_fp_slippage ON FACT_PRODUCTION(slippage_min);

-- ┌──────────────────────────────────────────────────────────┐
-- │  FACT_QUALITY indexes                                    │
-- └──────────────────────────────────────────────────────────┘
CREATE INDEX idx_fq_date     ON FACT_QUALITY(date_id);
CREATE INDEX idx_fq_line     ON FACT_QUALITY(line_id);
CREATE INDEX idx_fq_shift    ON FACT_QUALITY(shift_id);
CREATE INDEX idx_fq_model    ON FACT_QUALITY(model_id);
CREATE INDEX idx_fq_batch    ON FACT_QUALITY(batch_src_id);

-- ┌──────────────────────────────────────────────────────────┐
-- │  FACT_DEFECT indexes                                     │
-- └──────────────────────────────────────────────────────────┘
CREATE INDEX idx_fd_date     ON FACT_DEFECT(date_id);
CREATE INDEX idx_fd_line     ON FACT_DEFECT(line_id);
CREATE INDEX idx_fd_model    ON FACT_DEFECT(model_id);
CREATE INDEX idx_fd_defect   ON FACT_DEFECT(defect_id);
CREATE INDEX idx_fd_qc       ON FACT_DEFECT(qc_src_id);

-- ┌──────────────────────────────────────────────────────────┐
-- │  FACT_DOWNTIME indexes                                   │
-- └──────────────────────────────────────────────────────────┘
CREATE INDEX idx_fdt_date    ON FACT_DOWNTIME(date_id);
CREATE INDEX idx_fdt_line    ON FACT_DOWNTIME(line_id);
CREATE INDEX idx_fdt_shift   ON FACT_DOWNTIME(shift_id);
CREATE INDEX idx_fdt_machine ON FACT_DOWNTIME(machine_id);
CREATE INDEX idx_fdt_start   ON FACT_DOWNTIME(start_ts);
CREATE INDEX idx_fdt_planned ON FACT_DOWNTIME(is_planned);

-- ┌──────────────────────────────────────────────────────────┐
-- │  FACT_SENSOR indexes (heaviest table — time-series scan) │
-- └──────────────────────────────────────────────────────────┘
CREATE INDEX idx_fs_date     ON FACT_SENSOR(date_id);
CREATE INDEX idx_fs_machine  ON FACT_SENSOR(machine_id);
CREATE INDEX idx_fs_metric   ON FACT_SENSOR(metric_id);
CREATE INDEX idx_fs_window   ON FACT_SENSOR(window_start, window_end);
CREATE INDEX idx_fs_composite ON FACT_SENSOR(machine_id, metric_id, window_start);
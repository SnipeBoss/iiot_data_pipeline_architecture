-- ============================================================
-- Trigger 1: sync production_batch.status_code from event log
-- ============================================================
CREATE OR REPLACE FUNCTION fn_sync_batch_status()
RETURNS TRIGGER AS $$
DECLARE
    v_is_finished CHAR(1);
BEGIN
    SELECT is_finished INTO v_is_finished
    FROM batch_status WHERE status_code = NEW.status_code;

    UPDATE production_batch
    SET status_code = NEW.status_code,
        start_time = COALESCE(start_time,
            CASE WHEN NEW.status_code = 'STARTED' THEN NEW.event_ts END),
        end_time = CASE WHEN v_is_finished = 'Y' THEN NEW.event_ts ELSE end_time END
    WHERE batch_id = NEW.batch_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_batch_status
AFTER INSERT ON batch_status_event
FOR EACH ROW EXECUTE FUNCTION fn_sync_batch_status();



-- ============================================================
-- Trigger 2: auto-compute downtime_event.duration_min on close
-- ============================================================
CREATE OR REPLACE FUNCTION fn_compute_downtime_duration()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.end_ts IS NOT NULL THEN
        NEW.duration_min := EXTRACT(EPOCH FROM (NEW.end_ts - NEW.start_ts)) / 60;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_compute_downtime_duration
BEFORE INSERT OR UPDATE ON downtime_event
FOR EACH ROW EXECUTE FUNCTION fn_compute_downtime_duration();
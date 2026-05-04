-- ============================================================
-- ไฟล์: 02_trigger_functions.sql
-- บทบาท: Trigger functions สำหรับรักษาความสอดคล้องของข้อมูลใน Supabase OLTP
--
-- มี 2 ตัว:
--   1) fn_sync_batch_status         — sync สถานะของ batch จาก event log
--   2) fn_compute_downtime_duration — คำนวณระยะเวลา downtime อัตโนมัติ
--
-- หลักคิดร่วม: ใช้ trigger ทำงานที่ "ต้องเกิดทุกครั้ง" และไม่ควรพึ่ง
-- application layer เพื่อให้ data integrity ถูก enforce ที่ DB
-- ============================================================



-- ============================================================
-- Trigger 1: sync production_batch.status_code จาก event log
-- ============================================================
-- ทำไมต้องมี:
--   batch_status_event เป็น append-only event log (เก็บประวัติทุกการเปลี่ยน status)
--   แต่ production_batch ต้องรู้ "สถานะปัจจุบัน" เพื่อให้ query เร็ว
--   → ทุกครั้งที่ insert event ใหม่ ต้อง propagate สถานะล่าสุดมาที่ batch
--
-- พฤติกรรม:
--   - status_code      : ตามค่าใหม่ใน event เสมอ
--   - start_time       : ตั้งครั้งแรกที่เห็น status='STARTED' (COALESCE กันเขียนทับ)
--   - end_time         : ตั้งเมื่อ status เป็นกลุ่ม is_finished='Y' (เช่น COMPLETED, CANCELLED)
--                        ถ้ายังไม่จบ — คงค่าเดิมไว้
--
-- ลำดับการทำงาน:
--   AFTER INSERT บน batch_status_event → เห็นค่าใหม่แล้วค่อย sync ออกไปยัง batch
-- ============================================================
CREATE OR REPLACE FUNCTION fn_sync_batch_status()
RETURNS TRIGGER AS $$
DECLARE
    v_is_finished CHAR(1);  -- ตัวบอกว่า status ใหม่นี้ "จบงาน" หรือไม่ (Y/N)
BEGIN
    -- lookup ตาราง batch_status เพื่อรู้ว่า status_code ใหม่อยู่ในกลุ่ม "จบ" หรือไม่
    -- (เช่น COMPLETED='Y', RUNNING='N')
    SELECT is_finished INTO v_is_finished
    FROM batch_status WHERE status_code = NEW.status_code;

    -- อัปเดต production_batch ให้สะท้อน event ล่าสุด
    UPDATE production_batch
    SET status_code = NEW.status_code,
        
        -- start_time: ใช้ COALESCE เพื่อ "เขียนครั้งแรกครั้งเดียว"
        -- ถ้ามีค่าอยู่แล้ว — ไม่เขียนทับ (ป้องกัน restart event มาเซ็ตซ้ำ)
        start_time = COALESCE(start_time, CASE WHEN NEW.status_code = 'STARTED' THEN NEW.event_ts END),
        
        -- end_time: ตั้งเมื่อ status ใหม่ "จบงาน" เท่านั้น มิฉะนั้นคงเดิม
        end_time = CASE WHEN v_is_finished = 'Y' THEN NEW.event_ts ELSE end_time END
    
    WHERE batch_id = NEW.batch_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



-- ผูก trigger: ทำงาน "หลัง" insert event log สำเร็จ
-- (AFTER เพราะเราอ่านค่าจาก NEW อย่างเดียว ไม่ได้แก้ event ที่เพิ่ง insert)
CREATE TRIGGER trg_sync_batch_status
AFTER INSERT ON batch_status_event
FOR EACH ROW EXECUTE FUNCTION fn_sync_batch_status();








-- ============================================================
-- Trigger 2: คำนวณ downtime_event.duration_min อัตโนมัติเมื่อปิด event
-- ============================================================
-- ทำไมต้องมี:
--   duration_min ใช้ใน FACT_DOWNTIME / report บ่อย — คำนวณตอน query ทุกครั้งสิ้นเปลือง
--   เก็บเป็น stored column แล้วให้ trigger รักษาความสอดคล้องกับ start_ts/end_ts ดีกว่า
--
-- พฤติกรรม:
--   - end_ts IS NULL  → event ยังไม่ปิด ไม่คำนวณ (duration_min = NULL)
--   - end_ts ไม่ NULL → คำนวณเป็นนาที = (end_ts - start_ts) วินาที / 60
--
-- ลำดับการทำงาน:
--   BEFORE INSERT OR UPDATE — เพราะเราต้องการ "แก้ NEW" ก่อนเขียนลง row จริง
--   (ใช้ AFTER จะต้องทำ UPDATE ซ้ำอีกรอบ ไม่จำเป็น)
-- ============================================================
CREATE OR REPLACE FUNCTION fn_compute_downtime_duration()
RETURNS TRIGGER AS $$
BEGIN

    -- ปิด event แล้ว (end_ts มีค่า) → คำนวณ duration เป็นนาที , EXTRACT(EPOCH FROM interval) คืนค่าเป็น "วินาที" → หาร 60 = นาที
    IF NEW.end_ts IS NOT NULL THEN
        NEW.duration_min := EXTRACT(EPOCH FROM (NEW.end_ts - NEW.start_ts)) / 60;
    END IF;

    -- ถ้า end_ts ยัง NULL — ปล่อย duration_min เป็นค่าเดิม/NULL ไว้
    RETURN NEW;

END;
$$ LANGUAGE plpgsql;



-- ผูก trigger: ทำงานทั้งตอน INSERT (เปิด event) และ UPDATE (ปิด event ภายหลัง)
-- BEFORE เพื่อแก้ NEW.duration_min ก่อนเขียนจริง
CREATE TRIGGER trg_compute_downtime_duration
BEFORE INSERT OR UPDATE ON downtime_event
FOR EACH ROW EXECUTE FUNCTION fn_compute_downtime_duration();

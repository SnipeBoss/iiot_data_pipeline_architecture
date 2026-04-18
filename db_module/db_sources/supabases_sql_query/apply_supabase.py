"""รันไฟล์ SQL หลายไฟล์ตามลำดับต่อ Supabase ที่ตั้งค่าไว้ใน SUPABASE_* env

วิธีทำงาน:
- psycopg2 ส่ง string ทั้งก้อนไป server → server parse + รันทีละ statement
  ภายใน transaction เดียว (ไม่ต้องแยก statement เองเหมือน Oracle)
- Fail-fast: เจอ error ไหนจะ rollback ทั้งหมด

การใช้งาน:
    python db_module/db_sources/supabases_sql_query/apply_supabase.py

ลำดับ default:
    query/01_schema.sql       — สร้างตาราง + index
    query/02_master_data.sql  — insert master data (line/machine/product...)
    mock/03_mock_data.sql     — insert mock OLTP data 30 วัน

Override ได้โดยส่ง path ผ่าน argv — จะรันตามลำดับที่ระบุ
"""

from __future__ import annotations

import sys
from pathlib import Path

# เพิ่ม repo root เข้า sys.path เพื่อ import db_module
# path ใหม่: db_module/db_sources/supabases_sql_query/apply_supabase.py → parents[3] = repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from db_module.db_conn import SupabaseConnector  # noqa: E402

# directory ของไฟล์นี้ — ใช้สร้าง default path แบบ relative
HERE = Path(__file__).parent
DEFAULT_FILES = [
    HERE / "query" / "01_schema.sql",
    HERE / "query" / "02_master_data.sql",
    HERE / "mock" / "03_mock_data.sql",
]

# ตารางที่ต้อง audit count หลัง apply เสร็จ (ตาม schema ใหม่ 6 ตาราง)
_AUDIT_TABLES = (
    "production_line", "machine", "product",
    "production_order", "production_batch", "qc_record",
)


def apply_all(paths: list[Path]) -> int:
    """รัน SQL ทุกไฟล์ตามลำดับ ภายใน transaction เดียว

    คืน:
    - 0 สำเร็จ
    - 1 เจอ error ขณะ apply (rollback แล้ว)
    - 2 ไฟล์ SQL หายบาง path
    """
    # ตรวจ file ก่อนเปิด connection เพื่อ fail เร็วและไม่เสีย session
    for p in paths:
        if not p.exists():
            print(f"not found: {p}")
            return 2

    connector = SupabaseConnector()
    conn = connector.connect()
    cur = conn.cursor()
    try:
        for path in paths:
            sql = path.read_text()
            # แสดง path แบบ relative จาก repo root เพื่อ log อ่านง่าย
            print(f"[apply] {path.relative_to(HERE.parent.parent)} ({len(sql):,} bytes)")
            cur.execute(sql)
        conn.commit()
        print("\nOK — all files applied and committed.")

        # Audit row count ของทุกตารางเพื่อยืนยันข้อมูลถูก load จริง
        print("\nRow counts:")
        for table in _AUDIT_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table:<24} {cur.fetchone()[0]:>6}")
        return 0
    except Exception as exc:
        # Rollback ทั้ง transaction เพื่อไม่ให้ Supabase เหลือ state ครึ่ง ๆ
        conn.rollback()
        print(f"\nFAIL: {exc}")
        return 1
    finally:
        cur.close()
        conn.close()


def main() -> int:
    # ถ้ามี argv ใช้ตามนั้น; ไม่ก็ใช้ default 3 ไฟล์
    args = [Path(a) for a in sys.argv[1:]]
    paths = args if args else DEFAULT_FILES
    return apply_all(paths)


if __name__ == "__main__":
    sys.exit(main())

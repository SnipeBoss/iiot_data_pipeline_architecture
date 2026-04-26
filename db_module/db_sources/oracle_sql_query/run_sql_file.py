from __future__ import annotations
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from db_module.db_conn import OracleConnector  



"""
รัน DDL script ของ Oracle ต่อ schema AI03 ผ่าน OracleConnector

หลักการแยกคำสั่ง:
- คำสั่ง SQL ปกติแยกด้วย `;` ที่ท้ายบรรทัด
- PL/SQL block (เช่น BEGIN..END หรือ CREATE PROCEDURE) ต้องจบด้วย `/`
  อยู่บรรทัดเดียวตามธรรมเนียม Oracle

สคริปต์จะพิมพ์บรรทัดแรกของแต่ละคำสั่งเพื่อให้เห็น progress
ถ้า statement ไหน error จะ rollback ทั้งหมดแล้ว exit ทันที (fail-fast)

การใช้งาน:
    python db_module/db_sources/oracle_sql_query/run_sql_file.py [path_to.sql]

ถ้าไม่ระบุ path จะใช้ไฟล์ `query/01_schema.sql` ที่อยู่ใน directory เดียวกัน
"""



# Keywords ที่ใช้ตรวจจับการเริ่มต้น PL/SQL block
# ต้องระบุ object type ให้ชัด (PROCEDURE/FUNCTION/TRIGGER/PACKAGE) — ห้ามใช้
# `CREATE OR REPLACE ` แบบหลวม ๆ เพราะจะ match `CREATE OR REPLACE VIEW` ด้วย
# (view ไม่ใช่ PL/SQL block; จบด้วย `;` ปกติ ไม่ต้อง `/`)
_PLSQL_BLOCK_STARTS = (
    "BEGIN",
    "DECLARE",
    "CREATE OR REPLACE PROCEDURE",
    "CREATE OR REPLACE FUNCTION",
    "CREATE OR REPLACE TRIGGER",
    "CREATE OR REPLACE PACKAGE",
    "CREATE PROCEDURE",
    "CREATE FUNCTION",
    "CREATE TRIGGER",
    "CREATE PACKAGE",
)


def split_statements(sql: str) -> list[str]:
    """แยกไฟล์ DDL เป็น list ของคำสั่ง SQL/PL-SQL

    กติกา:
    - บรรทัดที่มีแค่ `/` คือตัวจบของ PL/SQL block → ทุกอย่างตั้งแต่
      terminator ก่อนหน้ากลายเป็น statement เดียว
    - นอกบล็อก PL/SQL: statement จบที่ `;` ที่เป็น non-whitespace
      char ตัวสุดท้ายของบรรทัด
    - comment บรรทัดเดียว (`--`) นอกบล็อกจะถูกข้าม
    """
    lines = sql.splitlines()
    out: list[str] = []
    buf: list[str] = []
    in_block = False

    for raw in lines:
        stripped = raw.strip()

        # Comment บรรทัดเดียว (นอก PL/SQL block) ข้ามไป
        if stripped.startswith("--") and not in_block:
            continue

        # ตรวจจับจุดเริ่ม PL/SQL block แบบ heuristic
        # (case-insensitive เพราะ SQL keyword ไม่สนตัวพิมพ์)
        if not in_block and stripped.upper().startswith(_PLSQL_BLOCK_STARTS):
            in_block = True

        if in_block:
            # เจอ `/` บรรทัดเดียว = จบ PL/SQL block
            if stripped == "/":
                stmt = "\n".join(buf).strip()
                if stmt:
                    out.append(stmt)
                buf = []
                in_block = False
                continue
            buf.append(raw)
            continue

        # SQL ธรรมดา: สะสมบรรทัดจนกว่าเจอ `;` ที่ท้ายบรรทัด
        buf.append(raw)
        if stripped.endswith(";"):
            # ตัด `;` ท้ายออกเพราะ jaydebeapi/JDBC ไม่ต้องการ
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            buf = []

    # เก็บ statement ท้ายไฟล์ที่ไม่ได้ลงท้ายด้วย `;` หรือ `/`
    tail = "\n".join(buf).strip()
    if tail:
        out.append(tail.rstrip(";").strip())
    return out





def apply(sql_path: Path) -> int:
    """อ่าน SQL ไฟล์ทั้งไฟล์และรันทีละ statement บน Oracle

    คืน 0 ถ้าสำเร็จทั้งหมด, 1 ถ้ามี statement ไหน error
    """
    sql_text = sql_path.read_text()
    statements = split_statements(sql_text)
    print(f"Loaded {len(statements)} statements from {sql_path.name}")

    connector = OracleConnector()
    conn = connector.connect()
    cur = conn.cursor()
    try:
        for i, stmt in enumerate(statements, start=1):
            
            # แสดง 80 ตัวแรกของบรรทัดแรกเพื่อให้ progress อ่านง่าย
            first_line = stmt.splitlines()[0][:80]
            print(f"  [{i:>3}/{len(statements)}] {first_line}")

            try:
                cur.execute(stmt)

            except Exception as exc:
                # Fail-fast: พบ error → rollback ทุกอย่างที่ทำไว้แล้ว exit
                print(f"\nFAIL on statement {i}:\n{stmt}\n---\n{exc}")
                conn.rollback()
                return 1
        conn.commit()
        print("\nOK — all statements applied and committed.")
        return 0
    
    finally:
        cur.close()
        conn.close()





def main() -> int:
    # ถ้า user ระบุ path ผ่าน argv ใช้ตัวนั้น ไม่ก็ default เป็น query/01_schema.sql
    script = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "query" / "01_schema.sql"
    if not script.exists():
        print(f"not found: {script}")
        return 2
    return apply(script)





if __name__ == "__main__":
    sys.exit(main())

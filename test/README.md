# การเชื่อมต่อ Oracle 10g (KMITL)

เอกสารสรุปวิธีเชื่อมต่อ Oracle source database ที่ใช้ในระบบ IIoT data architecture
และวิธี run สคริปต์ทดสอบใน [test/](.) folder

## ปลายทาง

| รายการ | ค่า |
|---|---|
| Host | `161.246.35.92` |
| Port | `1521` |
| Service name | `orcl` |
| iSQL*Plus web UI | `http://161.246.35.92:5560/isqlplus/workspace.uix` |
| DB version | Oracle Database 10g Enterprise Edition Release **10.2.0.3.0** |
| User | `AI03` |

> ข้อมูล connection ได้จากข้อความ "Connected as AI03@orcl" ที่ iSQL*Plus

## ทำไมต้องใช้ JDBC ไม่ใช้ python-oracledb

Oracle 10.2.0.3 เก่ามาก ทำให้ไม่ผ่าน client ทางตรงสองแบบ

1. **python-oracledb thin mode** — รองรับ server ตั้งแต่ 12c ขึ้นไปเท่านั้น ต่อ 10g แล้วได้ `DPY-6005: cannot connect to database` ทุก service name
2. **python-oracledb thick mode + Oracle Instant Client** — IC สำหรับ macOS ARM64 (Apple Silicon) มีแต่เวอร์ชัน 23c ซึ่งสื่อสารกับ 10g ไม่ได้ (compatibility matrix ต้องการ server ≥ 11.2); version x86_64 ต้องรัน Rosetta + Python x86_64

ทางออก: ใช้ **JDBC thin driver** ผ่าน `JayDeBeApi` → ข้ามปัญหา native client ทั้งหมด, ทำงานบน ARM64 ได้สะอาด, driver JAR เพียง 4.5 MB

## Prerequisites

ติดตั้ง 3 อย่าง

### 1. Java (OpenJDK 17)

```bash
brew install openjdk@17
```

เวลาจะรัน ต้องตั้ง `JAVA_HOME`

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

ถ้าอยากให้ถาวร เพิ่มบรรทัดข้างบนลงใน `~/.zshrc`

### 2. Python dependencies

ใช้ venv ของ uv ที่โปรเจกต์มีอยู่แล้ว ([.venv/](../.venv/))

```bash
uv pip install -r requirements.txt
```

JPype1 ต้องมี pre-built wheel สำหรับ ARM64 + Python 3.12 (ของ uv มี 3.12.13 อยู่แล้ว) ถ้า uv พยายาม build จาก source แล้ว fail ให้เพิ่ม `--only-binary=:all:`

```bash
uv pip install --only-binary=:all: JPype1 JayDeBeApi
```

### 3. Oracle JDBC driver

ใช้ `ojdbc8.jar` (version 19.23.0.0 จาก Maven Central) — วางไว้ที่ [drivers/ojdbc8.jar](drivers/ojdbc8.jar)

ถ้ายังไม่มี ดาวน์โหลดใหม่

```bash
curl -sL -o db_module/db_conn/oracle/drivers/ojdbc8.jar \
  https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.23.0.0/ojdbc8-19.23.0.0.jar
```

## การตั้งค่าที่สำคัญสำหรับ Oracle 10g

Oracle 10g ใช้ authentication protocol รุ่นเก่า (O3LOGON) เท่านั้น ต้องส่ง JVM flag ให้ driver ลด logon capability ลง ไม่งั้น handshake fail

```python
jpype.startJVM(
    "-Doracle.jdbc.thinLogonCapability=o3",
    "-Doracle.net.disableOob=true",
    classpath=["db_module/db_conn/oracle/drivers/ojdbc8.jar"],
)
```

JDBC URL รูปแบบ service name (ไม่ใช้ SID)

```
jdbc:oracle:thin:@//161.246.35.92:1521/orcl
```

> หมายเหตุ: JayDeBeApi เปิด autocommit เป็น default ถ้าจะเรียก `conn.commit()` ต้องปิดด้วย `conn.jconn.setAutoCommit(False)` ก่อน

## สคริปต์ทดสอบ

### test_connection.py — ตรวจการเชื่อมต่อ 3 ชั้น

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
.venv/bin/python test/test_connection.py
```

ผลลัพธ์ที่คาดหวัง

```
[1] HTTP iSQL*Plus           → 200 OK
[2] TCP 161.246.35.92:1521   → OPEN
[3] JDBC connect              → OK (sysdate, user, banner)
```

### test_create_table.py — ทดสอบสิทธิ์ DDL/DML

```bash
.venv/bin/python test/test_create_table.py
```

สคริปต์จะ

1. DROP `AI03_CONN_TEST` ถ้ามีของเก่าค้างอยู่
2. CREATE TABLE ใหม่ (id, note, created_at)
3. INSERT 2 rows แล้ว COMMIT
4. SELECT กลับมาแสดง
5. DROP table ทิ้ง

ถ้าทุก step ผ่าน แสดงว่า AI03 มีสิทธิ์ CREATE/INSERT/SELECT/DROP บน schema ของตัวเอง

## โค้ดตัวอย่างการเชื่อมต่อ

```python
import jaydebeapi
import jpype

DRIVER_JAR = "db_module/db_conn/oracle/drivers/ojdbc8.jar"
JDBC_URL = "jdbc:oracle:thin:@//161.246.35.92:1521/orcl"

if not jpype.isJVMStarted():
    jpype.startJVM(
        "-Doracle.jdbc.thinLogonCapability=o3",
        classpath=[DRIVER_JAR],
    )

conn = jaydebeapi.connect(
    "oracle.jdbc.OracleDriver",
    JDBC_URL,
    ["AI03", "<password>"],
    DRIVER_JAR,
)
conn.jconn.setAutoCommit(False)

cur = conn.cursor()
cur.execute("SELECT SYSDATE FROM DUAL")
print(cur.fetchone())
cur.close()
conn.close()
```

## ข้อควรระวัง

- **อย่า hardcode password** ในโค้ด production — ย้ายไป `.env` + อ่านผ่าน `os.getenv` (ปัจจุบันสคริปต์ใน test/ ฝังไว้เพื่อความง่ายของการทดสอบเท่านั้น)
- AI03 ไม่มีสิทธิ์อ่าน `v$database` / `v$instance` ถ้าต้องหาข้อมูล metadata ของ DB ให้ถามแอดมิน
- ทุก connection ที่เปิดต้อง `close()` เสมอ — การเปิดค้างจะกิน session slot ของ shared DB

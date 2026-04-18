"""ตัวเชื่อมต่อ Oracle 10g ผ่าน JDBC (JayDeBeApi + ojdbc8.jar)

ทำไมต้องใช้ JDBC ไม่ใช้ python-oracledb:
Server ของ KMITL รัน Oracle 10.2.0.3 ซึ่งเก่ากว่าที่ python-oracledb thin-mode
รองรับ (ต้อง >= 12c) และไม่มี Instant Client สำหรับ ARM64 (Apple Silicon)
JDBC thin driver พร้อม flag `o3` logon capability เป็นทางเดียวที่ใช้ได้

ข้อจำกัด JVM: jpype เริ่ม JVM ได้ครั้งเดียวต่อ process ดังนั้น `_ensure_jvm`
จึงไม่ทำอะไรถ้า JVM ถูกเริ่มแล้ว และ classpath จะถูก freeze ตั้งแต่ครั้งแรก
ถ้าเปลี่ยนค่า `ORACLE_JDBC_JAR` ต้อง restart Python ใหม่
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .._env import ConfigError, get, require, resolve_path

# JVM arguments สำหรับ jpype
# - thinLogonCapability=o3: บังคับให้ driver รองรับ Oracle 10g (รุ่นที่ KMITL ใช้)
# - disableOob=true: ปิด Out-of-Band breaks ป้องกันปัญหา network บาง environment
# - user.language/country: บังคับใช้ locale อังกฤษ ไม่งั้น host ที่ตั้ง locale ไทย
#   JVM จะใช้ปฏิทิน Buddhist ทำให้ `java.sql.Date.valueOf("2026-01-01")` ส่งเป็น
#   ปี 2569 บน wire (ดู gotchas ใน PLAN.md / CLAUDE.md)
_JVM_ARGS = (
    "-Doracle.jdbc.thinLogonCapability=o3",
    "-Doracle.net.disableOob=true",
    "-Duser.language=en",
    "-Duser.country=US",
)

# คำสั่ง ALTER SESSION ที่รันทุกครั้งที่เปิด connection ใหม่
# เหตุผล: default ของ KMITL server คือ NLS_CALENDAR='THAI BUDDHA' ทำให้
# วันที่ที่ดึงออกมา offset +543 ปี — บังคับ Gregorian + English ทุก session
# เพื่อความ consistent ฝั่ง Python
_SESSION_NLS_STATEMENTS = (
    "ALTER SESSION SET NLS_CALENDAR='GREGORIAN'",
    "ALTER SESSION SET NLS_DATE_LANGUAGE='ENGLISH'",
    "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'",
)


class OracleConnector:
    """Wrapper สำหรับเปิด connection Oracle 10g ผ่าน JDBC

    ใช้งาน:
        connector = OracleConnector()             # อ่านจาก .env
        with connector.cursor() as cur:           # auto-commit/rollback
            cur.execute("SELECT ...")

    หรือเปิด connection เองเพื่อจัดการ transaction:
        conn = connector.connect()
        cur = conn.cursor()
        ...
        conn.commit(); conn.close()
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        service: str | None = None,
        user: str | None = None,
        password: str | None = None,
        jdbc_jar: str | Path | None = None,
    ) -> None:
        # ถ้าไม่ได้ส่ง argument เข้ามา จะ fallback ไปอ่านจาก env variable
        # ค่าที่ `require` = ต้องมีเสมอ; ถ้าขาดจะ raise ConfigError
        self.host = host or require("ORACLE_HOST")
        self.port = int(port) if port else int(require("ORACLE_PORT"))
        self.service = service or require("ORACLE_SERVICE")
        self.user = user or require("ORACLE_USER")
        self.password = password or require("ORACLE_PASSWORD")

        # path ของ ojdbc8.jar สามารถส่งเป็น relative ได้
        # จะถูกแปลงเป็น absolute โดยยึด repo root
        jar = jdbc_jar or require("ORACLE_JDBC_JAR")
        self.jdbc_jar = Path(jar) if isinstance(jar, Path) else resolve_path(str(jar))
        if not self.jdbc_jar.exists():
            raise ConfigError(f"JDBC driver not found at {self.jdbc_jar}")

    @property
    def jdbc_url(self) -> str:
        """สร้าง JDBC URL ตาม format ของ Oracle thin driver"""
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"

    @classmethod
    def _ensure_jvm(cls, jdbc_jar: Path) -> None:
        """เริ่ม JVM ครั้งเดียวพร้อม classpath และ args ที่กำหนดไว้

        ถ้า JVM ถูกเริ่มไปแล้ว (เช่น จากการ connect() ก่อนหน้า) จะไม่ทำอะไร
        classpath ที่ส่งเข้าไปจะ freeze — เปลี่ยนแล้วต้อง restart Python
        """
        import jpype

        if jpype.isJVMStarted():
            return

        # ถ้า user ตั้ง JAVA_HOME ไว้ใน .env ให้ propagate ไปที่ os.environ
        # เพื่อให้ jpype หา libjvm.so/.dylib/.dll ได้ถูกต้อง
        import os
        java_home = get("JAVA_HOME")
        if java_home:
            os.environ.setdefault("JAVA_HOME", java_home)

        jpype.startJVM(*_JVM_ARGS, classpath=[str(jdbc_jar)])

    def connect(self):
        """เปิด connection ใหม่ พร้อมตั้ง NLS settings ให้เป็น Gregorian+English

        - autocommit=False: ต้องเรียก commit()/rollback() เอง (เจตนาเพื่อ safety)
        - ทุก connection จะรัน ALTER SESSION เพื่อให้วันที่ส่งกลับ Python เป็น
          ปฏิทิน Gregorian ไม่ใช่ Buddhist
        """
        self._ensure_jvm(self.jdbc_jar)
        import jaydebeapi

        conn = jaydebeapi.connect(
            "oracle.jdbc.OracleDriver",
            self.jdbc_url,
            [self.user, self.password],
            str(self.jdbc_jar),
        )
        conn.jconn.setAutoCommit(False)

        # รัน ALTER SESSION ครั้งเดียวต่อ connection เพื่อตั้งค่า locale
        setup_cur = conn.cursor()
        try:
            for stmt in _SESSION_NLS_STATEMENTS:
                setup_cur.execute(stmt)
        finally:
            setup_cur.close()
        return conn

    @contextmanager
    def cursor(self) -> Iterator:
        """Context manager ที่เปิด cursor + จัดการ transaction ให้อัตโนมัติ

        - สำเร็จ: commit
        - เกิด exception: rollback แล้ว re-raise
        - ท้ายสุด: ปิด cursor และ connection เสมอ

        หมายเหตุ: เปิด connection ใหม่ทุกครั้งที่เรียก (ยังไม่มี pool)
        ถ้างาน batch ใหญ่ควรใช้ `connect()` ตรง ๆ เพื่อ reuse connection
        """
        conn = self.connect()
        try:
            cur = conn.cursor()
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()

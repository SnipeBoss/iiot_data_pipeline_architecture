from __future__ import annotations
from contextlib import contextmanager
from typing import Iterator
import psycopg2
from .._env import get, require



class SupabaseConnector:
    """
    Wrapper เปิด connection psycopg2 สำหรับ Supabase PostgreSQL

    ตัวเชื่อมต่อ Supabase (PostgreSQL บน cloud) ผ่าน psycopg2
    Supabase เปิดพอร์ต PostgreSQL มาตรฐาน (5432) ให้เชื่อมต่อได้ตรง
    เราเลือกเชื่อมต่อ PostgreSQL โดยตรงแทนที่จะผ่าน PostgREST เพราะ:
    1. Connector ตัวเดียวใช้ซ้ำกับ Airflow PostgresHook ได้ภายหลัง
    2. Airflow/ETL ต้อง bulk insert/copy ซึ่ง REST API ไม่รองรับดี
    3. ลด abstraction layer เวลา debug

    ใช้งาน:
        connector = SupabaseConnector()           # อ่านจาก .env
        with connector.cursor() as cur:           # auto-commit/rollback
            cur.execute("SELECT ...")
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        sslmode: str | None = None,

    ) -> None:
        
        
        # host และ password เป็นค่าจำเป็น (require) ส่วนอื่นมี default ที่ปลอดภัย
        self.host = host or require("SUPABASE_HOST")

        self.port = int(port) if port else int(get("SUPABASE_PORT", "5432"))
        
        self.dbname = dbname or get("SUPABASE_DB", "postgres")
        
        self.user = user or get("SUPABASE_USER", "postgres")
        
        self.password = password or require("SUPABASE_PASSWORD")
        
        # sslmode=require เป็น default ของ Supabase (บังคับเข้ารหัส TLS)
        self.sslmode = sslmode or get("SUPABASE_SSLMODE", "require")



    def connect(self):
        """
        เปิด connection psycopg2
        psycopg2 default autocommit=False ต้องเรียก commit()/rollback() เอง
        """

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
        )



    @contextmanager
    def cursor(self) -> Iterator:
        """
        Context manager ที่จัดการ transaction และ cleanup ให้อัตโนมัติ

        - สำเร็จ: commit
        - เกิด exception: rollback แล้ว re-raise
        - ท้ายสุด: ปิด cursor และ connection เสมอ
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

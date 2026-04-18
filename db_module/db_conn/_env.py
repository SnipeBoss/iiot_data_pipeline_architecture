"""โมดูลช่วยโหลดค่า environment variable สำหรับทุก connector

ไฟล์นี้จะเรียก `load_dotenv` แค่ครั้งเดียวตอน import เพื่อให้โค้ดส่วนอื่น
เรียก `require()` หรือ `get()` ได้ทันทีโดยไม่ต้องโหลดซ้ำ

หลักสำคัญ: ถือว่า empty string ("") เทียบเท่ากับ "ไม่ได้ตั้งค่า"
เพื่อให้ `.env` ที่กรอกไม่ครบ fail ทันที (fail loud) แทนที่จะเชื่อมต่อไป
ที่ host เปล่าโดยไม่รู้ตัว
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Path ไปยัง repo root (ขึ้น 2 ชั้นจาก db_module/db_conn/_env.py)
REPO_ROOT = Path(__file__).resolve().parents[2]

# โหลดค่าจาก .env ที่ repo root; override=False = ไม่ทับ env ที่ระบบตั้งไว้แล้ว
load_dotenv(REPO_ROOT / ".env", override=False)


class ConfigError(RuntimeError):
    """ข้อผิดพลาดเมื่อ environment variable ที่จำเป็นหายไปหรือเป็นค่าว่าง"""


def get(name: str, default: str | None = None) -> str | None:
    """อ่านค่า env variable; ถ้าไม่มีหรือเป็น empty string คืน default

    ใช้สำหรับค่าที่ "มี default ใช้ได้" เช่น port, schema name
    """
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def require(name: str) -> str:
    """อ่านค่า env variable ที่ "ต้องมี"; ถ้าขาดให้ throw ConfigError ทันที

    ใช้กับค่าที่ไม่มี default ปลอดภัยได้ เช่น password, host
    """
    value = get(name)
    if value is None:
        raise ConfigError(
            f"Environment variable {name!r} is not set. "
            f"Copy .env.example to .env and fill it in."
        )
    return value


def resolve_path(raw: str) -> Path:
    """แปลง path ที่อาจเป็น relative ให้เป็น absolute โดยยึดจาก repo root

    ใช้กับค่าเช่น ORACLE_JDBC_JAR ที่ผู้ใช้อาจใส่แบบ relative
    (`drivers/ojdbc8.jar`) เพื่อให้รันสคริปต์จาก sub-directory แล้วยังหาไฟล์เจอ
    """
    p = Path(raw)
    return p if p.is_absolute() else REPO_ROOT / p

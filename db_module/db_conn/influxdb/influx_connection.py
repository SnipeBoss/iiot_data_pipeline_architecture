from __future__ import annotations
from contextlib import contextmanager
from typing import Iterator
from .._env import get, require
from influxdb_client import InfluxDBClient




class InfluxConnector:
    """
    Wrapper เปิด client InfluxDB 2.x

    ใช้งาน:
        influx = InfluxConnector()
        tables = influx.query('from(bucket: "sensors") |> range(start: -1h)')
    """

    def __init__(
        self,
        url: str | None = None,
        org: str | None = None,
        token: str | None = None,
        bucket: str | None = None,
    ) -> None:
        # url และ token ต้องมีเสมอ (require); org และ bucket มี default
        self.url = url or require("INFLUX_URL")
        self.org = org or get("INFLUX_ORG", "factory")
        self.token = token or require("INFLUX_TOKEN")
        self.bucket = bucket or get("INFLUX_BUCKET", "sensors")



    @contextmanager
    def client(self) -> Iterator:
        """
        Context manager สำหรับ InfluxDBClient เปิด-ปิดอัตโนมัติ
        ใช้เมื่อต้องการเรียก API ที่ไม่ได้ห่อใน wrapper method (เช่น delete_api)
        """

        # Calling InfluxDB Connection
        c = InfluxDBClient(url=self.url, token=self.token, org=self.org)

        try:
            yield c
            
        finally:
            c.close()



    def query(self, flux: str) -> list:
        """
        รัน Flux query และคืนค่า FluxTable list ดิบ ๆ
        ใช้เมื่อต้องการ access metadata เช่น column types, groupKey ของ table
        """
        with self.client() as c:
            return c.query_api().query(flux, org=self.org)

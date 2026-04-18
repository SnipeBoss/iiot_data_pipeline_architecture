"""InfluxDB 2.x connector wrapping the official `influxdb-client` SDK.

Thin wrapper that centralises url/org/token/bucket so callers only think in
terms of Flux queries and Point writes.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Sequence

from .._env import get, require


class InfluxConnector:
    def __init__(
        self,
        url: str | None = None,
        org: str | None = None,
        token: str | None = None,
        bucket: str | None = None,
    ) -> None:
        self.url = url or require("INFLUX_URL")
        self.org = org or get("INFLUX_ORG", "factory")
        self.token = token or require("INFLUX_TOKEN")
        self.bucket = bucket or get("INFLUX_BUCKET", "sensors")

    @contextmanager
    def client(self) -> Iterator:
        from influxdb_client import InfluxDBClient

        c = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        try:
            yield c
        finally:
            c.close()

    def query(self, flux: str) -> list:
        """Run a Flux query and return the raw FluxTable list."""
        with self.client() as c:
            return c.query_api().query(flux, org=self.org)

    def query_records(self, flux: str) -> list[dict]:
        """Flatten query result into a list of dicts keyed by field/tag."""
        tables = self.query(flux)
        out: list[dict] = []
        for table in tables:
            for record in table.records:
                row = dict(record.values)
                row.setdefault("_time", record.get_time())
                out.append(row)
        return out

    def write(self, points: Sequence) -> None:
        """Batch-write `Point` objects to the configured bucket."""
        from influxdb_client.client.write_api import SYNCHRONOUS

        with self.client() as c:
            writer = c.write_api(write_options=SYNCHRONOUS)
            writer.write(bucket=self.bucket, org=self.org, record=list(points))

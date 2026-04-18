"""Oracle 10g connector via JDBC (JayDeBeApi + ojdbc8.jar).

Why JDBC and not python-oracledb: the KMITL server runs Oracle 10.2.0.3 which
predates python-oracledb thin-mode support (needs >= 12c) and lacks an ARM64
Instant Client. JDBC thin driver with the `o3` logon capability flag works.

The JVM can only be started once per process, so `_ensure_jvm` is a no-op on
subsequent calls. Classpath is frozen at JVM start — if you change
`ORACLE_JDBC_JAR` you must restart Python.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .._env import ConfigError, get, require, resolve_path

_JVM_ARGS = (
    "-Doracle.jdbc.thinLogonCapability=o3",
    "-Doracle.net.disableOob=true",
    # Force Gregorian/English locale — otherwise a Thai-locale host JVM uses
    # the Buddhist calendar and `java.sql.Date.valueOf("2026-01-01")` stores
    # year 2569 on the wire. See PLAN.md / CLAUDE.md gotchas.
    "-Duser.language=en",
    "-Duser.country=US",
)


class OracleConnector:
    _jvm_started = False

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        service: str | None = None,
        user: str | None = None,
        password: str | None = None,
        jdbc_jar: str | Path | None = None,
    ) -> None:
        self.host = host or require("ORACLE_HOST")
        self.port = int(port) if port else int(require("ORACLE_PORT"))
        self.service = service or require("ORACLE_SERVICE")
        self.user = user or require("ORACLE_USER")
        self.password = password or require("ORACLE_PASSWORD")

        jar = jdbc_jar or require("ORACLE_JDBC_JAR")
        self.jdbc_jar = Path(jar) if isinstance(jar, Path) else resolve_path(str(jar))
        if not self.jdbc_jar.exists():
            raise ConfigError(f"JDBC driver not found at {self.jdbc_jar}")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"

    @classmethod
    def _ensure_jvm(cls, jdbc_jar: Path) -> None:
        import jpype

        if jpype.isJVMStarted():
            return
        import os

        java_home = get("JAVA_HOME")
        if java_home:
            os.environ.setdefault("JAVA_HOME", java_home)
        jpype.startJVM(*_JVM_ARGS, classpath=[str(jdbc_jar)])
        cls._jvm_started = True

    def connect(self):
        """Open a JayDeBeApi connection with autocommit off.

        The KMITL server's default `NLS_CALENDAR` is Thai Buddhist (dates
        come back offset by +543 years), so we force Gregorian + English on
        every new session for consistent Python-side handling.
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
        setup_cur = conn.cursor()
        try:
            setup_cur.execute("ALTER SESSION SET NLS_CALENDAR='GREGORIAN'")
            setup_cur.execute("ALTER SESSION SET NLS_DATE_LANGUAGE='ENGLISH'")
            setup_cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'")
        finally:
            setup_cur.close()
        return conn

    @contextmanager
    def cursor(self) -> Iterator:
        """Scoped cursor that commits on success, rolls back on exception."""
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

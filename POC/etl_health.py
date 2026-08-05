"""Write loader start/finish records to the destination database."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _table_name(connection: Connection, config: dict[str, Any]) -> str:
    schema = config.get("schema", "dbo")
    table = config.get("table", "etl_health")
    if not _IDENTIFIER.fullmatch(schema) or not _IDENTIFIER.fullmatch(table):
        raise ValueError("Health schema and table must be simple SQL identifiers")
    quote = connection.dialect.identifier_preparer.quote
    return f"{quote(schema)}.{quote(table)}"


def _create_table_if_needed(connection: Connection, config: dict[str, Any]) -> None:
    if not config.get("auto_create", False):
        return
    table = _table_name(connection, config)
    object_name = f"{config.get('schema', 'dbo')}.{config.get('table', 'etl_health')}"
    connection.execute(
        text(
            f"""
IF OBJECT_ID(N'{object_name}', N'U') IS NULL
BEGIN
    CREATE TABLE {table} (
        run_id uniqueidentifier NOT NULL,
        loader_name nvarchar(200) NOT NULL,
        target_table nvarchar(300) NOT NULL,
        status varchar(20) NOT NULL,
        started_at datetime2(3) NOT NULL,
        finished_at datetime2(3) NULL,
        rows_loaded bigint NOT NULL DEFAULT (0),
        error_message nvarchar(2000) NULL,
        PRIMARY KEY (run_id, loader_name)
    );
END
"""
        )
    )


def record_start(
    engine: Engine,
    health_config: dict[str, Any],
    run_id: str,
    loader_name: str,
    target_table: str,
    started_at: datetime,
) -> None:
    """Insert a RUNNING row using the loader's destination engine."""

    if not health_config.get("enabled", False):
        return
    with engine.begin() as connection:
        _create_table_if_needed(connection, health_config)
        table = _table_name(connection, health_config)
        connection.execute(
            text(
                f"""
INSERT INTO {table}
    (run_id, loader_name, target_table, status, started_at, rows_loaded)
VALUES
    (:run_id, :loader_name, :target_table, 'RUNNING', :started_at, 0)
"""
            ),
            {
                "run_id": run_id,
                "loader_name": loader_name,
                "target_table": target_table,
                "started_at": started_at,
            },
        )


def record_finish(
    engine: Engine,
    health_config: dict[str, Any],
    run_id: str,
    loader_name: str,
    status: str,
    finished_at: datetime,
    rows_loaded: int,
    error_message: str | None,
) -> None:
    """Update the matching health row after staging/production work ends."""

    if not health_config.get("enabled", False):
        return
    with engine.begin() as connection:
        table = _table_name(connection, health_config)
        connection.execute(
            text(
                f"""
UPDATE {table}
SET status = :status,
    finished_at = :finished_at,
    rows_loaded = :rows_loaded,
    error_message = :error_message
WHERE run_id = :run_id AND loader_name = :loader_name
"""
            ),
            {
                "run_id": run_id,
                "loader_name": loader_name,
                "status": status,
                "finished_at": finished_at,
                "rows_loaded": rows_loaded,
                "error_message": (error_message or "")[:2000] or None,
            },
        )

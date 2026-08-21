"""Write loader lifecycle records through the ETL control framework."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SOURCE_TYPES = {
    "csv": "CSV",
    "tsv": "CSV",
    "excel": "EXCEL",
    "sql": "DATABASE",
    "json": "OTHER",
    "parquet": "OTHER",
}


def _procedure_name(connection: Connection, config: dict[str, Any], name: str) -> str:
    schema = config.get("schema", "ETL")
    if not _IDENTIFIER.fullmatch(schema):
        raise ValueError("Health schema must be a simple SQL identifier")
    quote = connection.dialect.identifier_preparer.quote
    return f"{quote(schema)}.{quote(name)}"


def _source_object(source: dict[str, Any], config_dir: Path) -> str:
    configured = source.get("path") or source.get("query") or source.get("object") or ""
    if source.get("path") and not Path(configured).is_absolute():
        configured = str(config_dir / configured)
    return str(configured)[:500]


def _target_identity(
    health: dict[str, Any], destination: dict[str, Any], engine: Engine
) -> tuple[str | None, str, str, str]:
    connection = destination.get("connection", {})
    target_server = health.get("target_server") or connection.get("server") or engine.url.host
    target_database = health.get("target_database") or connection.get("database") or engine.url.database
    target_schema = health.get("target_schema") or destination.get("schema", "dbo")
    target_table = health.get("target_table") or destination["table"]
    if not target_database:
        raise ValueError("ETL health requires target_database when it cannot be derived")
    return target_server, target_database, target_schema, target_table


def record_start(
    engine: Engine,
    health_config: dict[str, Any],
    loader: dict[str, Any],
    destination: dict[str, Any],
    config_dir: Path,
) -> int | None:
    """Open an ETL.RunHistory row and return its generated ETLRunID."""

    if not health_config.get("enabled", False):
        return None
    source = loader["source"]
    source_type = health_config.get("source_type") or _SOURCE_TYPES.get(
        source.get("type", "csv").lower(), "OTHER"
    )
    target_server, target_database, target_schema, target_table = _target_identity(
        health_config, destination, engine
    )
    parameters = {
        "JobName": loader["name"],
        "JobDescription": health_config.get("job_description"),
        "SourceType": source_type,
        "SourceSystem": health_config.get("source_system"),
        "SourceObject": health_config.get("source_object") or _source_object(source, config_dir),
        "TargetServer": target_server,
        "TargetDatabase": target_database,
        "TargetSchema": target_schema,
        "TargetTable": target_table,
        "LoadType": health_config.get("load_type"),
        "ParentETLRunID": health_config.get("parent_etl_run_id"),
        "CreatedBy": health_config.get("created_by"),
    }
    with engine.begin() as connection:
        procedure = _procedure_name(connection, health_config, "usp_StartRun")
        row = connection.execute(
            text(
                f"""
EXEC {procedure}
    @JobName = :JobName,
    @JobDescription = :JobDescription,
    @SourceType = :SourceType,
    @SourceSystem = :SourceSystem,
    @SourceObject = :SourceObject,
    @TargetServer = :TargetServer,
    @TargetDatabase = :TargetDatabase,
    @TargetSchema = :TargetSchema,
    @TargetTable = :TargetTable,
    @LoadType = :LoadType,
    @ParentETLRunID = :ParentETLRunID,
    @CreatedBy = :CreatedBy
"""
            ),
            parameters,
        ).mappings().one()
    return int(row["ETLRunID"])


def record_finish(
    engine: Engine,
    health_config: dict[str, Any],
    etl_run_id: int | None,
    metrics: dict[str, Any],
) -> None:
    """Complete ETL.RunHistory with every usp_CompleteRun input."""

    if not health_config.get("enabled", False) or etl_run_id is None:
        return
    parameters = {
        "ETLRunID": etl_run_id,
        "RowsReceived": metrics["rows_received"],
        "RowsExtracted": metrics["rows_extracted"],
        "RowsStaged": metrics["rows_staged"],
        "RowsValid": metrics["rows_valid"],
        "RowsInvalid": metrics.get("rows_invalid", 0),
        "RowsExactDuplicate": metrics.get("rows_exact_duplicate", 0),
        "RowsConflictingDuplicate": metrics.get("rows_conflicting_duplicate", 0),
        "RowsAlreadyExist": metrics.get("rows_already_exist", 0),
        "RowsInserted": metrics["rows_inserted"],
        "RowsUpdated": metrics.get("rows_updated", 0),
        "RowsDeleted": metrics.get("rows_deleted", 0),
        "RowsUnchanged": metrics.get("rows_unchanged", 0),
        "RowsRejected": metrics.get("rows_rejected", 0),
        "RowsSkipped": metrics.get("rows_skipped", 0),
        "BatchCount": metrics["batch_count"],
        "FileCount": metrics["file_count"],
        "RejectFilePath": metrics.get("reject_file_path"),
        "DuplicateFilePath": metrics.get("duplicate_file_path"),
        "LogFilePath": metrics.get("log_file_path"),
        "RunStatus": metrics["run_status"],
        "ErrorCode": metrics.get("error_code"),
        "ErrorMessage": metrics.get("error_message"),
        "ValidateReconciliation": metrics.get("validate_reconciliation", True),
    }
    assignments = ",\n    ".join(f"@{name} = :{name}" for name in parameters)
    with engine.begin() as connection:
        procedure = _procedure_name(connection, health_config, "usp_CompleteRun")
        connection.execute(text(f"EXEC {procedure}\n    {assignments}"), parameters)


def record_legacy_status(
    engine: Engine,
    health_config: dict[str, Any],
    loader: dict[str, Any],
    destination: dict[str, Any],
    result: dict[str, Any],
    finished_at: Any,
    log_file_path: Path,
) -> None:
    """Insert one completed load attempt into dbo.ETL_Health_Status."""

    if not health_config.get("enabled", False):
        return

    schema = health_config.get("schema", "dbo")
    table_name = health_config.get("table", "ETL_Health_Status")
    if not _IDENTIFIER.fullmatch(schema) or not _IDENTIFIER.fullmatch(table_name):
        raise ValueError("Legacy health schema and table must be simple SQL identifiers")

    connection_config = destination.get("connection", {})
    loader_health = loader.get("health", {})
    source = loader["source"]
    source_path = source.get("path") or source.get("query") or source.get("object")
    target_server = connection_config.get("server") or engine.url.host or ""
    target_database = connection_config.get("database") or engine.url.database or ""
    target_schema = destination.get("schema", "dbo")
    target_table = destination["table"]
    target_name = loader_health.get("target_table_name") or (
        f"{target_server}.[{target_database}].[{target_schema}].[{target_table}]"
    )
    staging_name = loader_health.get("stg_table_name", "Not Applicable")

    package_path = health_config.get("package_path") or str(
        Path(__file__).with_name("main_loader.py")
    )
    parameters = {
        "PackageName": loader["name"],
        "DataFlowTaskName": loader_health.get(
            "data_flow_task_name", "CSV_to_SQL_Direct_TruncateInsert"
        ),
        "SourceFilePath": str(source_path) if source_path is not None else None,
        "LastRunTime": finished_at.replace(tzinfo=None),
        "TargetTableName": target_name[:255],
        "TaskStatus": "SUCCESS" if result["status"] == "SUCCESS" else "FAILURE",
        "RowCount": result["rows"],
        "PackagePath": package_path,
        "LogFilePath": str(log_file_path),
        "STGTableName": staging_name[:255] if staging_name else None,
        "ProcessFrequency": loader_health.get("process_frequency")
        or health_config.get("process_frequency", "Daily"),
        "Error": result.get("error"),
        "Owner": loader_health.get("owner") or health_config.get("owner"),
    }

    quote = engine.dialect.identifier_preparer.quote
    qualified_table = f"{quote(schema)}.{quote(table_name)}"
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
INSERT INTO {qualified_table}
(
    PackageName, DataFlowTaskName, SourceFilePath, LastRunTime,
    TargetTableName, TaskStatus, Row_Count, PackagePath, LogFilePath,
    STGTableName, ProcessFrequency, Error, Owner
)
VALUES
(
    :PackageName, :DataFlowTaskName, :SourceFilePath, :LastRunTime,
    :TargetTableName, :TaskStatus, :RowCount, :PackagePath, :LogFilePath,
    :STGTableName, :ProcessFrequency, :Error, :Owner
)
"""
            ),
            parameters,
        )

"""YAML-driven source -> staging -> production ETL runner."""

from __future__ import annotations

import argparse
import copy
import logging
import os
import re
import shutil
import sys
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterator
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from sqlalchemy import URL, create_engine, inspect, text
from sqlalchemy import types as sql_types
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import Connection, Engine

from etl_health import record_finish, record_start
from graph_email import send_summary_email

LOGGER = logging.getLogger("etl")
ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TYPE_PATTERN = re.compile(
    r"^(?P<name>[a-z][a-z0-9]*)(?:\((?P<first>max|\d+)(?:,(?P<second>\d+))?\))?$",
    re.IGNORECASE,
)
FILE_SOURCE_TYPES = {"csv", "tsv", "excel", "json", "parquet"}
HEALTH_SOURCE_TYPES = {"CSV", "SNOWFLAKE", "DATABASE", "API", "EXCEL", "OTHER"}
HEALTH_LOAD_TYPES = {
    "INSERT_ONLY",
    "UPSERT",
    "FULL_REFRESH",
    "SNAPSHOT",
    "INCREMENTAL",
    "DELETE_INSERT",
}
US_EASTERN = ZoneInfo("America/New_York")
LOG_DIRECTORY = Path(
    r"\\montefiore.org\centralfiles\data\Procurement PMO\_Data\CCX\LOGS"
)


class EasternFormatter(logging.Formatter):
    """Format log timestamps in U.S. Eastern time."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        value = datetime.fromtimestamp(record.created, US_EASTERN)
        return value.strftime(datefmt) if datefmt else value.isoformat(timespec="milliseconds")


def configure_logging(log_level: str) -> Path:
    """Log to both the console and a timestamped file on the shared drive."""

    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(US_EASTERN).strftime("%Y%m%d_%H%M%S_%f")
    log_path = LOG_DIRECTORY / f"etl_loader_{timestamp}.log"
    formatter = EasternFormatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(
        level=getattr(logging, log_level),
        handlers=[stream_handler, file_handler],
        force=True,
    )
    return log_path


def expand_environment(value: Any) -> Any:
    """Expand ${NAME} and ${NAME:-default} recursively in YAML values."""

    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ValueError(f"Environment variable {name} is not set")

        return ENV_PATTERN.sub(replace, value)
    if isinstance(value, list):
        return [expand_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: expand_environment(item) for key, item in value.items()}
    return value


def deep_merge(default: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(default)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def effective_destination(
    default: dict[str, Any], override: dict[str, Any]
) -> dict[str, Any]:
    """Apply a loader override while allowing a partial structured connection."""

    merged = deep_merge(default, {key: value for key, value in override.items() if key != "connection"})
    if "connection" not in override:
        return merged
    connection_override = override["connection"]
    default_connection = default.get("connection", {})
    changing_from_url_to_server = (
        "server" in connection_override
        and ("url" in default_connection or "url_env" in default_connection)
    )
    if (
        "url" in connection_override
        or "url_env" in connection_override
        or changing_from_url_to_server
    ):
        merged["connection"] = copy.deepcopy(connection_override)
    else:
        merged["connection"] = deep_merge(default.get("connection", {}), connection_override)
    return merged


def build_engine(connection: dict[str, Any], *, fast: bool = False) -> Engine:
    """Create a SQLAlchemy engine from a URL or SQL Server fields."""

    if connection.get("url_env"):
        env_name = connection["url_env"]
        if not os.environ.get(env_name):
            raise ValueError(f"Connection environment variable {env_name} is not set")
        url: str | URL = os.environ[env_name]
    elif connection.get("url"):
        url = connection["url"]
    else:
        for key in ("server", "database"):
            if not connection.get(key):
                raise ValueError(f"Connection is missing {key}")
        auth = connection.get("auth", {"mode": "trusted"})
        mode = auth.get("mode", "trusted")
        query = {
            "driver": connection.get("driver", "ODBC Driver 17 for SQL Server"),
            "Encrypt": "yes" if connection.get("encrypt", True) else "no",
            "TrustServerCertificate": (
                "yes" if connection.get("trust_server_certificate", False) else "no"
            ),
        }
        if mode == "trusted":
            query["Trusted_Connection"] = "yes"
        elif mode != "sql_password":
            raise ValueError("auth.mode must be trusted or sql_password")
        url = URL.create(
            connection.get("dialect", "mssql+pyodbc"),
            username=auth.get("username") if mode == "sql_password" else None,
            password=auth.get("password") if mode == "sql_password" else None,
            host=connection["server"],
            database=connection["database"],
            query=query,
        )
    options: dict[str, Any] = {"pool_pre_ping": True, "hide_parameters": True}
    if fast and str(url).lower().startswith("mssql+pyodbc"):
        options["fast_executemany"] = True
    return create_engine(url, **options)


def configured_path(value: str, config_dir: Path) -> Path:
    """Resolve a YAML path relative to the configuration file."""

    path = Path(value).expanduser()
    return path if path.is_absolute() else config_dir / path


def resolved_source_path(source: dict[str, Any], config_dir: Path) -> Path:
    """Resolve a fixed source path or render its date-based filename pattern."""

    configured = configured_path(source["path"], config_dir)
    pattern = source.get("filename_pattern")
    if not pattern:
        return configured
    try:
        filename = pattern.format(date=datetime.now(US_EASTERN))
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Invalid source filename_pattern: {pattern}") from exc
    if not filename or Path(filename).name != filename:
        raise ValueError("source.filename_pattern must render one filename")
    return configured / filename


def move_source_file(
    source: dict[str, Any], config_dir: Path, destination_directory: str
) -> Path:
    """Move a file source without overwriting an existing destination file."""

    source_path = resolved_source_path(source, config_dir)
    destination = configured_path(destination_directory, config_dir)
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file is not available to move: {source_path}")
    if not destination.is_dir():
        raise FileNotFoundError(f"Move destination does not exist: {destination}")
    if os.path.normcase(str(source_path.parent)) == os.path.normcase(str(destination)):
        raise ValueError("Move destination must differ from the source directory")

    target = destination / source_path.name
    if target.exists():
        raise FileExistsError(f"Move target already exists: {target}")
    return Path(shutil.move(str(source_path), str(target)))


def iter_source_chunks(
    source: dict[str, Any], config_dir: Path, batch_size: int
) -> Iterator[pd.DataFrame]:
    """Yield source rows in bounded chunks so large extracts do not fill memory."""

    source_type = source.get("type", "csv").lower()
    options = dict(source.get("options", {}))
    if source_type == "sql":
        engine = build_engine(source["connection"])
        try:
            with engine.connect() as connection:
                reader = pd.read_sql_query(
                    text(source["query"]),
                    connection,
                    params=source.get("parameters", {}),
                    chunksize=batch_size,
                )
                yield from reader
        finally:
            engine.dispose()
        return

    path = resolved_source_path(source, config_dir)
    if not path.is_file():
        raise FileNotFoundError(f"Source file does not exist: {path}")
    if source_type in {"csv", "tsv"}:
        if source_type == "tsv":
            options.setdefault("sep", "\t")
        options.pop("chunksize", None)
        reader = pd.read_csv(path, chunksize=batch_size, **options)
        try:
            yield from reader
        finally:
            reader.close()
        return
    if source_type == "excel":
        yield pd.read_excel(path, **options)
        return
    if source_type == "json":
        yield pd.read_json(path, **options)
        return
    if source_type == "parquet":
        yield pd.read_parquet(path, **options)
        return
    raise ValueError(f"Unsupported source type: {source_type}")


def parse_sql_type(type_name: str) -> tuple[str, int | str | None, int | None]:
    match = TYPE_PATTERN.fullmatch(type_name.replace(" ", ""))
    if not match:
        raise ValueError(f"Unsupported SQL type: {type_name}")
    name = match.group("name").lower()
    aliases = {"integer": "int", "boolean": "bit", "numeric": "decimal"}
    name = aliases.get(name, name)
    first_text = match.group("first")
    first: int | str | None = None
    if first_text:
        first = first_text.lower() if first_text.lower() == "max" else int(first_text)
    second = int(match.group("second")) if match.group("second") else None
    return name, first, second


def sqlalchemy_type(type_name: str) -> Any:
    name, first, second = parse_sql_type(type_name)
    if name == "tinyint":
        return mssql.TINYINT()
    if name == "smallint":
        return sql_types.SmallInteger()
    if name == "int":
        return sql_types.Integer()
    if name == "bigint":
        return sql_types.BigInteger()
    if name == "decimal":
        return sql_types.Numeric(int(first or 18), int(second or 0))
    if name in {"float", "real"}:
        return sql_types.Float()
    if name == "bit":
        return sql_types.Boolean()
    if name in {"varchar", "char", "nvarchar", "nchar"}:
        length = None if first in {None, "max"} else int(first)
        types = {
            "varchar": sql_types.VARCHAR,
            "char": sql_types.CHAR,
            "nvarchar": sql_types.NVARCHAR,
            "nchar": sql_types.NCHAR,
        }
        return types[name](length=length)
    if name == "date":
        return sql_types.Date()
    if name in {"datetime", "datetime2"}:
        return mssql.DATETIME2()
    if name == "uniqueidentifier":
        return mssql.UNIQUEIDENTIFIER()
    raise ValueError(f"Unsupported SQL type: {type_name}")


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    result = pd.isna(value)
    return bool(result) if not hasattr(result, "__len__") else False


def convert_columns(frame: pd.DataFrame, columns: list[dict[str, Any]]) -> pd.DataFrame:
    """Select, rename, and convert the configured columns."""

    actual = {str(name).casefold(): name for name in frame.columns}
    output = pd.DataFrame(index=frame.index)
    for column in columns:
        source_name = column.get("source", column.get("name"))
        target_name = column.get("target", column.get("name", source_name))
        if not source_name or source_name.casefold() not in actual:
            raise ValueError(f"Missing source column: {source_name}")
        if not target_name or not IDENTIFIER.fullmatch(target_name):
            raise ValueError(f"Invalid target column: {target_name}")
        type_name = column["type"]
        name, first, second = parse_sql_type(type_name)
        converted = []
        for value in frame[actual[source_name.casefold()]]:
            if _is_null(value):
                if not column.get("nullable", True):
                    raise ValueError(f"Column {source_name} does not allow NULL")
                converted.append(None)
            elif name in {"tinyint", "smallint", "int", "bigint"}:
                number = Decimal(str(value))
                if number != number.to_integral_value():
                    raise ValueError(f"Column {source_name} contains a non-integer value")
                converted.append(int(number))
            elif name == "decimal":
                number = Decimal(str(value))
                if not number.is_finite():
                    raise ValueError(f"Column {source_name} contains a non-finite decimal")
                converted.append(number)
            elif name in {"float", "real"}:
                converted.append(float(value))
            elif name == "bit":
                normalized = str(value).strip().lower()
                if normalized not in {"true", "false", "yes", "no", "1", "0"}:
                    raise ValueError(f"Column {source_name} contains an invalid boolean")
                converted.append(normalized in {"true", "yes", "1"})
            elif name == "date":
                converted.append(pd.to_datetime(value).date())
            elif name in {"datetime", "datetime2"}:
                converted.append(pd.to_datetime(value).to_pydatetime())
            elif name in {"varchar", "char", "nvarchar", "nchar"}:
                text_value = str(value)
                if isinstance(first, int) and len(text_value) > first:
                    raise ValueError(f"Column {source_name} exceeds length {first}")
                converted.append(text_value)
            else:
                converted.append(value)
        output[target_name] = pd.Series(converted, index=frame.index, dtype=object)
    return output


def quote_table(connection: Connection, schema: str, table: str) -> str:
    if not IDENTIFIER.fullmatch(schema) or not IDENTIFIER.fullmatch(table):
        raise ValueError("Schema/table names must be simple SQL identifiers")
    quote = connection.dialect.identifier_preparer.quote
    return f"{quote(schema)}.{quote(table)}"


def staging_to_production(
    connection: Connection,
    loader: dict[str, Any],
    destination: dict[str, Any],
    target_columns: list[str],
    run_id: str,
) -> None:
    """Promote staging rows with a generic insert, stored procedure, or SQL."""

    promotion = loader.get("staging_to_prod", {})
    mode = promotion.get("mode", "none")
    if mode == "none":
        return
    if mode == "stored_procedure":
        procedure_parts = promotion["procedure"].split(".")
        if len(procedure_parts) != 2 or not all(IDENTIFIER.fullmatch(part) for part in procedure_parts):
            raise ValueError("procedure must be in schema.procedure format")
        quote = connection.dialect.identifier_preparer.quote
        procedure = f"{quote(procedure_parts[0])}.{quote(procedure_parts[1])}"
        parameters = {
            key: run_id if value == "$RUN_ID" else value
            for key, value in promotion.get("parameters", {}).items()
        }
        for name in parameters:
            if not IDENTIFIER.fullmatch(name):
                raise ValueError(f"Invalid stored-procedure parameter: {name}")
        assignments = ", ".join(f"@{name} = :{name}" for name in parameters)
        command = f"EXEC {procedure}" + (f" {assignments}" if assignments else "")
        connection.execute(text(command), parameters)
        return
    if mode == "sql":
        parameters = {
            key: run_id if value == "$RUN_ID" else value
            for key, value in promotion.get("parameters", {}).items()
        }
        connection.execute(text(promotion["sql"]), parameters)
        return
    if mode not in {"append", "truncate_insert"}:
        raise ValueError(f"Unsupported staging_to_prod mode: {mode}")

    staging = quote_table(
        connection,
        destination.get("staging_schema", "stg"),
        destination["staging_table"],
    )
    production = quote_table(
        connection,
        promotion.get("production_schema", "dbo"),
        promotion["production_table"],
    )
    quote = connection.dialect.identifier_preparer.quote
    column_list = ", ".join(quote(name) for name in target_columns)
    if mode == "truncate_insert":
        connection.execute(text(f"TRUNCATE TABLE {production}"))
    connection.execute(
        text(f"INSERT INTO {production} ({column_list}) SELECT {column_list} FROM {staging}")
    )


def validate_loader(loader: dict[str, Any], destination: dict[str, Any]) -> None:
    for key in ("name", "source", "columns"):
        if not loader.get(key):
            raise ValueError(f"Loader is missing {key}")
    if not IDENTIFIER.fullmatch(loader["name"]):
        raise ValueError(f"Invalid loader name: {loader['name']}")
    if not destination.get("connection") or not destination.get("staging_table"):
        raise ValueError(f"Loader {loader['name']} needs destination connection/staging_table")
    quote_names = [destination.get("staging_schema", "stg"), destination["staging_table"]]
    if not all(IDENTIFIER.fullmatch(value) for value in quote_names):
        raise ValueError(f"Loader {loader['name']} has an invalid staging identifier")
    if destination.get("staging_load_mode", "replace") not in {
        "fail",
        "replace",
        "append",
        "truncate_append",
    }:
        raise ValueError(f"Loader {loader['name']} has an invalid staging_load_mode")
    source = loader["source"]
    source_type = source.get("type", "csv").lower()
    if source_type == "sql":
        if not source.get("connection") or not source.get("query"):
            raise ValueError(f"SQL loader {loader['name']} needs connection and query")
        if source.get("file_move"):
            raise ValueError(f"SQL loader {loader['name']} cannot use source.file_move")
    else:
        if source_type not in FILE_SOURCE_TYPES:
            raise ValueError(f"Loader {loader['name']} has unsupported source type {source_type}")
        if not source.get("path"):
            raise ValueError(f"File loader {loader['name']} needs a source path")
        pattern = source.get("filename_pattern")
        if pattern is not None:
            if not isinstance(pattern, str) or not pattern:
                raise ValueError(
                    f"Loader {loader['name']} source.filename_pattern must be text"
                )
            resolved_source_path(source, Path("."))
        file_move = source.get("file_move", {})
        if not isinstance(file_move, dict):
            raise ValueError(f"Loader {loader['name']} source.file_move must be a mapping")
        for key in ("success_directory", "error_directory"):
            if key in file_move and not isinstance(file_move[key], str):
                raise ValueError(
                    f"Loader {loader['name']} source.file_move.{key} must be a path"
                )

    target_names: set[str] = set()
    for column in loader["columns"]:
        source_name = column.get("source", column.get("name"))
        target_name = column.get("target", column.get("name", source_name))
        if not isinstance(source_name, str) or not source_name:
            raise ValueError(f"Loader {loader['name']} has a column without a source/name")
        if not isinstance(target_name, str) or not IDENTIFIER.fullmatch(target_name):
            raise ValueError(f"Loader {loader['name']} has invalid target column {target_name}")
        if target_name.casefold() in target_names:
            raise ValueError(f"Loader {loader['name']} repeats target column {target_name}")
        target_names.add(target_name.casefold())
        sqlalchemy_type(column["type"])

    promotion = loader.get("staging_to_prod", {})
    mode = promotion.get("mode", "none")
    if mode not in {"none", "append", "truncate_insert", "stored_procedure", "sql"}:
        raise ValueError(f"Loader {loader['name']} has invalid staging_to_prod mode {mode}")
    if mode in {"append", "truncate_insert"}:
        if not promotion.get("production_table"):
            raise ValueError(f"Loader {loader['name']} needs production_table")
        if destination.get("staging_load_mode", "replace") not in {
            "replace",
            "truncate_append",
        }:
            raise ValueError(
                f"Loader {loader['name']} must use staging_load_mode: replace or "
                "truncate_append "
                "with generic staging-to-production modes"
            )
    if mode == "stored_procedure" and not promotion.get("procedure"):
        raise ValueError(f"Loader {loader['name']} needs a procedure")
    if mode == "sql" and not promotion.get("sql"):
        raise ValueError(f"Loader {loader['name']} needs staging_to_prod.sql")


def validate_health(health: dict[str, Any], loader_name: str) -> None:
    """Validate values constrained by ETL.RunHistory before opening a connection."""

    if not health.get("enabled", False):
        return
    schema = health.get("schema", "ETL")
    if not isinstance(schema, str) or not IDENTIFIER.fullmatch(schema):
        raise ValueError(f"Loader {loader_name} has an invalid health schema")
    source_type = health.get("source_type")
    if source_type is not None and source_type not in HEALTH_SOURCE_TYPES:
        raise ValueError(
            f"Loader {loader_name} health.source_type must be one of "
            f"{', '.join(sorted(HEALTH_SOURCE_TYPES))}"
        )
    load_type = health.get("load_type")
    if load_type is not None and load_type not in HEALTH_LOAD_TYPES:
        raise ValueError(
            f"Loader {loader_name} health.load_type must be one of "
            f"{', '.join(sorted(HEALTH_LOAD_TYPES))}"
        )
    for key in ("target_schema", "target_table"):
        value = health.get(key)
        if value is not None and (
            not isinstance(value, str) or not IDENTIFIER.fullmatch(value)
        ):
            raise ValueError(f"Loader {loader_name} health.{key} is invalid")
    parent_run_id = health.get("parent_etl_run_id")
    if parent_run_id is not None and (
        not isinstance(parent_run_id, int) or parent_run_id <= 0
    ):
        raise ValueError(
            f"Loader {loader_name} health.parent_etl_run_id must be a positive integer"
        )


def load_configuration(path: Path) -> tuple[dict[str, Any], list[tuple[dict[str, Any], dict[str, Any]]]]:
    with path.open("r", encoding="utf-8") as stream:
        config = expand_environment(yaml.safe_load(stream))
    if not isinstance(config, dict) or not isinstance(config.get("loaders"), list):
        raise ValueError("YAML must contain a loaders list")
    email = config.get("email", {})
    if email.get("enabled", False):
        required_email = ["tenant_id", "client_id", "client_secret", "sender", "recipients"]
        missing_email = [name for name in required_email if not email.get(name)]
        if missing_email:
            raise ValueError(f"Email configuration is missing: {', '.join(missing_email)}")
    default_destination = config.get("default_destination", {})
    jobs = []
    for loader in config["loaders"]:
        destination = effective_destination(
            default_destination, loader.get("destination", {})
        )
        validate_loader(loader, destination)
        validate_health(deep_merge(config.get("health", {}), loader.get("health", {})), loader["name"])
        jobs.append((loader, destination))
    return config, jobs


def run_loader(
    loader: dict[str, Any],
    destination: dict[str, Any],
    default_health: dict[str, Any],
    config_dir: Path,
    run_id: str,
    log_path: Path,
) -> dict[str, Any]:
    started = datetime.now(US_EASTERN)
    target = f"{destination.get('staging_schema', 'stg')}.{destination['staging_table']}"
    result = {
        "loader": loader["name"],
        "target": target,
        "status": "FAILED",
        "rows": 0,
        "batches": 0,
        "started_at": started.isoformat(),
        "warnings": [],
        "error": None,
        "file_action": None,
    }
    engine: Engine | None = None
    source_chunks: Iterator[pd.DataFrame] | None = None
    failure_phase = "destination"
    health = deep_merge(default_health, loader.get("health", {}))
    health_run_id: int | None = None
    runtime_loader = copy.deepcopy(loader)
    try:
        if loader["source"].get("type", "csv").lower() != "sql":
            runtime_loader["source"]["path"] = str(
                resolved_source_path(loader["source"], config_dir)
            )
            runtime_loader["source"].pop("filename_pattern", None)
        engine = build_engine(destination["connection"], fast=True)
        try:
            health_run_id = record_start(
                engine, health, runtime_loader, destination, config_dir
            )
        except Exception as exc:
            if health.get("required", False):
                raise
            result["warnings"].append(f"Health start failed: {str(exc)[:500]}")

        batch_size = int(destination.get("batch_size", 1000))
        source_chunks = iter_source_chunks(runtime_loader["source"], config_dir, batch_size)
        first_frame: pd.DataFrame | None = None
        failure_phase = "source"
        for raw_frame in source_chunks:
            converted = convert_columns(raw_frame, loader["columns"])
            if not converted.empty:
                first_frame = converted
                break

        if first_frame is None and not destination.get("allow_empty", False):
            raise ValueError("Source returned zero rows; set allow_empty: true to permit it")

        staging_schema = destination.get("staging_schema", "stg")
        staging_table = destination["staging_table"]
        staging_mode = destination.get("staging_load_mode", "replace")
        target_columns = [
            column.get("target", column.get("name", column.get("source")))
            for column in loader["columns"]
        ]
        destination_types = {
            target_column: sqlalchemy_type(column["type"])
            for target_column, column in zip(target_columns, loader["columns"])
        }

        failure_phase = "destination"
        if staging_mode == "truncate_append":
            with engine.begin() as connection:
                if not inspect(connection).has_table(
                    staging_table, schema=staging_schema
                ):
                    raise ValueError(
                        f"Staging table {staging_schema}.{staging_table} does not exist; "
                        "run its CREATE TABLE script first"
                    )
                connection.execute(
                    text(
                        f"TRUNCATE TABLE {quote_table(connection, staging_schema, staging_table)}"
                    )
                )

        def write_chunk(frame: pd.DataFrame, if_exists: str) -> None:
            with engine.begin() as connection:
                frame.to_sql(
                    staging_table,
                    connection,
                    schema=staging_schema,
                    if_exists=if_exists,
                    index=False,
                    dtype=destination_types,
                    chunksize=batch_size,
                )

        chunk_number = 0
        if first_frame is not None:
            failure_phase = "destination"
            first_write_mode = "append" if staging_mode == "truncate_append" else staging_mode
            write_chunk(first_frame, first_write_mode)
            result["rows"] += len(first_frame)
            chunk_number = 1
            result["batches"] = chunk_number

        failure_phase = "source"
        for raw_frame in source_chunks:
            failure_phase = "source"
            frame = convert_columns(raw_frame, loader["columns"])
            if frame.empty:
                continue
            failure_phase = "destination"
            write_chunk(frame, "append")
            result["rows"] += len(frame)
            chunk_number += 1
            result["batches"] = chunk_number
            if chunk_number % 10 == 0:
                LOGGER.info(
                    "Loader %s committed %s staging rows",
                    loader["name"],
                    f"{result['rows']:,}",
                )
            failure_phase = "source"

        if first_frame is None and staging_mode in {"replace", "fail"}:
            failure_phase = "destination"
            write_chunk(pd.DataFrame(columns=target_columns), staging_mode)

        failure_phase = "promotion"
        with engine.begin() as connection:
            staging_to_production(
                connection,
                loader,
                destination,
                target_columns,
                run_id,
            )
        result["status"] = "SUCCESS"
    except Exception as exc:
        result["error"] = str(exc).replace("\r", " ").replace("\n", " ")[:2000]
        LOGGER.error("Loader %s failed: %s", loader["name"], result["error"])
    finally:
        if source_chunks is not None:
            try:
                source_chunks.close()
            except Exception as exc:
                result["warnings"].append(f"Source cleanup failed: {str(exc)[:500]}")

        source = runtime_loader["source"]
        file_move = source.get("file_move", {})
        move_directory = None
        move_label = None
        if result["status"] == "SUCCESS":
            move_directory = file_move.get("success_directory")
            move_label = "archive"
        elif failure_phase == "source":
            move_directory = file_move.get("error_directory")
            move_label = "error"
        if move_directory:
            failure_phase = "file_move"
            try:
                moved_path = move_source_file(source, config_dir, move_directory)
                result["file_action"] = f"Moved to {moved_path}"
                LOGGER.info("Loader %s moved source to %s", loader["name"], moved_path)
            except Exception as exc:
                message = f"Source {move_label} move failed: {str(exc)[:1000]}"
                if result["status"] == "SUCCESS":
                    result["status"] = "FAILED"
                    result["error"] = message
                else:
                    result["warnings"].append(message)
                LOGGER.error("Loader %s %s", loader["name"], message)

        finished = datetime.now(US_EASTERN)
        result["finished_at"] = finished.isoformat()
        if engine is not None:
            try:
                successful = result["status"] == "SUCCESS"
                failure_status = {
                    "source": "FAILED_EXTRACTION",
                    "destination": "FAILED_DATABASE_LOAD",
                    "promotion": "FAILED_DATABASE_LOAD",
                    "file_move": "FAILED",
                }.get(failure_phase, "FAILED")
                record_finish(
                    engine,
                    health,
                    health_run_id,
                    {
                        "rows_received": result["rows"],
                        "rows_extracted": result["rows"],
                        "rows_staged": result["rows"],
                        "rows_valid": result["rows"],
                        "rows_inserted": result["rows"] if successful else 0,
                        "batch_count": result["batches"],
                        "file_count": int(
                            runtime_loader["source"].get("type", "csv").lower() != "sql"
                        ),
                        "log_file_path": str(log_path),
                        "run_status": "COMPLETED" if successful else failure_status,
                        "error_code": None if successful else failure_phase.upper(),
                        "error_message": result["error"],
                        "validate_reconciliation": successful,
                    },
                )
            except Exception as exc:
                result["warnings"].append(f"Health finish failed: {str(exc)[:500]}")
                if health.get("required", False) and result["status"] == "SUCCESS":
                    result["status"] = "FAILED"
                    result["error"] = "Required health finish update failed"
            try:
                engine.dispose()
            except Exception as exc:
                result["warnings"].append(f"Engine cleanup failed: {str(exc)[:500]}")
    return result


def run(config_path: Path, log_path: Path) -> tuple[dict[str, Any], bool]:
    config, jobs = load_configuration(config_path)
    run_id = str(uuid.uuid4())
    started = datetime.now(US_EASTERN)
    results = []
    for loader, destination in jobs:
        if not loader.get("enabled", True):
            now = datetime.now(US_EASTERN).isoformat()
            results.append(
                {
                    "loader": loader["name"],
                    "target": f"{destination.get('staging_schema', 'stg')}.{destination['staging_table']}",
                    "status": "SKIPPED",
                    "rows": 0,
                    "started_at": now,
                    "finished_at": now,
                    "warnings": [],
                    "error": None,
                }
            )
            continue
        LOGGER.info("Starting loader %s", loader["name"])
        results.append(
            run_loader(
                loader,
                destination,
                config.get("health", {}),
                config_path.parent,
                run_id,
                log_path,
            )
        )

    failed = any(result["status"] == "FAILED" for result in results)
    summary = {
        "run_id": run_id,
        "status": "FAILED" if failed else "SUCCESS",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(US_EASTERN).isoformat(),
        "results": results,
    }
    email_failed = False
    try:
        send_summary_email(config.get("email", {}), summary)
    except Exception as exc:
        email_failed = True
        LOGGER.error("Summary email failed: %s", str(exc)[:1000])
    return summary, email_failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()
    try:
        log_path = configure_logging(args.log_level)
        LOGGER.info("Loader started; log file: %s", log_path)
        config_path = args.config.expanduser().resolve()
        config, jobs = load_configuration(config_path)
        if args.validate_only:
            LOGGER.info("Configuration is valid; %d loader(s) found", len(jobs))
            print(f"Configuration is valid. {len(jobs)} loader(s) found.")
            return 0
        summary, email_failed = run(config_path, log_path)
    except Exception as exc:
        LOGGER.exception("Configuration/startup error: %s", exc)
        print(f"Configuration/startup error: {exc}", file=sys.stderr)
        return 3

    completion_message = (
        f"Run {summary['run_id']} finished with {summary['status']}: "
        f"{sum(item['status'] == 'SUCCESS' for item in summary['results'])} succeeded, "
        f"{sum(item['status'] == 'FAILED' for item in summary['results'])} failed, "
        f"{sum(item['status'] == 'SKIPPED' for item in summary['results'])} skipped."
    )
    LOGGER.info(completion_message)
    print(completion_message)
    if summary["status"] == "FAILED":
        return 1
    if email_failed and config.get("email", {}).get("required", False):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

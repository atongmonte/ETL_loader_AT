"""YAML-driven source -> production ETL runner."""

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

from etl_health import record_legacy_status

# Temporarily disabled: the v0 ETL.RunHistory start/finish framework.
# from etl_health import record_finish, record_start
from graph_email import send_summary_email

LOGGER = logging.getLogger("etl")
ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TYPE_PATTERN = re.compile(
    r"^(?P<name>[a-z][a-z0-9]*)(?:\((?P<first>max|\d+)(?:,(?P<second>\d+))?\))?$",
    re.IGNORECASE,
)
FILE_SOURCE_TYPES = {"csv", "tsv", "excel", "json", "parquet"}
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
        return mssql.DATE()
    if name == "datetime":
        return sql_types.DateTime()
    if name == "datetime2":
        return mssql.DATETIME2(precision=int(first) if isinstance(first, int) else None)
    if name == "uniqueidentifier":
        return mssql.UNIQUEIDENTIFIER()
    raise ValueError(f"Unsupported SQL type: {type_name}")


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
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


def _type_signature(type_name: str) -> tuple[str, int | str | None, int | None]:
    """Return a normalized SQL type signature for strict comparisons."""

    name, first, second = parse_sql_type(type_name)
    if name == "datetime2" and first is None:
        first = 7
    return name, first, second


def _database_type_signature(connection: Connection, database_type: Any) -> tuple[str, int | str | None, int | None]:
    rendered = database_type.compile(dialect=connection.dialect)
    rendered = re.split(r"\s+COLLATE\s+", rendered, maxsplit=1, flags=re.IGNORECASE)[0]
    return _type_signature(rendered)


def validate_destination_alignment(
    connection: Connection,
    destination: dict[str, Any],
    columns: list[dict[str, Any]],
) -> None:
    """Require the configured columns to match the existing production table."""

    schema = destination.get("schema", "dbo")
    table_name = destination["table"]
    inspector = inspect(connection)
    if not inspector.has_table(table_name, schema=schema):
        raise ValueError(
            f"Production table {schema}.{table_name} does not exist; create it before loading"
        )

    database_columns = inspector.get_columns(table_name, schema=schema)
    database_by_name = {column["name"].casefold(): column for column in database_columns}
    configured_names = [
        column.get("target", column.get("name", column.get("source")))
        for column in columns
    ]
    configured_keys = {name.casefold() for name in configured_names}
    errors: list[str] = []

    missing = [name for name in configured_names if name.casefold() not in database_by_name]
    if missing:
        errors.append(f"columns not found in production: {', '.join(missing)}")

    generated_columns: set[str] = set()
    for database_column in database_columns:
        key = database_column["name"].casefold()
        if key in configured_keys:
            continue
        is_generated = bool(
            database_column.get("default") is not None
            or database_column.get("computed")
            or database_column.get("identity")
            or database_column.get("autoincrement") is True
        )
        if is_generated:
            generated_columns.add(key)
        else:
            errors.append(
                f"production column {database_column['name']} has no configured source mapping"
            )

    expected_order = [
        column["name"]
        for column in database_columns
        if column["name"].casefold() not in generated_columns
    ]
    if not missing and configured_names != expected_order:
        errors.append(
            "column order differs (configured: "
            + ", ".join(configured_names)
            + "; production: "
            + ", ".join(expected_order)
            + ")"
        )

    for configured_column, configured_name in zip(columns, configured_names):
        database_column = database_by_name.get(configured_name.casefold())
        if database_column is None:
            continue
        configured_type = _type_signature(configured_column["type"])
        database_type = _database_type_signature(connection, database_column["type"])
        if configured_type != database_type:
            errors.append(
                f"{configured_name} type is {configured_column['type']} in configuration "
                f"but {database_column['type']} in production"
            )
        configured_nullable = configured_column.get("nullable", True)
        if configured_nullable != database_column.get("nullable", True):
            errors.append(
                f"{configured_name} nullable is {configured_nullable} in configuration "
                f"but {database_column.get('nullable', True)} in production"
            )

    if errors:
        target = f"{schema}.{table_name}"
        raise ValueError(f"Destination alignment failed for {target}: " + "; ".join(errors))


def validate_loader(loader: dict[str, Any], destination: dict[str, Any]) -> None:
    for key in ("name", "source", "columns"):
        if not loader.get(key):
            raise ValueError(f"Loader is missing {key}")
    if not IDENTIFIER.fullmatch(loader["name"]):
        raise ValueError(f"Invalid loader name: {loader['name']}")
    if not destination.get("connection") or not destination.get("table"):
        raise ValueError(f"Loader {loader['name']} needs destination connection/table")
    quote_names = [destination.get("schema", "dbo"), destination["table"]]
    if not all(IDENTIFIER.fullmatch(value) for value in quote_names):
        raise ValueError(f"Loader {loader['name']} has an invalid destination identifier")
    if destination.get("load_mode", "truncate_append") not in {
        "append",
        "truncate_append",
    }:
        raise ValueError(
            f"Loader {loader['name']} destination.load_mode must be append or truncate_append"
        )
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

def validate_health(health: dict[str, Any], loader_name: str) -> None:
    """Validate the legacy ETL health-table configuration."""

    if not health.get("enabled", False):
        return
    for key, default in (("schema", "dbo"), ("table", "ETL_Health_Status")):
        value = health.get(key, default)
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise ValueError(f"Loader {loader_name} has an invalid health {key}")
    if not isinstance(health.get("connection"), dict):
        raise ValueError(f"Loader {loader_name} health.connection must be configured")


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
    log_path: Path,
) -> dict[str, Any]:
    started = datetime.now(US_EASTERN)
    target = f"{destination.get('schema', 'dbo')}.{destination['table']}"
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
    health_engine: Engine | None = None
    source_chunks: Iterator[pd.DataFrame] | None = None
    failure_phase = "destination"
    health = deep_merge(default_health, loader.get("health", {}))
    runtime_loader = copy.deepcopy(loader)
    try:
        if loader["source"].get("type", "csv").lower() != "sql":
            runtime_loader["source"]["path"] = str(
                resolved_source_path(loader["source"], config_dir)
            )
            runtime_loader["source"].pop("filename_pattern", None)
        engine = build_engine(destination["connection"], fast=True)
        # Temporarily disabled: record_start(...) for ETL.RunHistory.
        # The legacy status table is written once, after this load attempt finishes.

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

        destination_schema = destination.get("schema", "dbo")
        destination_table = destination["table"]
        load_mode = destination.get("load_mode", "truncate_append")
        target_columns = [
            column.get("target", column.get("name", column.get("source")))
            for column in loader["columns"]
        ]
        destination_types = {
            target_column: sqlalchemy_type(column["type"])
            for target_column, column in zip(target_columns, loader["columns"])
        }

        failure_phase = "destination"
        with engine.begin() as connection:
            validate_destination_alignment(connection, destination, loader["columns"])
            if load_mode == "truncate_append":
                connection.execute(
                    text(
                        f"TRUNCATE TABLE {quote_table(connection, destination_schema, destination_table)}"
                    )
                )

        def write_chunk(frame: pd.DataFrame, if_exists: str) -> None:
            with engine.begin() as connection:
                frame.to_sql(
                    destination_table,
                    connection,
                    schema=destination_schema,
                    if_exists=if_exists,
                    index=False,
                    dtype=destination_types,
                    chunksize=batch_size,
                )

        chunk_number = 0
        if first_frame is not None:
            failure_phase = "destination"
            write_chunk(first_frame, "append")
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
                    "Loader %s committed %s production rows",
                    loader["name"],
                    f"{result['rows']:,}",
                )
            failure_phase = "source"

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
        if health.get("enabled", False):
            try:
                # Temporarily disabled: record_finish(...) for ETL.RunHistory.
                health_engine = build_engine(health["connection"])
                record_legacy_status(
                    health_engine,
                    health,
                    runtime_loader,
                    destination,
                    result,
                    finished,
                    log_path,
                )
            except Exception as exc:
                result["warnings"].append(f"Legacy health update failed: {str(exc)[:500]}")
                if health.get("required", False) and result["status"] == "SUCCESS":
                    result["status"] = "FAILED"
                    result["error"] = "Required legacy health update failed"
            finally:
                if health_engine is not None:
                    health_engine.dispose()
        if engine is not None:
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
                    "target": f"{destination.get('schema', 'dbo')}.{destination['table']}",
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

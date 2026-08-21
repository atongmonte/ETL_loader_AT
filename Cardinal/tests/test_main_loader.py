from __future__ import annotations

import logging
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from sqlalchemy.dialects import mssql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main_loader


def _loader(*, pk_check: list[str] | None = None) -> dict:
    return {
        "name": "TestLoader",
        "source": {
            "type": "sql",
            "connection": {"url": "sqlite://"},
            "query": "SELECT 1",
        },
        "destination": {"table": "Target"},
        "direct_load": {
            "strategy": "truncate_insert",
            "batch_size": 2,
            "allow_empty": False,
            "pk_check": pk_check or [],
        },
        "columns": [
            {"source": "id", "target": "ID", "type": "int", "nullable": False},
            {"source": "name", "target": "Name", "type": "varchar(10)"},
        ],
    }


class PrepareSourceTests(unittest.TestCase):
    def test_generated_load_timestamp_is_constant_for_the_batch(self) -> None:
        frame = pd.DataFrame({"id": ["1", "2"]})
        expected = datetime(2026, 8, 21, 11, 45, 12, 340000)
        columns = [
            {"target": "Load_TS", "type": "datetime2(2)", "generated": "load_timestamp"}
        ]

        converted = main_loader.convert_columns(
            frame,
            columns,
            load_timestamp=expected,
        )

        self.assertEqual(converted["Load_TS"].tolist(), [expected, expected])

    def test_generated_year_month_uses_source_month(self) -> None:
        frame = pd.DataFrame({"Month": ["JAN 2024", "AUG 2026"]})
        columns = [
            {
                "source": "Month",
                "target": "YEAR_MONTH",
                "type": "varchar(7)",
                "generated": "year_month",
            }
        ]

        converted = main_loader.convert_columns(frame, columns)

        self.assertEqual(converted["YEAR_MONTH"].tolist(), ["2024-01", "2026-08"])

    def test_money_values_are_rounded_to_four_places(self) -> None:
        frame = pd.DataFrame({"Amount": ["1.23455"]})
        columns = [
            {"source": "Amount", "target": "Amount", "type": "money"}
        ]

        converted = main_loader.convert_columns(frame, columns)

        self.assertEqual(converted["Amount"].tolist(), [Decimal("1.2346")])

    def test_generated_load_date_is_added_without_source_validation(self) -> None:
        frame = pd.DataFrame({"id": ["1", "2"]})
        columns = [
            {"source": "id", "target": "ID", "type": "varchar(10)"},
            {"target": "LAST_UPDATE", "type": "date", "generated": "load_date"},
        ]

        converted = main_loader.convert_columns(
            frame,
            columns,
            validate_values=False,
            load_date=date(2026, 8, 13),
        )

        self.assertEqual(converted["LAST_UPDATE"].tolist(), [date(2026, 8, 13)] * 2)

    def test_disabled_source_validation_only_maps_columns(self) -> None:
        loader = _loader(pk_check=["ID"])
        loader["direct_load"]["validate_source"] = False
        frames = (
            frame
            for frame in [
                pd.DataFrame(
                    {"id": ["not-an-integer", "not-an-integer"], "name": ["x", "y"]}
                )
            ]
        )

        with patch.object(main_loader, "iter_source_chunks", return_value=frames):
            prepared, rows = main_loader.prepare_source(loader, Path("."))

        self.assertEqual(rows, 2)
        self.assertEqual(prepared[0]["ID"].tolist(), ["not-an-integer"] * 2)
        self.assertEqual(list(prepared[0].columns), ["ID", "Name"])

    def test_blank_varchar_is_preserved_as_text(self) -> None:
        frame = pd.DataFrame({"Name": ["", None]})
        columns = [
            {
                "source": "Name",
                "target": "Name",
                "type": "varchar(10)",
                "nullable": True,
            }
        ]

        converted = main_loader.convert_columns(frame, columns)

        self.assertEqual(converted.iloc[0]["Name"], "")
        self.assertIsNone(converted.iloc[1]["Name"])

    def test_decimal_values_are_rounded_to_destination_scale(self) -> None:
        frame = pd.DataFrame({"Price": ["55.00000000", "1.23455000"]})
        columns = [
            {
                "source": "Price",
                "target": "Price",
                "type": "decimal(19,4)",
                "nullable": False,
            }
        ]

        converted = main_loader.convert_columns(frame, columns)

        self.assertEqual(
            converted["Price"].tolist(),
            [Decimal("55.0000"), Decimal("1.2346")],
        )

    def test_duplicate_key_across_chunks_fails_before_load(self) -> None:
        frames = (
            frame
            for frame in (
                pd.DataFrame({"id": [1], "name": ["first"]}),
                pd.DataFrame({"id": [1], "name": ["second"]}),
            )
        )
        with patch.object(main_loader, "iter_source_chunks", return_value=frames):
            with self.assertRaisesRegex(ValueError, "Duplicate primary keys"):
                main_loader.prepare_source(_loader(pk_check=["ID"]), Path("."))

    def test_complete_source_is_converted_before_return(self) -> None:
        frames = (
            frame
            for frame in (
                pd.DataFrame({"id": [1], "name": ["first"]}),
                pd.DataFrame({"id": [2], "name": ["second"]}),
            )
        )
        with patch.object(main_loader, "iter_source_chunks", return_value=frames):
            prepared, rows = main_loader.prepare_source(
                _loader(pk_check=["ID"]), Path(".")
            )
        self.assertEqual(rows, 2)
        self.assertEqual([list(frame.columns) for frame in prepared], [["ID", "Name"]] * 2)


class AlignmentTests(unittest.TestCase):
    def test_generated_load_timestamp_may_be_unmapped(self) -> None:
        loader = _loader()
        database_columns = [
            {
                "name": "ID",
                "type": main_loader.sqlalchemy_type("int"),
                "nullable": False,
                "default": None,
                "autoincrement": False,
            },
            {
                "name": "Name",
                "type": main_loader.sqlalchemy_type("varchar(10)"),
                "nullable": True,
                "default": None,
                "autoincrement": False,
            },
            {
                "name": "Load_TS",
                "type": mssql.DATETIME2(precision=3),
                "nullable": False,
                "default": "(sysutcdatetime())",
                "autoincrement": False,
            },
        ]

        class Inspector:
            def has_table(self, table_name, schema=None):
                return True

            def get_columns(self, table_name, schema=None):
                return database_columns

        class Connection:
            dialect = mssql.dialect()

        destination = {"schema": "dbo", "table": "Target"}
        with patch.object(main_loader, "inspect", return_value=Inspector()):
            main_loader.validate_destination_alignment(
                Connection(), destination, loader["columns"]
            )

    def test_type_mismatch_is_rejected(self) -> None:
        loader = _loader()
        database_columns = [
            {
                "name": "ID",
                "type": main_loader.sqlalchemy_type("bigint"),
                "nullable": False,
                "default": None,
                "autoincrement": False,
            },
            {
                "name": "Name",
                "type": main_loader.sqlalchemy_type("varchar(10)"),
                "nullable": True,
                "default": None,
                "autoincrement": False,
            },
        ]

        class Inspector:
            def has_table(self, table_name, schema=None):
                return True

            def get_columns(self, table_name, schema=None):
                return database_columns

        class Connection:
            dialect = mssql.dialect()

        with patch.object(main_loader, "inspect", return_value=Inspector()):
            with self.assertRaisesRegex(ValueError, "ID type"):
                main_loader.validate_destination_alignment(
                    Connection(), {"schema": "dbo", "table": "Target"}, loader["columns"]
                )


class SourcePathTests(unittest.TestCase):
    def test_wildcard_pattern_resolves_the_only_match(self) -> None:
        expected = Path("inbound/daily-2026-08-21-12-00.xlsx")
        source = {
            "path": "inbound",
            "filename_pattern": "daily-{date:%Y-%m-%d}-*.xlsx",
        }
        with (
            patch.object(main_loader, "datetime") as current,
            patch.object(main_loader.glob, "glob", return_value=[str(expected)]),
        ):
            current.now.return_value = pd.Timestamp("2026-08-21").to_pydatetime()
            resolved = main_loader.resolved_source_path(source, Path("."))

        self.assertEqual(resolved, expected)


class AtomicLoadTests(unittest.TestCase):
    def test_append_strategy_preserves_existing_rows_and_reconciles_growth(self) -> None:
        frame = pd.DataFrame({"ID": [1], "Name": ["first"]})

        class CountResult:
            def __init__(self, value):
                self.value = value

            def scalar_one(self):
                return self.value

        class Transaction:
            def __init__(self, connection):
                self.connection = connection
                self.rolled_back = False

            def __enter__(self):
                return self.connection

            def __exit__(self, exc_type, exc, traceback):
                self.rolled_back = exc_type is not None
                return False

        class Connection:
            dialect = mssql.dialect()

            def __init__(self):
                self.counts = iter([100, 101])
                self.statements = []

            def execute(self, statement):
                self.statements.append(str(statement))
                return CountResult(next(self.counts))

        class Engine:
            def __init__(self):
                self.transaction = Transaction(Connection())

            def begin(self):
                return self.transaction

            def dispose(self):
                return None

        engine = Engine()
        loader = _loader()
        loader["direct_load"]["strategy"] = "append"
        destination = {
            "connection": {"url": "mssql+pyodbc://example"},
            "schema": "dbo",
            "table": "Target",
        }
        with (
            patch.object(main_loader, "build_engine", return_value=engine),
            patch.object(main_loader, "prepare_source", return_value=([frame], 1)),
            patch.object(main_loader, "validate_destination_alignment"),
            patch.object(pd.DataFrame, "to_sql"),
        ):
            result = main_loader.run_loader(
                loader,
                destination,
                {"enabled": False},
                Path("."),
                Path("test.log"),
            )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["rows"], 1)
        self.assertFalse(engine.transaction.rolled_back)
        self.assertFalse(
            any("TRUNCATE TABLE" in statement for statement in engine.transaction.connection.statements)
        )

    def test_insert_error_rolls_back_transaction(self) -> None:
        frame = pd.DataFrame({"ID": [1], "Name": ["first"]})

        class Transaction:
            def __init__(self, connection):
                self.connection = connection
                self.rolled_back = False

            def __enter__(self):
                return self.connection

            def __exit__(self, exc_type, exc, traceback):
                self.rolled_back = exc_type is not None
                return False

        class Connection:
            dialect = mssql.dialect()

            def execute(self, statement):
                return None

        class Engine:
            def __init__(self):
                self.transaction = Transaction(Connection())

            def begin(self):
                return self.transaction

            def dispose(self):
                return None

        engine = Engine()
        loader = _loader()
        destination = {
            "connection": {"url": "mssql+pyodbc://example"},
            "schema": "dbo",
            "table": "Target",
        }
        with (
            patch.object(main_loader, "build_engine", return_value=engine),
            patch.object(main_loader, "prepare_source", return_value=([frame], 1)),
            patch.object(main_loader, "validate_destination_alignment"),
            patch.object(pd.DataFrame, "to_sql", side_effect=RuntimeError("insert failed")),
        ):
            result = main_loader.run_loader(
                loader,
                destination,
                {"enabled": False},
                Path("."),
                Path("test.log"),
            )

        self.assertEqual(result["status"], "FAILED")
        self.assertTrue(engine.transaction.rolled_back)
        self.assertEqual(result["rows"], 0)

    def test_reconciliation_error_rolls_back_transaction(self) -> None:
        frame = pd.DataFrame({"ID": [1], "Name": ["first"]})

        class CountResult:
            def scalar_one(self):
                return 0

        class Transaction:
            def __init__(self, connection):
                self.connection = connection
                self.rolled_back = False

            def __enter__(self):
                return self.connection

            def __exit__(self, exc_type, exc, traceback):
                self.rolled_back = exc_type is not None
                return False

        class Connection:
            dialect = mssql.dialect()

            def execute(self, statement):
                return CountResult()

        class Engine:
            def __init__(self):
                self.transaction = Transaction(Connection())

            def begin(self):
                return self.transaction

            def dispose(self):
                return None

        engine = Engine()
        loader = _loader()
        destination = {
            "connection": {"url": "mssql+pyodbc://example"},
            "schema": "dbo",
            "table": "Target",
        }
        with (
            patch.object(main_loader, "build_engine", return_value=engine),
            patch.object(main_loader, "prepare_source", return_value=([frame], 1)),
            patch.object(main_loader, "validate_destination_alignment"),
            patch.object(pd.DataFrame, "to_sql"),
        ):
            result = main_loader.run_loader(
                loader,
                destination,
                {"enabled": False},
                Path("."),
                Path("test.log"),
            )

        self.assertEqual(result["status"], "FAILED")
        self.assertIn("Row reconciliation failed", result["error"])
        self.assertTrue(engine.transaction.rolled_back)
        self.assertEqual(result["rows"], 0)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main()

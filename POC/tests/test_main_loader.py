from __future__ import annotations

import logging
import sys
import unittest
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


class AtomicLoadTests(unittest.TestCase):
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

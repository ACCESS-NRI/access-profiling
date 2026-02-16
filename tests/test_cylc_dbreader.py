# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import sqlite3
from pathlib import Path

import pytest

from access.profiling.cylc_parser import CylcDBReader
from access.profiling.metrics import tmax


def create_db(test_file, table_name, columns):
    """Helper function to create an sqlite file with the given table name and column names.

    Args:
        table_name (str): The name to give the created table.
        columns (tuple[str]): A tuple names to give to each of the 5 columns.
    """

    sample_data = [
        ("20250101T0000Z", "task1", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z", 0),
        ("20250101T0000Z", "task2", "2025-01-01T00:00:00Z", "2025-01-01T00:00:10Z", 0),
    ]
    with sqlite3.connect(test_file) as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE {} (
                {} TEXT PRIMARY_KEY,
                {} TEXT PRIMARY_KEY,
                {} TEXT,
                {} TEXT,
                {} INTEGER
            )
        """.format(table_name, *columns)
        )
        con.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", sample_data)
        con.commit()


@pytest.fixture(scope="module")
def correct_cylc_task_data():
    """Fixture that returns correctly read Cylc database data."""
    return {
        "region": ["task1_cycle20250101T0000Z", "task2_cycle20250101T0000Z"],
        tmax: [86400, 10],
    }


@pytest.fixture(scope="module")
def cylcdbreader():
    """Fixture that returns a Cylc database reader."""
    return CylcDBReader()


def test_missingdb(cylcdbreader):
    """Tests correct exception is raised when a non-existent file is passed to the reader."""
    with pytest.raises(FileNotFoundError):
        cylcdbreader.parse(Path("/non-existent.db"))


def test_wrong_table(tmp_path, cylcdbreader):
    """Tests correct exception is raised when the expected table isn't present in the database."""
    dbpath = tmp_path / "cylc.db"
    create_db(dbpath, table_name="wrongtable", columns=cylcdbreader._required_cols)
    with pytest.raises(RuntimeError):
        cylcdbreader.parse(dbpath)


def test_wrong_cols(tmp_path, cylcdbreader):
    """Tests correct exception when expected columns aren't in the table."""
    dbpath = tmp_path / "cylc.db"
    wrong_column_names = tuple(f"col{i}" for i in range(len(cylcdbreader._required_cols)))
    create_db(dbpath, table_name=cylcdbreader._table, columns=wrong_column_names)
    with pytest.raises(RuntimeError):
        cylcdbreader.parse(dbpath)


def test_profiling_data(tmp_path, cylcdbreader, correct_cylc_task_data):
    """Tests data is read correctly from database."""
    dbpath = tmp_path / "cylc.db"
    create_db(dbpath, table_name=cylcdbreader._table, columns=cylcdbreader._required_cols)
    data = cylcdbreader.parse(dbpath)

    assert len(data["region"]) == len(correct_cylc_task_data["region"]), (
        f"Expected {len(correct_cylc_task_data['region'])} regions, found {len(data['region'])}."
    )
    for idx, expected_region in enumerate(correct_cylc_task_data["region"]):
        found_region = data["region"][idx]
        assert expected_region == found_region, f"Found {found_region} instead of {expected_region} at idx: {idx}."
        correct_tmax = correct_cylc_task_data[tmax][idx]
        found_tmax = data[tmax][idx]
        assert correct_tmax == found_tmax, (
            f"Incorrect {tmax} for {expected_region}: found {found_tmax} instead of {correct_tmax}."
        )

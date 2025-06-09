import csv
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.append(str(Path(__file__).parent.parent))

from src import address_matcher

validation_data = list(csv.reader(open("src/test/data/sample_addresses.csv")))


@pytest.fixture(scope="function")
def db_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect("gnaf.db", read_only=True)
    return conn


@pytest.mark.parametrize("input_line,output_line,gnaf_pid", validation_data)
def test_addresses(
    db_conn: duckdb.DuckDBPyConnection, input_line: str, output_line: str, gnaf_pid: str
) -> None:
    with db_conn.cursor() as cur:
        matched_result = address_matcher.match_address(cur, input_line.upper(), True)
        assert matched_result.get("address") == output_line

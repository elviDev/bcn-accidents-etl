from datetime import date, datetime

import polars as pl

from src.etl.timescale import SCHEMA_SQL, split_sql, to_copy_csv


def test_split_sql_keeps_dollar_blocks_whole_and_skips_comments():
    script = """
-- a comment; with a semicolon
CREATE TABLE t (a int);
DO $$
BEGIN
    PERFORM 1;
    PERFORM 2;
END $$;
SELECT 1;
"""
    stmts = split_sql(script)
    assert len(stmts) == 3
    assert stmts[0] == "CREATE TABLE t (a int);"
    assert stmts[1].startswith("DO $$") and "PERFORM 2;" in stmts[1] and stmts[1].endswith("END $$;")
    assert stmts[2] == "SELECT 1;"


def test_schema_file_splits_into_expected_statements():
    stmts = split_sql(SCHEMA_SQL.read_text(encoding="utf-8"))
    firsts = [s.split()[0].upper() for s in stmts]
    assert firsts == ["CREATE", "CREATE", "SELECT", "SELECT", "CREATE", "DO", "CALL", "CREATE", "SELECT"]


def test_copy_csv_format():
    df = pl.DataFrame({
        "numero_expedient": ["2024S1", "2024S2"],
        "nom_districte": ["Sant Martí", None],
        "accident_datetime": [datetime(2024, 3, 31, 2), datetime(2024, 1, 1, 23)],
        "accident_date": [date(2024, 3, 31), date(2024, 1, 1)],
        "longitude": [2.17, 2.18],
    })
    lines = to_copy_csv(df).decode("utf-8").splitlines()
    assert lines == [
        "2024S1,Sant Martí,2024-03-31 02:00:00,2024-03-31,2.17",
        "2024S2,,2024-01-01 23:00:00,2024-01-01,2.18",  # null -> empty field -> NULL in COPY csv
    ]

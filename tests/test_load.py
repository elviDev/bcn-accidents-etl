from datetime import date, datetime

import polars as pl

from src.etl.load import bq_schema, read_partitioned, write_clean_parquet


def sample() -> pl.DataFrame:
    return pl.DataFrame({
        "numero_expedient": ["A", "B"],
        "year": [2021, 2022],
        "latitude": [41.39, 41.40],
        "accident_datetime": [datetime(2021, 1, 1, 9), datetime(2022, 5, 1, 18)],
        "accident_date": [date(2021, 1, 1), date(2022, 5, 1)],
    })


def test_partitioned_write_round_trips_and_is_idempotent(tmp_path):
    out = tmp_path / "accidents"
    write_clean_parquet(sample(), out)
    write_clean_parquet(sample().head(1), out)  # rerun replaces, no stale year=2022
    assert sorted(p.name for p in out.iterdir()) == ["year=2021"]
    back = read_partitioned(out)
    assert back["numero_expedient"].to_list() == ["A"]
    assert back.schema["year"] == pl.Int64


def test_bq_schema_types():
    types = {f.name: f.field_type for f in bq_schema(sample())}
    assert types == {
        "numero_expedient": "STRING",
        "year": "INT64",
        "latitude": "FLOAT64",
        "accident_datetime": "DATETIME",
        "accident_date": "DATE",
    }

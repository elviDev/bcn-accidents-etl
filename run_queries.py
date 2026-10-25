"""Run the numbered queries in sql/<engine>/ and print the results.

    python run_queries.py                      # BigQuery
    python run_queries.py --engine timescale   # TimescaleDB
"""

import argparse
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv


def print_result(name: str, df: pl.DataFrame, note: str = "") -> None:
    print(f"\n=== {name}{note}")
    with pl.Config(tbl_rows=40, tbl_width_chars=160, tbl_hide_dataframe_shape=True, fmt_str_lengths=120):
        print(df)


def run_bigquery(paths: list[Path]) -> None:
    from google.cloud import bigquery

    project, dataset = os.environ["GCP_PROJECT"], os.environ["BQ_DATASET"]
    client = bigquery.Client(project=project)
    # Queries use the bare table name `accidents`; the dataset comes from .env.
    config = bigquery.QueryJobConfig(default_dataset=f"{project}.{dataset}")
    for path in paths:
        job = client.query(path.read_text(encoding="utf-8"), job_config=config)
        df = pl.from_arrow(job.result().to_arrow(create_bqstorage_client=False))
        print_result(path.name, df, f"  ({job.total_bytes_processed / 1e6:.2f} MB processed)")


def run_timescale(paths: list[Path]) -> None:
    import psycopg

    with psycopg.connect(os.environ["TIMESCALE_URL"], autocommit=True) as conn:
        for path in paths:
            cur = conn.execute(path.read_text(encoding="utf-8"))
            cols = [d.name for d in cur.description]
            df = pl.DataFrame(cur.fetchall(), schema=cols, orient="row", infer_schema_length=None)
            if cols == ["QUERY PLAN"]:  # EXPLAIN output: print as plain text
                print(f"\n=== {path.name}")
                print("\n".join(df["QUERY PLAN"]))
            else:
                print_result(path.name, df)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--engine", choices=["bigquery", "timescale"], default="bigquery")
    args = parser.parse_args()
    load_dotenv()
    paths = sorted(Path("sql", args.engine).glob("[0-9]*.sql"))
    run_bigquery(paths) if args.engine == "bigquery" else run_timescale(paths)


if __name__ == "__main__":
    main()

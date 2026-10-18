"""Run every query in sql/ against BigQuery and print the results."""

import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from google.cloud import bigquery


def main() -> None:
    load_dotenv()
    project, dataset = os.environ["GCP_PROJECT"], os.environ["BQ_DATASET"]
    client = bigquery.Client(project=project)
    # Queries use the bare table name `accidents`; the dataset comes from .env.
    config = bigquery.QueryJobConfig(default_dataset=f"{project}.{dataset}")
    for path in sorted(Path("sql").glob("*.sql")):
        job = client.query(path.read_text(encoding="utf-8"), job_config=config)
        result = pl.from_arrow(job.result().to_arrow(create_bqstorage_client=False))
        print(f"\n=== {path.name}  ({job.total_bytes_processed / 1e6:.2f} MB processed)")
        with pl.Config(tbl_rows=30, tbl_width_chars=160, tbl_hide_dataframe_shape=True):
            print(result)


if __name__ == "__main__":
    main()

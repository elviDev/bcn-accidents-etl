"""Run the Barcelona accidents ETL: CSV -> Polars -> Parquet -> BigQuery + TimescaleDB."""

import argparse
import os
import sys

import polars as pl
from dotenv import load_dotenv

from src.etl.extract import extract
from src.etl.load import (
    dir_size,
    load_to_bigquery,
    read_partitioned,
    write_clean_parquet,
    write_quarantine_parquet,
)
from src.etl.transform import quality_report, transform

RAW_DIR = "data/raw"
CLEAN_DIR = "data/processed/accidents"
QUARANTINE_PATH = "data/quarantine/quarantine.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-bigquery", action="store_true", help="do not load BigQuery")
    parser.add_argument("--skip-timescale", action="store_true", help="do not load TimescaleDB")
    args = parser.parse_args()
    load_dotenv()

    # 1. Extract
    raw = extract(RAW_DIR)
    raw_rows = raw.select(pl.len()).collect().item()
    print(f"Extracted {raw_rows:,} rows from {RAW_DIR}")

    # 2. Transform
    clean, quarantine = transform(raw)
    if raw_rows != clean.height + quarantine.height:
        sys.exit("Row accounting mismatch: raw != clean + quarantine")
    print(quality_report(raw_rows, clean, quarantine))

    # 3. Write Parquet
    write_clean_parquet(clean, CLEAN_DIR)
    write_quarantine_parquet(quarantine, QUARANTINE_PATH)
    csv_mb = dir_size(RAW_DIR, "*.csv") / 1e6
    pq_mb = dir_size(CLEAN_DIR, "*.parquet") / 1e6
    print(f"\nCSV {csv_mb:.2f} MB -> Parquet {pq_mb:.2f} MB ({pq_mb / csv_mb:.0%} of CSV size, zstd)")

    if not args.skip_bigquery:
        load_bigquery(quarantine)
    if not args.skip_timescale:
        load_timescale()


def load_bigquery(quarantine: pl.DataFrame) -> None:
    from google.cloud import bigquery

    project, dataset = os.environ["GCP_PROJECT"], os.environ["BQ_DATASET"]
    client = bigquery.Client(project=project)
    location = client.get_dataset(f"{project}.{dataset}").location

    table = load_to_bigquery(
        read_partitioned(CLEAN_DIR),
        f"{project}.{dataset}.accidents",
        location,
        client,
        partition_field="accident_date",
        cluster_fields=["nom_districte"],
    )
    print(f"\nLoaded {table.num_rows:,} rows into {table.full_table_id} "
          f"(partitioned by {table.time_partitioning.field}/{table.time_partitioning.type_}, "
          f"clustered by {table.clustering_fields})")

    q_table = load_to_bigquery(quarantine, f"{project}.{dataset}.accidents_quarantine", location, client)
    print(f"Loaded {q_table.num_rows:,} rows into {q_table.full_table_id}")


def load_timescale() -> None:
    url = os.environ.get("TIMESCALE_URL")
    if not url:
        print("\nTIMESCALE_URL not set in .env, skipping TimescaleDB")
        return
    from src.etl.timescale import load_to_timescale

    s = load_to_timescale(read_partitioned(CLEAN_DIR), url)
    print(f"\nTimescaleDB {s['version']}: loaded {s['rows']:,} rows into hypertable accidents "
          f"({s['chunks']} chunks)")
    print(f"  columnstore compression: {s['bytes_before'] / 1e6:.2f} MB -> {s['bytes_after'] / 1e6:.2f} MB "
          f"({s['bytes_before'] / s['bytes_after']:.1f}x smaller)")
    print(f"  continuous aggregate accidents_daily_district: {s['cagg_accidents']:,} accidents")


if __name__ == "__main__":
    main()

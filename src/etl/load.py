"""Load: write Parquet locally and load it into BigQuery."""

import io
import shutil
from pathlib import Path

import polars as pl
from google.cloud import bigquery

BQ_TYPES = {
    pl.String: "STRING",
    pl.Int64: "INT64",
    pl.Float64: "FLOAT64",
    pl.Date: "DATE",
    pl.Datetime: "DATETIME",  # local Barcelona time, no time zone in the source
}


def write_clean_parquet(df: pl.DataFrame, out_dir: str | Path) -> Path:
    """Write clean data as Parquet partitioned by year (hive layout: year=2021/).

    The output folder is replaced on every run so reruns never leave stale files.
    """
    out = Path(out_dir)
    if out.exists():
        shutil.rmtree(out)
    df.write_parquet(out, partition_by="year", mkdir=True)
    return out


def write_quarantine_parquet(df: pl.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def dir_size(path: str | Path, pattern: str) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob(pattern))


def read_partitioned(out_dir: str | Path) -> pl.DataFrame:
    """Read the year-partitioned dataset back, restoring the year column."""
    return pl.scan_parquet(
        Path(out_dir) / "**" / "*.parquet",
        hive_partitioning=True,
        hive_schema={"year": pl.Int64},
    ).collect()


def bq_schema(df: pl.DataFrame) -> list[bigquery.SchemaField]:
    return [bigquery.SchemaField(name, BQ_TYPES[dtype.base_type()]) for name, dtype in df.schema.items()]


def load_to_bigquery(
    df: pl.DataFrame,
    table_id: str,
    location: str,
    client: bigquery.Client,
    partition_field: str | None = None,
    cluster_fields: list[str] | None = None,
) -> bigquery.Table:
    """Load df as one Parquet load job with WRITE_TRUNCATE (idempotent reruns).

    One job for all years keeps the replace atomic: the table is either fully
    replaced or left as it was.
    """
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema(df),
    )
    if partition_field:
        # Monthly: ~38k rows over 5 years would make daily partitions tiny.
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field=partition_field
        )
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    job = client.load_table_from_file(buf, table_id, job_config=job_config, location=location)
    job.result()
    return client.get_table(table_id)

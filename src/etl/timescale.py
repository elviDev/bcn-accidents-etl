"""Load: TimescaleDB (PostgreSQL) target, alongside BigQuery."""

import io
from pathlib import Path

import polars as pl
import psycopg
from psycopg import sql

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "timescale" / "schema.sql"
TIME_ZONE = "Europe/Madrid"  # the source times are local Barcelona time
TABLE = "accidents"
CAGG = "accidents_daily_district"


def split_sql(script: str) -> list[str]:
    """Split a SQL script into statements on ';', ignoring ';' inside $$ blocks
    and -- comments. Each statement then runs on its own, outside a transaction
    block, which some TimescaleDB calls require."""
    statements, current, in_dollar = [], [], False
    for line in script.splitlines():
        if not in_dollar and line.lstrip().startswith("--"):
            continue
        current.append(line)
        if line.count("$$") % 2:
            in_dollar = not in_dollar
        if not in_dollar and line.rstrip().endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt.rstrip(";").strip():
                statements.append(stmt)
            current = []
    if "\n".join(current).strip():
        statements.append("\n".join(current).strip())
    return statements


def to_copy_csv(df: pl.DataFrame) -> bytes:
    """Serialise for COPY ... (FORMAT csv): no header, null -> empty field."""
    buf = io.BytesIO()
    df.write_csv(
        buf,
        include_header=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format="%Y-%m-%d",
    )
    return buf.getvalue()


def table_bytes(conn: psycopg.Connection) -> int:
    return conn.execute("SELECT total_bytes FROM hypertable_detailed_size(%s)", (TABLE,)).fetchone()[0]


def load_to_timescale(df: pl.DataFrame, url: str) -> dict:
    """Create the schema if needed, replace all rows atomically, then compress
    chunks and refresh the continuous aggregate. Returns load statistics."""
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(f"SET TIME ZONE '{TIME_ZONE}'")
        for stmt in split_sql(SCHEMA_SQL.read_text(encoding="utf-8")):
            conn.execute(stmt)

        # TRUNCATE + COPY in one transaction: readers see the old data or the
        # new data, never a half-loaded table. Reruns give the same result.
        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(
            sql.Identifier(TABLE), sql.SQL(", ").join(map(sql.Identifier, df.columns))
        )
        with conn.transaction():
            conn.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(TABLE)))
            with conn.cursor().copy(copy_stmt) as copy:
                copy.write(to_copy_csv(df))

        bytes_before = table_bytes(conn)
        chunks = [r[0] for r in conn.execute("SELECT show_chunks(%s)", (TABLE,)).fetchall()]
        for chunk in chunks:
            conn.execute("CALL convert_to_columnstore(%s, if_not_columnstore => true)", (chunk,))
        bytes_after = table_bytes(conn)

        conn.execute(f"CALL refresh_continuous_aggregate('{CAGG}', NULL, NULL)")

        return {
            "version": conn.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            ).fetchone()[0],
            "rows": conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(TABLE))).fetchone()[0],
            "chunks": len(chunks),
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
            "cagg_accidents": conn.execute(f"SELECT sum(accidents) FROM {CAGG}").fetchone()[0],
        }

"""Extract: read the yearly Guàrdia Urbana accident CSVs as raw text."""

import io
from pathlib import Path

import polars as pl

ENCODINGS = ("utf-8", "cp1252")
SEPARATORS = (",", ";")


def decode(raw: bytes) -> tuple[str, str]:
    """Decode bytes with the first encoding that works. Returns (text, encoding)."""
    for enc in ENCODINGS:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise ValueError("could not decode file with any known encoding")


def sniff_separator(header_line: str) -> str:
    """Pick the separator that splits the header into the most columns."""
    return max(SEPARATORS, key=header_line.count)


def clean_header(name: str) -> str:
    """Remove BOM, surrounding quotes and whitespace from a header name."""
    return name.replace("﻿", "").strip().strip('"').strip()


def read_csv_as_text(path: Path) -> pl.DataFrame:
    """Read one CSV with every column as a string, plus a source_file column."""
    text, _ = decode(path.read_bytes())
    text = text.lstrip("﻿")
    sep = sniff_separator(text.split("\n", 1)[0])
    df = pl.read_csv(io.StringIO(text), separator=sep, infer_schema=False)
    df = df.rename({c: clean_header(c) for c in df.columns})
    return df.with_columns(pl.lit(path.name).alias("source_file"))


def extract(raw_dir: str | Path) -> pl.LazyFrame:
    """Read all CSVs in raw_dir into one LazyFrame of strings.

    Files with different headers are aligned with a diagonal concat; the
    transform step maps the header variants onto one schema.
    """
    paths = sorted(Path(raw_dir).glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"no CSV files in {raw_dir}")
    frames = [read_csv_as_text(p) for p in paths]
    df = pl.concat(frames, how="diagonal").with_row_index("_row_id")
    return df.lazy()

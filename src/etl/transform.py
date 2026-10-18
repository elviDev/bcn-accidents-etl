"""Transform: standardise, type, validate and split the raw accident data.

Every step is a LazyFrame -> LazyFrame function so it can be tested alone.
"""

import polars as pl

# One mapping for every header variant seen across 2021-2025.
COLUMN_MAPPING = {
    "Numero_expedient": "numero_expedient",
    "Codi_districte": "codi_districte",
    "Nom_districte": "nom_districte",
    "Codi_barri": "codi_barri",
    "Nom_barri": "nom_barri",
    "Codi_carrer": "codi_carrer",
    "Nom_carrer": "nom_carrer",
    "Num_postal_caption": "num_postal",  # 2021-2022
    "Num_postal": "num_postal",  # 2023+ (header had a trailing space)
    "Descripcio_dia_setmana": "descripcio_dia_setmana",
    "NK_Any": "year",
    "Mes_any": "month",
    "Nom_mes": "nom_mes",
    "Dia_mes": "day",
    "Hora_dia": "hour",
    "Descripcio_torn": "descripcio_torn",
    "Descripcio_causa_vianant": "descripcio_causa_vianant",
    "Numero_morts": "numero_morts",
    "Numero_lesionats_lleus": "numero_lesionats_lleus",
    "Numero_lesionats_greus": "numero_lesionats_greus",
    "Numero_victimes": "numero_victimes",
    "Numero_vehicles_implicats": "numero_vehicles_implicats",
    # 2023+ headers label X and Y the wrong way round; fix_swapped_utm repairs values.
    "Coordenada_UTM_X_ED50": "utm_x_ed50",
    "Coordenada_UTM_Y_ED50": "utm_y_ed50",
    "Longitud_WGS84": "longitude",
    "Longitud": "longitude",  # 2022
    "Latitud_WGS84": "latitude",
    "Latitud": "latitude",  # 2022
    "source_file": "source_file",
    "_row_id": "_row_id",
}

UNKNOWN_VALUES = ["Desconegut", "Desconeguda", "Desconeguts", "-1", "NA", "N/A", "?"]

COUNT_COLUMNS = [
    "numero_morts",
    "numero_lesionats_lleus",
    "numero_lesionats_greus",
    "numero_victimes",
    "numero_vehicles_implicats",
]
INT_COLUMNS = ["codi_districte", "codi_barri", "codi_carrer", "year", "month", "day", "hour"]
FLOAT_COLUMNS = ["utm_x_ed50", "utm_y_ed50", "longitude", "latitude"]

NO_PEDESTRIAN_CAUSE = "No és causa del vianant"

# Barcelona municipality bounding box (WGS84), with a small margin.
BCN_BBOX = {"lon_min": 2.05, "lon_max": 2.23, "lat_min": 41.31, "lat_max": 41.47}


def standardise_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename raw headers to snake_case; coalesce variants that share a target.

    Always returns every target column (null if no source column exists), so
    the output schema does not depend on which file a year came from.
    Columns not in the mapping are dropped.
    """
    present = set(lf.collect_schema().names())
    targets: dict[str, list[str]] = {}
    for raw, target in COLUMN_MAPPING.items():
        sources = targets.setdefault(target, [])
        if raw in present:
            sources.append(raw)
    return lf.select(
        pl.coalesce([pl.col(c) for c in sources]).alias(target)
        if sources
        else pl.lit(None, dtype=pl.String).alias(target)
        for target, sources in targets.items()
    )


def _string_columns(lf: pl.LazyFrame) -> list[str]:
    return [c for c, t in lf.collect_schema().items() if t == pl.String]


def clean_strings(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Trim whitespace, collapse repeated spaces, empty string -> null."""
    return lf.with_columns(
        pl.col(c).str.strip_chars().str.replace_all(r"\s{2,}", " ").replace("", None)
        for c in _string_columns(lf)
    )


def unknown_to_null(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Replace 'Desconegut'-style placeholders with null (not in count columns,
    so a negative count still reaches the quality rules)."""
    cols = [c for c in _string_columns(lf) if c not in COUNT_COLUMNS]
    return lf.with_columns(
        pl.when(pl.col(c).is_in(UNKNOWN_VALUES)).then(None).otherwise(pl.col(c)).alias(c)
        for c in cols
    )


def cast_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Safe casts: unparseable values become null instead of raising.

    Blank counts mean 0 (2024+ files leave zero counts empty); non-numeric
    count text becomes null and is caught by the quality rules.
    """
    schema = lf.collect_schema()
    return lf.with_columns(
        *[pl.col(c).fill_null("0").cast(pl.Int64, strict=False) for c in COUNT_COLUMNS if c in schema],
        *[pl.col(c).cast(pl.Int64, strict=False) for c in INT_COLUMNS if c in schema],
        *[pl.col(c).cast(pl.Float64, strict=False) for c in FLOAT_COLUMNS if c in schema],
    )


def fix_swapped_utm(lf: pl.LazyFrame) -> pl.LazyFrame:
    """In Barcelona UTM X is ~430,000 and Y ~4,580,000; swap rows where X > Y."""
    swapped = pl.col("utm_x_ed50") > pl.col("utm_y_ed50")
    return lf.with_columns(
        pl.when(swapped).then(pl.col("utm_y_ed50")).otherwise(pl.col("utm_x_ed50")).alias("utm_x_ed50"),
        pl.when(swapped).then(pl.col("utm_x_ed50")).otherwise(pl.col("utm_y_ed50")).alias("utm_y_ed50"),
    )


def normalise_pedestrian_cause(lf: pl.LazyFrame) -> pl.LazyFrame:
    """2021-2023 write 'No és causa del vianant'; 2024+ leave it blank."""
    return lf.with_columns(pl.col("descripcio_causa_vianant").fill_null(NO_PEDESTRIAN_CAUSE))


def build_datetime(lf: pl.LazyFrame) -> pl.LazyFrame:
    """accident_datetime from year/month/day/hour; impossible dates -> null."""
    text = pl.format(
        "{}-{}-{} {}:00",
        pl.col("year"),
        pl.col("month").cast(pl.String).str.zfill(2),
        pl.col("day").cast(pl.String).str.zfill(2),
        pl.col("hour").cast(pl.String).str.zfill(2),
    )
    dt = text.str.to_datetime("%Y-%m-%d %H:%M", strict=False)
    return lf.with_columns(dt.alias("accident_datetime")).with_columns(
        pl.col("accident_datetime").dt.date().alias("accident_date")
    )


def split_duplicates(lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Keep the first row per numero_expedient (by _row_id); return (kept, dropped).

    In the source, a repeated case number is the same accident listed once per
    pedestrian cause, so the extra rows are moved to quarantine.
    """
    rank = pl.col("_row_id").rank("ordinal").over("numero_expedient")
    ranked = lf.with_columns(rank.alias("_dup_rank"))
    # Null case numbers are not duplicates of each other; the quality rules flag them.
    is_dup = (pl.col("_dup_rank") > 1) & pl.col("numero_expedient").is_not_null()
    kept = ranked.filter(~is_dup).drop("_dup_rank")
    dropped = (
        ranked.filter(is_dup)
        .drop("_dup_rank")
        .with_columns(pl.lit("duplicate_numero_expedient").alias("quality_issues"))
    )
    return kept, dropped


def quality_issues_expr() -> pl.Expr:
    """';'-joined list of failed rules, or null when the row passes."""
    b = BCN_BBOX
    lon, lat = pl.col("longitude"), pl.col("latitude")
    rules = {
        "missing_numero_expedient": pl.col("numero_expedient").is_null(),
        "missing_coordinates": lon.is_null() | lat.is_null(),
        "coordinates_outside_bcn": lon.is_not_null()
        & lat.is_not_null()
        & ~(lon.is_between(b["lon_min"], b["lon_max"]) & lat.is_between(b["lat_min"], b["lat_max"])),
        "invalid_or_negative_count": pl.any_horizontal(
            pl.col(c).is_null() | (pl.col(c) < 0) for c in COUNT_COLUMNS
        ),
        "invalid_date": pl.col("accident_datetime").is_null(),
    }
    parts = [pl.when(cond).then(pl.lit(name)) for name, cond in rules.items()]
    joined = pl.concat_str(parts, separator=";", ignore_nulls=True)
    return pl.when(joined == "").then(None).otherwise(joined)


def apply_quality_rules(lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Return (clean, quarantine). Quarantine rows carry quality_issues."""
    checked = lf.with_columns(quality_issues_expr().alias("quality_issues"))
    clean = checked.filter(pl.col("quality_issues").is_null()).drop("quality_issues")
    quarantine = checked.filter(pl.col("quality_issues").is_not_null())
    return clean, quarantine


def transform(raw: pl.LazyFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Full transform. Returns (clean, quarantine) DataFrames."""
    typed = (
        raw.pipe(standardise_columns)
        .pipe(clean_strings)
        .pipe(unknown_to_null)
        .pipe(cast_types)
        .pipe(fix_swapped_utm)
        .pipe(normalise_pedestrian_cause)
        .pipe(build_datetime)
    )
    deduped, duplicates = split_duplicates(typed)
    clean, failed = apply_quality_rules(deduped)
    quarantine = pl.concat([failed, duplicates], how="diagonal")
    clean_df, quarantine_df = pl.collect_all([clean.sort("_row_id"), quarantine.sort("_row_id")])
    return clean_df.drop("_row_id"), quarantine_df.drop("_row_id")


def quality_report(raw_rows: int, clean: pl.DataFrame, quarantine: pl.DataFrame) -> str:
    """Short text summary of what passed and why rows were quarantined."""
    lines = [
        "Quality report",
        f"  raw rows        : {raw_rows:,}",
        f"  clean rows      : {clean.height:,}",
        f"  quarantined rows: {quarantine.height:,}",
    ]
    if quarantine.height:
        reasons = (
            quarantine.select(pl.col("quality_issues").str.split(";").alias("reason"))
            .explode("reason")
            .group_by("reason")
            .len()
            .sort("len", descending=True)
        )
        for reason, n in reasons.iter_rows():
            lines.append(f"    - {reason}: {n:,}")
    nulls = {c: clean[c].null_count() for c in ("nom_districte", "nom_barri", "nom_carrer")}
    lines.append("  unknown -> null in clean data: " + ", ".join(f"{c}={n}" for c, n in nulls.items()))
    per_year = clean.group_by("year").len().sort("year")
    lines.append("  clean rows per year: " + ", ".join(f"{y}={n:,}" for y, n in per_year.iter_rows()))
    return "\n".join(lines)

from datetime import date, datetime

import polars as pl

from src.etl.transform import (
    COLUMN_MAPPING,
    NO_PEDESTRIAN_CAUSE,
    apply_quality_rules,
    build_datetime,
    cast_types,
    clean_strings,
    fix_swapped_utm,
    normalise_pedestrian_cause,
    split_duplicates,
    standardise_columns,
    transform,
    unknown_to_null,
)


def valid_row(**overrides) -> dict:
    """A typed, clean row that passes every quality rule."""
    row = {
        "numero_expedient": "2023S000001",
        "longitude": 2.17,
        "latitude": 41.39,
        "numero_morts": 0,
        "numero_lesionats_lleus": 1,
        "numero_lesionats_greus": 0,
        "numero_victimes": 1,
        "numero_vehicles_implicats": 2,
        "accident_datetime": datetime(2023, 1, 1, 10),
    }
    return row | overrides


# --- column mapping ---------------------------------------------------------

def test_mapping_renames_to_snake_case():
    lf = pl.LazyFrame({"Numero_expedient": ["A"], "NK_Any": ["2021"], "Hora_dia": ["9"]})
    out = standardise_columns(lf).collect()
    expected = {"numero_expedient": "A", "year": "2021", "hour": "9"}
    assert expected.items() <= out.row(0, named=True).items()
    assert all(c == c.lower() and " " not in c for c in out.columns)


def test_mapping_always_returns_full_schema():
    out = standardise_columns(pl.LazyFrame({"Numero_expedient": ["A"]})).collect()
    assert out.columns == list(dict.fromkeys(COLUMN_MAPPING.values()))
    assert out["latitude"].to_list() == [None]


def test_mapping_coalesces_header_variants_across_years():
    # 2022 used Longitud/Latitud, other years Longitud_WGS84/Latitud_WGS84
    lf = pl.LazyFrame({
        "Longitud_WGS84": ["2.1", None],
        "Longitud": [None, "2.2"],
        "Num_postal_caption": ["1", None],
        "Num_postal": [None, "2"],
    })
    out = standardise_columns(lf).collect()
    assert "Longitud" not in out.columns and "Num_postal" not in out.columns
    assert out["longitude"].to_list() == ["2.1", "2.2"]
    assert out["num_postal"].to_list() == ["1", "2"]


def test_mapping_drops_unknown_columns():
    lf = pl.LazyFrame({"Numero_expedient": ["A"], "Some_new_column": ["x"]})
    assert "Some_new_column" not in standardise_columns(lf).collect().columns


# --- unknown values / cleaning / casting -------------------------------------

def test_unknown_values_become_null():
    lf = pl.LazyFrame({
        "nom_districte": ["Desconegut", "Eixample", "  Desconegut  "],
        "codi_districte": ["-1", "2", "3"],
    })
    out = lf.pipe(clean_strings).pipe(unknown_to_null).collect()
    assert out["nom_districte"].to_list() == [None, "Eixample", None]
    assert out["codi_districte"].to_list() == [None, "2", "3"]


def test_unknown_to_null_leaves_count_columns_for_quality_rules():
    lf = pl.LazyFrame({"numero_morts": ["-1"]})
    assert unknown_to_null(lf).collect()["numero_morts"][0] == "-1"


def test_clean_strings_trims_padding_and_blank_to_null():
    lf = pl.LazyFrame({"x": ["2021S000001    ", "   ", "No és causa del  vianant"]})
    out = clean_strings(lf).collect()["x"].to_list()
    assert out == ["2021S000001", None, "No és causa del vianant"]


def test_cast_is_safe_and_blank_counts_are_zero():
    lf = pl.LazyFrame({
        "numero_morts": [None, "1", "abc"],
        "codi_districte": ["2", "x", "010"],
        "longitude": ["2.1", "bad", None],
    })
    out = cast_types(lf).collect()
    assert out["numero_morts"].to_list() == [0, 1, None]
    assert out["codi_districte"].to_list() == [2, None, 10]
    assert out["longitude"].to_list() == [2.1, None, None]


def test_swapped_utm_columns_are_fixed():
    lf = pl.LazyFrame({"utm_x_ed50": [4580907.46, 427409.24], "utm_y_ed50": [427409.24, 4580907.46]})
    out = fix_swapped_utm(lf).collect()
    assert out["utm_x_ed50"].to_list() == [427409.24, 427409.24]
    assert out["utm_y_ed50"].to_list() == [4580907.46, 4580907.46]


def test_blank_pedestrian_cause_means_no_cause():
    lf = pl.LazyFrame({"descripcio_causa_vianant": [None, "Altres"]})
    out = normalise_pedestrian_cause(lf).collect()["descripcio_causa_vianant"].to_list()
    assert out == [NO_PEDESTRIAN_CAUSE, "Altres"]


def test_build_datetime_and_invalid_dates_become_null():
    lf = pl.LazyFrame({"year": [2023, 2023, 2023], "month": [1, 2, 5], "day": [3, 30, 1], "hour": [9, 10, 25]})
    out = build_datetime(lf).collect()
    assert out["accident_datetime"].to_list() == [datetime(2023, 1, 3, 9), None, None]
    assert out["accident_date"].to_list() == [date(2023, 1, 3), None, None]


# --- dedup ---------------------------------------------------------------

def test_dedup_keeps_first_row_and_quarantines_the_rest():
    lf = pl.LazyFrame({
        "_row_id": [0, 1, 2, 3, 4],
        "numero_expedient": ["A", "A", "B", None, None],
        "descripcio_causa_vianant": ["Altres", "Semàfor", "x", "y", "z"],
    })
    kept, dropped = split_duplicates(lf)
    kept, dropped = kept.collect(), dropped.collect()
    assert kept["_row_id"].to_list() == [0, 2, 3, 4]  # null ids are not duplicates
    assert dropped["_row_id"].to_list() == [1]
    assert dropped["quality_issues"].to_list() == ["duplicate_numero_expedient"]


# --- quality rules --------------------------------------------------------

def test_valid_row_passes():
    clean, quarantine = apply_quality_rules(pl.LazyFrame([valid_row()]))
    assert clean.collect().height == 1
    assert quarantine.collect().height == 0


def test_quality_rules_quarantine_with_reasons():
    rows = [
        valid_row(numero_expedient="ok"),
        valid_row(numero_expedient="madrid", longitude=-3.70, latitude=40.42),
        valid_row(numero_expedient="nocoords", longitude=None),
        valid_row(numero_expedient="neg", numero_victimes=-1),
        valid_row(numero_expedient="badcount", numero_morts=None),
        valid_row(numero_expedient="nodate", accident_datetime=None),
        valid_row(numero_expedient="many", longitude=None, numero_victimes=-2, accident_datetime=None),
    ]
    clean, quarantine = apply_quality_rules(pl.LazyFrame(rows))
    assert clean.collect()["numero_expedient"].to_list() == ["ok"]
    reasons = dict(quarantine.collect().select("numero_expedient", "quality_issues").iter_rows())
    assert reasons == {
        "madrid": "coordinates_outside_bcn",
        "nocoords": "missing_coordinates",
        "neg": "invalid_or_negative_count",
        "badcount": "invalid_or_negative_count",
        "nodate": "invalid_date",
        "many": "missing_coordinates;invalid_or_negative_count;invalid_date",
    }


# --- end to end on raw strings ----------------------------------------------

def test_transform_end_to_end_accounts_for_every_row():
    raw = pl.LazyFrame({
        "_row_id": [0, 1, 2, 3],
        "Numero_expedient": ["2021S1   ", "2021S1   ", "2021S2", "2021S3"],
        "Nom_districte": ["Eixample", "Eixample", "Desconegut", "Gràcia"],
        "NK_Any": ["2021"] * 4,
        "Mes_any": ["2", "2", "2", "2"],
        "Dia_mes": ["1", "1", "1", "30"],
        "Hora_dia": ["8", "8", "8", "8"],
        "Descripcio_causa_vianant": ["Altres", "Semàfor", None, None],
        "Numero_morts": ["0", "0", "", "0"],
        "Numero_lesionats_lleus": ["1", "1", "", "0"],
        "Numero_lesionats_greus": ["0", "0", "", "0"],
        "Numero_victimes": ["1", "1", "", "0"],
        "Numero_vehicles_implicats": ["2", "2", "1", "1"],
        "Longitud": ["2.17", "2.17", "2.15", "2.16"],
        "Latitud": ["41.39", "41.39", "41.40", "41.40"],
        "source_file": ["2021.csv"] * 4,
    })
    clean, quarantine = transform(raw)
    assert clean.height + quarantine.height == 4
    assert clean["numero_expedient"].to_list() == ["2021S1", "2021S2"]
    assert clean["nom_districte"].to_list() == ["Eixample", None]
    assert clean["numero_victimes"].to_list() == [1, 0]
    reasons = dict(quarantine.select("numero_expedient", "quality_issues").iter_rows())
    assert reasons == {"2021S3": "invalid_date", "2021S1": "duplicate_numero_expedient"}

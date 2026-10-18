import polars as pl

from src.etl.extract import extract, read_csv_as_text


def test_bom_and_padded_header_are_cleaned(tmp_path):
    p = tmp_path / "2024_x.csv"
    p.write_bytes("﻿Numero_expedient,Num_postal \n2024S1,12\n".encode("utf-8"))
    df = read_csv_as_text(p)
    assert df.columns == ["Numero_expedient", "Num_postal", "source_file"]
    assert df["source_file"][0] == "2024_x.csv"


def test_cp1252_and_semicolon_fallback(tmp_path):
    p = tmp_path / "old.csv"
    p.write_bytes("Nom_districte;Codi_districte\nSant Martí;10\n".encode("cp1252"))
    df = read_csv_as_text(p)
    assert df["Nom_districte"][0] == "Sant Martí"
    assert df["Codi_districte"][0] == "10"  # kept as text


def test_extract_aligns_different_headers(tmp_path):
    (tmp_path / "a.csv").write_text("Numero_expedient,Longitud\nA,2.1\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("Numero_expedient,Longitud_WGS84\nB,2.2\n", encoding="utf-8")
    df = extract(tmp_path).collect()
    assert df.height == 2
    assert set(df.columns) >= {"Longitud", "Longitud_WGS84", "source_file", "_row_id"}
    assert df.schema["Longitud"] == pl.String

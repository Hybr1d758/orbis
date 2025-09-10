import pandas as pd

from orbis import (
    normalize_column_name,
    normalize_column_names,
    strip_and_standardize_strings,
    convert_mostly_numeric_columns,
    drop_empty_columns,
    remove_duplicate_rows,
    clean_dataframe,
)


def test_normalize_column_name():
    assert normalize_column_name("  Total Cost ($)  ") == "total_cost"
    assert normalize_column_name("SKU-ID") == "sku_id"


def test_normalize_column_names():
    df = pd.DataFrame({"Total Cost ($)": [1], "SKU-ID": ["A"]})
    out = normalize_column_names(df)
    assert list(out.columns) == ["total_cost", "sku_id"]


def test_strip_and_standardize_strings():
    df = pd.DataFrame({"a": [" x ", "", None], "b": [1, 2, 3]})
    out = strip_and_standardize_strings(df)
    assert out.loc[0, "a"] == "x"
    assert pd.isna(out.loc[1, "a"])  # empty string becomes NA


def test_convert_mostly_numeric_columns():
    df = pd.DataFrame({"a": ["1", "2", "x", None], "b": ["x", "y", "z", None]})
    out = convert_mostly_numeric_columns(df, threshold=0.5)
    assert pd.api.types.is_numeric_dtype(out["a"])  # became numeric
    assert not pd.api.types.is_numeric_dtype(out["b"])  # stays object


def test_drop_empty_columns():
    df = pd.DataFrame({"a": [None, None], "b": [1, None]})
    out = drop_empty_columns(df)
    assert list(out.columns) == ["b"]


def test_remove_duplicate_rows():
    df = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
    out = remove_duplicate_rows(df)
    assert len(out) == 1


def test_clean_dataframe_end_to_end():
    df = pd.DataFrame({
        " Total Cost ($) ": ["10 ", " 10", ""],
        "SKU-ID": [" A ", "A", "B"],
        "empty": [None, None, None],
    })
    out = clean_dataframe(df)
    assert "total_cost" in out.columns
    assert "sku_id" in out.columns
    assert "empty" not in out.columns  # dropped
    assert len(out) == 2  # duplicates removed


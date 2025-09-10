import pandas as pd

from orbis import (
    profile_columns,
    gather_validation_issues,
)


def test_profile_columns_basic():
    df = pd.DataFrame({"a": [1, 2, None], "b": ["x", None, "y"]})
    prof = profile_columns(df)
    assert set(["column_name", "dtype", "count", "null_count"]).issubset(prof.columns)
    assert len(prof) == 2


def test_gather_validation_issues():
    df = pd.DataFrame({
        "source_file": ["a.csv", None, "bad.csv"],
        "qty": [1, -1, 2],
    })
    issues = gather_validation_issues(df, faf5_dir=".")
    # Should have at least a negative_values and source_file_null issue
    assert (issues["issue_type"] == "negative_values").any()
    assert (issues["issue_type"] == "source_file_null").any()


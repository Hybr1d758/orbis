import os
import time
import logging
from uuid import uuid4
from datetime import datetime
import pandas as pd


def build_faf5_directory_path() -> str:
    """Return absolute path to the FAF5 directory next to this file."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    faf5_dir = os.path.join(project_dir, "FAF5")
    return faf5_dir


def read_all_faf5_csvs(faf5_dir: str) -> pd.DataFrame:
    """
    Read and vertically concatenate all CSV files in the provided FAF5 directory.

    Adds a 'source_file' column to identify where each row came from.
    Returns an empty DataFrame if no CSV files are found.
    """
    if not os.path.isdir(faf5_dir):
        raise FileNotFoundError(f"FAF5 directory not found: {faf5_dir}")

    dataframes = []
    excluded_filenames = {
        "FAF5_MERGED.csv",
        "FAF5_MERGED_CLEANED.csv",
        "FAF5_VALIDATION_COLUMNS.csv",
        "FAF5_VALIDATION_ISSUES.csv",
    }
    for file_name in sorted(os.listdir(faf5_dir)):
        if not file_name.lower().endswith(".csv"):
            continue
        if file_name in excluded_filenames:
            continue
        file_path = os.path.join(faf5_dir, file_name)
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            raise RuntimeError(f"Failed to read CSV: {file_path}") from exc
        df["source_file"] = file_name
        dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    merged = pd.concat(dataframes, ignore_index=True, sort=False)
    return merged


def save_merged_dataframe(df: pd.DataFrame, faf5_dir: str) -> str:
    """Save merged DataFrame to a CSV inside FAF5 directory and return the path."""
    output_path = os.path.join(faf5_dir, "FAF5_MERGED.csv")
    df.to_csv(output_path, index=False)
    return output_path


def normalize_column_name(name: str) -> str:
    """Normalize a single column name to snake_case with safe characters."""
    normalized = name.strip().lower()
    safe_chars = [c if c.isalnum() else "_" for c in normalized]
    normalized = "".join(safe_chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("_")
    return normalized or "column"


def strip_and_standardize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and standardize empty strings to NaN for object columns."""
    cleaned = df.copy()
    object_columns = cleaned.select_dtypes(include=["object"]).columns
    for column_name in object_columns:
        cleaned[column_name] = (
            cleaned[column_name]
            .astype("string")
            .str.strip()
            .replace({"": pd.NA})
        )
    return cleaned


def convert_mostly_numeric_columns(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """
    Attempt to convert object columns to numeric if at least `threshold` fraction
    of non-null values can be parsed as numbers.
    """
    converted_df = df.copy()
    object_columns = converted_df.select_dtypes(include=["object", "string"]).columns
    for column_name in object_columns:
        original_series = converted_df[column_name]
        # Compute on non-null entries only to avoid penalizing missing data
        non_null_mask = original_series.notna()
        if non_null_mask.sum() == 0:
            continue
        coerced = pd.to_numeric(original_series, errors="coerce")
        convertible_ratio = coerced[non_null_mask].notna().mean()
        if convertible_ratio >= threshold:
            converted_df[column_name] = coerced
    return converted_df


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are fully empty (all NA)."""
    return df.dropna(axis=1, how="all")


def remove_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    return df.drop_duplicates()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Apply normalization to all column names in the DataFrame."""
    rename_map = {name: normalize_column_name(name) for name in df.columns}
    # Resolve any collisions by suffixing with an index
    seen = {}
    for original, normalized in list(rename_map.items()):
        if normalized not in seen:
            seen[normalized] = 1
            continue
        seen[normalized] += 1
        rename_map[original] = f"{normalized}_{seen[normalized]}"
    return df.rename(columns=rename_map)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform generic cleaning:
    - Normalize column names
    - Trim strings and standardize empty strings to NA
    - Convert mostly-numeric text columns to numeric
    - Drop fully empty columns
    - Drop duplicate rows
    """
    step1 = normalize_column_names(df)
    step2 = strip_and_standardize_strings(step1)
    step3 = convert_mostly_numeric_columns(step2, threshold=0.9)
    step4 = drop_empty_columns(step3)
    step5 = remove_duplicate_rows(step4)
    return step5


def save_cleaned_dataframe(df: pd.DataFrame, faf5_dir: str) -> str:
    """Save cleaned DataFrame to a CSV inside FAF5 directory and return the path."""
    output_path = os.path.join(faf5_dir, "FAF5_MERGED_CLEANED.csv")
    df.to_csv(output_path, index=False)
    return output_path


def profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a simple profile for each column in the DataFrame."""
    profiles = []
    for column_name in df.columns:
        series = df[column_name]
        non_null_count = int(series.notna().sum())
        null_count = int(series.isna().sum())
        total_count = int(len(series))
        null_pct = float(null_count / total_count) if total_count else 0.0
        num_unique = int(series.nunique(dropna=True))
        dtype_str = str(series.dtype)

        profile = {
            "column_name": column_name,
            "dtype": dtype_str,
            "count": total_count,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "null_pct": round(null_pct, 6),
            "num_unique": num_unique,
        }

        if pd.api.types.is_numeric_dtype(series):
            profile.update(
                {
                    "min": float(series.min()) if non_null_count else None,
                    "max": float(series.max()) if non_null_count else None,
                    "mean": float(series.mean()) if non_null_count else None,
                    "std": float(series.std()) if non_null_count else None,
                }
            )
        else:
            sample_values = (
                series.dropna().astype(str).unique().tolist()[:5]
            )
            profile.update({"sample_values": ", ".join(sample_values)})

        profiles.append(profile)

    return pd.DataFrame(profiles)


def gather_validation_issues(df: pd.DataFrame, faf5_dir: str) -> pd.DataFrame:
    """Run a set of validation checks and return a DataFrame of issues found."""
    issues = []

    # Prepare allowed source filenames from the directory (exclude outputs)
    try:
        allowed_files = set(
            name
            for name in os.listdir(faf5_dir)
            if name.lower().endswith(".csv")
            and name
            not in {
                "FAF5_MERGED.csv",
                "FAF5_MERGED_CLEANED.csv",
                "FAF5_VALIDATION_COLUMNS.csv",
                "FAF5_VALIDATION_ISSUES.csv",
            }
        )
    except Exception:
        allowed_files = set()

    # 1) Missing data by column
    for column_name in df.columns:
        null_count = int(df[column_name].isna().sum())
        if null_count > 0:
            issues.append(
                {
                    "issue_type": "missing_values",
                    "column": column_name,
                    "count": null_count,
                    "pct": round(null_count / len(df), 6) if len(df) else 0.0,
                    "details": "Column contains missing values",
                }
            )

    # 2) Negative values for numeric columns
    for column_name in df.select_dtypes(include=["number"]).columns:
        neg_mask = df[column_name] < 0
        neg_count = int(neg_mask.sum())
        if neg_count > 0:
            sample_idx = df.index[neg_mask].tolist()[:10]
            issues.append(
                {
                    "issue_type": "negative_values",
                    "column": column_name,
                    "count": neg_count,
                    "pct": round(neg_count / len(df), 6) if len(df) else 0.0,
                    "details": f"Sample row indices: {sample_idx}",
                }
            )

    # 3) source_file integrity
    if "source_file" not in df.columns:
        issues.append(
            {
                "issue_type": "source_file_missing",
                "column": "source_file",
                "count": len(df),
                "pct": 1.0 if len(df) else 0.0,
                "details": "Column 'source_file' not found",
            }
        )
    else:
        sf_series = df["source_file"].astype("string")
        null_count = int(sf_series.isna().sum())
        if null_count > 0:
            issues.append(
                {
                    "issue_type": "source_file_null",
                    "column": "source_file",
                    "count": null_count,
                    "pct": round(null_count / len(df), 6) if len(df) else 0.0,
                    "details": "Null values present in 'source_file'",
                }
            )
        empty_count = int((sf_series.str.strip() == "").sum())
        if empty_count > 0:
            issues.append(
                {
                    "issue_type": "source_file_empty",
                    "column": "source_file",
                    "count": empty_count,
                    "pct": round(empty_count / len(df), 6) if len(df) else 0.0,
                    "details": "Empty string values present in 'source_file'",
                }
            )
        if allowed_files:
            invalid_mask = ~sf_series.isna() & ~sf_series.isin(list(allowed_files))
            invalid_count = int(invalid_mask.sum())
            if invalid_count > 0:
                sample_vals = (
                    sf_series[invalid_mask].unique().tolist()[:10]
                )
                issues.append(
                    {
                        "issue_type": "source_file_invalid",
                        "column": "source_file",
                        "count": invalid_count,
                        "pct": round(invalid_count / len(df), 6)
                        if len(df)
                        else 0.0,
                        "details": f"Values not in FAF5 directory: {sample_vals}",
                    }
                )

    # 4) Duplicate rows
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        issues.append(
            {
                "issue_type": "duplicate_rows",
                "column": "",
                "count": dup_count,
                "pct": round(dup_count / len(df), 6) if len(df) else 0.0,
                "details": "Exact duplicate rows detected",
            }
        )

    return pd.DataFrame(issues)


def save_validation_profiles(df_profile: pd.DataFrame, faf5_dir: str) -> str:
    output_path = os.path.join(faf5_dir, "FAF5_VALIDATION_COLUMNS.csv")
    df_profile.to_csv(output_path, index=False)
    return output_path


def save_validation_issues(df_issues: pd.DataFrame, faf5_dir: str) -> str:
    output_path = os.path.join(faf5_dir, "FAF5_VALIDATION_ISSUES.csv")
    df_issues.to_csv(output_path, index=False)
    return output_path


def configure_logger(run_id: str, project_dir: str) -> logging.LoggerAdapter:
    """Configure console and file logging; return an adapter that injects run_id."""
    logger = logging.getLogger("orbis")
    logger.setLevel(logging.INFO)

    # Reset handlers if re-run in same interpreter
    logger.handlers.clear()

    logs_dir = os.path.join(project_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"orbis_{run_id}.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s run_id=%(run_id)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logging.LoggerAdapter(logger, extra={"run_id": run_id})


def export_to_duckdb(
    cleaned_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    run_id: str,
    project_dir: str,
    logger: logging.LoggerAdapter,
) -> None:
    """
    Optionally export cleaned data and validation issues to a local DuckDB database.

    - Creates/overwrites tables: orbis_cleaned, orbis_validation_issues
    - Adds run_id column to each table
    """
    try:
        import duckdb  # type: ignore
    except Exception:
        logger.info("DuckDB not installed; skipping DuckDB export")
        return

    db_path = os.path.join(project_dir, "orbis.duckdb")
    logger.info(f"Exporting to DuckDB at: {db_path}")

    # Ensure run_id present
    cleaned_with_meta = cleaned_df.copy()
    cleaned_with_meta["run_id"] = run_id
    issues_with_meta = issues_df.copy()
    issues_with_meta["run_id"] = run_id

    con = duckdb.connect(db_path)
    try:
        con.register("cleaned_df", cleaned_with_meta)
        con.register("issues_df", issues_with_meta)

        con.execute(
            """
            CREATE OR REPLACE TABLE orbis_cleaned AS
            SELECT * FROM cleaned_df
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE orbis_validation_issues AS
            SELECT * FROM issues_df
            """
        )

        # Basic stats
        cleaned_count = int(con.execute("SELECT COUNT(*) FROM orbis_cleaned").fetchone()[0])
        issues_count = int(
            con.execute("SELECT COUNT(*) FROM orbis_validation_issues").fetchone()[0]
        )
        logger.info(
            f"DuckDB export complete (rows: cleaned={cleaned_count}, issues={issues_count})"
        )
    finally:
        con.close()


def main() -> None:
    project_dir = os.path.dirname(os.path.abspath(__file__))
    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    logger = configure_logger(run_id, project_dir)

    faf5_dir = build_faf5_directory_path()
    logger.info(f"Starting pipeline; FAF5 directory: {faf5_dir}")

    try:
        t0 = time.perf_counter()
        merged_df = read_all_faf5_csvs(faf5_dir)
        t1 = time.perf_counter()
        if merged_df.empty:
            logger.warning("No CSV files found to merge in FAF5 directory. Exiting.")
            return
        merged_output_path = save_merged_dataframe(merged_df, faf5_dir)
        logger.info(
            f"Merged {len(merged_df)} rows from CSV files into: {merged_output_path} (elapsed={t1 - t0:.2f}s)"
        )

        t2 = time.perf_counter()
        cleaned_df = clean_dataframe(merged_df)
        cleaned_output_path = save_cleaned_dataframe(cleaned_df, faf5_dir)
        t3 = time.perf_counter()
        logger.info(
            f"Cleaned dataset has {len(cleaned_df)} rows and {cleaned_df.shape[1]} columns: {cleaned_output_path} (elapsed={t3 - t2:.2f}s)"
        )

        # Validation reports
        t4 = time.perf_counter()
        profile_df = profile_columns(cleaned_df)
        issues_df = gather_validation_issues(cleaned_df, faf5_dir)
        profile_path = save_validation_profiles(profile_df, faf5_dir)
        issues_path = save_validation_issues(issues_df, faf5_dir)
        t5 = time.perf_counter()
        logger.info(f"Validation column profile written to: {profile_path}")
        logger.info(
            f"Validation issues written to: {issues_path} (elapsed={t5 - t4:.2f}s)"
        )

        total = time.perf_counter() - t0
        logger.info(f"Pipeline completed successfully (total_elapsed={total:.2f}s)")

        # Optional: export to DuckDB if available
        export_to_duckdb(cleaned_df=cleaned_df, issues_df=issues_df, run_id=run_id, project_dir=project_dir, logger=logger)
    except Exception:
        logger.exception("Pipeline failed with an unhandled exception")
        raise

    # Validation reports
    profile_df = profile_columns(cleaned_df)
    issues_df = gather_validation_issues(cleaned_df, faf5_dir)
    profile_path = save_validation_profiles(profile_df, faf5_dir)
    issues_path = save_validation_issues(issues_df, faf5_dir)
    print(f"Validation column profile written to: {profile_path}")
    print(f"Validation issues written to: {issues_path}")


if __name__ == "__main__":
    main()

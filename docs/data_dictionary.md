## Data dictionary

This project produces two primary datasets by default and optional DuckDB tables.

### CSV outputs (in `FAF5/`)
- `FAF5_MERGED.csv`
  - Description: raw vertical append of all input CSVs
  - Columns: `source_file` (string) + all columns present in inputs (types may vary)

- `FAF5_MERGED_CLEANED.csv`
  - Description: cleaned dataset
  - Columns:
    - `source_file` (string): originating file name
    - Other columns: normalized to `snake_case`, trimmed strings, mostly-numeric cast to numeric

- `FAF5_VALIDATION_COLUMNS.csv`
  - Description: per-column profiling
  - Columns: `column_name`, `dtype`, `count`, `non_null_count`, `null_count`, `null_pct`, `num_unique`, plus numeric stats (`min`, `max`, `mean`, `std`) when applicable

- `FAF5_VALIDATION_ISSUES.csv`
  - Description: summary of validation findings
  - Columns: `issue_type`, `column`, `count`, `pct`, `details`

### DuckDB tables (optional, in `orbis.duckdb`)
- `orbis_cleaned`
  - Description: cleaned dataset with a `run_id` column
  - Columns: `run_id` (text), `source_file` (text), other normalized columns

- `orbis_validation_issues`
  - Description: validation issues per run
  - Columns: `run_id` (text), `issue_type` (text), `column` (text), `count` (bigint), `pct` (double), `details` (text)

### Notes
- Column sets depend on the input CSVs. Use `FAF5_VALIDATION_COLUMNS.csv` to see actual profiles.
- Prefer Parquet for curated/analytics layers if you extend this project.


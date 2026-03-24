import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

load_dotenv()
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

EXPECTED_CITIES = {"Singapore", "London", "Tokyo", "New York", "Sydney"}
EXPECTED_ROWS   = 120  # 5 cities x 24 hours

def log_result(stage: str, status: str, rows_checked: int, issues_found: int, details: str):
    """Write check result to pipeline_runs table."""
    query = text("""
        INSERT INTO bronze.pipeline_runs 
        (stage, status, rows_checked, issues_found, details)
        VALUES (:stage, :status, :rows_checked, :issues_found, :details)
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "stage":        stage,
            "status":       status,
            "rows_checked": rows_checked,
            "issues_found": issues_found,
            "details":      details
        })

def check_row_count(df: pd.DataFrame) -> bool:
    """Check that we have exactly 120 rows."""
    actual = len(df)
    passed = actual == EXPECTED_ROWS
    status = "PASS" if passed else "FAIL"
    details = f"Expected {EXPECTED_ROWS} rows, got {actual}"
    log_result("row_count", status, actual, 0 if passed else 1, details)
    print(f"[{status}] Row count: {details}")
    return passed

def check_nulls(df: pd.DataFrame) -> bool:
    """Check for null values in key columns."""
    key_cols = [
        "city", "date", "temperature_2m", "precipitation",
        "wind_speed_10m", "weather_code"
    ]
    null_counts = df[key_cols].isnull().sum()
    issues = null_counts[null_counts > 0]
    passed = len(issues) == 0
    status = "PASS" if passed else "FAIL"
    details = "No nulls found" if passed else f"Nulls found: {issues.to_dict()}"
    log_result("null_check", status, len(df), len(issues), details)
    print(f"[{status}] Null check: {details}")
    return passed

def check_temperature_range(df: pd.DataFrame) -> bool:
    """Check that temperatures are within realistic bounds."""
    out_of_range = df[
        (df["temperature_2m"] < -50) | (df["temperature_2m"] > 60)
    ]
    passed = len(out_of_range) == 0
    status = "PASS" if passed else "FAIL"
    details = "All temperatures in range" if passed else \
              f"{len(out_of_range)} rows outside -50 to 60°C"
    log_result("temperature_range", status, len(df), len(out_of_range), details)
    print(f"[{status}] Temperature range: {details}")
    return passed

def check_city_completeness(df: pd.DataFrame) -> bool:
    """Check that all 5 cities are present."""
    actual_cities = set(df["city"].unique())
    missing = EXPECTED_CITIES - actual_cities
    passed = len(missing) == 0
    status = "PASS" if passed else "FAIL"
    details = "All cities present" if passed else f"Missing cities: {missing}"
    log_result("city_completeness", status, len(df), len(missing), details)
    print(f"[{status}] City completeness: {details}")
    return passed

def check_duplicates(df: pd.DataFrame) -> bool:
    """Check for duplicate city/date combinations."""
    dupes = int(df.duplicated(subset=["city", "date"]).sum())
    passed = dupes == 0
    status = "PASS" if passed else "FAIL"
    details = "No duplicates found" if passed else f"{dupes} duplicate rows found"
    log_result("duplicate_check", status, len(df), dupes, details)
    print(f"[{status}] Duplicate check: {details}")
    return passed

def run():
    """Fetch latest bronze data and run all quality checks."""
    print("Running quality checks...")

    query = text("""
        SELECT * FROM bronze.weather_raw
        WHERE ingested_at = (
            SELECT MAX(ingested_at) FROM bronze.weather_raw
        )
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    results = [
        check_row_count(df),
        check_nulls(df),
        check_temperature_range(df),
        check_city_completeness(df),
        check_duplicates(df),
    ]

    passed = sum(results)
    total  = len(results)
    print(f"\nQuality checks complete: {passed}/{total} passed")

    if passed == total:
        print("All checks passed — pipeline is healthy.")
    else:
        print(f"WARNING: {total - passed} check(s) failed — review pipeline_runs table.")

if __name__ == "__main__":
    run()
"""
backfill.py — Manually fetch and process missing days of weather data.

Usage:
    python backfill.py --start 2025-03-01 --end 2025-03-05
    python backfill.py --start 2025-03-01            # backfills from start to yesterday
    python backfill.py --days 7                       # backfills the last N days
    python backfill.py --start 2025-03-01 --skip-bronze  # rebuild silver+gold from existing bronze only

What it does:
    For each missing day in the given range, this script runs the full pipeline:
    ingest (bronze) → transform (silver) → gold → quality checks

    It skips days that already have data in bronze to avoid duplicates.
    Use --skip-bronze to rebuild silver and gold without touching bronze at all.
"""

import argparse
import openmeteo_requests
import pandas as pd
import requests_cache
from datetime import datetime, timezone, timedelta, date
from sqlalchemy import create_engine, text
from retry_requests import retry
from dotenv import load_dotenv
from typing import Optional
import time
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

CITIES = [
    {"name": "Singapore", "latitude": 1.3667,  "longitude": 103.8},
    {"name": "London",    "latitude": 51.51,   "longitude": -0.13},
    {"name": "Tokyo",     "latitude": 35.68,   "longitude": 139.69},
    {"name": "New York",  "latitude": 40.71,   "longitude": -74.01},
    {"name": "Sydney",    "latitude": -33.87,  "longitude": 151.21},
]

EXPECTED_CITIES = {"Singapore", "London", "Tokyo", "New York", "Sydney"}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def date_range(start: date, end: date):
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def get_existing_bronze_dates() -> set:
    """Return set of dates (SGT) already in bronze."""
    query = text("""
        SELECT DISTINCT DATE(ingested_at AT TIME ZONE 'Asia/Singapore') AS day
        FROM bronze.weather_raw
    """)
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    return set(result["day"].tolist())


def get_existing_silver_dates() -> set:
    """Return set of dates (SGT) already in silver."""
    query = text("""
        SELECT DISTINCT DATE(transformed_at AT TIME ZONE 'Asia/Singapore') AS day
        FROM silver.weather_cleaned
    """)
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    return set(result["day"].tolist())


def get_existing_gold_dates() -> set:
    """Return set of days already in gold."""
    query = text("SELECT DISTINCT day FROM gold.weather_daily")
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    return set(pd.to_datetime(result["day"]).dt.date.tolist())


# ─────────────────────────────────────────────
# INGEST (BRONZE)
# ─────────────────────────────────────────────

def fetch_day(target_date: date) -> pd.DataFrame:
    """Fetch hourly weather for all cities on a specific historical date."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    date_str = target_date.isoformat()

    params = {
        "latitude":  [c["latitude"]  for c in CITIES],
        "longitude": [c["longitude"] for c in CITIES],
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "precipitation_probability", "precipitation", "rain",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
            "weather_code", "visibility", "uv_index", "cloud_cover"
        ],
        "timezone":   "UTC",
        "start_date": date_str,
        "end_date":   date_str,
    }

    # Use the archive API for older dates — higher rate limits than the forecast
    # endpoint and designed for historical data. Fall back to forecast API for
    # the last ~5 days which the archive hasn't ingested yet.
    days_ago = (date.today() - target_date).days
    api_url = (
        "https://archive-api.open-meteo.com/v1/archive"
        if days_ago >= 5
        else "https://api.open-meteo.com/v1/forecast"
    )
    print(f"  Using {'archive' if days_ago >= 5 else 'forecast'} API")

    responses = openmeteo.weather_api(api_url, params=params)
    all_dataframes = []

    for i, response in enumerate(responses):
        try:
            city_name = CITIES[i]["name"]
            hourly = response.Hourly()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                "city":                      city_name,
                "temperature_2m":            hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_2m":      hourly.Variables(1).ValuesAsNumpy(),
                "apparent_temperature":      hourly.Variables(2).ValuesAsNumpy(),
                "precipitation_probability": hourly.Variables(3).ValuesAsNumpy(),
                "precipitation":             hourly.Variables(4).ValuesAsNumpy(),
                "rain":                      hourly.Variables(5).ValuesAsNumpy(),
                "wind_speed_10m":            hourly.Variables(6).ValuesAsNumpy(),
                "wind_direction_10m":        hourly.Variables(7).ValuesAsNumpy(),
                "wind_gusts_10m":            hourly.Variables(8).ValuesAsNumpy(),
                "weather_code":              hourly.Variables(9).ValuesAsNumpy(),
                "visibility":               hourly.Variables(10).ValuesAsNumpy(),
                "uv_index":                 hourly.Variables(11).ValuesAsNumpy(),
                "cloud_cover":              hourly.Variables(12).ValuesAsNumpy(),
            }

            df = pd.DataFrame(data=hourly_data)
            all_dataframes.append(df)
            print(f"  Fetched {city_name}: {len(df)} rows")

        except Exception as e:
            print(f"  Failed to fetch {CITIES[i]['name']}: {e}")

    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()


def load_bronze(df: pd.DataFrame, target_date: date) -> None:
    """Load raw data into bronze, stamping ingested_at as midnight SGT of that day."""
    if df.empty:
        print("  No data to load into bronze.")
        return

    # Stamp with midnight SGT of the backfilled date so daily checks work correctly
    ingested_at = datetime.combine(target_date, datetime.min.time()).replace(
        tzinfo=timezone(timedelta(hours=8))
    )
    df["ingested_at"] = ingested_at

    df.to_sql(
        name="weather_raw",
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"  Loaded {len(df)} rows into bronze.weather_raw (ingested_at={ingested_at.date()})")


# ─────────────────────────────────────────────
# TRANSFORM (SILVER)
# ─────────────────────────────────────────────

def transform_day(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich raw rows — same logic as transform.py."""

    df["weather_code"] = df["weather_code"].astype(int)

    float_cols = [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation_probability", "precipitation", "rain",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
        "visibility", "uv_index", "cloud_cover"
    ]
    df[float_cols] = df[float_cols].round(2)

    df["feels_hotter"] = df["apparent_temperature"] > (df["temperature_2m"] + 2)

    def categorise_heat(temp):
        if temp < 10:   return "Cold"
        elif temp < 18: return "Cool"
        elif temp < 25: return "Warm"
        elif temp < 32: return "Hot"
        else:           return "Extreme"

    df["heat_category"] = df["temperature_2m"].apply(categorise_heat)

    def categorise_rain(precip):
        if precip == 0:   return "None"
        elif precip < 1:  return "Light"
        elif precip < 5:  return "Moderate"
        else:             return "Heavy"

    df["rain_category"] = df["precipitation"].apply(categorise_rain)

    def categorise_wind(speed):
        if speed < 10:   return "Calm"
        elif speed < 30: return "Breezy"
        elif speed < 60: return "Windy"
        else:            return "Storm"

    df["wind_category"] = df["wind_speed_10m"].apply(categorise_wind)

    extreme_codes = set(range(95, 100)) | {85, 86, 75, 76, 77}
    df["is_extreme_weather"] = df["weather_code"].isin(extreme_codes)

    df["transformed_at"] = datetime.now(timezone.utc)

    return df


def load_silver(df: pd.DataFrame) -> None:
    """Write cleaned data to silver.weather_cleaned."""
    cols_to_load = [
        "ingested_at", "transformed_at", "city", "date",
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation_probability", "precipitation", "rain",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
        "weather_code", "visibility", "uv_index", "cloud_cover",
        "feels_hotter", "heat_category", "rain_category",
        "wind_category", "is_extreme_weather"
    ]
    df[cols_to_load].to_sql(
        name="weather_cleaned",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"  Loaded {len(df)} rows into silver.weather_cleaned")


# ─────────────────────────────────────────────
# GOLD
# ─────────────────────────────────────────────

def build_gold(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly silver rows into one row per city per day."""
    df_silver["day"] = pd.to_datetime(df_silver["date"]).dt.tz_convert("Asia/Singapore").dt.date

    def dominant(series):
        return series.value_counts().index[0]

    gold = df_silver.groupby(["city", "day"]).agg(
        avg_temperature          = ("temperature_2m",        "mean"),
        min_temperature          = ("temperature_2m",        "min"),
        max_temperature          = ("temperature_2m",        "max"),
        avg_apparent_temperature = ("apparent_temperature",  "mean"),
        avg_humidity             = ("relative_humidity_2m",  "mean"),
        total_precipitation      = ("precipitation",         "sum"),
        max_precipitation        = ("precipitation",         "max"),
        avg_wind_speed           = ("wind_speed_10m",        "mean"),
        max_wind_gusts           = ("wind_gusts_10m",        "max"),
        avg_cloud_cover          = ("cloud_cover",           "mean"),
        max_uv_index             = ("uv_index",              "max"),
        avg_visibility           = ("visibility",            "mean"),
        dominant_heat_category   = ("heat_category",         dominant),
        dominant_rain_category   = ("rain_category",         dominant),
        dominant_wind_category   = ("wind_category",         dominant),
        extreme_weather_hours    = ("is_extreme_weather",    "sum"),
        feels_hotter_hours       = ("feels_hotter",          "sum"),
        hours_of_data            = ("date",                  "count"),
    ).reset_index()

    float_cols = [
        "avg_temperature", "min_temperature", "max_temperature",
        "avg_apparent_temperature", "avg_humidity", "total_precipitation",
        "max_precipitation", "avg_wind_speed", "max_wind_gusts",
        "avg_cloud_cover", "max_uv_index", "avg_visibility"
    ]
    gold[float_cols] = gold[float_cols].round(2)

    return gold


def load_gold(df_gold: pd.DataFrame) -> None:
    """Append gold rows, skipping any days already present."""
    existing_query = text("SELECT day FROM gold.weather_daily")
    with engine.connect() as conn:
        existing = pd.read_sql(existing_query, conn)

    if not existing.empty:
        existing_days = pd.to_datetime(existing["day"]).dt.date.tolist()
        df_gold = df_gold[~df_gold["day"].isin(existing_days)]

    if df_gold.empty:
        print("  Gold already up to date — nothing to load.")
        return

    df_gold.to_sql(
        name="weather_daily",
        schema="gold",
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"  Loaded {len(df_gold)} rows into gold.weather_daily")


# ─────────────────────────────────────────────
# QUALITY CHECKS
# ─────────────────────────────────────────────

def log_result(stage: str, status: str, rows_checked: int, issues_found: int, details: str):
    query = text("""
        INSERT INTO bronze.pipeline_runs 
        (stage, status, rows_checked, issues_found, details)
        VALUES (:stage, :status, :rows_checked, :issues_found, :details)
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "stage": stage, "status": status,
            "rows_checked": rows_checked, "issues_found": issues_found,
            "details": details
        })


def run_quality_checks(df: pd.DataFrame, target_date: date) -> bool:
    """Run the same quality checks as quality_checks.py against a specific day's data."""
    print(f"  Running quality checks for {target_date}...")
    EXPECTED_ROWS = 120

    results = []

    # Row count
    actual = len(df)
    passed = actual == EXPECTED_ROWS
    details = f"Expected {EXPECTED_ROWS} rows, got {actual}"
    log_result("row_count", "PASS" if passed else "FAIL", actual, 0 if passed else 1, details)
    print(f"    [{'PASS' if passed else 'FAIL'}] Row count: {details}")
    results.append(passed)

    # Nulls
    key_cols = ["city", "date", "temperature_2m", "precipitation", "wind_speed_10m", "weather_code"]
    null_counts = df[key_cols].isnull().sum()
    issues = null_counts[null_counts > 0]
    passed = len(issues) == 0
    details = "No nulls found" if passed else f"Nulls found: {issues.to_dict()}"
    log_result("null_check", "PASS" if passed else "FAIL", len(df), len(issues), details)
    print(f"    [{'PASS' if passed else 'FAIL'}] Null check: {details}")
    results.append(passed)

    # Temperature range
    out_of_range = df[(df["temperature_2m"] < -50) | (df["temperature_2m"] > 60)]
    passed = len(out_of_range) == 0
    details = "All temperatures in range" if passed else f"{len(out_of_range)} rows outside -50 to 60°C"
    log_result("temperature_range", "PASS" if passed else "FAIL", len(df), len(out_of_range), details)
    print(f"    [{'PASS' if passed else 'FAIL'}] Temperature range: {details}")
    results.append(passed)

    # City completeness
    actual_cities = set(df["city"].unique())
    missing = EXPECTED_CITIES - actual_cities
    passed = len(missing) == 0
    details = "All cities present" if passed else f"Missing cities: {missing}"
    log_result("city_completeness", "PASS" if passed else "FAIL", len(df), len(missing), details)
    print(f"    [{'PASS' if passed else 'FAIL'}] City completeness: {details}")
    results.append(passed)

    # Duplicates
    dupes = int(df.duplicated(subset=["city", "date"]).sum())
    passed = dupes == 0
    details = "No duplicates found" if passed else f"{dupes} duplicate rows found"
    log_result("duplicate_check", "PASS" if passed else "FAIL", len(df), dupes, details)
    print(f"    [{'PASS' if passed else 'FAIL'}] Duplicate check: {details}")
    results.append(passed)

    all_passed = all(results)
    print(f"  Quality checks: {sum(results)}/{len(results)} passed")
    return all_passed


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill missing days of weather data through the full pipeline."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", type=date.fromisoformat,
                       help="Start date (YYYY-MM-DD). Use with optional --end.")
    group.add_argument("--days", type=int,
                       help="Backfill the last N days (not including today).")
    parser.add_argument("--end", type=date.fromisoformat,
                        help="End date (YYYY-MM-DD). Defaults to yesterday if --start is used alone.")
    parser.add_argument("--force", action="store_true",
                        help="Re-process days even if bronze data already exists.")
    parser.add_argument("--skip-bronze", action="store_true",
                        help="Skip bronze ingestion entirely — rebuild silver and gold from existing bronze only.")
    return parser.parse_args()


def main():
    args = parse_args()

    yesterday = date.today() - timedelta(days=1)

    if args.days:
        start = date.today() - timedelta(days=args.days)
        end   = yesterday
    else:
        start = args.start
        end   = args.end or yesterday

    if start > end:
        print(f"Error: start date {start} is after end date {end}.")
        return

    print(f"\n{'='*55}")
    print(f"  Backfill: {start} → {end}")
    print(f"{'='*55}")

    existing_bronze = get_existing_bronze_dates()
    existing_silver = get_existing_silver_dates()
    existing_gold   = get_existing_gold_dates()

    days_to_process = list(date_range(start, end))
    print(f"  Days in range : {len(days_to_process)}")

    if args.skip_bronze:
        # Only process days that actually exist in bronze
        days_to_process = [d for d in days_to_process if d in existing_bronze]
        print(f"  Days in bronze: {len(days_to_process)}")
        print(f"  Mode          : skip-bronze (rebuilding silver + gold only)")
    elif not args.force:
        days_to_process = [d for d in days_to_process if d not in existing_bronze]
        print(f"  Missing days  : {len(days_to_process)}")

    if not days_to_process:
        if args.skip_bronze:
            print("\n  No bronze data found for this range.\n")
        else:
            print("\n  All days already have data. Use --force to re-process.\n")
        return

    summary = []

    for target_date in days_to_process:
        print(f"\n--- Processing {target_date} ---")

        if args.skip_bronze:
            # Read directly from existing bronze — don't touch it
            print("  [1/4] Reading from existing bronze...")
            query = text("""
                SELECT * FROM bronze.weather_raw
                WHERE DATE(ingested_at AT TIME ZONE 'Asia/Singapore') = :day
            """)
            with engine.connect() as conn:
                df_raw = pd.read_sql(query, conn, params={"day": target_date})
            if df_raw.empty:
                print(f"  No bronze data found for {target_date} — skipping.")
                summary.append((target_date, "SKIPPED", "No bronze data found"))
                continue
            print(f"  Found {len(df_raw)} rows in bronze")
        else:
            # 1. Ingest from API
            print("  [1/4] Fetching from Open-Meteo API...")
            df_raw = fetch_day(target_date)
            if df_raw.empty:
                print("  No data returned — skipping this day.")
                summary.append((target_date, "SKIPPED", "API returned no data"))
                continue
            load_bronze(df_raw, target_date)

        # 2. Transform
        print("  [2/4] Transforming to silver...")
        df_silver = transform_day(df_raw.copy())
        load_silver(df_silver)

        # 3. Gold
        print("  [3/4] Aggregating to gold...")
        if target_date not in existing_gold:
            df_gold = build_gold(df_silver.copy())
            load_gold(df_gold)
        else:
            print(f"  Gold already has data for {target_date} — skipping.")

        # 4. Quality checks
        print("  [4/4] Quality checks...")
        passed = run_quality_checks(df_raw, target_date)

        summary.append((target_date, "OK" if passed else "WARN", "All checks passed" if passed else "Some checks failed"))

        # Pause between days to respect Open-Meteo rate limits (only when fetching from API)
        if not args.skip_bronze and target_date != days_to_process[-1]:
            print("  Waiting 2s before next day...")
            time.sleep(2)

    # Final summary
    print(f"\n{'='*55}")
    print("  Backfill complete. Summary:")
    print(f"{'='*55}")
    for d, status, note in summary:
        print(f"  {d}  [{status}]  {note}")
    print()


if __name__ == "__main__":
    main()
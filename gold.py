import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
from typing import Optional

load_dotenv()
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def extract() -> Optional[pd.DataFrame]:
    """Read silver rows not yet aggregated into gold."""

    # Find missing days with enough data
    missing_query = text("""
        SELECT DATE(date) as day
        FROM silver.weather_cleaned
        WHERE DATE(date) NOT IN (
            SELECT day FROM gold.weather_daily
        )
        GROUP BY DATE(date)
        HAVING COUNT(*) >= 80
        ORDER BY day
    """)

    with engine.connect() as conn:
        missing_days = pd.read_sql(missing_query, conn)

    if missing_days.empty:
        print("Gold is already up to date — skipping.")
        return None

    missing_days_list = missing_days['day'].tolist()
    print(f"Found {len(missing_days_list)} days to aggregate: {missing_days_list}")

    # Pull only those specific days from silver
    query = text("""
        SELECT * FROM silver.weather_cleaned
        WHERE DATE(date) = ANY(:days)
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"days": missing_days_list})

    print(f"Extracted {len(df)} rows from silver")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly silver data into one row per city per day."""

    # Extract the calendar date from the timestamp
    df["day"] = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Singapore").dt.date

    # Helper to find the most common value in a group
    def dominant(series):
        return series.value_counts().index[0]

    gold = df.groupby(["city", "day"]).agg(
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

    # Round all float columns
    float_cols = [
        "avg_temperature", "min_temperature", "max_temperature",
        "avg_apparent_temperature", "avg_humidity", "total_precipitation",
        "max_precipitation", "avg_wind_speed", "max_wind_gusts",
        "avg_cloud_cover", "max_uv_index", "avg_visibility"
    ]
    gold[float_cols] = gold[float_cols].round(2)

    print(f"Transformed into {len(gold)} gold rows")
    return gold

def load(df: pd.DataFrame) -> None:
    """Write aggregated data to gold table, skipping existing days."""

    # Check which days already exist in gold
    existing_query = text("SELECT day FROM gold.weather_daily")
    with engine.connect() as conn:
        existing = pd.read_sql(existing_query, conn)

    if not existing.empty:
        existing_days = pd.to_datetime(existing['day']).dt.date.tolist()
        df = df[~df['day'].isin(existing_days)]

    if df.empty:
        print("All days already in gold — nothing to load.")
        return

    df.to_sql(
        name="weather_daily",
        schema="gold",
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"Loaded {len(df)} rows into gold.weather_daily")

def run():
    df_silver = extract()
    if df_silver is not None:
        df_gold = transform(df_silver)
        load(df_gold)
        print("Gold layer complete.")

if __name__ == "__main__":
    run()
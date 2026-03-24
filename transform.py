import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import os
from typing import Optional

# Transformation
"""
    1. Cast weather_code from float to int      (It came in as 27.0, should be 27)
    2. Round float columns to 2 decimal places  (Remove noise like 27.400001)
    3. Add feels_hotter flag                    (True when apparent temp > actual temp by 2 deg +)
    4. Add heat_category                        (Labels like "Cool", "Warm", "Hot" , "Extreme")
    5. Add rain_category                        (Labels like "None", "Light", "Moderate", "Heavy")
    6. Add wind_category                        (Labels like "Calm", "Breezy", "Windy", "Storm")
    7. Add is_extreme_weather flag              (True when weather_code indicates storm/thunderstorm)
    8. Add transformed_at timestamp             (Audit trail to show when row was processed)
"""

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
    """Read latest bronze rows — skip if already transformed today (SGT)."""

    # Check if silver already has data transformed today (Singapore time)
    today_sgt = datetime.now(timezone(timedelta(hours=8))).date()
    check_query = text("""
        SELECT COUNT(*) FROM silver.weather_cleaned
        WHERE DATE(transformed_at AT TIME ZONE 'Asia/Singapore') = :today
    """)
    with engine.connect() as conn:
        already_done = conn.execute(check_query, {"today": today_sgt}).scalar()

    if already_done > 0:
        print(f"Silver data for {today_sgt} already exists ({already_done} rows) — skipping transform.")
        return None

    # Pull latest bronze batch
    query = text("""
        SELECT * FROM bronze.weather_raw
        WHERE ingested_at = (
            SELECT MAX(ingested_at) FROM bronze.weather_raw
        )
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    print(f"Extracted {len(df)} rows from bronze")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the raw weather data."""

    # 1. Cast weather_code from float to int
    df["weather_code"] = df["weather_code"].astype(int)

    # 2. Round all float columns to 2 decimal places
    float_cols = [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation_probability", "precipitation", "rain",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
        "visibility", "uv_index", "cloud_cover"
    ]
    df[float_cols] = df[float_cols].round(2)

    # 3. Feels hotter flag
    df["feels_hotter"] = df["apparent_temperature"] > (df["temperature_2m"] + 2)

    # 4. Heat category
    def categorise_heat(temp):
        if temp < 10:   return "Cold"
        elif temp < 18: return "Cool"
        elif temp < 25: return "Warm"
        elif temp < 32: return "Hot"
        else:           return "Extreme"

    df["heat_category"] = df["temperature_2m"].apply(categorise_heat)

    # 5. Rain category
    def categorise_rain(precip):
        if precip == 0:   return "None"
        elif precip < 1:  return "Light"
        elif precip < 5:  return "Moderate"
        else:             return "Heavy"

    df["rain_category"] = df["precipitation"].apply(categorise_rain)

    # 6. Wind category
    def categorise_wind(speed):
        if speed < 10:   return "Calm"
        elif speed < 30: return "Breezy"
        elif speed < 60: return "Windy"
        else:            return "Storm"

    df["wind_category"] = df["wind_speed_10m"].apply(categorise_wind)

    # 7. Extreme weather flag (WMO codes)
    extreme_codes = set(range(95, 100)) | {85, 86, 75, 76, 77}
    df["is_extreme_weather"] = df["weather_code"].isin(extreme_codes)

    # 8. Audit timestamp
    df["transformed_at"] = datetime.now(timezone.utc)

    print(f"Transformed {len(df)} rows")
    return df

def load(df: pd.DataFrame) -> None:
    """Write cleaned data to silver table."""
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
    print(f"Loaded {len(df)} rows into silver.weather_cleaned")

if __name__ == "__main__":
    df_raw = extract()
    if df_raw is not None:
        df_cleaned = transform(df_raw)
        load(df_cleaned)
        print("Transform complete.")
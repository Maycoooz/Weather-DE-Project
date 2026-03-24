import openmeteo_requests
import pandas as pd
import requests_cache
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from retry_requests import retry
from dotenv import load_dotenv
import os

load_dotenv()
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CITIES = [
    {"name": "Singapore", "latitude": 1.3667,  "longitude": 103.8},
    {"name": "London",    "latitude": 51.51,   "longitude": -0.13},
    {"name": "Tokyo",     "latitude": 35.68,   "longitude": 139.69},
    {"name": "New York",  "latitude": 40.71,   "longitude": -74.01},
    {"name": "Sydney",    "latitude": -33.87,  "longitude": 151.21},
]

def fetch() -> pd.DataFrame:
    """Fetch weather data from Open-Meteo for all cities."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude":  [c["latitude"]  for c in CITIES],
        "longitude": [c["longitude"] for c in CITIES],
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "precipitation_probability", "precipitation", "rain",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
            "weather_code", "visibility", "uv_index", "cloud_cover"
        ],
        "timezone": "UTC",
        "forecast_days": 1,
    }

    responses = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
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
            print(f"Fetched {city_name}: {len(df)} rows")

        except Exception as e:
            print(f"Failed to process {CITIES[i]['name']}: {e}")

    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()

def load(combined_df: pd.DataFrame) -> None:
    """Load fetched data into bronze table if not already ingested today."""
    if combined_df.empty:
        print("No data to load.")
        return

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Check if we already have data ingested today (Singapore time)
    today_sgt = datetime.now(timezone(timedelta(hours=8))).date()
    check_query = text("""
        SELECT COUNT(*) FROM bronze.weather_raw
        WHERE DATE(ingested_at AT TIME ZONE 'Asia/Singapore') = :today
    """)

    with engine.connect() as conn:
        result = conn.execute(check_query, {"today": today_sgt}).scalar()

    if result > 0:
        print(f"Data for {today_sgt} already exists ({result} rows) — skipping ingest.")
    else:
        combined_df.to_sql(
            name="weather_raw",
            schema="bronze",
            con=engine,
            if_exists="append",
            index=False,
        )
        print(f"Loaded {len(combined_df)} rows into bronze.weather_raw")

if __name__ == "__main__":
    df = fetch()
    load(df)
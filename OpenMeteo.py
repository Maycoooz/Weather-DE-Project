import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup client with cache and retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Define cities
CITIES = [
    {"name": "Singapore", "latitude": 1.3667,  "longitude": 103.8},
    {"name": "London",    "latitude": 51.51,   "longitude": -0.13},
    {"name": "Tokyo",     "latitude": 35.68,   "longitude": 139.69},
    {"name": "New York",  "latitude": 40.71,   "longitude": -74.01},
    {"name": "Sydney",    "latitude": -33.87,  "longitude": 151.21},
]

url = "https://api.open-meteo.com/v1/forecast"
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
    "forecast_days": 1,  # only fetch today's 24 hours
}

responses = openmeteo.weather_api(url, params=params)

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
            "visibility":                hourly.Variables(10).ValuesAsNumpy(),
            "uv_index":                  hourly.Variables(11).ValuesAsNumpy(),
            "cloud_cover":               hourly.Variables(12).ValuesAsNumpy(),
        }

        df = pd.DataFrame(data=hourly_data)
        all_dataframes.append(df)
        print(f"Fetched {city_name}: {len(df)} rows")

    except Exception as e:
        print(f"Failed to process {CITIES[i]['name']}: {e}")

# Combine all cities into one dataframe
if all_dataframes:
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"\nTotal rows: {len(combined_df)}")
    print(combined_df)
else:
    print("No data fetched — check your connection or API response.")
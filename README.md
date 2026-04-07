# Weather Data Engineering Pipeline

An end-to-end data engineering project that ingests real-time weather data 
from 5 global cities, transforms and stores it in a PostgreSQL data warehouse, 
orchestrates daily runs with Apache Airflow, and visualises insights in Power BI.

## Architecture
```
Open-Meteo API → Python ETL → PostgreSQL (Bronze → Silver → Gold) → Power BI
                                    ↑
                              Apache Airflow
                           (daily orchestration)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Open-Meteo API |
| Transformation | pandas |
| Orchestration | Apache Airflow (Docker) |
| Data Warehouse | PostgreSQL |
| Visualisation | Power BI |
| Containerisation | Docker, docker-compose |
| Version Control | Git, GitHub |

## Data Architecture

This project implements the **medallion architecture**:

- **Bronze** — raw hourly weather data exactly as returned by the API
- **Silver** — cleaned, typed, and enriched with derived categories
- **Gold** — daily aggregates per city, ready for reporting

## Cities Tracked

Singapore, London, Tokyo, New York, Sydney

## Weather Metrics

Temperature, apparent temperature, humidity, precipitation, wind speed, 
wind direction, wind gusts, UV index, cloud cover, weather code, visibility

## Pipeline Stages

1. **Ingest** (`ingest.py`) — fetches hourly forecast data from Open-Meteo API 
   for all 5 cities and loads raw JSON into the bronze layer
2. **Transform** (`transform.py`) — cleans data, casts types, adds derived 
   columns (heat_category, rain_category, wind_category, feels_hotter flag)
3. **Gold** (`gold.py`) — aggregates hourly silver data into daily summaries 
   per city with avg/min/max temperature, total precipitation, dominant categories
4. **Quality Checks** (`quality_checks.py`) — validates row counts, nulls, 
   temperature ranges, city completeness, and duplicates. Results logged to 
   `bronze.pipeline_runs`

## Airflow DAG

The pipeline runs daily at midnight UTC (8am Singapore time):
```
ingest → transform → gold → quality_checks
```

## Project Structure
```
weather-de-project/
├── dags/
│   └── weather_dag.py       # Airflow DAG definition
├── ingest.py                # Bronze layer — API ingestion
├── transform.py             # Silver layer — cleaning and enrichment
├── gold.py                  # Gold layer — daily aggregations
├── quality_checks.py        # Data quality validation
├── backfill.py              # Manual backfill for missing historical days
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Airflow + PostgreSQL containers
└── README.md
```

## Setup Instructions

### Prerequisites
- Python 3.10+
- Docker Desktop
- PostgreSQL
- Power BI Desktop

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/weather-de-project.git
cd weather-de-project
```

2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

3. Create `.env` file with your PostgreSQL credentials
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_pipeline
DB_USER=postgres
DB_PASSWORD=your_password
```

4. Create the database, schemas, and tables in PostgreSQL
```sql
CREATE DATABASE weather_pipeline
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_Singapore.1252'
    LC_CTYPE = 'English_Singapore.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
```

Then connect to the `weather_pipeline` database and run:

```sql
-- Bronze
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    id                        SERIAL PRIMARY KEY,
    ingested_at               TIMESTAMP DEFAULT NOW(),
    city                      VARCHAR(100),
    date                      TIMESTAMPTZ,
    temperature_2m            FLOAT,
    relative_humidity_2m      FLOAT,
    apparent_temperature      FLOAT,
    precipitation_probability FLOAT,
    precipitation             FLOAT,
    rain                      FLOAT,
    wind_speed_10m            FLOAT,
    wind_direction_10m        FLOAT,
    wind_gusts_10m            FLOAT,
    weather_code              FLOAT,
    visibility                FLOAT,
    uv_index                  FLOAT,
    cloud_cover               FLOAT
);

CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    id           SERIAL PRIMARY KEY,
    run_at       TIMESTAMP DEFAULT NOW(),
    stage        VARCHAR(50),
    status       VARCHAR(20),
    rows_checked INT,
    issues_found INT,
    details      TEXT
);

-- Silver
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.weather_cleaned (
    id                        SERIAL PRIMARY KEY,
    ingested_at               TIMESTAMPTZ,
    transformed_at            TIMESTAMPTZ,
    city                      VARCHAR(100),
    date                      TIMESTAMPTZ,
    temperature_2m            FLOAT,
    relative_humidity_2m      FLOAT,
    apparent_temperature      FLOAT,
    precipitation_probability FLOAT,
    precipitation             FLOAT,
    rain                      FLOAT,
    wind_speed_10m            FLOAT,
    wind_direction_10m        FLOAT,
    wind_gusts_10m            FLOAT,
    weather_code              INT,
    visibility                FLOAT,
    uv_index                  FLOAT,
    cloud_cover               FLOAT,
    feels_hotter              BOOLEAN,
    heat_category             VARCHAR(20),
    rain_category             VARCHAR(20),
    wind_category             VARCHAR(20),
    is_extreme_weather        BOOLEAN
);

-- Gold
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.weather_daily (
    id                       SERIAL PRIMARY KEY,
    city                     VARCHAR(100),
    day                      DATE,
    avg_temperature          FLOAT,
    min_temperature          FLOAT,
    max_temperature          FLOAT,
    avg_apparent_temperature FLOAT,
    avg_humidity             FLOAT,
    total_precipitation      FLOAT,
    max_precipitation        FLOAT,
    avg_wind_speed           FLOAT,
    max_wind_gusts           FLOAT,
    avg_cloud_cover          FLOAT,
    max_uv_index             FLOAT,
    avg_visibility           FLOAT,
    dominant_heat_category   VARCHAR(20),
    dominant_rain_category   VARCHAR(20),
    dominant_wind_category   VARCHAR(20),
    extreme_weather_hours    INT,
    feels_hotter_hours       INT,
    hours_of_data            INT,
    loaded_at                TIMESTAMPTZ DEFAULT NOW()
);
```

5. Start Airflow
```bash
docker-compose up airflow-init
docker-compose up -d
```

6. Run the pipeline manually
```bash
python ingest.py
python transform.py
python gold.py
python quality_checks.py
```

## Backfilling Missing Data

If the pipeline misses a day (e.g. Airflow was down, or the API was unreachable),
`backfill.py` recovers missing days by fetching from the Open-Meteo historical API
and running the full pipeline — ingest, transform, gold aggregation, and quality 
checks — for each missing day. Results are logged to `bronze.pipeline_runs` exactly 
like a normal pipeline run.

### Usage

```bash
# Backfill a specific date range
python backfill.py --start 2026-03-01 --end 2026-03-05

# Backfill from a start date up to yesterday
python backfill.py --start 2026-03-01

# Backfill the last N days
python backfill.py --days 7

# Force re-process days even if bronze data already exists
python backfill.py --start 2026-03-01 --end 2026-03-05 --force

# Rebuild silver and gold from existing bronze without hitting the API
python backfill.py --start 2026-03-01 --end 2026-03-05 --skip-bronze
```

### Flags

| Flag | Description |
|---|---|
| `--start` | Start date in YYYY-MM-DD format |
| `--end` | End date in YYYY-MM-DD format (defaults to yesterday) |
| `--days N` | Backfill the last N days instead of a fixed range |
| `--force` | Re-process days even if bronze data already exists |
| `--skip-bronze` | Rebuild silver and gold from existing bronze without hitting the API — useful for fixing downstream layer issues without risking bronze duplication |

### Notes

- **Safe to re-run** — skips any day that already has bronze data by default, so running it multiple times without `--force` will not create duplicates
- **First day of the pipeline** may have fewer than 120 silver rows if the pipeline first ran mid-day — this is expected and not a data quality issue
- The Open-Meteo API returns some hours beyond midnight UTC, so a small number of rows with tomorrow's UTC date will appear in bronze and silver — this is normal and those rows are picked up by the following day's pipeline run

## Data Quality Checks

Every pipeline run logs results to `bronze.pipeline_runs`:

| Check | Description |
|---|---|
| Row count | Validates exactly 120 rows per run (5 cities × 24 hours) |
| Null check | Ensures no missing values in key columns |
| Temperature range | Flags any temperature outside -50°C to 60°C |
| City completeness | Confirms all 5 cities are present |
| Duplicate check | Detects any duplicate city/date combinations |

## Dashboard

Power BI dashboard connected directly to the PostgreSQL gold layer showing:
- Temperature trends across all cities over time
- Daily precipitation comparison
- Peak temperature, minimum temperature, UV index cards
- Interactive city slicer filtering all visuals

## Key Learnings

- Medallion architecture (Bronze → Silver → Gold)
- ETL pipeline design and idempotency
- Apache Airflow DAG authoring and orchestration
- Docker and docker-compose for local development
- Data quality monitoring and audit logging
- Power BI connected to a live PostgreSQL warehouse
- Backfill patterns for recovering missing historical data

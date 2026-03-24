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
   bronze.pipeline_runs

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

4. Create the database schemas in PostgreSQL
```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
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
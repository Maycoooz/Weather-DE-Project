from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import sys
import os

sys.path.insert(0, '/opt/airflow')

from ingest import fetch, load as ingest_load
from transform import extract as transform_extract, transform, load as transform_load
from gold import extract as gold_extract, transform as gold_transform, load as gold_load
from quality_checks import run as quality_run

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 21),
    'retries': 1,
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Daily weather ETL pipeline — ingest, transform, gold, quality checks',
    tags=['weather', 'etl'],
) as dag:

    def run_ingest():
        df = fetch()
        ingest_load(df)

    def run_transform():
        df = transform_extract()
        if df is not None:
            df_cleaned = transform(df)
            transform_load(df_cleaned)

    def run_gold():
        df = gold_extract()
        if df is not None:
            df_gold = gold_transform(df)
            gold_load(df_gold)

    def run_quality():
        quality_run()

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=run_ingest,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    gold_task = PythonOperator(
        task_id='gold',
        python_callable=run_gold,
    )

    quality_task = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality,
    )

    ingest_task >> transform_task >> gold_task >> quality_task
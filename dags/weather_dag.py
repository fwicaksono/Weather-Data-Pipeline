from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to path so we can import them
sys.path.append('/opt/airflow/scripts')

from extract import extract_weather
from transform import transform_weather
from load import load_to_postgres

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'skywatch_weather_pipeline',
    default_args=default_args,
    description='ETL pipeline for Weather Data (Jogja vs Aceh vs Muntilan)',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_weather,
    )

    t2 = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_weather,
    )

    t3 = PythonOperator(
        task_id='load_gold',
        python_callable=load_to_postgres,
    )

    # The Flow
    t1 >> t2 >> t3
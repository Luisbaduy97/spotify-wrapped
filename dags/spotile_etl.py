from sched import scheduler
import pandas as pd
import spotipy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 12, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_etl',
    default_args=default_args,
    description='Extract data from spotify!',
    schedule_interval="@daily"
)

def is_db_available() -> str:
    return """
    SELECT version()
    """

database_available = PostgresOperator(
    task_id='is_db_available',
    sql=is_db_available,
    dag=dag
)

# Extract songs data
# Incloude song popularity, minutes, timestamp, extraer mes, dia, año, artista landing
# Ejecutar las vistar de artista, y canciones en staging

database_available
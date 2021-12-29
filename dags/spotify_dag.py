import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from spotify_scrapper import spotify_etl
from spotify_analytics import song_analytics, artist_analytics

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
    'spotify_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

def just_a_function():
    print("I'm going to show you something :)")

run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=spotify_etl,
    dag=dag,
)

song_stats = PythonOperator(
    task_id='get_song_stats',
    python_callable=song_analytics,
    dag=dag
)

artist_stats = PythonOperator(
    task_id='get_artist_stats',
    python_callable=artist_analytics,
    dag=dag
)

run_etl >> [song_stats, artist_stats]
import pandas as pd
import spotipy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime
from datetime import timedelta
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 6, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'spotify_etl',
    default_args=default_args,
    description='Extract data from spotify',
    schedule_interval="@daily"
)

is_db_available = """
    select version();
    """

def get_music():
    scope = "user-read-recently-played"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Variable.get('SPOTIPY_CLIENT_ID'),
        client_secret=Variable.get('SPOTIPY_SECRET'),
        redirect_uri=Variable.get('SPOTIPY_REDIRECT_URI'),
        scope=scope))
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = int(yesterday.replace(hour=0, minute=0, second=0).timestamp()) * 1000
    results = sp.current_user_recently_played(after=end_date)
    return results

def extract_data(ti):
    music = ti.xcom_pull(task_ids='fetch_music')
    df = []
    for song in music['items']:
        artist = song['track']['album']['artists'][0]['name']
        album = song['track']['album']['name']
        song_name = song['track']['name']
        duration = song['track']['duration_ms']
        played_at = song['played_at']
        df.append([song_name, duration, artist, album, played_at])
    return df

database_available = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='is_db_available',
    sql=is_db_available,
    dag=dag
)

fetch_music = PythonOperator(
    task_id = 'fetch_music',
    python_callable = get_music,
    dag=dag
)

prepare_data = PythonOperator(
    task_id = 'prepare_data',
    python_callable = extract_data,
    dag=dag
)

# Extract songs data -- check
# Incloude song popularity, minutes, timestamp, extraer mes, dia, año, artista landing
# Ejecutar las vistas de artista, y canciones en staging

database_available >> fetch_music >> prepare_data
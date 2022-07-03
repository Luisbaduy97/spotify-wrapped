import pandas as pd
import spotipy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

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

song_t = """
        create table if not exists stg.songs (
            song_name text,
            duration int,
            artist text,
            album text,
            album_cover text,
            played_at text unique
        )
    """

minutes_per_artist = """
        create or replace view stg.total_artist_m as (
            select artist, sum(duration)/60000 as total_m from stg.songs rp group by artist order by total_m desc
        )
    """

songs_m = """
        create or replace view stg.songs_m as (
            select song_name, album, album_cover, sum(duration)/60000 as total_m from stg.songs rp group by song_name, album, album_cover  order by total_m desc
        )
    """

load_to_stg = """
        insert into stg.songs as s select * from lnd.recently_played as rp on conflict (played_at) do nothing 
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
        album_cover = song['track']['album']['images'][0]['url']
        df.append([song_name, duration, artist, album, album_cover, played_at])
    return df

def load_data_to_db(ti):
    df = ti.xcom_pull(task_ids='prepare_data')
    columns = ['song_name', 'duration', 'artist', 'album', 'album_cover', 'played_at'] # Extract month and year from date
    post_conn = PostgresHook(postgres_conn_id='spoti_p')
    df = pd.DataFrame(data=df, columns=columns)
    df.to_sql(name='recently_played', schema='lnd', con=post_conn.get_sqlalchemy_engine(), if_exists='replace', index=False)
    return 'complete'

def export_data():
    post_conn = PostgresHook(postgres_conn_id='spoti_p')
    df = pd.read_sql('SELECT * FROM stg.songs', con=post_conn.get_sqlalchemy_engine())
    df.to_csv('./data/songs.csv', sep=',', index=False)
    return 'complete'

database_available = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='is_db_available',
    sql=is_db_available,
    dag=dag
)

song_table = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='song_table',
    sql=song_t,
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

load_data = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data_to_db,
    dag=dag
)

load_stg_songs = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='add_to_stg',
    sql=load_to_stg,
    dag=dag
)

aritst_minutes = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='artist_vw',
    sql=minutes_per_artist,
    dag=dag
)

songs_m_vw = PostgresOperator(
    postgres_conn_id= 'spoti_p',
    task_id='song_vw',
    sql=songs_m,
    dag=dag
)

songs_backup = PythonOperator(
    task_id = 'backup_songs',
    python_callable = export_data,
    dag=dag
)

# Extract songs data -- check
# Incloude song popularity, minutes, timestamp, extraer mes, dia, año, artista landing
# Ejecutar las vistas de artista, y canciones en staging

database_available >> song_table >> fetch_music >> prepare_data >> load_data >> load_stg_songs >> [aritst_minutes, songs_m_vw] >> songs_backup
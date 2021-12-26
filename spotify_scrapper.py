from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import warnings
from datetime import datetime
import datetime
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker
warnings.filterwarnings('ignore') #avoid deprecation warning for access token

load_dotenv()

today = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

song_name = []
artist_name = []
song_duration = []
played_at = []
spotify_id = []
song_cover = []

results = sp.current_user_recently_played()
for song in results['items']:
    song_name.append(song['track']['name'])
    artist_name.append(song['track']['album']['artists'][0]['name'])
    song_cover.append(song['track']['album']['images'][0]['url'])
    song_duration.append(song['track']['duration_ms'])
    spotify_id.append(song['track']['id'])
    played_at.append(song['played_at'])

data_collection = {
    "song_name": song_name,
    "artist_name": artist_name,
    "song_duration_ms": song_duration,
    "spotify_id": spotify_id,
    "played_at": played_at,
    "cover_image": song_cover
}

data = pd.DataFrame(data=data_collection, columns=data_collection.keys())

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        spotify_id VARCHAR(200),
        song_duration_ms INT,
        played_at VARCHAR(200),
        cover_image LONGTEXT,
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
cursor.execute(sql_query)
print("Open database successfully")
try:
    data.to_sql("my_played_tracks", engine, index=False, if_exists='replace')
except:
    print("Data already exists in the database")

conn.close()
print("Close database successfully")
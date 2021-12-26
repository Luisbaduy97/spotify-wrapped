from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import warnings
from datetime import datetime
import datetime
import pandas as pd
warnings.filterwarnings('ignore') #avoid deprecation warning for access token

load_dotenv()

today = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

song_name = []
artist_name = []
song_duration = []
played_at = []
spotify_id = []

results = sp.current_user_recently_played()
for song in results['items']:
    song_name.append(song['track']['name'])
    artist_name.append(song['track']['album']['artists'][0]['name'])
    song_duration.append(song['track']['duration_ms'])
    spotify_id.append(song['track']['id'])
    played_at.append(song['played_at'])

data_collection = {
    "song_name": song_name,
    "artist_name": artist_name,
    "song_duration_ms": song_duration,
    "spotify_id": spotify_id,
    "played_at": played_at
}

data = pd.DataFrame(data=data_collection, columns=data_collection.keys())
print(data['spotify_id'].value_counts())
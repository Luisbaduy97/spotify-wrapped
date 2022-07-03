from __future__ import annotations
#import pandas as pd
import time
import datetime
from datetime import datetime as dt
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable
import pandas as pd

today = datetime.date.today()
start_date = datetime.datetime(
    year=today.year,
    month=1,
    day=1
)
end_date = datetime.datetime(
     year=today.year,
    month=today.month,
    day=today.day
)
#print(date_generated)

def get_intervals() -> list[dt]:
    date_generated = [start_date + datetime.timedelta(days=x) for x in range(0, (end_date-start_date).days)]
    intervals = []
    for i in range(len(date_generated) -1):
        intervals.append(date_generated[i])
        intervals.append(date_generated[i]+datetime.timedelta(hours=6))
        intervals.append(date_generated[i]+datetime.timedelta(hours=12))
        intervals.append(date_generated[i]+datetime.timedelta(hours=18))
    intervals.append(date_generated[-1])
    return intervals

def get_music(start_date:dt, end_date:dt):
    scope = "user-read-recently-played"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id='ca464432d7a5422a9397c7dfffed8235',
        client_secret='1eccb91a361c45ad8370a8afd86d682c',
        redirect_uri='http://localhost:8888/callback',
        scope=scope))
    start = int(start_date.timestamp()) * 1000
    end = int(end_date.timestamp()) * 1000
    results = sp.current_user_recently_played(before=1641180845)
    return results

data = get_intervals()
# Get music from intervals
df = []
for i in range(len(data) - 1):
    try:
        music = get_music(data[i], data[i+1])
        for song in music['items']:
            artist = song['track']['album']['artists'][0]['name']
            album = song['track']['album']['name']
            song_name = song['track']['name']
            duration = song['track']['duration_ms']
            played_at = song['played_at']
            album_cover = song['track']['album']['images'][0]['url']
            df.append([song_name, duration, artist, album, album_cover, played_at])
        print(f"Data from {data[i]} to {data[i+1]} saved")
    except:
        print(f"Can't get data from {data[i]} to {data[i+1]}")
    time.sleep(1)
    break

def save_csv(data: list) -> str:
    columns = ['song_name', 'duration', 'artist', 'album', 'album_cover', 'played_at']
    df2 = pd.DataFrame(data=data, columns=columns)
    df2.to_csv('./data/raw_data.csv', sep=',', index=False)
    return 'complete'

save_csv(df)
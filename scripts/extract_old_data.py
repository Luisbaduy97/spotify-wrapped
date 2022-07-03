from __future__ import annotations
#import pandas as pd
import time
import datetime
from datetime import datetime as dt
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable

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
        client_id=Variable.get('SPOTIPY_CLIENT_ID'),
        client_secret=Variable.get('SPOTIPY_SECRET'),
        redirect_uri=Variable.get('SPOTIPY_REDIRECT_URI'),
        scope=scope))
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = int(yesterday.replace(hour=0, minute=0, second=0).timestamp()) * 1000
    results = sp.current_user_recently_played(after=start_date.timestamp()*1000, before = end_date.timestamp()*1000)
    return results

data = get_intervals()


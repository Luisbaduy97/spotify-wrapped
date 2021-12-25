from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import warnings
from datetime import datetime
import datetime
warnings.filterwarnings('ignore') #avoid deprecation warning for access token

load_dotenv()

today = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
print(int(today.timestamp())*1000)
print(int(yesterday.timestamp())*1000)

scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

results = sp.current_user_recently_played()
for i in results['items']:
    print(i['track']['name'])
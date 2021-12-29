import pandas as pd
import sqlalchemy
import sqlite3

def song_analytics():
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    spotify = pd.read_sql_query("SELECT * from my_played_tracks", con=conn)
    if spotify.shape[0] <= 0:
        raise Exception('Cannot get songs')
    most_listened = spotify.groupby(
        ['song_name', 'played_year']
        ).agg(minutes_listened=pd.NamedAgg(
            column='song_duration_ms', aggfunc=lambda x: round(x.sum()/60000)
            )
        ).reset_index()
    sql_query = """
            CREATE TABLE IF NOT EXISTS track_stats(
                song_name VARCHAR(200),
                minutes_listened INT,
                played_year INT
            )
            """
    cursor.execute(sql_query)
    print("Open database successfully")
    try:
        most_listened.to_sql("track_stats", engine, index=False, if_exists='replace')
    except:
        print("Data already exists in the database")
    conn.close()
    print("Close database successfully")


def artist_analytics():
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    spotify = pd.read_sql_query("SELECT * from my_played_tracks", con=conn)
    if spotify.shape[0] <= 0:
        raise Exception('Cannot get songs')
    most_listened = spotify.groupby(
        ['artist_name', 'played_year']
        ).agg(minutes_listened=pd.NamedAgg(
            column='song_duration_ms', aggfunc=lambda x: round(x.sum()/60000)
            )
        ).reset_index()
    sql_query = """
            CREATE TABLE IF NOT EXISTS artist_stats(
                artist_name VARCHAR(200),
                minutes_listened INT,
                played_year INT
            )
            """
    cursor.execute(sql_query)
    print("Open database successfully")
    try:
        most_listened.to_sql("artist_stats", engine, index=False, if_exists='replace')
    except:
        print("Data already exists in the database")
    conn.close()
    print("Close database successfully")
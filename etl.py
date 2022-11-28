import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process and insert information from file into artist and song tables"""
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert artist record
    artist_new_record(cur, df)

    # insert song record
    song_new_record(cur, df)


def artist_new_record(cur, df):
    """Add new artist record into artists table"""
    artist_data = (
        df['artist_id'], df['artist_name'], df['artist_location'], df['artist_latitude'], df['artist_longitude'])
    cur.execute(artist_table_insert, artist_data)


def song_new_record(cur, df):
    """Add new song record into songs table"""

    song_data = (df['song_id'], df['title'], df['artist_id'], df['year'], df['duration'])
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Process and insert log information from file into time, user and songplay table """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    df = extend_data_frame_model_with_time_data(df, t)

    time_add_records(cur, df)

    user_add_records(cur, df)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        # print((row.song, row.artist, row.length))
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def extend_data_frame_model_with_time_data(df, t):
    """Add new columns do dataframe splitting start_time in order to have detailed date information for data
    analytics"""
    df['start_time'] = t
    df['hour'] = df['start_time'].dt.hour
    df['day'] = df['start_time'].dt.day
    df['week'] = df['start_time'].dt.week
    df['month'] = df['start_time'].dt.month
    df['year'] = df['start_time'].dt.year
    df['weekday'] = df['start_time'].dt.weekday
    return df


def time_add_records(cur, df):
    """Process and insert time data records"""
    # insert time data records
    time_data = df
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = time_data[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def user_add_records(cur, df):
    """Process and insert user data records"""
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


def process_data(cur, conn, filepath, func):
    """Process a file path in order to read and process file information into database applying an specific parameter
    function """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Obtain a connection to database and process song and log data"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

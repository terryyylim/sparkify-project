from typing import Any
from typing import List
from typing import Callable

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert
from sql_queries import song_select


# Helper function to convert numpy dtype to base dtype
def convert_dtype(row_entry: List[Any]) -> List[Any]:
    finalItem = []
    for item in row_entry:
        if type(item) != str:
            finalItem.append(item.item())
        else:
            finalItem.append(item)
    return finalItem


def process_song_file(cur: Any, filepath: str) -> None:
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0].values)
    song_data = convert_dtype(song_data)
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].iloc[0].values)
    artist_data = convert_dtype(artist_data)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: Any, filepath: str) -> None:
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    print(f'Length of df: {len(df)}')

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    print(f'Length of time_df: {len(time_df)}')

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts']), int(row.userId),
                         row.level, songid, artistid, row.sessionId, row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: Any, conn: Any, filepath: str, func: Callable) -> None:
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


def main() -> None:
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    conn.close()


if __name__ == "__main__":
    main()

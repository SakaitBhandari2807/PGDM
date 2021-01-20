import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - recieves the cursor to database and filepath to be processed.
    - process the song file and insert into the respective tables.
    - populates the table song and artist.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = list(song[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = list(artist[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - recieves the cursor to database and filepath to be processed.
    - process the log file and insert into the respective tables.
    - populates the table user, time and songplay table.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    #print(df.columns)
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = {
        'ts': df['ts'],
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.week,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    }
    # start_time,hour,day,week,month,year,weekday
    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data, columns=column_labels)
    # print(time_df.head(3))
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
        cur.execute(song_select, (row.song.upper(), "{:.2f}".format(row.length), row.artist.upper()))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - recieves a path and find all files under that path.
    - process all the files for the given path using the func passed as param.
    """
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
    """
    - Create a connection to the database sparkifydb
    - acquire a cursor and pass these details to the function that process the data.
    - Close the connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

import os
import glob
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Parse and insert relevant song data from song files into the database.

    Arguments:
    cur -- database cursor used to insert data
    filepath -- path to JSON file containing song data
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # efficiently insert song records in batchs by minimizing server round-trips
    song_data = [tuple(x) for x in df[[
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"]].values]
    execute_batch(cur, song_table_insert, song_data)

    # efficiently insert artist records in batchs by minimizing server round-trips
    artist_data = [tuple(x) for x in df[[
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"]].values]
    execute_batch(cur, artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Parse and insert relevant service usage data from log files into the database.

    Arguments:
    cur -- database cursor used to insert data
    filepath -- path to JSON file containing log data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page.isin(["NextSong"])]

    # convert timestamp column to datetime
    df = df.assign(ts=pd.to_datetime(df.ts, unit="ms", utc=True))

    # insert time data records
    time_df = pd.DataFrame(
        {
            "timestamp": df.ts,
            "hour": df.ts.dt.hour,
            "day": df.ts.dt.day,
            "week_of_year": df.ts.dt.week,
            "month": df.ts.dt.month,
            "year": df.ts.dt.year,
            "weekday": df.ts.dt.weekday,
        })

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[[
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"]].drop_duplicates(subset="userId", keep="last")

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
        songplay_data = (
            row.userId,
            songid,
            artistid,
            row.sessionId,
            row.ts,
            row.level,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Resolve files under filepath root, parse and insert elements into the database.

    Arguments:
    cur -- database cursor used to insert data
    conn -- database network connection
    filepath -- path to JSON data files
    func -- function used to extract data elements
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


def main(debug=False):
    """Opens database connection and inserts items parsed from data files into database.

    Arguments:
    debug -- True the database is cleared of data by dropping and re-creating all tables
             False data from files is inserted into existing tables
    """
    # connect to database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    if debug:
        # drop and re-create all tables
        for sql_cmd in drop_table_queries + create_table_queries:
            cur.execute(sql_cmd)

    # pipeline: read in files > insert into postgres tables
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main(debug=False)
